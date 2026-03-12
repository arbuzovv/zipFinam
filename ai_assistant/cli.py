"""
ИИ-ассистент Ziplime для бэктестинга — интерактивный CLI.

Использование:
    python -m ai_assistant                  # интерактивный выбор модели
    python -m ai_assistant --default-model  # модель по умолчанию без выбора
    python -m ai_assistant --show-code      # показывать код стратегии

Переменные окружения (необязательно):
    OPENROUTER_API_KEY   Ваш API-ключ OpenRouter
    OPENROUTER_MODEL     Модель LLM (пропускает меню выбора)
"""
from __future__ import annotations

import asyncio
import os
import sys
import datetime

# Ensure parent directory is on the path so ziplime is importable
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.prompt import Prompt

from .agent import ZiplimeAgent
from .data_manager import DataManager
from .executor import BacktestExecutor, BacktestResult
from .prompts import HELP_TEXT

console = Console()


# ------------------------------------------------------------------ #
# Display helpers                                                      #
# ------------------------------------------------------------------ #

def display_welcome():
    console.print(
        Panel(
            "[bold green]ИИ-ассистент Ziplime для бэктестинга[/bold green]\n\n"
            "Опишите любую торговую стратегию простыми словами.\n"
            "ИИ сгенерирует и запустит бэктест автоматически.\n\n"
            "[dim]Источник данных: Финам Trade API\n"
            "Введите [bold]помощь[/bold] для примеров, [bold]выход[/bold] для выхода.[/dim]",
            border_style="green",
            padding=(1, 2),
        )
    )


def display_results(result: BacktestResult):
    """Print a rich table with the key backtest metrics."""
    # Determine colour coding
    ret_color    = "green" if result.total_return_pct >= 0 else "red"
    sharpe_color = "green" if result.sharpe_ratio >= 1.0 else ("yellow" if result.sharpe_ratio >= 0 else "red")
    dd_color     = "green" if result.max_drawdown_pct > -10 else ("yellow" if result.max_drawdown_pct > -20 else "red")

    table = Table(
        title="[bold]Результаты бэктеста[/bold]",
        box=box.ROUNDED,
        border_style="bright_blue",
        show_header=False,
        padding=(0, 2),
    )
    table.add_column("Показатель", style="dim", width=30)
    table.add_column("Значение", justify="right", width=18)

    table.add_row("Стратегия", ", ".join(result.symbols))
    table.add_row("Период", f"{result.start_date}  →  {result.end_date}")
    table.add_row("Торговых дней", str(result.num_trading_days))
    table.add_row("─" * 30, "─" * 18)
    table.add_row("Начальный капитал",      f"₽{result.starting_capital:>16,.0f}")
    table.add_row("Итоговая стоимость портфеля", f"₽{result.final_portfolio_value:>16,.2f}")
    table.add_row(
        "Общая доходность",
        f"[{ret_color}]{result.total_return_pct:>+15.2f}%[/{ret_color}]",
    )
    table.add_row(
        "Годовая доходность",
        f"[{ret_color}]{result.annualized_return_pct:>+15.2f}%[/{ret_color}]",
    )
    table.add_row(
        "Коэффициент Шарпа",
        f"[{sharpe_color}]{result.sharpe_ratio:>17.3f}[/{sharpe_color}]",
    )
    table.add_row(
        "Макс. просадка",
        f"[{dd_color}]{result.max_drawdown_pct:>15.2f}%[/{dd_color}]",
    )

    if result.alpha is not None or result.beta is not None:
        table.add_row("─" * 30, "─" * 18)
        if result.alpha is not None:
            alpha_color = "green" if result.alpha >= 0 else "red"
            table.add_row(
                "Альфа (к бенчмарку)",
                f"[{alpha_color}]{result.alpha:>+17.4f}[/{alpha_color}]",
            )
        if result.beta is not None:
            beta_color = "green" if 0.5 <= result.beta <= 1.2 else "yellow"
            table.add_row(
                "Бета (к бенчмарку)",
                f"[{beta_color}]{result.beta:>17.4f}[/{beta_color}]",
            )

    console.print()
    console.print(table)

    if result.html_report_path:
        console.print(
            f"  [dim]Отчёт QuantStats → [link=file://{result.html_report_path}]"
            f"{result.html_report_path}[/link][/dim]"
        )
    if result.strategy_file_path:
        console.print(
            f"  [dim]Код стратегии   → [link=file://{result.strategy_file_path}]"
            f"{result.strategy_file_path}[/link][/dim]"
        )
    console.print()


def display_code(code: str):
    """Отображает сгенерированный код алгоритма (опционально, режим --show-code)."""
    console.print(
        Panel(
            f"[dim]{code}[/dim]",
            title="[bold dim]Сгенерированный код алгоритма[/bold dim]",
            border_style="dim",
            padding=(1, 2),
        )
    )


def display_ai_message(text: str):
    if text.strip():
        console.print(
            Panel(
                Markdown(text),
                title="[bold blue]Анализ ИИ[/bold blue]",
                border_style="blue",
                padding=(1, 2),
            )
        )


def display_run_params(config, model: str):
    """Показывает параметры запуска перед выполнением бэктеста."""
    table = Table(
        title="[bold]Параметры бэктеста[/bold]",
        box=box.SIMPLE_HEAVY,
        border_style="cyan",
        show_header=False,
        padding=(0, 2),
    )
    table.add_column("Параметр", style="dim", width=22)
    table.add_column("Значение", width=40)

    table.add_row("Тикеры",         "[bold]" + ", ".join(config.symbols) + "[/bold]")
    table.add_row("Начало периода", config.start_date)
    table.add_row("Конец периода",  config.end_date)
    table.add_row("Капитал",        f"₽{config.capital:,.0f}")
    table.add_row("Бенчмарк",       config.benchmark or "нет")
    table.add_row("Модель",         f"[dim]{model}[/dim]")

    console.print()
    console.print(table)
    console.print()


def display_errors(errors: list[str], max_shown: int = 5):
    """Показывает первые несколько ошибок симуляции для отладки."""
    if not errors:
        return
    lines = [f"[yellow]{len(errors)} некритичных ошибок симуляции.[/yellow]"]
    shown = errors[:max_shown]
    for i, err in enumerate(shown, 1):
        short = err if len(err) <= 300 else err[:297] + "…"
        lines.append(f"  [dim]{i}.[/dim] {short}")
    if len(errors) > max_shown:
        lines.append(f"  [dim]… и ещё {len(errors) - max_shown}[/dim]")
    console.print(
        Panel(
            "\n".join(lines),
            title="[bold yellow]Предупреждения симуляции[/bold yellow]",
            border_style="yellow",
            padding=(0, 2),
        )
    )


# ------------------------------------------------------------------ #
# Main REPL                                                            #
# ------------------------------------------------------------------ #

async def run_assistant(
    api_key: str,
    model: str = "z-ai/glm-4.5-air:free",
    show_code: bool = False,
):
    """Основной интерактивный цикл."""
    import pathlib
    _db_path = pathlib.Path.home() / ".ziplime" / "assets.sqlite"
    db_path = str(_db_path) if _db_path.exists() else None

    agent        = ZiplimeAgent(api_key=api_key, model=model, db_path=db_path)
    data_manager = DataManager(on_progress=lambda msg: console.print(f"  [dim]{msg}[/dim]"))
    executor     = BacktestExecutor(data_manager=data_manager)

    display_welcome()
    console.print(f"[dim]Модель: {model}[/dim]\n")

    while True:
        try:
            user_input = console.input("[bold green]Вы:[/bold green] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]До свидания![/dim]")
            break

        if not user_input:
            continue

        lower = user_input.lower()
        if lower in {"quit", "exit", "q", "bye", "выход", "пока"}:
            console.print("[dim]До свидания![/dim]")
            break
        if lower in {"help", "h", "?", "помощь"}:
            console.print(Panel(HELP_TEXT, border_style="dim"))
            continue
        if lower in {"clear", "reset", "new", "очистить", "сброс"}:
            agent.clear_history()
            console.print("[dim]История разговора очищена. Начинаем заново.[/dim]\n")
            continue

        # --- Запрос к LLM -----------------------------------------------
        with console.status("[bold blue]Думаю…[/bold blue]", spinner="dots"):
            try:
                response = await agent.chat(user_input)
            except Exception as exc:
                console.print(f"[red]Ошибка LLM:[/red] {exc}")
                continue

        # --- Show explanation text first (if any) -----------------------
        if response.text:
            display_ai_message(response.text)

        # --- Запуск бэктеста если LLM запросил его ----------------------
        if response.has_backtest:
            # Показываем параметры до запуска
            display_run_params(response.backtest_config, model)

            if show_code and response.algorithm_code:
                display_code(response.algorithm_code)

            # 1. Проверяем наличие данных
            try:
                start_dt = datetime.datetime.strptime(
                    response.backtest_config.start_date, "%Y-%m-%d"
                ).replace(tzinfo=datetime.timezone.utc)
                end_dt = datetime.datetime.strptime(
                    response.backtest_config.end_date, "%Y-%m-%d"
                ).replace(tzinfo=datetime.timezone.utc)

                # Добавляем бенчмарк в список символов
                all_symbols = list(response.backtest_config.symbols)
                bm = response.backtest_config.benchmark
                if bm and bm not in all_symbols:
                    all_symbols.append(bm)

                await data_manager.ensure_data(
                    all_symbols,
                    start_dt,
                    end_dt,
                )
            except Exception as exc:
                console.print(f"[red]Ошибка загрузки данных:[/red] {exc}")
                continue

            # 2. Запускаем бэктест
            with console.status("[bold blue]Запускаю бэктест…[/bold blue]", spinner="dots"):
                try:
                    result = await executor.run(
                        algorithm_code=response.algorithm_code,
                        config=response.backtest_config,
                    )
                except Exception as exc:
                    console.print(f"[red]Ошибка бэктеста:[/red] {exc}")
                    import traceback
                    console.print(f"[dim]{traceback.format_exc()}[/dim]")
                    continue

            # 3. Показываем предупреждения симуляции
            if result.errors:
                display_errors(result.errors)

            # 4. Показываем таблицу результатов
            display_results(result)

            # 5. Просим LLM интерпретировать результаты
            agent.add_result_context(result.to_summary_text())
            with console.status("[bold blue]Анализирую результаты…[/bold blue]", spinner="dots"):
                try:
                    interpretation = await agent.chat(
                        "Пожалуйста, интерпретируй эти результаты бэктеста простым языком. "
                        "Используй все доступные метрики (Шарп, Сортино, Калмар, просадка, "
                        "win rate и т.д.) для детальной оценки стратегии."
                    )
                except Exception as exc:
                    console.print(f"[red]Ошибка интерпретации:[/red] {exc}")
                    continue

            if interpretation.text:
                display_ai_message(interpretation.text)

        console.print()


# ------------------------------------------------------------------ #
# Model selection                                                      #
# ------------------------------------------------------------------ #

_MODELS = [
    # (display_name, model_id, category)
    ("nvidia/nemotron-3-super-120b-a12b:free",  "nvidia/nemotron-3-super-120b-a12b:free",  "free"),
    ("qwen/qwen3-next-80b-a3b-instruct:free",   "qwen/qwen3-next-80b-a3b-instruct:free",   "free"),
    ("z-ai/glm-4.5-air:free",                   "z-ai/glm-4.5-air:free",                   "free"),
    ("stepfun/step-3.5-flash:free",             "stepfun/step-3.5-flash:free",             "free"),
    ("deepseek/deepseek-v3.2",                  "deepseek/deepseek-v3.2",                  "paid-ru"),
    ("xiaomi/mimo-v2-flash",                    "xiaomi/mimo-v2-flash",                    "paid-ru"),
    ("qwen/qwen3-coder-next",                   "qwen/qwen3-coder-next",                   "paid-ru"),
    ("z-ai/glm-5",                              "z-ai/glm-5",                              "paid-ru"),
    ("moonshotai/kimi-k2.5",                    "moonshotai/kimi-k2.5",                    "paid-ru"),
    ("google/gemini-3.1-flash-lite-preview",    "google/gemini-3.1-flash-lite-preview",    "paid-noru"),
    ("x-ai/grok-code-fast-1",                   "x-ai/grok-code-fast-1",                   "paid-noru"),
    ("openai/gpt-5-mini",                       "openai/gpt-5-mini",                       "paid-noru"),
]

_DEFAULT_MODEL = "z-ai/glm-4.5-air:free"

_CATEGORY_LABELS = {
    "free":      "[green]Бесплатные[/green]",
    "paid-ru":   "[yellow]Платные (доступны из РФ)[/yellow]",
    "paid-noru": "[red]Платные (недоступны из РФ)[/red]",
}


def _choose_model() -> str:
    """Интерактивный выбор LLM-модели из списка."""
    table = Table(
        title="[bold]Выберите модель LLM[/bold]",
        box=box.SIMPLE_HEAVY,
        border_style="cyan",
        show_header=True,
        padding=(0, 2),
    )
    table.add_column("#",       style="dim", width=4, justify="right")
    table.add_column("Модель",  width=45)
    table.add_column("Тип",     width=32)

    default_idx = 1
    for i, (name, model_id, cat) in enumerate(_MODELS, 1):
        label = _CATEGORY_LABELS[cat]
        marker = " [bold cyan]← по умолчанию[/bold cyan]" if model_id == _DEFAULT_MODEL else ""
        table.add_row(str(i), name + marker, label)
        if model_id == _DEFAULT_MODEL:
            default_idx = i

    console.print()
    console.print(table)

    while True:
        raw = console.input(
            f"[bold cyan]Введите номер модели[/bold cyan] "
            f"[dim](Enter = {default_idx})[/dim]: "
        ).strip()
        if raw == "":
            return _DEFAULT_MODEL
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(_MODELS):
                chosen = _MODELS[idx - 1][1]
                console.print(f"[dim]Выбрана модель: {chosen}[/dim]\n")
                return chosen
        console.print(f"[yellow]Введите число от 1 до {len(_MODELS)}[/yellow]")


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #

def main():
    """Точка входа CLI. Вызывается через `python -m ai_assistant` или напрямую.

    Флаги:
        --default-model   запуск без выбора модели, используется модель по умолчанию
        --show-code / -v  показывать сгенерированный код стратегии
    """
    args = sys.argv[1:]

    # --- Получаем API-ключ --------------------------------------------
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        console.print(
            "[yellow]Переменная окружения OPENROUTER_API_KEY не найдена.[/yellow]\n"
            "Получите бесплатный API-ключ на [link=https://openrouter.ai]https://openrouter.ai[/link]\n"
        )
        api_key = Prompt.ask("Введите ваш API-ключ OpenRouter", password=True)
        if not api_key:
            console.print("[red]API-ключ обязателен. Выход.[/red]")
            sys.exit(1)

    # --- Определяем модель --------------------------------------------
    # Приоритет: переменная окружения → флаг --default-model → интерактивный выбор
    model = os.environ.get("OPENROUTER_MODEL", "").strip()
    if not model:
        if "--default-model" in args:
            model = _DEFAULT_MODEL
            console.print(f"[dim]Модель по умолчанию: {model}[/dim]\n")
        else:
            model = _choose_model()

    # --- Разбираем флаги ----------------------------------------------
    show_code = "--show-code" in args or "-v" in args

    # --- Запускаем ----------------------------------------------------
    try:
        asyncio.run(run_assistant(api_key=api_key, model=model, show_code=show_code))
    except KeyboardInterrupt:
        console.print("\n[dim]Прервано. До свидания![/dim]")


if __name__ == "__main__":
    main()
