<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/arbuzovv/zipFinam/main/img/zipfinam-demo-dark.gif">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/arbuzovv/zipFinam/main/img/zipfinam-demo-light.gif">
    <img src="https://raw.githubusercontent.com/arbuzovv/zipFinam/main/img/zipfinam-demo-light.gif" width="100%" alt="zipFinam: бэктест и алготорговля на Мосбирже через Финам">
  </picture>
</p>

<h1 align="center">zipFinam <br> Бэктест и алготорговля на Мосбирже через Финам.</h1>

<p align="center">
  <b>zipFinam — открытая Python-библиотека для бэктеста и алготорговли на Московской бирже:<br>
  котировки Финама, торговые календари MOEX и исполнение заявок через Finam Trade API.</b><br><br>
  Стратегия, проверенная на истории, без переписывания запускается на вашем счёте в Финаме.
</p>

<p align="center">
  <a href="https://pypi.org/project/zipfinam/"><img src="https://img.shields.io/pypi/v/zipfinam" alt="PyPI"></a>
  <a href="https://pypi.org/project/zipfinam/"><img src="https://img.shields.io/pypi/pyversions/zipfinam" alt="Python"></a>
  <a href="#license"><img src="https://img.shields.io/github/license/arbuzovv/zipFinam" alt="License"></a>
  <a href="https://github.com/arbuzovv/zipFinam"><img src="https://img.shields.io/github/stars/arbuzovv/zipFinam.svg?style=social&label=Star" alt="Stars"></a>
</p>

<p align="center">
  <a href="#why">Зачем это нужно</a> ·
  <a href="#finam-data">Котировки Финама</a> ·
  <a href="#algopack">MOEX AlgoPack</a> ·
  <a href="#live">Торговля на своём счёте</a> ·
  <a href="#quick-start">Начало работы</a> ·
  <a href="#license">Лицензия</a>
</p>

<p align="center">
  акции и фонды MOEX из Финам Trade API · календари MOEX и FORTS · заявки целыми лотами ·
  капитал стратегии, а не весь счёт · данные MOEX AlgoPack по желанию · движок ziplime
</p>

---

<a id="why"></a>
## Зачем это нужно

Большинство инструментов для бэктеста сделаны для американского рынка. На Мосбирже с ними
начинаются проблемы:

- **Нет данных.** Популярные бесплатные источники вроде `yfinance` российские бумаги толком не
  покрывают.
- **Свечей мало.** OHLCV показывает, *куда* сходила цена, но не *кто* её двигал: агрессивный
  покупатель или пустой стакан, толпа или один крупный игрок.
- **Тестер заглядывает в будущее.** Биржевая статистика публикуется с задержкой, иногда
  пересчитывается задним числом, и наивный бэктест незаметно читает то, чего в тот момент ещё не
  было.
- **От теста до торговли — вторая разработка.** Стратегию, проверенную в одном инструменте,
  приходится переписывать под API брокера.

zipFinam закрывает все четыре проблемы.

---

<a id="finam-data"></a>
## 📈 Котировки Финама — вся Мосбиржа одной командой

Бары и справочник инструментов приходят напрямую из **Финам Trade API**: акции и фонды MOEX,
таймфреймы от минуты до квартала. Нужен только токен из личного кабинета Финама.

```bash
cd examples
python ingest_finam.py      # справочник инструментов и дневные бары
python run_backtest.py      # бэктест на голубых фишках
```

Финам подключается к движку как штатный провайдер данных: `get_market_data_source("finam")`
работает так же, как встроенный Yahoo для американского рынка.

---

<a id="algopack"></a>
## 🔬 MOEX AlgoPack — то, чего не видно в свечах

[AlgoPack](https://data.moex.com) — аналитика, которую Мосбиржа считает по собственной ленте
сделок и стакану. Их не восстановить по свечам или ленте: биржа видит каждую заявку, включая снятые.
В zipFinam таблицы AlgoPack читаются той же строкой, что и цены:

```python
import zipfinam

flow = await data.history(assets=[sber], fields=["disb", "val"], bar_count=5,
                          data_source="algopack://eq/tradestats")
```

| Таблица | Что видит стратегия | Рынки |
|---|---|---|
| `tradestats` | Кто пересекал спред: объём агрессивных покупок против продаж (`disb`) | акции, фьючерсы, валюта |
| `orderstats` | Какие заявки выставили и какие сняли: настоящий спрос против декорации | акции, фьючерсы, валюта |
| `obstats` | Спред в б.п., глубина и дисбаланс стакана каждые 5 минут | акции, фьючерсы, валюта |
| `hi2` | Концентрация потока: покупает толпа или один игрок | акции, фьючерсы |
| `futoi` | Открытый интерес физлиц против юрлиц | фьючерсы |
| `alerts` | Сигналы биржи об аномальной активности | акции, фьючерсы, валюта |

### Без заглядывания в будущее

Это главное отличие от самодельного парсера. Каждая строка AlgoPack становится видна стратегии в
момент, когда **биржа её опубликовала**, а не в момент, к которому она относится:

- дневной итог виден на следующей сессии, а не в течение своей;
- `hi2` биржа публикует примерно через шесть минут после своей метки времени, и раньше этого
  стратегия его не увидит;
- строки, которые биржа пересчитала задним числом (бывает, спустя месяцы), не пропадают из
  бэктеста до даты пересчёта, а остаются на своём месте.

Сводка за сессию собрана по правилу для каждой колонки: объёмы суммируются, VWAP взвешиваются,
снимки стакана усредняются, дисбаланс пересчитывается по итогам дня. Агрегация сверена вручную
с реальными данными SBER. Нужны пятиминутки — `algopack://eq/tradestats/5min`.

Загруженная история кэшируется на диске: инструмент-год `tradestats` занимает около 3 МБ, и
повторный бэктест по тем же годам не скачивает их заново.

### Пять готовых стратегий

В [`examples/algorithms/algopack/`](https://github.com/arbuzovv/zipFinam/tree/main/examples/algorithms/algopack) по стратегии на каждую таблицу:

| Стратегия | Идея |
|---|---|
| `flow_tilt` | Держать голубые фишки, которые покупают агрессивные деньги |
| `orderstats_conviction` | Покупать там, где выставленные биды не снимают |
| `obstats_liquidity` | Держать то, что поддерживает стакан, а не случайный принт ленты |
| `hi2_crowding` | Идти за бумагами, где покупает кто-то один против рассеянного рынка |
| `futoi_crowd` | Стоять против толпы физлиц во фьючерсе на доллар/рубль |

```bash
python run_backtest.py algorithms/algopack/flow_tilt.py
```

---

<a id="live"></a>
## 💼 От бэктеста к вашему счёту

Та же стратегия, без изменений, торгует на вашем брокерском счёте в Финаме через Trade API.
Укажите данные счёта и запустите пример:

```bash
export FINAM_TRADE_TOKEN=...                 # токен счёта, на котором исполняются заявки
export FINAM_ACCOUNT_ID=...                  # номер этого счёта
export FINAM_TRADE_URL=https://api.finam.ru  # заявки уходят на ваш счёт
cd examples && python run_live_finam.py
```

Исполнение рассчитано на реальные деньги:

- **Стратегии выделяется капитал, а не весь счёт.** `order_target_percent(0.5)` — это половина
  её бюджета, а не половина брокерского счёта.
- **Лоты.** Объём округляется до целых лотов (GAZP — по 10 штук). Если размер лота неизвестен,
  заявка не отправляется вовсе: наугад ничего не торгуется.
- **Принятая заявка ≠ исполненная.** После каждого тика статус заявок запрашивается у брокера, и
  отклонённая заявка отмечается как ошибка, а не как успех.

---

<a id="quick-start"></a>
## Начало работы

```bash
pip install "zipfinam[all]"
```

Создайте `.env` в рабочей папке:

```env
GRPC_TOKEN=...          # токен Финам Trade API: котировки, справочник, лоты
ALGOPACK_API=...        # ключ MOEX AlgoPack, подписка на https://data.moex.com
```

Для бэктестов на свечах хватит `GRPC_TOKEN`. Можно поставить не всё:

| Extra | Что добавляет |
|---|---|
| — | Данные Финама, календари, адреса `algopack://` |
| `algopack` | Клиент MOEX AlgoPack |
| `live` | Исполнение заявок через Финам |

Стратегия — обычный Python:

```python
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.stocks = [await context.symbol(s) for s in ("SBER@MISX", "GAZP@MISX")]


async def handle_data(context, data):
    for stock in context.stocks:
        bars = await data.history(assets=[stock], fields=["close"], bar_count=50)
        if len(bars) < 50:
            continue
        closes = bars["close"].to_numpy()
        target = 0.5 if closes[-20:].mean() > closes.mean() else 0.0
        await context.order_target_percent(asset=stock, target=target, style=MarketOrder())
```

Тикеры пишутся с кодом площадки (`SBER@MISX`). Имя `context.assets` занято движком, своим
переменным давайте другие имена.

### Календарь срочного рынка

Стандартный календарь XMOS заканчивается в 18:45, и фьючерсная стратегия теряет на нём вечернюю
сессию. zipFinam добавляет календарь `FORTS` (09:00–23:50, клиринг 18:50–19:05), а для портфеля
из бумаг нескольких бирж подбирает общий: `clock_calendar_name(["MISX", "XNYS"])` → `24/5`.

---

## Под капотом

zipFinam — надстройка над [ziplime](https://github.com/Limex-com/ziplime), открытым движком
бэктестинга и живой торговли от Limex. Движок, метрики и модели исполнения берутся из ziplime,
а zipFinam добавляет российский рынок: данные, календари и брокера.

```
zipfinam/
├── finam/               # Финам: бары, справочник, исполнение (REST и gRPC)
├── algopack/            # MOEX AlgoPack: каталог таблиц, клиент, источник данных
└── venue_calendars.py   # FORTS и календари площадок
examples/                # загрузка, бэктест, живая торговля, стратегии
```

Тесты работают без сети: `pip install -e ".[dev]" && pytest`.

<details>
<summary>Переход с версии 0.x и ограничения</summary>

- Нужен ziplime 2.x. Импорты: `from zipfinam import FinamDataSource, FinamAssetDataSource`;
  старый модуль `ziplime_grpc_data_source` оставлен на время перехода.
- Готовый `assets.sqlite` больше не поставляется: справочник загружается из Финама скриптом
  `examples/ingest_finam.py`.
- В стратегиях `data.history` и `data.current` вызываются с `await`, у заявок обязателен
  `style=MarketOrder()`.
- Из справочника Финама импортируются акции и фонды; фьючерсы и опционы пропускаются.
- Торговый день AlgoPack (07:00–23:50) длиннее основной сессии акций (10:00–18:45), поэтому
  `vol` и `pr_close` включают утро и вечер.
- Адреса `algopack://` начинают работать после `import zipfinam`, сделать его нужно до запуска
  симуляции.

</details>

<a id="license"></a>
## Лицензия

GNU GPL v3, как у ziplime.
