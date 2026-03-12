"""
Поиск тикеров в базе данных активов assets.sqlite.

Используется перед вызовом LLM чтобы подставить актуальные тикеры из БД
вместо того чтобы полагаться на память модели.
"""
from __future__ import annotations

import sqlite3
from typing import Optional


class TickerResolver:
    """
    Ищет тикеры в таблице equity_symbol_mappings по названию компании
    или по тикеру. Поддерживает нечёткое (подстрочное) совпадение.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._cache: list[dict] | None = None

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def resolve(self, query: str, default_exchange: str = "MISX") -> Optional[str]:
        """
        Возвращает лучший тикер для запроса в формате TICKER@EXCHANGE.
        Приоритет: точное совпадение тикера → совпадение названия компании.
        Если ничего не найдено — возвращает None.
        """
        results = self.search(query, default_exchange)
        if results:
            r = results[0]
            return f"{r['symbol']}@{r['exchange']}"
        return None

    def search(self, query: str, exchange: str = "MISX") -> list[dict]:
        """
        Ищет записи подходящие под query.
        Возвращает список словарей {symbol, company, exchange}.
        """
        query = query.strip()
        if not query:
            return []

        all_entries = self._load()
        results: list[dict] = []
        seen: set[str] = set()

        q_upper = query.upper()
        q_lower = query.lower()

        def add(entry: dict) -> None:
            key = f"{entry['symbol']}@{entry['exchange']}"
            if key not in seen:
                results.append(entry)
                seen.add(key)

        # 1. Точное совпадение тикера (SBER → SBER@MISX)
        for e in all_entries:
            if e["symbol"] == q_upper and e["exchange"] == exchange:
                add(e)

        if results:
            return results

        # 2. Точное совпадение названия компании (регистронезависимо)
        for e in all_entries:
            if e["exchange"] == exchange and e["company"].lower() == q_lower:
                add(e)

        if results:
            return results

        # 3. Название компании содержит запрос как подстроку
        for e in all_entries:
            if e["exchange"] == exchange and q_lower in e["company"].lower():
                add(e)

        # 4. Запрос содержит название компании как подстроку
        for e in all_entries:
            if e["exchange"] == exchange and len(e["company"]) >= 3 \
                    and e["company"].lower() in q_lower:
                add(e)

        return results

    def search_in_text(self, text: str, exchange: str = "MISX") -> list[dict]:
        """
        Ищет все упомянутые в тексте компании и тикеры.
        Возвращает дедуплицированный список совпадений.
        """
        all_entries = self._load()
        found: list[dict] = []
        seen: set[str] = set()
        text_lower = text.lower()

        def add(entry: dict) -> None:
            key = f"{entry['symbol']}@{entry['exchange']}"
            if key not in seen:
                found.append(entry)
                seen.add(key)

        for e in all_entries:
            if e["exchange"] != exchange:
                continue
            sym_lower = e["symbol"].lower()
            company_lower = e["company"].lower()

            # Тикер упомянут в тексте как отдельное слово
            if sym_lower and sym_lower in text_lower:
                add(e)
                continue

            # Название компании (минимум 4 символа) упомянуто в тексте
            if len(company_lower) >= 4 and company_lower in text_lower:
                add(e)
                continue

            # Первые 4 символа названия компании совпадают (для сокращений)
            prefix = company_lower[:4]
            if len(prefix) == 4 and prefix in text_lower:
                add(e)

        return found

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _load(self) -> list[dict]:
        if self._cache is not None:
            return self._cache
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, company_symbol, exchange
                FROM equity_symbol_mappings
                WHERE company_symbol != ''
                ORDER BY exchange, symbol
                """
            )
            self._cache = [
                {"symbol": row[0], "company": row[1], "exchange": row[2]}
                for row in cur.fetchall()
            ]
            conn.close()
        except Exception:
            self._cache = []
        return self._cache
