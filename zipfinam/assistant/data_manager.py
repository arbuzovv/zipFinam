"""
Менеджер загрузки рыночных данных через Финам API.
"""
from __future__ import annotations

import datetime
import pathlib
from typing import Callable
from zoneinfo import ZoneInfo

DEFAULT_ASSETS_DB  = pathlib.Path.home() / ".ziplime" / "assets.sqlite"
DEFAULT_BUNDLES    = pathlib.Path.home() / ".ziplime" / "data"
BUNDLE_NAME        = "grpc_daily_data"
TRADING_CALENDAR   = "XMOS"
MOSCOW             = ZoneInfo("Europe/Moscow")

# Number of extra calendar days to ingest *before* the requested start date.
# This guarantees bundle_start_date < simulation start_date (avoiding the
# "Start date is before bundle start date" ValueError) and also provides
# warmup bars for indicators like moving averages.
WARMUP_BUFFER_DAYS = 30

# Benchmark always included in every ingest so every bundle version contains it.
BENCHMARK_SYMBOL = "IMOEX@MISX"


class DataManager:
    """
    Управляет загрузкой рыночных данных через Финам API для ИИ-ассистента.

    Без кэша состояния — каждый запрос грузит нужные символы плюс бенчмарк
    напрямую из Финам API. Это гарантирует что последняя версия бандла
    всегда содержит все необходимые данные.
    """

    def __init__(
        self,
        assets_db_path: pathlib.Path = DEFAULT_ASSETS_DB,
        on_progress: Callable[[str], None] | None = None,
        bundle_storage_path: pathlib.Path = DEFAULT_BUNDLES,
    ):
        self.assets_db_path = assets_db_path
        self.bundle_storage_path = bundle_storage_path
        self.on_progress = on_progress or (lambda msg: None)

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def ensure_data(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """
        Загружает данные для указанных символов за заданный период.

        Инициирует загрузку начиная с WARMUP_BUFFER_DAYS до start_date,
        чтобы bundle_start_date < simulation start_date.
        """
        ingest_start = _to_moscow_midnight(start_date) - datetime.timedelta(days=WARMUP_BUFFER_DAYS)
        ingest_end   = _to_moscow_midnight(end_date) + datetime.timedelta(days=1)

        self.on_progress(
            f"Загружаю данные для {', '.join(symbols)} "
            f"({ingest_start.date()} → {ingest_end.date()})…"
        )
        await self._ingest(symbols, ingest_start, ingest_end)
        self.on_progress("Загрузка данных завершена.")

    def get_bundle_name(self) -> str:
        return BUNDLE_NAME

    def get_assets_db_path(self) -> pathlib.Path:
        return self.assets_db_path

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    async def _ingest(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """Загружает данные через Финам gRPC API.

        Бенчмарк (IMOEX@MISX) всегда добавляется к списку символов,
        потому что каждый ingest создаёт новую версию бандла и load_bundle
        берёт только последнюю версию.
        """
        from ziplime.core.ingest_data import get_asset_service, ingest_market_data
        from zipfinam import FinamDataSource

        # Always include benchmark so every bundle version contains it.
        ingest_symbols = list(symbols)
        if BENCHMARK_SYMBOL not in ingest_symbols:
            ingest_symbols.append(BENCHMARK_SYMBOL)

        asset_service = get_asset_service(
            clear_asset_db=False,
            db_path=str(self.assets_db_path),
        )
        listings = await self.listings(asset_service, ingest_symbols)

        data_source = FinamDataSource.from_env()
        await data_source.get_token()

        await ingest_market_data(
            start_date=start_date,
            end_date=end_date,
            symbols=ingest_symbols,
            trading_calendar=TRADING_CALENDAR,
            bundle_name=BUNDLE_NAME,
            data_bundle_source=data_source,
            data_frequency=datetime.timedelta(days=1),
            asset_service=asset_service,
            assets=listings,
            bundle_storage_path=str(self.bundle_storage_path),
        )

    async def listings(self, asset_service, symbols: list[str]) -> list:
        """The listings of ``symbols`` (``TICKER@MIC``), filling the instrument list on first use.

        The list of instruments comes from Finam once and is kept in ``assets_db_path``; delete
        the file to read it again.
        """
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol
        from ziplime.core.ingest_data import ingest_assets
        from zipfinam import FinamAssetDataSource

        def wanted() -> list[AssetSymbol]:
            return [AssetSymbol(symbol=s.split("@")[0], mic=s.split("@")[1] if "@" in s else "MISX")
                    for s in symbols]

        found = await asset_service.get_exchange_assets_by_symbols(
            symbols=wanted(), asset_type=AssetType.EQUITY)
        if not all(found):
            self.on_progress("Загружаю список инструментов Финам (один раз)…")
            await ingest_assets(asset_service=asset_service,
                                asset_data_source=FinamAssetDataSource.from_env())
            found = await asset_service.get_exchange_assets_by_symbols(
                symbols=wanted(), asset_type=AssetType.EQUITY)
        missing = [s for s, listing in zip(symbols, found) if listing is None]
        if missing:
            raise ValueError(f"Финам не знает инструментов: {', '.join(missing)}")
        return found


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _to_moscow_midnight(dt: datetime.datetime) -> datetime.datetime:
    """Midnight of the same Moscow day.

    ziplime hands the ingest window to exchange_calendars with the zone dropped, and XMOS accepts
    only a time of 00:00 -- so the bound has to be midnight in Moscow, not in UTC (which reads
    as 03:00 once the zone is gone).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MOSCOW)
    return dt.astimezone(MOSCOW).replace(hour=0, minute=0, second=0, microsecond=0)
