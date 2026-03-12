"""
Менеджер загрузки рыночных данных через Финам API.
"""
from __future__ import annotations

import datetime
import pathlib
from typing import Callable

DEFAULT_ASSETS_DB  = pathlib.Path.home() / ".ziplime" / "assets.sqlite"
BUNDLE_NAME        = "grpc_daily_data"
TRADING_CALENDAR   = "XMOS"

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
    ):
        self.assets_db_path = assets_db_path
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
        ingest_start = _to_utc_midnight(start_date) - datetime.timedelta(days=WARMUP_BUFFER_DAYS)
        ingest_end   = _to_utc_midnight(end_date) + datetime.timedelta(days=1)

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
        from ziplime_grpc_data_source.grpc_data_source import GrpcDataSource

        # Always include benchmark so every bundle version contains it.
        ingest_symbols = list(symbols)
        if BENCHMARK_SYMBOL not in ingest_symbols:
            ingest_symbols.append(BENCHMARK_SYMBOL)

        asset_service = get_asset_service(
            clear_asset_db=False,
            db_path=str(self.assets_db_path),
        )
        data_source = GrpcDataSource.from_env()
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
        )


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _to_utc_midnight(dt: datetime.datetime) -> datetime.datetime:
    """Convert any datetime to UTC-aware midnight."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
