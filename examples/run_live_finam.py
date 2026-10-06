"""Шаг 3. Живая торговля через Финам: бумажный счёт Arena или реальный счёт.

Arena (https://arena.finam.ru) и боевой Trade API (https://api.finam.ru) говорят на одном
протоколе, разница только в адресе. По умолчанию — Arena.

    export GRPC_TOKEN=...            # токен Trade API: котировки, бары и размер лота
    export FINAM_TRADE_TOKEN=...     # токен счёта, на котором исполняются заявки
    export FINAM_ACCOUNT_ID=...      # номер этого счёта
    # export FINAM_TRADE_URL=https://api.finam.ru   # реальные деньги: только осознанно
    python examples/run_live_finam.py

Стратегия получает не весь счёт, а капитал CAPITAL: order_target_percent(0.5) — это половина
CAPITAL, а не половина счёта. Остаток меньше лота не отправляется (GAZP торгуется по 10 штук).
"""
import asyncio
import datetime
import logging
import os
from pathlib import Path

import polars as pl
import structlog
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation_iter
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.logging_utils import configure_logging

from zipfinam.exchanges import FinamExchange
from zipfinam.finam.order_status import NOT_EXECUTED, describe_order
from settings import ASSET_DB_PATH, BENCHMARK, BUNDLE_NAME, BUNDLE_PATH, START, SYMBOLS

logger = structlog.get_logger(__name__)

STRATEGY = Path(__file__).parent / "algorithms" / "buy_and_hold_moex.py"
CAPITAL = 100_000.0
ARENA = "https://arena.finam.ru"


async def main():
    trade_url = os.environ.get("FINAM_TRADE_URL", ARENA).rstrip("/")
    if trade_url != ARENA:
        logger.warning("Заявки уходят на реальный счёт", url=trade_url)

    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    symbols = SYMBOLS + [BENCHMARK]
    listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s.split("@")[0], mic=s.split("@")[1]) for s in symbols],
        asset_type=AssetType.EQUITY)
    now = datetime.datetime.now(tz=START.tzinfo)
    # История для индикаторов на первом тике; свежие бары биржа отдаёт сама.
    bundle, _missing = await get_bundle_service(BUNDLE_PATH).load_bundle(
        bundle_name=BUNDLE_NAME, bundle_version=None, frequency=datetime.timedelta(days=1),
        start_date=START, end_date=now, assets=[listing for listing in listings if listing],
        aggregations=[pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
                      pl.col("close").last(), pl.col("volume").sum(), pl.col("symbol").last()])

    calendar = get_calendar("XMOS")
    clock = RealtimeClock(trading_calendar=calendar, start_date=now,
                          emission_rate=datetime.timedelta(minutes=1))
    exchange = FinamExchange(
        name="finam", canonical_name="finam", country_code="RU",
        asset_service=asset_service, trading_calendar=calendar, data_source=bundle, clock=clock,
        finam_arena_server_url=trade_url,
        authorization_token=os.environ["FINAM_TRADE_TOKEN"],
        account_id=os.environ["FINAM_ACCOUNT_ID"],
        finam_api_server_url=os.environ.get("FINAM_API_SERVER_URL", "https://api.finam.ru"),
        finam_api_token=os.environ["GRPC_TOKEN"],
        start_cash_balance=CAPITAL, capital_limit=CAPITAL, is_default=True)

    async for status in run_simulation_iter(
            start_date=now, end_date=now + datetime.timedelta(days=365), trading_calendar="XMOS",
            algorithm_file=str(STRATEGY), total_cash=CAPITAL, market_data_source=bundle,
            custom_data_sources=[], emission_rate=datetime.timedelta(minutes=1),
            benchmark_asset_symbol=BENCHMARK, stop_on_error=True, asset_service=asset_service,
            clock=clock, exchange=exchange):
        if status.errors:
            logger.error("Ошибка на тике", errors=status.errors)
        elif status.result:
            logger.info("Стратегия завершилась")
        # Принята — ещё не исполнена: спросить брокера, что стало с заявками этого тика.
        for row in await exchange.refresh_order_states():
            if row["outcome"] in NOT_EXECUTED:
                logger.error(describe_order(row))
            else:
                logger.info(describe_order(row))


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="zipfinam_live.log")
    asyncio.run(main())
