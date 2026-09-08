import asyncio
import datetime
import logging
from os import environ

import polars as pl
import structlog
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.utils.calendar_utils import get_calendar

from ziplime.utils.logging_utils import configure_logging

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation, run_simulation_iter
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry

from ziplime_grpc_data_source.grpc_exchange import GrpcExchange

logger = structlog.get_logger(__name__)


async def _run_live_trading():
    bundle_storage_path = str(Path(Path.home(), ".ziplime", "data"))
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    asset_service = get_asset_service(
        clear_asset_db=False,
    )
    # symbols = ["AAPL", "AMZN", "GOOGL", "NFLX"]
    # benchmark_asset_symbol = "GOOGL"
    # timezone = "America/New_York"
    # calendar = "NYSE"

    symbols = ["SBER@MISX", "UGLD@MISX", "UKUZ@MISX", "WUSH@MISX"]
    benchmark_asset_symbol = "SBER@MISX"
    timezone = "Europe/Moscow"
    calendar = "XMOS"



    tz = pytz.timezone(timezone)
    start_local = datetime.datetime(2025, 9, 1, 0, 0)  # 2025-09-01 00:00 local clock time
    end_local = datetime.datetime(2025, 9, 17, 0, 0)  # 2025-09-01 00:00 local clock time

    start_date = tz.localize(start_local)  # Correct: EDT (UTC-04:00)
    end_date = tz.localize(end_local)
    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last()
    ]
    market_data_bundle = await bundle_service.load_bundle(bundle_name="grpc_daily_data",
                                                          bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=start_date,
                                                          end_date=end_date + datetime.timedelta(days=1),
                                                          symbols=[s.split("@")[0] for s in symbols],
                                                          aggregations=aggregations
                                                          )


    # By default, SimulationExchange with LIME name is used
    trading_calendar = get_calendar(calendar)
    clock = RealtimeClock(trading_calendar= trading_calendar,
                          start_date=datetime.datetime.now(tz=trading_calendar.tz),
                          timedelta_diff_from_current_time=-datetime.timedelta(hours=0),
                          emission_rate=datetime.timedelta(minutes=1))
    exchange = GrpcExchange(
        name="grpc_exchange",
        canonical_name="grpc_exchange",
        country_code="RU",
        asset_service=asset_service,
        trading_calendar=trading_calendar,
        data_source=market_data_bundle,
        clock=clock,
        authorization_token=environ.get("GRPC_TOKEN", ""),
        server_url=environ.get("GRPC_SERVER_URL", ""),
        start_cash_balance=100,
        account_id=environ.get("GRPC_ACCOUNT_ID", ""),
        is_default=True
    )
    # run daily simulation
    async for status in run_simulation_iter(
        start_date=start_date,
        end_date=end_date,
        trading_calendar=calendar,
        algorithm_file=str(Path("algorithms/test_algo/test_algo_live_trading.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=[],
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol=benchmark_asset_symbol,
        benchmark_returns=None,
        stop_on_error=True,
        asset_service=asset_service,
        default_exchange_name="grpc_exchange",
        clock=clock,
        exchange=exchange
    ):
        if status.errors:
            logger.error(status.errors)

        if status.result:
            logger.info("Algorithm finished")
            print(status.result.perf.head(n=10).to_markdown())
        else:
            logger.info("Algo stats")
            print(status.cumulative_perf)


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_live_trading())
