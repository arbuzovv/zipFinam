import asyncio
import datetime
import logging

import polars as pl
import structlog

from ziplime.utils.logging_utils import configure_logging

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.domain.asset_type import AssetType

logger = structlog.get_logger(__name__)


async def _run_simulation():
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

    symbols_equities = ["SBER@MISX"]
    symbols_currencies = ["USDRUB@_NPRO"]

    benchmark_asset_symbol = "SBER@MISX"
    timezone = "Europe/Moscow"
    calendar = "XMOS"

    exchange_assets_equities = await asset_service.get_exchange_assets_by_symbols(symbols=[AssetSymbol(
        symbol=symbol.split("@")[0], mic=symbol.split("@")[1]
    ) for symbol in symbols_equities], asset_type=AssetType.EQUITY)

    exchange_assets_currencies = await asset_service.get_exchange_assets_by_symbols(symbols=[AssetSymbol(
        symbol=symbol.split("@")[0], mic=symbol.split("@")[1]
    ) for symbol in symbols_currencies], asset_type=AssetType.CURRENCY)

    tz = pytz.timezone(timezone)
    start_local = datetime.datetime(2025, 9, 2, 0, 0)  # 2025-09-01 00:00 local clock time
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
    market_data_bundle, missing_data = await bundle_service.load_bundle(bundle_name="grpc_daily_data",
                                                          bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=start_date - datetime.timedelta(days=1),
                                                          end_date=end_date + datetime.timedelta(days=1),
                                                          assets=exchange_assets_equities + exchange_assets_currencies,
                                                          aggregations=aggregations
                                                          )


    # By default, SimulationExchange with LIME name is used

    # run daily simulation
    res = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar=calendar,
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=[],
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol=benchmark_asset_symbol,
        benchmark_returns=None,
        stop_on_error=True,
        asset_service=asset_service,
        default_exchange_name="grpc_exchange"
    )
    if res.errors:
        logger.error(res.errors)
    print(res.perf.head(n=10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())
