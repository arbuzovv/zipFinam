import asyncio
import datetime
import logging
from pathlib import Path

import polars as pl
import pytz
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.utils.logging_utils import configure_logging

SYMBOLS = ["SBER@MISX", "LKOH@MISX", "GAZP@MISX"]
TIMEZONE = "Europe/Moscow"
BUNDLE_NAME = "moex_daily"
BENCHMARK = "SBER@MISX"


async def main():
    tz = pytz.timezone(TIMEZONE)
    start_date = tz.localize(datetime.datetime(2024, 1, 1))
    end_date   = tz.localize(datetime.datetime(2024, 12, 31))

    bundle_registry = FileSystemBundleRegistry(
        base_data_path=str(Path.home() / ".ziplime" / "data")
    )
    bundle_service = BundleService(bundle_registry=bundle_registry)

    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last(),
    ]
    market_data = await bundle_service.load_bundle(
        bundle_name=BUNDLE_NAME,
        bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start_date,
        end_date=end_date + datetime.timedelta(days=1),
        symbols=[s.split("@")[0] for s in SYMBOLS],
        aggregations=aggregations,
    )

    asset_service = get_asset_service(clear_asset_db=False)

    results, errors = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="XMOS",
        algorithm_file=str(Path(__file__).parent / "algo.py"),
        total_cash=1_000_000.0,
        market_data_source=market_data,
        custom_data_sources=[],
        config_file=None,
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol=BENCHMARK,
        benchmark_returns=None,
        stop_on_error=False,
        asset_service=asset_service,
        default_exchange_name="XMOS",
    )

    if errors:
        print("Ошибки:", errors)

    print(results.head(20).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    asyncio.run(main())
