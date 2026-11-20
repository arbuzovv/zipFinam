"""Шаг 2. Бэктест стратегии на загруженных барах.

    python examples/run_backtest.py                                   # купи и держи
    python examples/run_backtest.py algorithms/algopack/flow_tilt.py  # AlgoPack, нужен ALGOPACK_API
"""
import asyncio
import datetime
import logging
import sys
from pathlib import Path

import polars as pl
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerDollar
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging

import zipfinam  # noqa: F401  (схема algopack:// для data.history)
from settings import ASSET_DB_PATH, BENCHMARK, BUNDLE_NAME, BUNDLE_PATH, END, START, SYMBOLS

HERE = Path(__file__).parent


async def main(strategy: Path):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    symbols = SYMBOLS + [BENCHMARK]
    listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s.split("@")[0], mic=s.split("@")[1]) for s in symbols],
        asset_type=AssetType.EQUITY)

    bundle, _missing = await get_bundle_service(BUNDLE_PATH).load_bundle(
        bundle_name=BUNDLE_NAME, bundle_version=None, frequency=datetime.timedelta(days=1),
        start_date=START, end_date=END, assets=[listing for listing in listings if listing],
        aggregations=[pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
                      pl.col("close").last(), pl.col("volume").sum(), pl.col("symbol").last()])

    result = await run_simulation(
        start_date=START, end_date=END, trading_calendar="XMOS",
        algorithm_file=str(strategy), total_cash=1_000_000.0,
        market_data_source=bundle, custom_data_sources=[],
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol=BENCHMARK, stop_on_error=True, asset_service=asset_service,
        # Брокерская комиссия Финам по умолчанию — порядка 0,04% от оборота.
        equity_commission=PerDollar(cost=0.0004))
    if result.errors:
        raise SystemExit(result.errors)
    print(result.perf[["portfolio_value", "returns", "ending_cash"]].tail(10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="zipfinam.log")
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("algorithms/buy_and_hold_moex.py")
    asyncio.run(main(path if path.is_absolute() else HERE / path))
