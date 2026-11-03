"""Report one Finam SBER backtest in RUB and USD using ziplime's currency API.

Run from the repository root with a ziplime checkout containing CurrencyReporting:

    python -m examples.currencies.run_simulation_grpc

The local assets database and daily data bundle must already contain SBER and the
selected USD/RUB listing, including FX history before the simulation starts.
"""

import argparse
import asyncio
import datetime
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging

HERE = Path(__file__).resolve().parent
DAY = datetime.timedelta(days=1)
MAX_RATE_AGE = DAY * 7
MOSCOW = ZoneInfo("Europe/Moscow")


async def run_example(args):
    start_date = datetime.datetime.combine(args.start, datetime.time.min, tzinfo=MOSCOW)
    end_date = datetime.datetime.combine(args.end, datetime.time.max, tzinfo=MOSCOW)
    if start_date > end_date:
        raise ValueError("--start must not follow --end.")
    ticker, separator, mic = args.fx_symbol.partition("@")
    if not ticker or not separator or not mic:
        raise ValueError("--fx-symbol must name a listing, e.g. USDRUB@_NPRO.")

    data_path = args.ziplime_dir / "data"
    asset_service = get_asset_service(db_path=str(args.ziplime_dir / "assets.sqlite"))
    bundle_service = get_bundle_service(bundle_storage_path=str(data_path))
    [sber] = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol="SBER", mic="MISX")], asset_type=AssetType.EQUITY,
    )
    fx_listing = await asset_service.get_exchange_currency_by_symbol(
        symbol=AssetSymbol(symbol=ticker, mic=mic),
    )
    if sber is None or fx_listing is None:
        raise ValueError("SBER or the FX listing is missing from the local assets database.")
    if sber.quote.asset_name != "RUB":
        raise ValueError("This example requires a RUB-quoted SBER listing.")
    if {fx_listing.asset.asset_name, fx_listing.quote.asset_name} != {"USD", "RUB"}:
        raise ValueError("Select a USD/RUB or RUB/USD listing for --fx-symbol.")

    definition, storage = await bundle_service.load_bundle_definition(bundle_name=args.bundle)
    # Use the timestamps stored during ingestion. The CBR reference rates in this
    # bundle are effective on those dates and do not need a market-close delay.
    conversion = await CurrencyConversionService.from_bundle(
        storage=storage,
        bundle=definition,
        listings=[fx_listing],
        start=start_date,
        end=end_date,
        max_rate_age=MAX_RATE_AGE,
    )
    reporting = CurrencyReporting(
        base_currency="RUB",
        currencies=("RUB", "USD"),
        conversion=conversion,
    )

    market_data, missing_data = await bundle_service.load_bundle(
        bundle_name=args.bundle,
        bundle_version=definition.version,
        assets=[sber],
        frequency=DAY,
        start_date=start_date,
        end_date=end_date,
    )
    if missing_data or market_data.data["close"].null_count():
        raise ValueError("SBER daily bars are missing from the local bundle for the requested dates.")

    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="XMOS",
        emission_rate=DAY,
        total_cash=100_000.0,  # RUB, the execution/base currency.
        market_data_source=market_data,
        custom_data_sources=[],
        algorithm_file=str(HERE / "strategy.py"),
        benchmark_asset_symbol="SBER@MISX",  # Its RUB benchmark is also translated.
        stop_on_error=True,
        asset_service=asset_service,
        same_bar_execution=False,
        print_algo=False,
        currency_reporting=reporting,
    )
    if result.errors:
        raise RuntimeError(f"Simulation failed: {result.errors}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    columns = ["portfolio_value", "pnl", "returns", "algorithm_period_return", "benchmark_period_return"]
    for code, performance in result.perf_by_currency.items():
        path = args.output_dir / f"performance_{code}.csv"
        performance[columns].to_csv(path, index_label="date")
        print(f"\n{code} (returns are fractions; pnl is daily):")
        print(performance[columns].tail().to_string(float_format=lambda value: f"{value:,.6f}"))
        print(f"Full daily report: {path.resolve()}")
    return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=datetime.date.fromisoformat, default=datetime.date(2025, 9, 2))
    parser.add_argument("--end", type=datetime.date.fromisoformat, default=datetime.date(2025, 9, 17))
    parser.add_argument("--fx-symbol", default="USDRUB@_NPRO", help="Exact FX ticker@MIC in Finam.")
    parser.add_argument("--bundle", default="grpc_daily_data", help="Existing daily equity/FX bundle name.")
    parser.add_argument("--ziplime-dir", type=Path, default=Path.home() / ".ziplime",
                        help="Directory containing assets.sqlite and data/ (default: ~/.ziplime).")
    parser.add_argument("--output-dir", type=Path, default=Path("currency_reports"),
                        help="Directory for daily CSV reports (default: currency_reports).")
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    asyncio.run(run_example(parse_args()))
