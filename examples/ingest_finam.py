"""Шаг 1. Загрузить справочник инструментов и дневные бары из Финам Trade API.

    export GRPC_TOKEN=...   # токен из личного кабинета https://tradeapi.finam.ru
    python examples/ingest_finam.py
"""
import asyncio
import datetime
import logging

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service, ingest_assets, ingest_market_data
from ziplime.utils.logging_utils import configure_logging

from zipfinam import FinamAssetDataSource, FinamDataSource
from settings import ASSET_DB_PATH, BENCHMARK, BUNDLE_NAME, BUNDLE_PATH, END, START, SYMBOLS


async def main():
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    await ingest_assets(asset_service=asset_service,
                        asset_data_source=FinamAssetDataSource.from_env())

    symbols = SYMBOLS + [BENCHMARK]
    listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s.split("@")[0], mic=s.split("@")[1]) for s in symbols],
        asset_type=AssetType.EQUITY)
    missing = [s for s, listing in zip(symbols, listings) if listing is None]
    if missing:
        raise SystemExit(f"Финам не знает инструментов: {missing}")

    # С запасом в 60 дней до начала: индикаторам нужна история на первом баре.
    await ingest_market_data(
        start_date=START - datetime.timedelta(days=60), end_date=END, symbols=symbols,
        trading_calendar="XMOS", bundle_name=BUNDLE_NAME,
        data_bundle_source=FinamDataSource.from_env(),
        data_frequency=datetime.timedelta(days=1), asset_service=asset_service,
        bundle_storage_path=BUNDLE_PATH, assets=listings)


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="zipfinam.log")
    asyncio.run(main())
