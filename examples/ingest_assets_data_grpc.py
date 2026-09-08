import asyncio
import datetime
import logging

from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime_grpc_data_source.finam_asset_data_source import FinamAssetDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_grpc():
    asset_data_source = FinamAssetDataSource.from_env()
    asset_service = get_asset_service(
        clear_asset_db=True,
    )
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)
    dividend_assets = await asset_service.get_exchange_equities_by_symbols(
        symbols=[
            AssetSymbol(symbol="SBER", mic="MISX"),
            AssetSymbol(symbol="UKUZ", mic="MISX"),
        ]
    )
    dividends = await asset_data_source.get_dividends(
        assets=dividend_assets,
        asset_service=asset_service,
        date_from=datetime.datetime(year=2000, month=1, day=1),
        date_to=datetime.datetime(year=2026, month=8, day=1)
    )
    await asset_service.import_dividends(dividends=dividends)

    splits = await asset_data_source.get_splits(
        assets=dividend_assets,
        asset_service=asset_service,
        date_from=datetime.datetime(year=2000, month=1, day=1),
        date_to=datetime.datetime(year=2026, month=8, day=1)
    )
    await asset_service.import_splits(splits=splits)

if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_assets_data_grpc())
