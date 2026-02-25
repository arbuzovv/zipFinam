import asyncio
import logging
from zipfinam import GrpcAssetDataSource
from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime.utils.logging_utils import configure_logging


async def main():
    asset_data_source = GrpcAssetDataSource.from_env()
    asset_service = get_asset_service(clear_asset_db=True)
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)
    print("Активы загружены.")


if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    asyncio.run(main())
