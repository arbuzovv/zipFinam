import asyncio
import datetime
import logging

import pytz

from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from zipfinam import GrpcDataSource
from ziplime.utils.logging_utils import configure_logging


async def _ingest_data_grpc():
    asset_service = get_asset_service(
        clear_asset_db=False,
    )

    # symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL", "VXX@BATS"]
    # timezone = "America/New_York"
    # calendar = "NYSE"

    symbols = ["SBER@MISX", "UGLD@MISX", "UKUZ@MISX", "WUSH@MISX"]
    timezone = "Europe/Moscow"
    calendar = "XMOS"

    start_date = datetime.datetime(year=2025, month=9, day=1, tzinfo=pytz.timezone(timezone))
    end_date = datetime.datetime(year=2025, month=9, day=18, tzinfo=pytz.timezone(timezone))
    market_data_bundle_source = GrpcDataSource.from_env()
    await market_data_bundle_source.get_token()

    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar=calendar,
        bundle_name="grpc_daily_data",
        data_bundle_source=market_data_bundle_source,
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service
    )


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_ingest_data_grpc())
