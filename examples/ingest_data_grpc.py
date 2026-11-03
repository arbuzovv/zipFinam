import asyncio
import datetime
import logging

import pytz

from ziplime.core.ingest_data import get_asset_service, ingest_market_data, ingest_custom_data

from ziplime_grpc_data_source.grpc_data_source import GrpcDataSource
from ziplime.utils.logging_utils import configure_logging


async def _ingest_data_grpc():
    asset_service = get_asset_service(
        clear_asset_db=False,
    )

    # symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL", "VXX@BATS"]
    # timezone = "America/New_York"
    # calendar = "NYSE"

    symbols = ["SBER@MISX",
               "USD/RUB_ON@_NPRO",
               "USD/RUB_TN@_NPRO",
               "USD/RUB_TOD@_NPRO",
               "USD/RUB_TODSPOT@_NPRO",
               "USD/RUB_SPOT@_NPRO",
               "USD/RUB_1M@_NPRO",
               "USD/RUB_1W@_NPRO",
               "USD/RUB_1Y@_NPRO",
               "USD/RUB_2M@_NPRO",
               "USD/RUB_2W@_NPRO",
               "USD/RUB_3M@_NPRO",
               "USD/RUB_6M@_NPRO",
               "USD/RUB_9M@_NPRO",
               "USD/RUB_TOM2M@_NPRO",
               "USDRUB"
               ]
    timezone = "Europe/Moscow"
    calendar = "XMOS"

    for y in range(2025, 2026):
        start_date = datetime.datetime(year=y, month=1, day=1, tzinfo=pytz.timezone(timezone))
        end_date = datetime.datetime(year=y + 1, month=1, day=1, tzinfo=pytz.timezone(timezone))
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
            asset_service=asset_service,
            merge=True
        )


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_ingest_data_grpc())
