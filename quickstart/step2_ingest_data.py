import asyncio
import datetime
import logging
import pytz
from zipfinam import GrpcDataSource
from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.utils.logging_utils import configure_logging

SYMBOLS = ["SBER@MISX", "LKOH@MISX", "GAZP@MISX"]
TIMEZONE = "Europe/Moscow"
BUNDLE_NAME = "moex_daily"


async def main():
    tz = pytz.timezone(TIMEZONE)
    start_date = tz.localize(datetime.datetime(2024, 1, 1))
    end_date   = tz.localize(datetime.datetime(2024, 12, 31))

    data_source = GrpcDataSource.from_env()
    await data_source.get_token()  # проверка подключения

    asset_service = get_asset_service(clear_asset_db=False)

    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=SYMBOLS,
        trading_calendar="XMOS",
        bundle_name=BUNDLE_NAME,
        data_bundle_source=data_source,
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
    )
    print("Данные загружены.")


if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    asyncio.run(main())
