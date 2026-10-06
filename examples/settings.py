"""Общие настройки примеров: где лежат данные и какими бумагами торгуем."""
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

MOSCOW = ZoneInfo("Europe/Moscow")

#: Справочник инструментов и бандлы с барами.
ZIPLIME_HOME = Path.home() / ".ziplime"
ASSET_DB_PATH = str(ZIPLIME_HOME / "assets.sqlite")
BUNDLE_PATH = str(ZIPLIME_HOME / "data")
BUNDLE_NAME = "finam_daily"

SYMBOLS = ["SBER@MISX", "GAZP@MISX", "LKOH@MISX", "GMKN@MISX", "ROSN@MISX",
           "NVTK@MISX", "TATN@MISX", "MTSS@MISX"]
BENCHMARK = "IMOEX@MISX"

START = datetime.datetime(2024, 1, 1, tzinfo=MOSCOW)
END = datetime.datetime(2025, 6, 30, tzinfo=MOSCOW)
