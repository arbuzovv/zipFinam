"""zipfinam plugs into ziplime 2.11 through its extension points, not by patching it.

Installing zipfinam is all a deployment does -- the backtesting service never imports it -- so the
entry points have to carry the Moscow venues, the FORTS calendar and the ``algopack://`` scheme on
their own.
"""

import asyncio
import datetime
import importlib.metadata
import types
import unittest

import pandas as pd
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.data.data_sources import address_schemes
from ziplime.domain.bar_data import BarData
from ziplime.finance.asset_restrictions import NoRestrictions
from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.utils import venue_calendars as core
from ziplime.utils.calendar_utils import get_calendar

import zipfinam  # noqa: F401 -- registers explicitly as well; the tests below undo that first
from zipfinam.algopack import AlgoPackDataSource
from zipfinam.venue_calendars import MOSCOW_VENUES

FAR_PAST = datetime.date(1990, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)
SESSION = datetime.date(2025, 9, 24)


def installed(group: str) -> list[str]:
    return [ep.value for ep in importlib.metadata.entry_points(group=group)]


def listing(symbol: str, mic: str) -> ExchangeAsset:
    exchange = ExchangeInfo(mic=mic, name=mic, canonical_name=mic, country_code="US")
    equity = Equity(id=10_001, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    rub = Currency(id=2, isin=None, asset_name="RUB", start_date=FAR_PAST, end_date=FAR_FUTURE,
                   first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=1, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=exchange, asset=equity, quote=rub)


def moscow(hour: int) -> datetime.datetime:
    return pd.Timestamp(datetime.datetime(SESSION.year, SESSION.month, SESSION.day, hour),
                        tz="Europe/Moscow").to_pydatetime()


@unittest.skipUnless(installed("ziplime.venue_calendars"),
                     "zipfinam is not installed (pip install -e .), so it advertises nothing")
class VenueEntryPointTests(unittest.TestCase):
    """As a process that never imported zipfinam would see it."""

    def setUp(self):
        core.unregister_venue_calendars(MOSCOW_VENUES)
        core._entry_points_loaded = False
        self.addCleanup(core.register_venue_calendars, MOSCOW_VENUES)

    def test_the_entry_point_is_advertised(self):
        self.assertIn("zipfinam.venue_calendars:moscow_venues", installed("ziplime.venue_calendars"))

    def test_moscow_venues_arrive_through_the_entry_point(self):
        self.assertEqual(core.calendar_name_for_mic("MISX"), "XMOS")
        self.assertEqual(core.calendar_for_mic("RTSX").name, "FORTS")

    def test_moscow_shares_beside_moscow_futures_are_clocked_on_forts(self):
        self.assertEqual(core.clock_calendar_name(["MISX", "RTSX"]), "FORTS")
        self.assertEqual(core.clock_calendar_name(["MISX", "ARCX"]), core.MIXED_VENUE_CLOCK)


class FORTSInTheEngineTests(unittest.TestCase):
    """What the calendar is for: the engine asking a Moscow future about its own hours."""

    @staticmethod
    def can_trade(asset, dt, clock):
        source = types.SimpleNamespace(name="test", last_available_bar=lambda sid=None: None)
        bar_data = BarData(data_sources={"test": source}, simulation_dt_func=lambda: dt,
                           trading_calendar=get_calendar(clock), restrictions=NoRestrictions(),
                           emission_rate=datetime.timedelta(hours=1))
        return bool(bar_data.can_trade(assets=[asset]).iloc[0])

    def test_a_futures_listing_on_a_mixed_clock_trades_into_the_evening(self):
        """XMOS would have said no at 21:00; FORTS is open until 23:50."""
        self.assertTrue(self.can_trade(listing("NGZ6", "RTSX"), moscow(21), core.MIXED_VENUE_CLOCK))

    def test_a_moscow_share_on_the_same_clock_is_shut_at_that_hour(self):
        self.assertFalse(self.can_trade(listing("SBER", "MISX"), moscow(21),
                                        core.MIXED_VENUE_CLOCK))


class AddressSchemeTests(unittest.TestCase):

    def setUp(self):
        self.addCleanup(zipfinam.algopack.install_address_scheme)

    def test_ziplime_is_not_patched(self):
        """zipfinam 1.26.10 wrapped this method; the extension point replaced the wrapper."""
        self.assertFalse(getattr(TradingAlgorithm._resolve_named_data_source, "_zipfinam", False))

    @unittest.skipUnless(installed("ziplime.address_schemes"), "zipfinam is not installed")
    def test_the_scheme_arrives_through_the_entry_point(self):
        address_schemes.unregister_address_scheme("algopack")
        address_schemes._entry_points_loaded = False
        self.assertIs(address_schemes.mount_for("algopack://eq/tradestats"),
                      zipfinam.algopack.mount_address)

    def test_a_strategy_address_mounts_an_algopack_source(self):
        calendar = get_calendar("XMOS")
        algorithm = types.SimpleNamespace(
            asset_service=None,
            clock=types.SimpleNamespace(start_session=datetime.date(2025, 1, 6),
                                        end_session=datetime.date(2025, 3, 31),
                                        trading_calendar=calendar))
        source = asyncio.run(TradingAlgorithm._resolve_named_data_source(
            algorithm, "algopack://eq/tradestats/5min"))
        self.assertIsInstance(source, AlgoPackDataSource)
        self.assertEqual(source.dataset.name, "tradestats")
        self.assertEqual(source.granularity.value, "5min")
        self.assertIs(source.trading_calendar, calendar)


class LightImportTests(unittest.TestCase):
    """What ziplime loads through the entry points must not drag the Finam gRPC stubs in."""

    def test_calendars_and_algopack_import_without_the_finam_sources(self):
        import subprocess
        import sys

        code = ("import sys, zipfinam.venue_calendars, zipfinam.algopack; "
                "print(any(m.startswith('zipfinam.finam') for m in sys.modules))")
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                                check=True)
        self.assertEqual(result.stdout.strip(), "False")

    def test_the_finam_sources_are_still_reachable_by_their_old_names(self):
        from zipfinam import FinamAssetDataSource, FinamDataSource, GrpcDataSource

        self.assertIs(FinamDataSource, GrpcDataSource)
        self.assertEqual(FinamAssetDataSource.__name__, "GrpcAssetDataSource")


if __name__ == "__main__":
    unittest.main()
