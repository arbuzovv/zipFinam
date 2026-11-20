"""A listing is judged by its own venue's hours, and a mixed run is clocked wide enough for all.

The bug these cover: a simulation ran on one calendar and every listing was judged by it, so an
American ETF on a Moscow clock was declared tradeable at 10:01 Moscow -- 03:01 in New York -- and
the order died in `_calculate_order_value_amount` with a message about delisting. Daily runs hid
it, because a daily bar sits at the Moscow close, which is the middle of the American session.

The strategies this was fixed for are cross-market spreads, where most bars have one leg shut.
"""

import datetime
import os
import unittest

import pandas as pd

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.utils.calendar_utils import get_calendar
from zipfinam.venue_calendars import (
    CONTINUOUS_CLOCK,
    MIXED_VENUE_CLOCK,
    VENUE_CALENDARS_ENV,
    calendar_for_mic,
    calendar_name_for_mic,
    clock_calendar_name,
    trades_every_day,
)

FAR_PAST = datetime.date(1990, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)

#: A Wednesday both markets trade, chosen so no holiday of either is involved.
SESSION = datetime.date(2025, 9, 24)


def listing(symbol: str, mic: str, sid: int = 1) -> ExchangeAsset:
    exchange = ExchangeInfo(mic=mic, name=mic, canonical_name=mic, country_code="US")
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    usd = Currency(id=2, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
                   first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=exchange, asset=equity, quote=usd)


def moscow(hour: int, minute: int = 0) -> datetime.datetime:
    return pd.Timestamp(datetime.datetime(SESSION.year, SESSION.month, SESSION.day, hour, minute),
                        tz="Europe/Moscow").to_pydatetime()


class FORTSCalendarTests(unittest.TestCase):
    """The Moscow derivatives session, which `exchange_calendars` does not ship."""

    def setUp(self):
        self.calendar = get_calendar("FORTS")

    def test_the_evening_session_is_inside_it(self):
        """19:00-23:50 Moscow is where the overlap with the American session lives."""
        self.assertTrue(self.calendar.is_open_on_minute(moscow(21)))

    def test_it_opens_an_hour_before_the_equity_market(self):
        self.assertTrue(self.calendar.is_open_on_minute(moscow(9, 30)))
        self.assertFalse(get_calendar("XMOS").is_open_on_minute(moscow(9, 30)))

    def test_the_clearing_break_is_closed(self):
        self.assertFalse(self.calendar.is_open_on_minute(moscow(18, 55)))

    def test_it_keeps_the_russian_holidays(self):
        """Inherited from XMOS rather than restated, so the two cannot drift apart."""
        self.assertFalse(self.calendar.is_session(pd.Timestamp("2025-05-09")))
        self.assertTrue(self.calendar.is_session(pd.Timestamp("2025-11-27")),
                        "Thanksgiving is an ordinary Moscow session")

    def test_it_is_longer_than_the_equity_session(self):
        session = pd.Timestamp(SESSION)
        self.assertGreater(len(self.calendar.session_minutes(session)),
                           len(get_calendar("XMOS").session_minutes(session)))


class VenueLookupTests(unittest.TestCase):

    def test_moscow_equities_and_derivatives_keep_different_hours(self):
        self.assertEqual(calendar_name_for_mic("MISX"), "XMOS")
        self.assertEqual(calendar_name_for_mic("RTSX"), "FORTS")

    def test_every_american_equity_venue_keeps_the_nyse_session(self):
        for mic in ("ARCX", "BATS", "XASE", "XNCM", "XNGS", "XNMS", "XNYS"):
            self.assertEqual(calendar_name_for_mic(mic), "XNYS", mic)

    def test_an_unknown_venue_has_no_opinion_rather_than_a_wrong_one(self):
        self.assertIsNone(calendar_name_for_mic("ZZZZ"))
        self.assertIsNone(calendar_for_mic("ZZZZ"))
        self.assertIsNone(calendar_name_for_mic(None))



class ClockSelectionTests(unittest.TestCase):
    """What a run is clocked on, given where its instruments list."""

    def test_one_market_keeps_its_own_calendar(self):
        self.assertEqual(clock_calendar_name(["MISX"]), "XMOS")
        self.assertEqual(clock_calendar_name(["ARCX", "XNGS", "XNMS"]), "XNYS")

    def test_venues_that_keep_different_hours_need_a_wider_clock(self):
        self.assertEqual(clock_calendar_name(["MISX", "ARCX"]), MIXED_VENUE_CLOCK)
        self.assertEqual(clock_calendar_name(["RTSX", "XNYM"]), MIXED_VENUE_CLOCK)

    def test_the_mixed_clock_covers_both_markets_whole_sessions(self):
        """Because it keeps every hour of every weekday, which neither market's calendar does."""
        clock = get_calendar(MIXED_VENUE_CLOCK)
        for minute in (moscow(9, 30), moscow(12), moscow(17), moscow(21), moscow(23, 30)):
            self.assertTrue(clock.is_open_on_minute(minute), minute)

    def test_nothing_known_leaves_the_choice_to_the_caller(self):
        self.assertIsNone(clock_calendar_name([]))
        self.assertIsNone(clock_calendar_name(["ZZZZ"]))


class CryptoVenueTests(unittest.TestCase):
    """Venues that never close, which a weekday clock cannot hold at all."""

    #: A Saturday, and a Sunday two days later.
    SATURDAY = pd.Timestamp("2025-09-27")
    SUNDAY = pd.Timestamp("2025-09-28")

    def test_the_crypto_venues_in_the_asset_databases_are_mapped(self):
        for mic in ("KRME", "_BNCC", "_BNCF"):
            self.assertEqual(calendar_name_for_mic(mic), CONTINUOUS_CLOCK, mic)

    def test_a_crypto_venue_trades_at_the_weekend(self):
        calendar = calendar_for_mic("_BNCC")
        self.assertTrue(calendar.is_session(self.SATURDAY))
        self.assertTrue(calendar.is_session(self.SUNDAY))

    def test_a_seven_day_week_is_read_off_the_calendar_rather_than_listed_here(self):
        """So a venue added through the environment is classified without this module knowing."""
        self.assertTrue(trades_every_day(CONTINUOUS_CLOCK))
        for name in (MIXED_VENUE_CLOCK, "XNYS", "XMOS", "FORTS"):
            self.assertFalse(trades_every_day(name), name)

    def test_crypto_alone_keeps_the_continuous_clock(self):
        self.assertEqual(clock_calendar_name(["_BNCC"]), CONTINUOUS_CLOCK)
        self.assertEqual(clock_calendar_name(["_BNCC", "KRME"]), CONTINUOUS_CLOCK)

    def test_anything_mixed_with_crypto_needs_the_weekend_too(self):
        """A weekday clock would not mis-time the weekend bars, it would delete them."""
        for mics in (["_BNCC", "XNGS"], ["_BNCC", "MISX"], ["_BNCF", "RTSX", "XNYM"]):
            self.assertEqual(clock_calendar_name(mics), CONTINUOUS_CLOCK, mics)

    def test_a_mix_without_crypto_still_gets_the_cheaper_weekday_clock(self):
        self.assertEqual(clock_calendar_name(["MISX", "ARCX"]), MIXED_VENUE_CLOCK)





class VenueOverrideTests(unittest.TestCase):
    """A new venue can be added without rebuilding the core, which crypto makes routine."""

    def setUp(self):
        self.addCleanup(os.environ.pop, VENUE_CALENDARS_ENV, None)

    def set_override(self, value: str):
        os.environ[VENUE_CALENDARS_ENV] = value

    def test_a_venue_absent_from_the_table_can_be_named(self):
        self.assertIsNone(calendar_name_for_mic("MEXC"))
        self.set_override("MEXC=24/7")
        self.assertEqual(calendar_name_for_mic("MEXC"), CONTINUOUS_CLOCK)
        self.assertEqual(clock_calendar_name(["MEXC", "XNGS"]), CONTINUOUS_CLOCK)

    def test_several_entries_and_untidy_spacing_are_accepted(self):
        self.set_override(" mexc = 24/7 , XHKG=XHKG ")
        self.assertEqual(calendar_name_for_mic("MEXC"), CONTINUOUS_CLOCK)
        self.assertEqual(calendar_name_for_mic("XHKG"), "XHKG")

    def test_an_override_wins_over_the_shipped_table(self):
        self.set_override("MISX=24/7")
        self.assertEqual(calendar_name_for_mic("MISX"), CONTINUOUS_CLOCK)

    def test_the_table_comes_back_once_the_override_is_gone(self):
        self.set_override("MISX=24/7")
        self.assertEqual(calendar_name_for_mic("MISX"), CONTINUOUS_CLOCK)
        del os.environ[VENUE_CALENDARS_ENV]
        self.assertEqual(calendar_name_for_mic("MISX"), "XMOS")

    def test_rubbish_is_ignored_rather_than_fatal(self):
        """It is set on a deployment, and a typo there must not take every run down."""
        self.set_override("nonsense,,=,MEXC=24/7,XXXX=NoSuchCalendar")
        self.assertEqual(calendar_name_for_mic("MEXC"), CONTINUOUS_CLOCK)
        self.assertIsNone(calendar_for_mic("XXXX"), "an unresolvable name behaves as unknown")




if __name__ == "__main__":
    unittest.main()


class ContainedCalendarTests(unittest.TestCase):
    """A venue whose hours already hold the others' does not need a 24-hour clock."""

    def test_the_moscow_derivatives_session_contains_the_equity_one(self):
        from zipfinam.venue_calendars import covers

        self.assertTrue(covers("FORTS", "XMOS"))
        self.assertFalse(covers("XMOS", "FORTS"), "the narrow one does not contain the wide one")

    def test_containment_needs_the_same_zone(self):
        """Otherwise 24/7 would swallow everything and the zones would not line up."""
        from zipfinam.venue_calendars import covers

        self.assertFalse(covers(CONTINUOUS_CLOCK, "XNYS"))
        self.assertFalse(covers(MIXED_VENUE_CLOCK, "XMOS"))

    def test_moscow_shares_beside_moscow_futures_are_clocked_on_forts(self):
        self.assertEqual(clock_calendar_name(["MISX", "RTSX"]), "FORTS")

    def test_venues_that_genuinely_miss_each_other_still_need_the_wide_clock(self):
        self.assertEqual(clock_calendar_name(["MISX", "ARCX"]), MIXED_VENUE_CLOCK)
        self.assertEqual(clock_calendar_name(["RTSX", "XNYM"]), MIXED_VENUE_CLOCK)
