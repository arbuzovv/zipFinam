"""Sessions the packaged calendars invent, and what a backtest does with them.

A calendar decides which days exist. A day it believes in but the exchange did not open is a day
the engine demands a price for, and with forward fill on -- the default -- it gets the last close,
repeated, at zero volume, with nothing raised. The run then trades a frozen market for the length
of the halt, fills stops at prices nobody quoted, and dilutes the volatility of the whole window
with a stretch of exactly zero returns.

``exchange_calendars`` 4.13.1 has no record of the Moscow Exchange closing for most of March 2022,
which is the largest such gap anyone running this engine on MOEX data will hit.
"""
import datetime
import unittest

from ziplime_grpc_data_source.moex.calendars import MOEX_2022_CLOSURE
from ziplime.utils.calendar_utils import get_calendar

#: Trading stopped after Friday 2022-02-25 and equities came back on Thursday 2022-03-24.
LAST_SESSION_BEFORE = datetime.date(2022, 2, 25)
FIRST_SESSION_AFTER = datetime.date(2022, 3, 24)


class MoexClosureTests(unittest.TestCase):
    def setUp(self):
        self.calendar = get_calendar("XMOS")

    def test_the_exchange_is_shut_for_the_whole_closure(self):
        for day in MOEX_2022_CLOSURE:
            self.assertFalse(self.calendar.is_session(day.date()), day.date())

    def test_the_sessions_on_either_side_survive(self):
        self.assertTrue(self.calendar.is_session(LAST_SESSION_BEFORE))
        self.assertTrue(self.calendar.is_session(FIRST_SESSION_AFTER))

    def test_the_closure_is_one_unbroken_gap(self):
        sessions = self.calendar.sessions_in_range(LAST_SESSION_BEFORE, FIRST_SESSION_AFTER)
        self.assertEqual([s.date() for s in sessions],
                         [LAST_SESSION_BEFORE, FIRST_SESSION_AFTER])

    def test_surrounding_years_are_untouched(self):
        """The correction is one span of 2022 and nothing else."""
        self.assertEqual(len(self.calendar.sessions_in_range("2021-01-01", "2021-12-31")), 255)
        self.assertEqual(len(self.calendar.sessions_in_range("2023-01-01", "2023-12-31")), 254)

    def test_the_packaged_calendar_invents_sixteen_sessions(self):
        """The size of the thing: sixteen sessions a run would otherwise demand prices for, and
        with forward fill on would be handed the 2022-02-25 close for, at zero volume."""
        from exchange_calendars.exchange_calendar_xmos import XMOSExchangeCalendar as Upstream

        upstream = Upstream(start="2022-01-01", end="2022-12-31")
        window = ("2022-02-26", "2022-03-23")
        self.assertEqual(len(upstream.sessions_in_range(*window)), 16)
        self.assertEqual(len(self.calendar.sessions_in_range(*window)), 0)

    def test_the_correction_reaches_calendars_asked_for_directly(self):
        """Bundle ingestion imports ``get_calendar`` from ``exchange_calendars`` itself. A bundle
        repaired against a different set of sessions than the run uses puts the gap back."""
        import exchange_calendars

        self.assertFalse(exchange_calendars.get_calendar("XMOS").is_session("2022-03-02"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
