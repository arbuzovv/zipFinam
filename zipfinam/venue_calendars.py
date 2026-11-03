"""Moscow's venues for ziplime's per-venue calendars, and the FORTS calendar they need.

ziplime judges each listing by its own venue's hours and clocks a run that spans venues on a
calendar wide enough for all of them (:mod:`ziplime.utils.venue_calendars`, ziplime 2.11). It ships
the venues `exchange_calendars` already describes and leaves the rest to extensions. This module is
that extension for the Moscow Exchange:

* ``MISX`` -- equities, on XMOS, the equity session `exchange_calendars` ships (10:00-18:45);
* ``RTSX`` -- derivatives and indices, on :class:`FORTSExchangeCalendar`, which `exchange_calendars`
  does not ship: 07:00-23:50 today, and weekends since 2025-08-16.

ziplime finds it through the ``ziplime.venue_calendars`` entry point, so installing zipfinam is
enough; importing it registers the same venues explicitly, for a source checkout with no installed
metadata.

The lookups -- :func:`calendar_for_mic`, :func:`clock_calendar_name` and the rest -- are ziplime's,
re-exported here for code written against zipfinam 1.26.10, which carried its own copy.
"""

import datetime
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
from exchange_calendars.exchange_calendar import HolidayCalendar
from exchange_calendars.exchange_calendar_xmos import XMOSExchangeCalendar
from ziplime.utils.venue_calendars import (  # noqa: F401 -- re-exported
    CONTINUOUS_CLOCK,
    MIXED_VENUE_CLOCK,
    VENUE_CALENDARS,
    VENUE_CALENDARS_ENV,
    calendar_for_mic,
    calendar_name_for_mic,
    clock_calendar_name,
    covers,
    register_venue_calendars,
    trades_every_day,
)


_D = pd.Timestamp

#: The first day of the week from which the derivatives market trades on Saturdays and Sundays.
#: Measured: the first weekend with trades in three or more chains is 2025-08-16.
WEEKEND_SESSIONS_FROM = datetime.date(2025, 8, 16)
#: How far ahead weekends are taken to be sessions. Revisit before it arrives.
WEEKEND_SESSIONS_UNTIL = datetime.date(2030, 12, 31)

#: Days the Russian calendar takes off on which the derivatives market traded anyway. Each traded
#: every chain at the ordinary weekday hours of its year unless `HOLIDAY_SESSION_HOURS` says
#: otherwise.
TRADED_HOLIDAYS = frozenset(_D(day) for day in (
    "2014-06-13", "2022-01-07", "2022-02-23", "2025-02-24", "2025-03-10",
    "2026-02-23", "2026-03-09", "2026-05-01", "2026-05-11", "2026-06-12",
))

#: Holiday sessions kept at other hours: (open, close), None meaning the ordinary time.
HOLIDAY_SESSION_HOURS = {
    _D("2026-02-23"): (datetime.time(9, 59), datetime.time(19)),
    _D("2026-05-01"): (datetime.time(10), None),
    _D("2026-06-12"): (datetime.time(9, 59), None),
}

#: Sessions of the equity calendar on which the derivatives market did not trade, and the weekends
#: since `WEEKEND_SESSIONS_FROM` with no trade in any chain (about one a month, plus the New Year
#: and Victory Day holidays). Weekends after the last of these are taken to trade.
CLOSED_SESSIONS = frozenset(_D(day) for day in (
    "2022-02-28", "2022-03-01",
    "2025-09-20", "2025-09-21", "2025-10-25", "2025-10-26", "2025-11-02", "2025-11-22",
    "2025-11-23", "2025-12-06", "2025-12-07", "2026-01-03", "2026-01-04", "2026-01-10",
    "2026-01-11", "2026-02-14", "2026-02-15", "2026-03-07", "2026-03-08", "2026-03-21",
    "2026-03-22", "2026-05-09", "2026-05-10", "2026-06-20", "2026-06-21", "2026-08-01",
    "2026-08-02", "2026-08-15", "2026-08-16", "2026-09-12", "2026-09-13", "2026-10-24",
    "2026-10-25",
))

#: Saturdays worked as weekdays, at weekday hours, that the equity calendar does not know.
WORKING_SATURDAYS = frozenset({_D("2025-11-01")})

#: One-off early closes.
EARLY_CLOSES = {_D("2026-03-20"): datetime.time(18, 50)}


def _weekend_hours(day: pd.Timestamp) -> tuple[datetime.time, datetime.time]:
    """A weekend session's hours: 10:00-18:50 at first, 09:59 (the auction) to 19:00 since."""
    opens = datetime.time(10) if day < _D("2025-12-20") else datetime.time(9, 59)
    closes = datetime.time(18, 50) if day < _D("2025-12-20") else datetime.time(19)
    return opens, closes


class _HolidaysExcept(HolidayCalendar):
    """A holiday calendar less some dates: the Russian holidays FORTS traded through."""

    def __init__(self, rules, kept_open):
        super().__init__(rules=rules)
        self._kept_open = pd.DatetimeIndex(sorted(kept_open))

    def holidays(self, start=None, end=None, return_name=False):
        found = super().holidays(start=start, end=end, return_name=return_name)
        index = found.index if return_name else found
        return found[~index.isin(self._kept_open)]


class FORTSExchangeCalendar(XMOSExchangeCalendar):
    """Moscow Exchange's derivatives market, which `exchange_calendars` does not carry.

    Its hours are not the equity session's (XMOS, 10:00-18:45), and they have changed more often.
    Measured from the minute bars of Si, RI and BR, union across them:

    =============  ===========  ======================  ========================================
    from           open         close                   evening clearing break
    =============  ===========  ======================  ========================================
    (history)      10:30        23:50                   18:45-19:00
    2010-05-17     10:00
    2021-03-01     07:00
    2022-02-25     10:00
    2022-03-02                  18:45, then 18:50       none: there was no evening session
                                from 2022-05-31
    2022-07-12                  23:50                   18:50-19:05
    2022-09-12     09:00
    2024-06-13     10:00
    2025-01-27     09:00
    2025-07-02     08:59 (the opening auction's minute)
    2026-03-23                                          none
    2026-07-14     07:00
    =============  ===========  ======================  ========================================

    **Weekends trade since 2025-08-16**: 10:00-18:50 at first, 09:59-19:00 from 2025-12-20.
    Chains joined at different times -- RI by December 2025, BR in June 2026, Si in July 2026 -- and
    the calendar is the venue's, so it opens for the first of them.

    Holidays are XMOS's, which is the Russian calendar, less the days the derivatives market
    traded through them (`TRADED_HOLIDAYS`) and plus the sessions it did not trade
    (`CLOSED_SESSIONS`). The five-minute 14:00 clearing break is not modelled, because
    `exchange_calendars` carries one break per session; a weekend or holiday session at its own
    hours has none.
    """

    name = "FORTS"

    tz = ZoneInfo("Europe/Moscow")

    open_times = (
        (None, datetime.time(10, 30)),
        (_D("2010-05-17"), datetime.time(10)),
        (_D("2021-03-01"), datetime.time(7)),
        (_D("2022-02-25"), datetime.time(10)),
        (_D("2022-09-12"), datetime.time(9)),
        (_D("2024-06-13"), datetime.time(10)),
        (_D("2025-01-27"), datetime.time(9)),
        (_D("2025-07-02"), datetime.time(8, 59)),
        (_D("2026-07-14"), datetime.time(7)),
    )
    close_times = (
        (None, datetime.time(23, 50)),
        (_D("2022-03-02"), datetime.time(18, 45)),
        (_D("2022-05-31"), datetime.time(18, 50)),
        (_D("2022-07-12"), datetime.time(23, 50)),
    )
    break_start_times = (
        (None, datetime.time(18, 45)),
        (_D("2022-03-02"), None),
        (_D("2022-07-12"), datetime.time(18, 50)),
        (_D("2026-03-23"), None),
    )
    break_end_times = (
        (None, datetime.time(19)),
        (_D("2022-03-02"), None),
        (_D("2022-07-12"), datetime.time(19, 5)),
        (_D("2026-03-23"), None),
    )

    @property
    def special_weekmasks(self):
        # From the Monday of that week: a week mask applies to whole weeks.
        monday = WEEKEND_SESSIONS_FROM - datetime.timedelta(days=WEEKEND_SESSIONS_FROM.weekday())
        weekends = (_D(monday), _D(WEEKEND_SESSIONS_UNTIL), "1111111")
        return [*super().special_weekmasks, weekends]

    @property
    def regular_holidays(self):
        return _HolidaysExcept(super().regular_holidays.rules, TRADED_HOLIDAYS)

    @property
    def adhoc_holidays(self):
        kept = [day for day in super().adhoc_holidays if pd.Timestamp(day) not in TRADED_HOLIDAYS]
        return kept + sorted(CLOSED_SESSIONS)

    def _weekend_sessions(self) -> list[pd.Timestamp]:
        """Saturdays and Sundays that are sessions, which is what special hours may name."""
        days = pd.date_range(WEEKEND_SESSIONS_FROM, WEEKEND_SESSIONS_UNTIL, freq="D")
        days = days[days.dayofweek >= 5]
        holidays = set(self.regular_holidays.holidays(days[0], days[-1])) | set(self.adhoc_holidays)
        return [day for day in days if day not in holidays and day not in WORKING_SATURDAYS]

    @property
    def special_opens_adhoc(self):
        by_time: dict[datetime.time, list[pd.Timestamp]] = {}
        for day in self._weekend_sessions():
            by_time.setdefault(_weekend_hours(day)[0], []).append(day)
        for day, (opens, _) in HOLIDAY_SESSION_HOURS.items():
            if opens is not None:
                by_time.setdefault(opens, []).append(day)
        return [(time, pd.DatetimeIndex(days)) for time, days in by_time.items()]

    @property
    def special_closes_adhoc(self):
        by_time: dict[datetime.time, list[pd.Timestamp]] = {}
        for day in self._weekend_sessions():
            by_time.setdefault(_weekend_hours(day)[1], []).append(day)
        for day, (_, closes) in HOLIDAY_SESSION_HOURS.items():
            if closes is not None:
                by_time.setdefault(closes, []).append(day)
        for day, closes in EARLY_CLOSES.items():
            by_time.setdefault(closes, []).append(day)
        return [(time, pd.DatetimeIndex(days)) for time, days in by_time.items()]


def _register_calendar() -> None:
    """Make ``FORTS`` resolvable by name, everywhere.

    Every calendar in ziplime is looked up by string through
    :func:`ziplime.utils.calendar_utils.get_calendar`, so registering the type is all it takes for
    a bundle build, a backtest and a live run to agree on what `FORTS` means.
    """
    try:
        xcals.register_calendar_type(FORTSExchangeCalendar.name, FORTSExchangeCalendar)
    except ValueError:
        # Already registered: this module is imported from several entry points.
        pass


_register_calendar()

#: The Moscow venues, by MIC. Keyed on MIC and nothing else: ``exchanges.country_code`` is ``US``
#: for every row of the asset databases this was built against, MISX and RTSX included, so reading
#: the region off an exchange would put Moscow on the American session.
MOSCOW_VENUES: dict[str, str] = {
    "MISX": "XMOS",
    "RTSX": FORTSExchangeCalendar.name,
}


def moscow_venues() -> dict[str, str]:
    """What the ``ziplime.venue_calendars`` entry point hands ziplime.

    Loading this module is what registers the FORTS calendar, so by the time ziplime reads the
    mapping the name it maps RTSX to resolves.
    """
    return dict(MOSCOW_VENUES)


register_venue_calendars(MOSCOW_VENUES)
