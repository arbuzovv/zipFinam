"""Moscow's venues for ziplime's per-venue calendars, and the FORTS calendar they need.

ziplime judges each listing by its own venue's hours and clocks a run that spans venues on a
calendar wide enough for all of them (:mod:`ziplime.utils.venue_calendars`, ziplime 2.11). It ships
the venues `exchange_calendars` already describes and leaves the rest to extensions. This module is
that extension for the Moscow Exchange:

* ``MISX`` -- equities, on XMOS, the equity session `exchange_calendars` ships (10:00-18:45);
* ``RTSX`` -- derivatives and indices, on :class:`FORTSExchangeCalendar` (09:00-23:50), which
  `exchange_calendars` does not ship.

ziplime finds it through the ``ziplime.venue_calendars`` entry point, so installing zipfinam is
enough; importing it registers the same venues explicitly, for a source checkout with no installed
metadata.

The lookups -- :func:`calendar_for_mic`, :func:`clock_calendar_name` and the rest -- are ziplime's,
re-exported here for code written against zipfinam 1.26.10, which carried its own copy.
"""

import datetime
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
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


class FORTSExchangeCalendar(XMOSExchangeCalendar):
    """Moscow Exchange's derivatives market, which `exchange_calendars` does not carry.

    FORTS trades 09:00-23:50 Moscow, against the 10:00-18:45 of XMOS -- the equity session that
    `exchange_calendars` does ship. Clocking a futures run on XMOS therefore hides the evening
    session, which for a cross-market spread is the half that matters: 19:00-23:50 Moscow is
    where the overlap with the American session lives.

    Holidays, weekmasks and the rest are XMOS's, because the derivatives market observes the same
    Russian calendar. Only the hours differ, so only the hours are restated.

    The 18:50-19:05 clearing break is modelled; the five-minute 14:00-14:05 one is not, because
    `exchange_calendars` carries a single break per session and the evening one is the break that
    separates two sessions traders treat as separate.
    """

    name = "FORTS"

    tz = ZoneInfo("Europe/Moscow")

    open_times = ((None, datetime.time(9)),)
    break_start_times = ((None, datetime.time(18, 50)),)
    break_end_times = ((None, datetime.time(19, 5)),)
    close_times = ((None, datetime.time(23, 50)),)


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
