"""Sessions the packaged exchange calendars get wrong, corrected in one place.

A trading calendar is not decoration in a backtester: it decides which days exist. A day the
calendar believes in but the exchange did not open is a day the engine demands a price for, and
with ``forward_fill_missing_ohlcv_data`` on -- the default -- it gets one: the last close,
repeated. Volume reads zero and nothing raises, so a strategy trades a frozen market, a stop is
"hit" at a price nobody quoted, and the volatility of the whole window is diluted by a stretch of
exactly zero returns.

Importing this module registers the corrections with ``exchange_calendars``, so every path that
asks for one of these calendars -- ziplime's :func:`ziplime.utils.calendar_utils.get_calendar`,
bundle ingestion, or ``exchange_calendars.get_calendar`` called directly -- gets the corrected
one. Registration is idempotent.
"""
import pandas as pd
from exchange_calendars import register_calendar_type
from exchange_calendars.exchange_calendar_xmos import (
    XMOSExchangeCalendar as UpstreamXMOSExchangeCalendar,
)

#: The Moscow Exchange securities market was shut from 2022-02-28 to 2022-03-23 inclusive.
#:
#: Trading stopped after the session of Friday 2022-02-25 and did not resume for almost a month:
#: the Bank of Russia suspended it a day at a time, and equities came back on Thursday
#: 2022-03-24 -- a shortened session in 33 index constituents -- with the rest of the list
#: following on 2022-03-28. ``exchange_calendars`` 4.13.1 has none of this: it knows only that
#: 2022-03-07 and 2022-03-08 were holidays, and counts the other sixteen weekdays in the span as
#: ordinary sessions.
#:
#: Two things this deliberately does not model, because they are narrower than a closed day and
#: the data says them better than the calendar can. OFZ trading resumed a few days before
#: equities, on 2022-03-21, so a bond run over this window loses three real sessions here. And
#: 2022-03-24 and 2022-03-25 were partial sessions, roughly 09:50-14:00 MSK, which this treats as
#: full ones.
MOEX_2022_CLOSURE = tuple(pd.date_range("2022-02-28", "2022-03-23", freq="D"))


class XMOSExchangeCalendar(UpstreamXMOSExchangeCalendar):
    """Moscow Exchange, with the 2022 closure that the packaged calendar is missing."""

    @property
    def adhoc_holidays(self):
        return list(super().adhoc_holidays) + list(MOEX_2022_CLOSURE)


def register() -> None:
    """Point ``exchange_calendars`` at the corrected calendars. Safe to call repeatedly."""
    register_calendar_type("XMOS", XMOSExchangeCalendar, force=True)


register()
