"""MOEX option conventions, registered with ziplime at import time.

An option is five conventions, not one instrument, and a venue fixes all five at once: how the
premium settles, whether it can be exercised early, what it settles into, how many units a
contract covers, and what the contract is called. ziplime ships ``OPRA`` and a
:func:`~ziplime.data.data_sources.options.venues.register_venue` hook; this module contributes the
Moscow Exchange row, because a market's conventions belong with the connector that speaks to that
market rather than in the library.

The values were **measured** against the live Limex reference feed rather than looked up. Against
``OPRA`` they disagree on four of the five:

=====================  ==========================  ==========================
                       ``OPRA`` (SPY)              ``MOEX`` (SBER)
=====================  ==========================  ==========================
premium                UPFRONT -- paid at trade    MARGINED -- variation margin
exercise               AMERICAN                    EUROPEAN
settlement             PHYSICAL                    CASH
contract covers        100 shares                  1 futures contract
named                  ``SPY   260914C00765000``   ``SR310CI6``
listed on              ``OPRA``                    ``RTSX``
=====================  ==========================  ==========================

Importing this module registers the venue. After that, ``get_venue("MOEX")`` answers, and any
ziplime option source can be pointed at it by name.
"""
import datetime
import re

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.options.venues import OptionVenue, register_venue

#: MOEX option code: root, strike, a settlement letter, a letter encoding month *and* side, and
#: the last digit of the year. ``SR310CI6`` is the September 310 call on the Sber future;
#: ``SR280CU6`` the September 280 put.
#:
#: The fourth position was ``C`` in every contract the Limex feed returned, on every root. What it
#: denotes is not documented here -- MOEX uses that slot for the settlement kind -- so it is
#: captured and carried through rather than interpreted, and written back as observed.
_MOEX_CODE = re.compile(
    r"^(?P<root>[A-Z]{2})(?P<strike>\d+)(?P<settlement>[A-Z])(?P<kind>[A-X])(?P<year>\d)"
    r"(?P<week>[A-Z])?$")

#: The settlement letter every observed code carried. Used when writing a code, since the feed is
#: the only evidence available for what belongs there.
MOEX_SETTLEMENT_LETTER = "C"

#: A-L are calls January to December, M-X are puts January to December. One letter carries both
#: facts, which is why a MOEX code cannot be read with the OCC parser and vice versa.
_MOEX_MONTH_LETTERS = "ABCDEFGHIJKL"


def is_monthly_expiry(date: datetime.date) -> bool:
    """Whether ``date`` is a third Wednesday -- the monthly series MOEX names without a suffix."""
    return date.weekday() == 2 and 15 <= date.day <= 21


def format_moex_symbol(root: str, strike: float, option_type: OptionType,
                       expiration_date: datetime.date, week_suffix: str | None = None) -> str:
    """Build a MOEX option code, e.g. ``SR310CI6``.

    **Only monthly series can be named from their date alone**, and this refuses anything else
    rather than guessing. The code carries the month but not the day, so the three September 2026
    series would otherwise collapse onto one symbol -- which is not a cosmetic clash: the listings
    deduplicate by name, two of the three chains vanish, and the strategy trades a contract that
    expires on a different day than it thinks.

    What the feed actually returns, measured on SBER::

        2026-09-16  SR300CI6     monthly, third Wednesday, no suffix
        2026-09-23  SR300CI6D    weekly, September month letter + a week letter
        2026-09-30  SR300CJ6A    weekly, but already October's month letter

    The rule relating a weekly's date to its month letter and week letter is not derivable from
    those three, so it is not invented here. A real feed supplies the native symbol directly --
    ``ContractSpec.symbol`` exists for that -- and a caller who knows the suffix can pass it.

    Args:
        week_suffix: The week letter for a weekly series. Required for any expiry that is not a
            third Wednesday.

    Raises:
        ValueError: for a fractional strike, or for a non-monthly expiry with no ``week_suffix``.
    """
    if strike != int(strike):
        raise ValueError(
            f"MOEX option codes carry an integer strike and {strike} is not one, so it cannot be "
            f"named without landing on a different contract.")
    if week_suffix is None and not is_monthly_expiry(expiration_date):
        raise ValueError(
            f"{expiration_date} is not a third Wednesday, so it is a weekly series, and a weekly "
            f"code needs a week letter that its date does not determine -- 2026-09-23 is "
            f"'SR300CI6D' but 2026-09-30 is 'SR300CJ6A'. Without one the code would collide with "
            f"the month's other series and the listings would silently deduplicate onto one "
            f"contract. Pass week_suffix=..., or take the symbol from the feed.")
    offset = 0 if option_type is OptionType.CALL else 12
    letter = chr(ord(_MOEX_MONTH_LETTERS[expiration_date.month - 1]) + offset)
    return (f"{root.upper()}{int(strike)}{MOEX_SETTLEMENT_LETTER}{letter}"
            f"{expiration_date.year % 10}{week_suffix or ''}")


#: MOEX derivatives-market options, as FORTS lists them. Measured on SBER.
#:
#: Margined and European. Expirations are monthly on the third Wednesday, with weeklies between
#: them, so "0DTE" here means the one session on which a series expires -- not every session, the
#: way SPY does it.
#:
#: ``contract_size`` is the one value here that is **not** measured. The feed reports
#: ``multiplier = 0`` and ``contract_size = 1`` for every MOEX option, which are placeholder
#: values rather than the instrument: a SBER put quoted at 16.59 against a 283 underlying is
#: quoted per share, and the contract covers a SBRF future, which is a hundred shares. Taking the
#: feed literally makes one contract worth about three roubles, and a strategy sizing on risk then
#: asks for twelve thousand lots of an instrument that trades thirty a day. Confirm it against the
#: contract specification for any root other than SBER.
MOEX = OptionVenue(
    name="MOEX",
    mic="RTSX",
    premium_style=PremiumStyle.MARGINED,
    exercise_style=ExerciseStyle.EUROPEAN,
    settlement_type=SettlementType.CASH,
    contract_size=100.0,
    tick_size=0.01,
    symbol_format="moex",
    symbol_formatter=format_moex_symbol,
)


def parse_moex_symbol(symbol: str, century: int = 2020) -> tuple[str, float, OptionType, int]:
    """Read a MOEX option code back: ``(root, strike, type, year)``.

    The month is in the same letter as the side, and the year is one digit, so the expiration
    **day** is not recoverable from the code -- only the month. That is a property of the scheme,
    not of this function: MOEX weekly series in the same month share a month letter and are told
    apart by the week number that some codes carry and others do not.
    """
    match = _MOEX_CODE.match(symbol.strip().upper())
    if match is None:
        raise ValueError(
            f"{symbol!r} is not a MOEX option code. Expected root + strike + a settlement letter "
            f"+ a month/side letter + a year digit, for example SR310CI6.")
    letter = match["kind"]
    index = ord(letter) - ord("A")
    option_type = OptionType.CALL if index < 12 else OptionType.PUT
    return (match["root"], float(match["strike"]), option_type,
            century + int(match["year"]))


register_venue(MOEX)
