"""A short vertical call spread on a monthly SBER series, held to expiry.

The simplest structure that shows what separates a margined option from a premium-paid one.

**No premium arrives at the trade.** On OPRA, selling a spread is a credit received, and the
position is sized off it. Here nothing moves at entry: the cash flow is zero, and the result
arrives as variation margin every session, exactly as it would on a future. So the sizing here is
off the **maximum loss and the initial margin** rather than off a credit -- there is simply
nothing to count as "premium collected". The margin consumed is written to ``margin_log`` every
session.

**The risk is still defined.** The long wing caps the loss exactly as it does on OPRA: the
difference between the strikes, less whatever is left of the premium. `OptionStrategy.max_loss`
computes that without reference to the venue, because it is a property of the payoff rather than
of the settlement.

**The expiries are monthly -- the third Wednesday.** A series lives about twenty sessions, so the
"zero day" comes once a month rather than every session as it does for SPY. The strategy enters
after the previous expiry and holds to the next. Weekly series exist too, but their codes carry a
week suffix whose rule does not follow from the data -- see the directory's README.

The option prices are synthetic -- see the directory's README.
"""
import datetime

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options import strategies as option_strategies

STRATEGY_INFO = {
    "description": "A short monthly SBER call spread, sized off risk and margin",
    "sessions_back": 120,
}

#: How many strikes from the money the short and long legs sit.
SHORT_STEPS = 1
LONG_STEPS = 3

#: The share of the account allowed to be at risk in one cycle. A margined option carries no
#: premium to size against, so the figure comes from the known maximum loss and from the initial
#: margin -- whichever is the smaller.
RISK_BUDGET = 0.02


async def initialize(context):
    context.underlying = await context.symbol("SBER", mic="MISX")
    context.open_legs = []
    context.expiry = None
    context.margin_log = []
    context.opened = []


async def handle_data(context, data):
    session = context.get_datetime().date()
    context.margin_log.append(
        {"session": session, "margin": context._ledger.futures_margin_requirement()})

    if context.open_legs:
        if session >= context.expiry:
            # The position leaves on its own: the engine closes the contract at intrinsic value
            # on expiry. All that happens here is forgetting the reference to it.
            context.open_legs = []
            context.expiry = None
        return

    chain = await _todays_chain(context)
    if chain is None:
        return

    spot_frame = await data.current(assets=[context.underlying], fields=["close"])
    if spot_frame.is_empty():
        return
    spot = float(spot_frame["close"][-1])

    short_leg = chain.strike_offset(spot, OptionType.CALL, SHORT_STEPS)
    long_leg = chain.strike_offset(spot, OptionType.CALL, LONG_STEPS)
    if short_leg.sid == long_leg.sid:
        return
    spread = option_strategies.vertical_spread(long_listing=long_leg, short_listing=short_leg)

    prices = {}
    for leg in spread.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return
        prices[leg.listing.sid] = float(quote["close"][-1])

    net = spread.net_premium(prices)
    if net >= 0:
        # A spread that costs money rather than paying it is not this trade. Nor is zero: the far
        # wing is then worthless, and the structure degenerates into a naked short for no credit.
        return
    worst = spread.max_loss(net)
    if not worst:
        return

    budget = context.portfolio.portfolio_value * RISK_BUDGET
    by_risk = int(budget // abs(worst))
    # And the same again against margin. The rate is the one the harness's margin model uses,
    # applied to the short leg's notional: that is the leg the exchange collects against.
    margin_per_lot = abs(prices[short_leg.sid]) * short_leg.asset.multiplier * 0.15
    by_margin = int(budget // margin_per_lot) if margin_per_lot else by_risk
    quantity = min(by_risk, by_margin)
    if quantity < 1:
        return

    for listing, amount in spread.orders(quantity):
        await context.order(asset=listing, amount=amount, style=MarketOrder())

    context.open_legs = spread.orders(quantity)
    context.expiry = short_leg.asset.expiration_date
    context.opened.append({
        "session": session, "spot": spot, "structure": spread.name, "quantity": quantity,
        "net_premium": net, "max_loss": worst * quantity,
        "expiry": context.expiry,
    })


async def _todays_chain(context):
    """The nearest series that has not expired, or ``None`` when there is none.

    The series are monthly, so on most sessions no chain expiring "today" exists -- unlike SPY,
    where one does every day. A missing chain is the ordinary case here rather than a failure,
    which is why the ``ValueError`` from ``option_chain`` is swallowed.
    """
    session = context.get_datetime().date()
    for ahead in range(0, 45):
        candidate = session + datetime.timedelta(days=ahead)
        try:
            chain = await context.option_chain(context.underlying, expiration_date=candidate)
        except ValueError:
            continue
        # A series expiring today has no time left in it: opening one is pointless.
        if candidate > session:
            return chain
    return None
