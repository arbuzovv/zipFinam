"""A short SBER strangle whose exit is margin usage rather than loss.

This shows something a premium-paid market simply does not have.

**What forces a margined position out is the collateral, not the loss.** An option bought on OPRA
has its risk capped at the premium paid, and once that is paid the broker has nothing further to
demand. A short margined strangle has paid nothing: the exchange holds initial margin on **both**
sides and recomputes it at the current price. The market moves, the margin grows, and what closes
the position is not a stop-loss decision but a shortage of free funds. So the strategy watches
``futures_margin_requirement`` rather than P&L, and cuts back when account usage crosses a
threshold.

**Variation margin arrives every session.** The cash flow at entry is zero; everything that
happens afterwards reaches cash daily. ``equity_log`` shows the portfolio value moving while the
position does not -- that is the margin, not a revaluation of an asset: a margined option has no
position value at all.

The option prices are synthetic -- see the directory's README.
"""
import datetime

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options import strategies as option_strategies

STRATEGY_INFO = {
    "description": "A short SBER strangle, cut back on margin usage",
    "sessions_back": 120,
}

#: How many strikes from the money the wings are sold.
WING_STEPS = 2

#: Target margin usage of the account at entry. The position is built up to exactly this: a
#: margined option carries no premium received to size against.
TARGET_MARGIN = 0.15

#: The usage above which the position is halved. This is the exit mechanism: not a price stop but
#: a shortage of free funds. The threshold sits deliberately close to the target, which is how it
#: works in life: a volatility seller builds the position near the limit, and an ordinary move
#: against it is enough to grow the margin and force part of the position back.
MARGIN_CEILING = 0.25


async def initialize(context):
    context.underlying = await context.symbol("SBER", mic="MISX")
    context.legs = []
    context.expiry = None
    context.margin_log = []
    context.equity_log = []
    context.reductions = []


async def handle_data(context, data):
    session = context.get_datetime().date()
    margin = context._ledger.futures_margin_requirement()
    equity = context.portfolio.portfolio_value
    context.margin_log.append({"session": session, "margin": margin})
    context.equity_log.append({"session": session, "equity": equity,
                               "cash": context.portfolio.cash})

    if context.legs:
        if session >= context.expiry:
            context.legs = []
            context.expiry = None
            return
        # The only exit there is here: margin usage.
        if equity and margin / equity > MARGIN_CEILING:
            await _halve(context, session, margin, equity)
        return

    await _open(context, data, session)


async def _open(context, data, session):
    chain = await _next_chain(context)
    if chain is None:
        return

    spot_frame = await data.current(assets=[context.underlying], fields=["close"])
    if spot_frame.is_empty():
        return
    spot = float(spot_frame["close"][-1])

    call = chain.strike_offset(spot, OptionType.CALL, WING_STEPS)
    put = chain.strike_offset(spot, OptionType.PUT, WING_STEPS)
    strangle = option_strategies.strangle(call, put, ratio=-1.0)

    prices = {}
    for leg in strangle.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return
        prices[leg.listing.sid] = float(quote["close"][-1])
    if min(prices.values()) <= 0:
        return  # the wing is worthless: there is nothing to sell

    # A short strangle's loss is unbounded, so "maximum loss" is no use for sizing. The figure
    # comes from the notional instead: how much margin one structure will tie up.
    notional = sum(prices[leg.listing.sid] * leg.multiplier for leg in strangle.legs)
    margin_per_lot = notional * 0.15
    if margin_per_lot <= 0:
        return
    quantity = int(context.portfolio.portfolio_value * TARGET_MARGIN // margin_per_lot)
    if quantity < 1:
        return

    for listing, amount in strangle.orders(quantity):
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.legs = strangle.orders(quantity)
    context.expiry = call.asset.expiration_date


async def _halve(context, session, margin, equity):
    """Close half of each leg. Not a price stop -- a response to the collateral growing."""
    remaining = []
    for listing, amount in context.legs:
        close_amount = -int(amount / 2)
        if close_amount:
            await context.order(asset=listing, amount=close_amount, style=MarketOrder())
        left = amount + close_amount
        if left:
            remaining.append((listing, left))
    context.reductions.append({"session": session, "margin": margin,
                               "utilisation": margin / equity if equity else 0.0})
    context.legs = remaining


async def _next_chain(context):
    session = context.get_datetime().date()
    for ahead in range(0, 45):
        candidate = session + datetime.timedelta(days=ahead)
        try:
            chain = await context.option_chain(context.underlying, expiration_date=candidate)
        except ValueError:
            continue
        if candidate > session:
            return chain
    return None
