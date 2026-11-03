"""Buy SBER once with RUB; reporting in USD does not place FX orders."""

from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.sber = await context.symbol(symbol="SBER", mic="MISX")
    context.bought = False


async def handle_data(context, data):
    if not context.bought:
        # Leave some cash for commissions and the next session's execution price.
        await context.order_target_percent(
            asset=context.sber, target=0.9, style=MarketOrder(),
        )
        context.bought = True
