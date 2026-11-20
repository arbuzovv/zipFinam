"""Купи и держи: равные доли в нескольких акциях Мосбиржи."""
import structlog
from ziplime.finance.execution import MarketOrder

logger = structlog.get_logger(__name__)

SYMBOLS = ["SBER@MISX", "GAZP@MISX", "LKOH@MISX"]


async def initialize(context):
    # Не context.assets: это имя занято движком.
    context.stocks = [await context.symbol(symbol) for symbol in SYMBOLS]
    context.invested = False


async def handle_data(context, data):
    if context.invested:
        return
    for stock in context.stocks:
        await context.order_target_percent(asset=stock, target=1.0 / len(context.stocks),
                                           style=MarketOrder())
    context.invested = True
