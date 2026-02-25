import structlog
from ziplime.finance.execution import MarketOrder

logger = structlog.get_logger(__name__)

# Список символов должен совпадать с теми, что загружены на шаге 2
SYMBOLS = ["SBER@MISX", "LKOH@MISX", "GAZP@MISX"]


async def initialize(context):
    """Вызывается один раз при старте симуляции."""
    context.assets = [await context.symbol(s) for s in SYMBOLS]


async def handle_data(context, data):
    """Вызывается на каждом баре (каждый день при дневных данных).
    Стратегия: равновзвешенный портфель — каждой акции одинаковая доля.
    """
    target_percent = 1.0 / len(context.assets)
    for asset in context.assets:
        await context.order_target_percent(
            asset=asset,
            target=target_percent,
            style=MarketOrder(),
        )
