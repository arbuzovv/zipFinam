import datetime
import logging

import numpy as np
import structlog
from pydantic import BaseModel

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)


class EquityToTrade(BaseModel):
    symbol: str
    target_percentage: float


class AlgorithmConfig(BaseAlgorithmConfig):
    currency: str
    equities_to_trade: list[EquityToTrade]


async def initialize(context):
    context.stocks = [
        await context.symbol("SBER@MISX"),
        # await context.symbol("SBER@RUSX"),
        # await context.symbol("UGLD@MISX"),
        # await context.symbol("UKUZ@MISX")
    ]
    # read config file
    logger.info("Algorithm config: ", config=context.algorithm.config)


async def handle_data(context, data):
    num_assets = len(context.stocks)
    target_percent = 1.0 / num_assets
    # await context.order_target_percent(asset=context.stocks[0], exchange_name="grpc_exchange",
    #                                    style=MarketOrder(), target=0.5)
    for asset in context.stocks:
        await context.order_target_percent(asset=asset,
                                           target=target_percent, style=MarketOrder())
