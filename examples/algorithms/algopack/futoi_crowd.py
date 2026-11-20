"""Take the other side of the retail crowd in the dollar-rouble future.

MOEX publishes open interest split by who holds it: `fiz` are private individuals and `yur` are
legal entities. The two are mirror images -- every long has a short -- so the interesting number
is not the level but how one-sided the retail book has become. When private clients are as long
as they get, the institutions on the other side of them are as short as they get, and the move
that follows has historically been the one that hurts the crowd.

`algopack://fo/futoi` is keyed on the futures **root**, not on a contract: the position is `Si`'s,
and every listed delivery on `Si` carries it. That is why this reads the open interest with the
front contract it is about to trade -- the number belongs to both.

Two things the hosted runtime does not model for futures yet: margin is not charged, and the
contract's own commission is not applied.
"""

import polars as pl
from ziplime.finance.execution import MarketOrder

import zipfinam  # noqa: F401  (схема algopack://)

OPEN_INTEREST = "algopack://fo/futoi"
WINDOW = 20
CONTRACTS = 2
STRETCHED = 0.8


async def initialize(context):
    return


async def handle_data(context, data):
    front = await context.front_contract("Si", roll_before_days=7)
    if front is None:
        return

    rows = await data.history(assets=[front], fields=["pos_long_fiz", "pos_short_fiz"],
                              bar_count=WINDOW, data_source=OPEN_INTEREST)
    if rows is None or len(rows) < WINDOW:
        return

    lean = rows.select(
        ((pl.col("pos_long_fiz") - pl.col("pos_short_fiz").abs())
         / (pl.col("pos_long_fiz") + pl.col("pos_short_fiz").abs())).alias("lean")
    ).drop_nulls()
    if not len(lean):
        return

    today = lean["lean"][-1]
    stretch = lean["lean"].rank() / len(lean)
    percentile = stretch[-1]

    # Retail as long as it has been all window: stand short against it. As short as it has been:
    # stand long. Anything in between is not a crowd, so hold nothing.
    if percentile >= STRETCHED and today > 0:
        target = -CONTRACTS
    elif percentile <= 1 - STRETCHED and today < 0:
        target = CONTRACTS
    else:
        target = 0

    held = await context.portfolio.get_asset_positions_amount(front)
    if held == target:
        return
    await context.rebalance({})
    if target:
        await context.order(asset=front, amount=target, style=MarketOrder())
