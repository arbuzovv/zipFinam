"""Hold what the order book supports, not what the tape happened to print.

`obstats` photographs the book every five minutes: the spread at the touch, how many price levels
are stacked on each side, and how much volume is resting there. A bar cannot carry any of it. Two
names can close at the same price on the same volume while one of them has a two-basis-point
spread against a thousand levels of depth and the other has twenty against a hundred.

Spreads here are in **basis points**, which is what makes them comparable across prices: SBER sits
near 1, a small cap near 15. `imbalance_vol` is `(bid - ask) / (bid + ask)` over the whole book,
so a positive number is more size wanting to buy than to sell.

These are snapshots, not flows, so the session aggregate averages them -- adding up the volume
resting in the book at 200 successive snapshots would report two hundred times the liquidity that
was ever there.
"""

import polars as pl

import zipfinam  # noqa: F401  (схема algopack://)

BOOK = "algopack://eq/obstats"
WINDOW = 5
TOP_N = 3
WIDEST_SPREAD_BP = 6.0


async def initialize(context):
    context.universe = [
        await context.symbol(ticker, mic="MISX")
        for ticker in ("SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "TATN", "MTSS")
    ]


async def handle_data(context, data):
    book = await data.history(assets=context.universe,
                              fields=["spread_bbo", "imbalance_vol", "levels_b"],
                              bar_count=WINDOW, data_source=BOOK)
    if book is None or not len(book):
        return

    state = book.group_by("sid").agg(
        pl.col("spread_bbo").mean().alias("spread"),
        pl.col("imbalance_vol").mean().alias("lean"),
        pl.col("levels_b").mean().alias("depth"))

    # Tradeable first, attractive second. A name whose book has widened is one where the exit
    # costs more than the entry did, whatever the signal says.
    tradeable = state.filter(pl.col("spread") <= WIDEST_SPREAD_BP, pl.col("lean") > 0)
    if not len(tradeable):
        await context.rebalance({})
        return

    leaders = tradeable.sort("lean", descending=True).head(TOP_N)
    picks = set(leaders["sid"].to_list())
    await context.rebalance({asset: 1.0 / len(picks)
                             for asset in context.universe if asset.sid in picks})
