"""Tell demand from decoration: buy where the bids placed are the bids that stayed.

`orderstats` counts what was put into the book and what was pulled back out of it, by side. Most
of what is placed on a Moscow session is cancelled within it -- that is normal market making, not
a signal. What is worth reading is the *difference* between the sides: a name where buyers place
size and leave it while sellers place size and withdraw it is one where the resting demand is
real.

`put_vol_b - cancel_vol_b` is the buy volume that survived the session, and the same on the sell
side. Both are flows over the bucket, so the session aggregate sums them -- which is the whole
reason this table cannot be collapsed with the same rule as `obstats`, where the identically
named columns are snapshots of the book and average instead.

Scaled by what was placed, so a large name and a small one are comparable.
"""

import polars as pl

import zipfinam  # noqa: F401  (схема algopack://)

ORDERS = "algopack://eq/orderstats"
WINDOW = 5
TOP_N = 3


async def initialize(context):
    context.universe = [
        await context.symbol(ticker, mic="MISX")
        for ticker in ("SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "TATN", "MTSS")
    ]


async def handle_data(context, data):
    rows = await data.history(
        assets=context.universe,
        fields=["put_vol_b", "put_vol_s", "cancel_vol_b", "cancel_vol_s"],
        bar_count=WINDOW, data_source=ORDERS)
    if rows is None or not len(rows):
        return

    survived = rows.group_by("sid").agg(
        (pl.col("put_vol_b") - pl.col("cancel_vol_b").abs()).sum().alias("bid"),
        (pl.col("put_vol_s") - pl.col("cancel_vol_s").abs()).sum().alias("ask"),
        pl.col("put_vol_b").sum().alias("placed_b"),
        pl.col("put_vol_s").sum().alias("placed_s"))

    conviction = (
        survived.filter((pl.col("placed_b") > 0) & (pl.col("placed_s") > 0))
        .with_columns((pl.col("bid") / pl.col("placed_b")
                       - pl.col("ask") / pl.col("placed_s")).alias("edge"))
        .drop_nulls("edge")
        .sort("edge", descending=True)
        .head(TOP_N))

    picks = {sid for sid, edge in conviction.select("sid", "edge").rows() if edge > 0}
    if not picks:
        await context.rebalance({})
        return

    await context.rebalance({asset: 1.0 / len(picks)
                             for asset in context.universe if asset.sid in picks})
