"""Hold the Moscow blue chips the aggressive money is buying.

AlgoPack computes its statistics on the trade stream itself, so it knows something a bar does
not: which side crossed the spread. `disb` is that split, `(bought - sold) / (bought + sold)` in
lots, and over a week it separates a rally the buyers had to pay for from one that happened
because nobody was selling.

The tilt here is turnover-weighted rather than averaged across sessions. A quiet day and the day
of a placement both produce one `disb`, and treating them as equal votes lets a session that
traded nothing outvote one that traded the float.

`algopack://eq/tradestats` is the session view: the exchange publishes five-minute buckets and
the mount collapses each Moscow trading day into one row -- volumes summed, VWAPs weighted, the
imbalance recomputed from the day's own totals. A day's row becomes readable on the next day,
because its volume is not known until it is over.
"""

import polars as pl

import zipfinam  # noqa: F401  (схема algopack://)

FLOW = "algopack://eq/tradestats"
WINDOW = 5
TOP_N = 3


async def initialize(context):
    context.universe = [
        await context.symbol(ticker, mic="MISX")
        for ticker in ("SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "TATN", "MTSS")
    ]


async def handle_data(context, data):
    rows = await data.history(assets=context.universe, fields=["disb", "val"],
                              bar_count=WINDOW, data_source=FLOW)
    # Empty until the window holds a completed session, which is correct rather than a fault.
    if rows is None or not len(rows):
        return

    pressure = rows.group_by("sid").agg(
        (pl.col("disb") * pl.col("val")).sum().alias("weighted"),
        pl.col("val").sum().alias("turnover"))
    tilt = (pressure.filter(pl.col("turnover") > 0)
            .with_columns((pl.col("weighted") / pl.col("turnover")).alias("tilt"))
            .sort("tilt", descending=True)
            .head(TOP_N))

    picks = {sid for sid, value in tilt.select("sid", "tilt").rows() if value and value > 0}
    if not picks:
        # Everything is being sold into. Holding nothing is the position.
        await context.rebalance({})
        return

    await context.rebalance({asset: 1.0 / len(picks)
                             for asset in context.universe if asset.sid in picks})
