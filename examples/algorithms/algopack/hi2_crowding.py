"""Follow the names where one buyer is doing the buying.

`hi2` is a Herfindahl index over the participants in a session: high means the flow came from few
hands, low means it came from the crowd. MOEX publishes eleven of them per instrument, and the
pair that matters here is `hhi_agressive_buy` against `hhi_agressive_sell` -- concentration among
the people crossing the spread to buy, against those crossing it to sell.

A name where aggressive buying is concentrated and selling is diffuse is one where somebody is
accumulating against a scattered market. That is a different statement from "it went up", and it
is one no price series contains.

`hi2` is published once per session, around 18:40, and this is the table where the publication
lag matters most: MOEX writes it about **six minutes** after the timestamp it carries, which is
longer than a bucket. The connector holds each row until its own `systime`, so an intraday
strategy cannot read a concentration index before it existed. A daily one like this never
notices -- the session is stamped at the end of the day either way.
"""

import polars as pl

import zipfinam  # noqa: F401  (схема algopack://)

CONCENTRATION = "algopack://eq/hi2"
WINDOW = 5
TOP_N = 3


async def initialize(context):
    context.universe = [
        await context.symbol(ticker, mic="MISX")
        for ticker in ("SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "TATN", "MTSS")
    ]


async def handle_data(context, data):
    rows = await data.history(assets=context.universe,
                              fields=["hhi_agressive_buy", "hhi_agressive_sell"],
                              bar_count=WINDOW, data_source=CONCENTRATION)
    if rows is None or not len(rows):
        return

    tilt = rows.group_by("sid").agg(
        (pl.col("hhi_agressive_buy") - pl.col("hhi_agressive_sell")).mean().alias("crowding"))
    leaders = tilt.drop_nulls().sort("crowding", descending=True).head(TOP_N)

    picks = {sid for sid, crowding in leaders.rows() if crowding and crowding > 0}
    if not picks:
        # Selling is the concentrated side everywhere. Nothing to accumulate alongside.
        await context.rebalance({})
        return

    await context.rebalance({asset: 1.0 / len(picks)
                             for asset in context.universe if asset.sid in picks})
