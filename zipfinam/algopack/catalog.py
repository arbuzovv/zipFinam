"""What AlgoPack publishes, and how a five-minute bucket becomes a session.

AlgoPack has no manifest. Unlike a Hugging Face dataset, which describes its own columns and its
own knowledge date, these tables are a fixed catalogue documented at
https://moexalgo.github.io -- so the catalogue lives here, as data, and a dataset the exchange
adds later is a few lines rather than a new module.

Two things have to be written down per dataset, and neither can be guessed:

**Which market serves it.** ``tradestats`` exists for equities, futures and FX under three
different paths; ``futoi`` exists only for futures and under a different product entirely.

**How its columns collapse into a day.** This is the part that silently produces nonsense if it
is left to a default. The supercandles are five-minute buckets, and a daily backtest wants one
row per session, but there is no single rule that is right for all of them:

* ``vol`` over a day is the **sum** of its buckets; ``pr_close`` is the **last**;
* ``pr_vwap`` is neither. It is the volume-weighted mean, and taking ``val/vol`` instead is wrong
  by a factor of the lot size -- SBER trades in lots of ten, so ``val/vol`` reads 2647 against a
  close of 264;
* ``vol_b`` is a **flow** in ``tradestats`` (volume bought during the bucket, so it sums) and a
  **stock** in ``obstats`` (volume resting in the book at the snapshot, so it averages). Same
  name, opposite rule, which is why these tables are enumerated one at a time instead of matched
  by column-name pattern.

A column the exchange adds that is not listed here still comes through; it falls back to
:data:`FALLBACK`, and the mount reports that it did.
"""
import dataclasses
import datetime

import polars as pl

#: AlgoPack's three markets, as they appear in an address and in the REST path.
#: The paths are what ``moexalgo``'s own market resolution produces -- see its ``RESOLVE_MAP``.
MARKET_PATHS: dict[str, str] = {
    "eq": "datashop/algopack/eq",
    "fo": "datashop/algopack/fo",
    "fx": "datashop/algopack/fx",
}

#: Open interest is not part of the algopack product tree; it is its own analytical product.
FUTOI_PATH = "analyticalproducts/futoi/securities"

#: The exchange publishes in Moscow time and says so nowhere in the payload: ``tradedate`` and
#: ``tradetime`` are bare local date and time strings.
EXCHANGE_TIMEZONE = "Europe/Moscow"
#: Calendar a source is built on when the caller gives none: the Moscow equity session.
DEFAULT_CALENDAR = "XMOS"

#: Supercandle bucket length, confirmed against the exchange: SBER on 2026-09-07 published 202
#: stamps, 07:00 to 23:50, five minutes apart. ``tradetime`` labels the **end** of the bucket, so
#: a row stamped ``t`` covers ``(t - 5min, t]`` and the engine's ``date < now`` filter already
#: keeps a strategy out of the bucket it is trading inside. Nothing needs to be shifted.
#:
#: Note the span: MOEX trades a morning session from 07:00 and an evening one to 23:50, well
#: outside the 10:00-18:40 that ``exchange_calendars``' XMOS knows about. A session here is the
#: whole of that, which is what the daily aggregate covers.
BUCKET = datetime.timedelta(minutes=5)

#: The column every AlgoPack table names its instrument with. The raw ISS payload calls it
#: ``SECID``; ``moexalgo`` lowercases every column and renames that one, so by the time rows reach
#: us it is ``ticker`` on all six tables -- including ``futoi``, which publishes it under that
#: name to begin with.
ENTITY_COLUMN = "ticker"

#: The column carrying when MOEX wrote the row. Read before it is dropped, because for a row
#: published in the ordinary course it *is* the moment the data became knowable -- see
#: :data:`MAX_CREDIBLE_PUBLICATION_LAG`.
PUBLICATION_COLUMN = "systime"

#: How late ``systime`` may be and still be believed as the moment of publication.
#:
#: Measured against the exchange on 2026-09-27, the two populations do not overlap and are not
#: close to overlapping:
#:
#: * genuine publication -- supercandles and open interest **7 to 20 seconds** after the bucket
#:   closes, MegaAlerts about **90 seconds**, and ``hi2`` about **six minutes**, which is longer
#:   than a bucket and therefore a real source of look-ahead if it is ignored;
#: * reprocessing -- ``fo/orderstats`` carried a systime **2.5 days** after its session, and the
#:   2023-10-10 supercandles carry one from 2024-08-13, **ten months** later.
#:
#: An hour sits in the gap. Below it the lag is taken at face value and the row is held back until
#: it was really published; above it the systime is a rewrite rather than a publication, and the
#: row falls back to the end of its own bucket. Believing a rewrite would make a 2023 backtest see
#: nothing at all until the day MOEX happened to rebuild the table.
MAX_CREDIBLE_PUBLICATION_LAG = datetime.timedelta(hours=1)

#: Carried in every payload and never useful to a strategy once the knowledge instant has been
#: taken from it. ``sec_pr_*`` record which second of the bucket a price occurred on and have no
#: meaning once buckets are added together.
DROPPED_COLUMNS = frozenset({
    "systime", "board", "boardid", "seqnum", "sess_id",
    "sec_pr_open", "sec_pr_high", "sec_pr_low", "sec_pr_close",
    # Open interest repeats the session's own date beside `tradedate`.
    "trade_session_date",
})


# --------------------------------------------------------------------------- aggregation rules

@dataclasses.dataclass(frozen=True)
class Simple:
    """One of polars' own aggregations, by name: first, last, min, max, sum, mean."""

    how: str

    def expression(self, column: str) -> pl.Expr:
        return getattr(pl.col(column), self.how)().alias(column)

    def inputs(self, column: str) -> tuple[str, ...]:
        return (column,)


@dataclasses.dataclass(frozen=True)
class WeightedMean:
    """A price averaged over the day in proportion to what traded at it.

    The only correct way to collapse a VWAP. Buckets are weighted by their own volume, and a
    bucket with no value contributes to neither the numerator nor the denominator -- otherwise an
    empty five minutes would drag the day's average towards zero.
    """

    weight: str

    def expression(self, column: str) -> pl.Expr:
        weight = pl.col(self.weight).filter(pl.col(column).is_not_null())
        return ((pl.col(column) * pl.col(self.weight)).sum() / weight.sum()).alias(column)

    def inputs(self, column: str) -> tuple[str, ...]:
        return (column, self.weight)


@dataclasses.dataclass(frozen=True)
class Imbalance:
    """``(buy - sell) / (buy + sell)`` recomputed from the day's own totals.

    Averaging the buckets' own imbalances would weight a five minutes that traded nothing exactly
    as heavily as the opening auction.
    """

    buy: str
    sell: str

    def expression(self, column: str) -> pl.Expr:
        buy, sell = pl.col(self.buy).sum(), pl.col(self.sell).sum()
        total = buy + sell
        return (
            pl.when(total == 0).then(None).otherwise((buy - sell) / total)
            .cast(pl.Float64).alias(column)
        )

    def inputs(self, column: str) -> tuple[str, ...]:
        return (self.buy, self.sell)


@dataclasses.dataclass(frozen=True)
class PctChange:
    """The session's own return, from its first open to its last close, in percent."""

    open: str
    close: str

    def expression(self, column: str) -> pl.Expr:
        first_open = pl.col(self.open).drop_nulls().first()
        last_close = pl.col(self.close).drop_nulls().last()
        return (
            pl.when(first_open == 0).then(None)
            .otherwise((last_close / first_open - 1) * 100)
            .cast(pl.Float64).alias(column)
        )

    def inputs(self, column: str) -> tuple[str, ...]:
        return (self.open, self.close)


type Rule = Simple | WeightedMean | Imbalance | PctChange

FIRST, LAST, MIN, MAX = Simple("first"), Simple("last"), Simple("min"), Simple("max")
SUM, MEAN = Simple("sum"), Simple("mean")

#: What an unlisted column does. ``last`` is the one answer that is never *absurd*: it is the
#: state at the end of the session, which is right for a snapshot and merely incomplete for a
#: flow. A default of ``sum`` would quietly invent volume out of a ratio.
FALLBACK: Rule = LAST


# --------------------------------------------------------------------------- long-format tables

@dataclasses.dataclass(frozen=True)
class LongFormat:
    """A table that publishes one row per measurement rather than one row per instrument.

    ``hi2`` emits a ``metric``/``value`` pair per row, so a single timestamp for SBER arrives as
    eight rows. ``futoi`` emits one row per client group. Neither is readable as a data source
    until it is pivoted into columns, which is what a strategy asking for ``fields=["hhi_volume"]``
    expects to find.
    """

    #: Column whose values become column names.
    pivot_on: str
    #: Columns holding the measurements. One means the new columns are named after the pivot
    #: values alone; several means each is prefixed with its own name.
    values: tuple[str, ...]
    #: Dropped before pivoting -- free text that would otherwise collide across metrics.
    drop: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class Dataset:
    """One AlgoPack table, on one market."""

    name: str
    #: Markets that serve it, as address segments.
    markets: tuple[str, ...]
    #: One line, shown in the error listing the known datasets.
    description: str
    #: Column -> how it collapses into a session. Anything absent uses :data:`FALLBACK`.
    daily: dict[str, Rule] = dataclasses.field(default_factory=dict)
    #: Set when the table needs pivoting before it can be read.
    long_format: LongFormat | None = None
    #: ``futoi`` is keyed on the two-letter code of a futures *root*, not on a contract.
    keyed_on_futures_root: bool = False
    #: ``futoi`` lives outside the algopack product tree.
    path: str | None = None

    def rule(self, column: str) -> Rule:
        return self.daily.get(column, FALLBACK)


#: Prices, volumes and the buy/sell split, computed on the trade flow. The table most strategies
#: want: it is the one that carries what actually traded.
_TRADESTATS_DAILY: dict[str, Rule] = {
    "pr_open": FIRST, "pr_high": MAX, "pr_low": MIN, "pr_close": LAST,
    # Standard deviation *within* a bucket. Averaging the buckets understates a day that trended,
    # but the alternative -- recomputing from bucket closes -- is a different statistic under the
    # same name, which is worse.
    "pr_std": MEAN,
    "vol": SUM, "val": SUM, "trades": SUM,
    "trades_b": SUM, "trades_s": SUM,
    "val_b": SUM, "val_s": SUM, "vol_b": SUM, "vol_s": SUM,
    "pr_vwap": WeightedMean("vol"),
    "pr_vwap_b": WeightedMean("vol_b"),
    "pr_vwap_s": WeightedMean("vol_s"),
    "disb": Imbalance("vol_b", "vol_s"),
    "pr_change": PctChange("pr_open", "pr_close"),
    # Futures only: margin is a level, open interest an open/high/low/close of its own.
    "im": LAST,
    "oi_open": FIRST, "oi_high": MAX, "oi_low": MIN, "oi_close": LAST,
    "asset_code": FIRST,
}

#: Orders placed and orders pulled. Every column is a count or a notional over the bucket, so
#: everything sums except the four VWAPs, which weight by their own side's volume.
_ORDERSTATS_DAILY: dict[str, Rule] = {
    column: SUM for column in (
        "put_orders_b", "put_orders_s", "put_val_b", "put_val_s", "put_vol_b", "put_vol_s",
        "put_vol", "put_val", "put_orders",
        "cancel_orders_b", "cancel_orders_s", "cancel_val_b", "cancel_val_s",
        "cancel_vol_b", "cancel_vol_s", "cancel_vol", "cancel_val", "cancel_orders",
    )
} | {
    "put_vwap_b": WeightedMean("put_vol_b"),
    "put_vwap_s": WeightedMean("put_vol_s"),
    "cancel_vwap_b": WeightedMean("cancel_vol_b"),
    "cancel_vwap_s": WeightedMean("cancel_vol_s"),
    "asset_code": FIRST,
}


def _obstats_daily() -> dict[str, Rule]:
    """Every order-book column averages, and that is the whole rule.

    ``obstats`` is a photograph of the book taken every five minutes: spreads, level counts,
    resting volume, imbalance. None of it accumulates -- adding up the volume resting in the book
    at the 202 snapshots a Moscow session actually produces would report two hundred times the
    liquidity that was ever there. ``asset_code`` is the exception only because it is a label.
    """
    columns = (
        "spread_bbo", "spread_lv10", "spread_1mio",
        "levels_b", "levels_s", "vol_b", "vol_s", "val_b", "val_s",
        "imbalance_vol_bbo", "imbalance_val_bbo", "imbalance_vol", "imbalance_val",
        "vwap_b", "vwap_s", "vwap_b_1mio", "vwap_s_1mio",
        # The FX and futures books publish depth by level instead.
        "mid_price", "micro_price",
        *(f"spread_l{level}" for level in (1, 2, 3, 5, 10, 20)),
        *(f"{side}_l{level}" for side in ("vol_b", "vol_s", "val_b", "val_s")
          for level in (1, 2, 3, 5, 10, 20)),
        *(f"vwap_{side}_l{level}" for side in ("b", "s") for level in (1, 2, 3, 5, 10, 20)),
    )
    return {column: MEAN for column in columns} | {"asset_code": FIRST}


#: The catalogue. Keyed by dataset name, which is the second segment of an address.
DATASETS: dict[str, Dataset] = {
    "tradestats": Dataset(
        name="tradestats", markets=("eq", "fo", "fx"),
        description="prices, volumes and the buy/sell split, computed on the trade flow",
        daily=_TRADESTATS_DAILY),
    "orderstats": Dataset(
        name="orderstats", markets=("eq", "fo", "fx"),
        description="orders placed and cancelled, by side",
        daily=_ORDERSTATS_DAILY),
    "obstats": Dataset(
        name="obstats", markets=("eq", "fo", "fx"),
        description="order-book snapshots: spreads, depth, imbalance",
        daily=_obstats_daily()),
    "hi2": Dataset(
        name="hi2", markets=("eq", "fo"),
        description="market concentration (Herfindahl) indices, published once per session",
        # Published once a session already, at 18:40, so there is nothing to collapse. Eleven
        # metrics come back per instrument, confirmed against the exchange: hhi_volume, hhi_buy,
        # hhi_sell, hhi_agressive{,_buy,_sell}, hhi_passive{,_buy,_sell} and
        # hhi_netflow_{buy,sell}. Each becomes a column of that name.
        long_format=LongFormat(pivot_on="metric", values=("value",), drop=("reference",))),
    "alerts": Dataset(
        name="alerts", markets=("eq", "fo", "fx"),
        description="MegaAlerts: flags raised on anomalous activity",
        # An event table rather than a series, and a sparse one: SBER raised 26 alerts in a week
        # and CNYRUB_TOM and SiH7 raised none at all. `alert_type` names the rule that fired
        # ("vol_b_99_9_pctl"), `value` and `threshold` are what it compared, and `reference` is a
        # JSON blob of the surrounding flow. Collapsing a day to its last alert is defensible but
        # lossy, so a strategy that acts on these should read `5min`, or ask by time with
        # `since=` rather than by a count of rows.
        daily={"alert_type": LAST, "threshold": LAST, "value": LAST, "reference": LAST}),
    "futoi": Dataset(
        name="futoi", markets=("fo",), path=FUTOI_PATH, keyed_on_futures_root=True,
        description="futures open interest split between retail (fiz) and institutional (yur)",
        # A position is a level, so the session's figure is its last snapshot, never a sum.
        # Note the sign the exchange uses: `pos_short` comes back **negative** and `pos` is the
        # net, so a strategy comparing the two sides has to take the absolute value.
        long_format=LongFormat(
            pivot_on="clgroup",
            values=("pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num"))),
}


def known_datasets() -> list[str]:
    return sorted(DATASETS)


def path_for(market: str, dataset: Dataset) -> str:
    """The REST path the rows are read from."""
    return dataset.path or MARKET_PATHS[market]
