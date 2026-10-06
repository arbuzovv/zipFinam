"""MOEX AlgoPack mounted as a ziplime data source.

    df = await data.history(assets=[sber], bar_count=30,
                            data_source="algopack://eq/tradestats")

The same shape as a Hugging Face dataset and for the same reason: a strategy names a source where
it would name a bundle, nothing is ingested, and the first read fetches only what the simulation
window can reach. What arrives is a flat frame of ``date``, ``sid`` and the exchange's own
columns, which is the shape every other source in the engine returns.

Addresses
---------

``algopack://<market>/<dataset>[/<granularity>]``

* ``market`` -- ``eq``, ``fo`` or ``fx``. Optional; equities are assumed.
* ``dataset`` -- ``tradestats``, ``orderstats``, ``obstats``, ``hi2``, ``alerts`` or ``futoi``.
* ``granularity`` -- ``1d`` (the default) or ``5min``.

So ``algopack://tradestats`` is the equity trade-flow table by session, ``algopack://fo/obstats``
the futures order book, and ``algopack://eq/tradestats/5min`` the raw supercandles.

Sessions, not buckets
---------------------

AlgoPack publishes five-minute buckets. A daily backtest asking for ``bar_count=20`` means twenty
*sessions*, and handing it twenty buckets -- an hour and a half of one morning -- would be wrong
in a way nothing in the output would reveal. So ``1d`` collapses each Moscow trading day into one
row before the engine ever sees it, with a rule per column rather than one rule for the table:
volumes sum, prices take their open, high, low and close, VWAPs weight by their own side's
volume, and order-book depth averages. :mod:`~zipfinam.algopack.catalog` holds
the table, and says why each one is what it is.

``5min`` keeps the buckets, for an intraday simulation. Mounting ``5min`` and then asking for
daily bars is the one combination to avoid: the engine's own downsampling takes the last value in
each day, which turns a day's volume into the last five minutes of it.

Point-in-time
-------------

The engine windows every source with ``date < the simulation's clock``, so what makes a mount
honest is where its rows are stamped.

A bucket is stamped at ``tradetime``, which MOEX writes as the **end** of the bucket -- the first
row of a Moscow equity session, which opens at 10:00, is stamped 10:05. A strategy on the 10:05
bar therefore cannot see the bucket it is trading inside; it sees the one before.

A session is stamped at the **end of its own trading day**, in Moscow. It has to be: the day's
volume is not known until the day is over, and stamping it at the session's open -- which is what
a source keyed on a bare date does -- would let a strategy trade the morning on the afternoon's
numbers. So day D's row becomes visible on D+1, and a strategy comparing today to yesterday is
reading yesterday.
"""
import asyncio
import datetime
import enum
import re
from typing import Any, Self
from zoneinfo import ZoneInfo

from exchange_calendars import ExchangeCalendar, get_calendar

import polars as pl
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from zipfinam.algopack import api
from zipfinam.algopack.catalog import (
    BUCKET, DATASETS, DEFAULT_CALENDAR, DROPPED_COLUMNS, ENTITY_COLUMN, EXCHANGE_TIMEZONE, FALLBACK,
    MARKET_PATHS, MAX_CREDIBLE_PUBLICATION_LAG, PUBLICATION_COLUMN, Dataset, LongFormat,
    known_datasets,
)
from ziplime.data.services.data_source import DataSource

_logger = structlog.get_logger(__name__)

#: The scheme that names an AlgoPack table inline.
ADDRESS_SCHEME = "algopack://"

#: Assumed when an address names no market. Equities are what almost every strategy reads.
DEFAULT_MARKET = "eq"

#: Venues AlgoPack covers. A listing anywhere else is skipped rather than asked about -- MOEX has
#: never heard of it, and a universe that mixes markets should not fail because of that.
MOEX_MICS = frozenset({"MISX", "RTSX"})

#: The exchange's own clock, which is what ``tradedate`` and ``tradetime`` are written in.
_MOSCOW = ZoneInfo(EXCHANGE_TIMEZONE)


class Granularity(enum.Enum):
    """How much of the day one row covers."""

    #: One row per Moscow trading session, aggregated here. The default.
    DAILY = "1d"

    #: One row per five-minute bucket, exactly as published.
    INTRADAY = "5min"

    @property
    def frequency(self) -> datetime.timedelta:
        return datetime.timedelta(days=1) if self is Granularity.DAILY else BUCKET


#: Spellings accepted in an address, beyond the enum's own.
_GRANULARITY_ALIASES = {
    "1d": Granularity.DAILY, "daily": Granularity.DAILY, "day": Granularity.DAILY,
    "5min": Granularity.INTRADAY, "5m": Granularity.INTRADAY,
    "intraday": Granularity.INTRADAY, "bucket": Granularity.INTRADAY,
}

#: A futures contract's root, which is what open interest is reported against: ``SRM7`` -> ``SR``.
_ROOT_CODE = re.compile(r"^([A-Za-z]{2})[A-Za-z]\d$")


def is_address(value: Any) -> bool:
    """Whether ``value`` is an ``algopack://`` address."""
    return isinstance(value, str) and value.startswith(ADDRESS_SCHEME)


def parse_address(address: str) -> tuple[str, Dataset, Granularity]:
    """Split ``algopack://market/dataset/granularity`` into its three parts.

    The market and the granularity are both optional and neither can be confused with the other:
    the market names are the only segments that are ever ``eq``, ``fo`` or ``fx``, and the
    granularity is the only one that is ever ``1d`` or ``5min``.

    Raises:
        ValueError: The address names no dataset, names one that does not exist, or names a
            market that does not serve it.
    """
    if not is_address(address):
        raise ValueError(
            f"{address!r} is not an AlgoPack address; expected {ADDRESS_SCHEME}dataset or "
            f"{ADDRESS_SCHEME}market/dataset.")
    segments = [segment for segment in address[len(ADDRESS_SCHEME):].split("/") if segment]

    market = DEFAULT_MARKET
    if segments and segments[0].lower() in MARKET_PATHS:
        market = segments.pop(0).lower()
    if not segments:
        raise ValueError(
            f"{address!r} names no dataset. The form is {ADDRESS_SCHEME}market/dataset, and the "
            f"datasets are {', '.join(known_datasets())}.")

    name = segments.pop(0).lower()
    dataset = DATASETS.get(name)
    if dataset is None:
        raise ValueError(
            f"{address!r} names no AlgoPack dataset. Known datasets: "
            f"{', '.join(known_datasets())}.")
    if market not in dataset.markets:
        raise ValueError(
            f"AlgoPack does not publish {name!r} for {market!r}; it is served for "
            f"{', '.join(dataset.markets)}.")

    granularity = Granularity.DAILY
    if segments:
        granularity = _GRANULARITY_ALIASES.get(segments[0].lower())
        if granularity is None:
            raise ValueError(
                f"{segments[0]!r} is not a granularity. Use 1d for one row per session, or 5min "
                f"for the buckets as published.")
        segments.pop(0)
    if segments:
        raise ValueError(
            f"{address!r} has trailing segments {segments}. The form is "
            f"{ADDRESS_SCHEME}market/dataset/granularity.")
    return market, dataset, granularity


class AlgoPackDataSource(DataSource):
    """One AlgoPack table, read per instrument and only for the instruments a strategy asks about.

    Unlike a published dataset, which arrives whole and is then filtered down, AlgoPack is a
    per-instrument API: there is no file to download, only a request to make. So this source is
    lazy twice over. Nothing is fetched at mount; and when a read arrives, only the instruments in
    it are fetched, once each, and kept for the rest of the run.

    That is what makes a wide universe affordable. A strategy that mounts the table in
    ``initialize`` and only ever reads three names pays for three names.

    What it holds is clipped to the window it was mounted with, so a trailing window is empty for
    the first ``bar_count`` sessions of a run -- the same as any other source. Pass an earlier
    ``start_date`` to :meth:`mount` to read history before the simulation begins.

    Build it with :meth:`mount`.
    """

    def __init__(self, name: str, market: str, dataset: Dataset, granularity: Granularity,
                 start_date: datetime.date, end_date: datetime.date,
                 fields: frozenset[str] | None = None, session_timezone: str = EXCHANGE_TIMEZONE,
                 trading_calendar: ExchangeCalendar | None = None):
        # Bounds are presented in the simulation's own timezone rather than in Moscow's, because
        # every window filter in the engine compares this source's `date` against the simulation
        # clock and polars refuses to compare two tz-aware columns in different zones. The instant
        # is unchanged; only its label moves.
        if trading_calendar is None:
            trading_calendar = get_calendar(DEFAULT_CALENDAR)
        self.session_timezone = session_timezone
        super().__init__(name=name,
                         trading_calendar=trading_calendar,
                         start_date=_at_zone(start_date, session_timezone),
                         end_date=_at_zone(end_date, session_timezone, end_of_day=True),
                         frequency=granularity.frequency,
                         original_frequency=granularity.frequency,
                         data_type=DataType.CUSTOM)
        # ziplime 2.x relabels the bounds in the calendar's zone; put them back in the session's,
        # which is the zone of the `date` column they are compared against.
        self.start_date = self.start_date.astimezone(ZoneInfo(session_timezone))
        self.end_date = self.end_date.astimezone(ZoneInfo(session_timezone))
        self.market = market
        self.dataset = dataset
        self.granularity = granularity
        self.window_start = _as_date(start_date)
        self.window_end = _as_date(end_date)
        self.requested_fields = frozenset(fields) if fields else None
        self.data: pl.DataFrame | None = None
        #: sids already fetched, so a second read of the same instrument costs nothing.
        self._loaded: set[int] = set()
        #: What was fetched, what was skipped and why. Filled in as instruments arrive.
        self.mount_report: dict[str, Any] = {
            "dataset": f"{market}/{dataset.name}", "granularity": granularity.value,
            "instruments_loaded": 0, "instruments_empty": 0, "instruments_skipped": 0, "rows": 0,
        }
        self._logger = structlog.get_logger(__name__)

    # ------------------------------------------------------------------ mounting

    @classmethod
    def mount(cls, address: str, start_date: datetime.date, end_date: datetime.date,
              fields: list[str] | frozenset[str] | None = None, name: str | None = None,
              session_timezone: str | None = None,
              granularity: Granularity | str | None = None,
              trading_calendar: ExchangeCalendar | None = None) -> Self:
        """Prepare a table for reading. Touches no network.

        Args:
            address: ``algopack://market/dataset/granularity``, or a bare dataset name.
            start_date, end_date: The simulation window. Instrument-years outside it are never
                requested, which is most of what keeps a long backtest cheap.
            fields: Columns to keep besides ``date`` and ``sid``. All of them by default.
            name: What the strategy calls this source. Defaults to the address.
            session_timezone: Zone to present timestamps in. Must match the simulation's trading
                calendar, since every window filter compares the two. Defaults to the zone of
                ``trading_calendar``.
            trading_calendar: The simulation's calendar. Defaults to XMOS.
            granularity: Overrides what the address says.

        Returns:
            The mounted source. The first read fetches the instruments it is asked about.
        """
        market, dataset, from_address = parse_address(
            address if is_address(address) else f"{ADDRESS_SCHEME}{address}")
        if granularity is not None:
            from_address = (granularity if isinstance(granularity, Granularity)
                            else _require_granularity(granularity))

        source = cls(
            name=name or f"{ADDRESS_SCHEME}{market}/{dataset.name}/{from_address.value}",
            market=market, dataset=dataset, granularity=from_address,
            start_date=start_date, end_date=end_date, fields=fields,
            session_timezone=session_timezone or (
                str(trading_calendar.tz) if trading_calendar is not None else EXCHANGE_TIMEZONE),
            trading_calendar=trading_calendar)
        _logger.info("Mounted an AlgoPack dataset", dataset=f"{market}/{dataset.name}",
                     granularity=from_address.value,
                     window=f"{source.window_start}..{source.window_end}")
        return source

    # ------------------------------------------------------------------ fetching

    async def load(self, assets) -> pl.DataFrame:
        """Fetch whatever of ``assets`` is not already held. Idempotent; the first read triggers it.

        Blocking HTTP, moved off the event loop: the exchange is not fast and a simulation has
        other things to be doing. The library's own throttle still applies.
        """
        wanted = [asset for asset in assets if getattr(asset, "sid", None) not in self._loaded]
        if not wanted:
            return self.data if self.data is not None else pl.DataFrame()

        requests = self._requests_for(wanted)
        loaded_before = self.mount_report["instruments_loaded"]
        if requests:
            frames = await asyncio.to_thread(self._fetch_all, requests)
            self._absorb(frames)
        self._report_if_nothing_matched(wanted, loaded_before)
        # Marked once the fetch has come back, including for an instrument the exchange holds
        # nothing for -- that one must not be asked about again on every bar for the rest of the
        # run. A fetch that *failed* raised out of here and marked nothing, so it can be retried.
        self._loaded.update(asset.sid for asset in wanted if getattr(asset, "sid", None))
        return self.data if self.data is not None else pl.DataFrame()

    def _report_if_nothing_matched(self, wanted, loaded_before: int) -> None:
        """Say so when a read matched nothing, because silence here looks like a quiet market.

        A mount that matches no instrument returns an empty frame, and a strategy written the
        usual way returns early on an empty frame -- so the backtest finishes, trades nothing
        and reports success, with the reason nowhere. Every other line this connector writes
        is ``info``, and the deployed image runs at ``LOG_LEVEL=WARNING``, so none of them
        survive. This one has to.
        """
        if not wanted or self.mount_report["instruments_loaded"] > loaded_before:
            return
        self._logger.warning(
            "An AlgoPack read matched no instrument, so this source returns nothing",
            dataset=self.name, asked_for=len(wanted),
            skipped=self.mount_report["instruments_skipped"],
            no_rows=self.mount_report["instruments_empty"],
            detail="A strategy reading this sees an empty frame and quietly does not trade.")

    def _requests_for(self, assets) -> dict[str, list[int]]:
        """Group the instruments by the exchange code that has to be requested for them.

        Normally one code per listing. ``futoi`` is the exception: open interest is reported
        against a futures *root*, so ``SRM7`` and ``SRU7`` are one request whose answer belongs to
        both -- the position is the root's, and a strategy holds a contract.
        """
        requests: dict[str, list[int]] = {}
        skipped: list[str] = []
        for asset in assets:
            sid, symbol = getattr(asset, "sid", None), getattr(asset, "symbol", None)
            if sid is None or not symbol:
                continue
            mic = getattr(asset, "mic", None)
            if mic is not None and mic not in MOEX_MICS:
                skipped.append(f"{symbol}@{mic}")
                continue
            secid = self._secid_for(asset, symbol)
            if secid is None:
                skipped.append(symbol)
                continue
            requests.setdefault(secid, []).append(sid)
        if skipped:
            self.mount_report["instruments_skipped"] += len(skipped)
            self._logger.info(
                "Skipped instruments AlgoPack does not cover", dataset=self.name,
                skipped=sorted(skipped)[:20], count=len(skipped),
                detail="AlgoPack serves Moscow listings only, and open interest only futures.")
        return requests

    def _secid_for(self, asset, symbol: str) -> str | None:
        if not self.dataset.keyed_on_futures_root:
            return symbol
        contract = getattr(asset, "asset", asset)
        if not isinstance(contract, FuturesContract):
            return None
        # The contract's own root first, and the code is **not** upper-cased. MOEX writes these
        # in its own mixed case -- the dollar-rouble future is `Si` and the euro one `Eu`, beside
        # `SR` and `RI` -- and `SI` is not a code the exchange knows.
        root = getattr(contract, "root_symbol", None)
        if root:
            return root
        match = _ROOT_CODE.match(symbol)
        return match.group(1) if match else None

    def _fetch_all(self, requests: dict[str, list[int]]) -> list[pl.DataFrame]:
        """Read every instrument the batch needs, several at a time.

        The exchange pages a thousand rows at a time and will not page wider, so the only lever
        on a universe is how many instruments are in flight at once. Reading them one after
        another made a ten-name backtest wait minutes for its first bar.
        """
        raw_by_secid = api.fetch_many(self.dataset, self.market, list(requests),
                                      self.window_start, self.window_end)
        frames = []
        for secid, sids in requests.items():
            raw = raw_by_secid.get(secid)
            shaped = self._shape(raw) if raw is not None and not raw.is_empty() else None
            if shaped is None or shaped.is_empty():
                self.mount_report["instruments_empty"] += len(sids)
                continue
            self.mount_report["instruments_loaded"] += len(sids)
            # One frame per listing the code answers for. For everything but open interest that
            # is a single sid and the loop runs once.
            frames.extend(shaped.with_columns(pl.lit(sid, dtype=pl.Int64).alias("sid"))
                          for sid in sids)
        return frames

    def _absorb(self, frames: list[pl.DataFrame]) -> None:
        if not frames:
            return
        # `diagonal_relaxed`: hi2 publishes a different set of metrics for different instruments,
        # so two listings genuinely have different columns.
        arriving = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")
        arriving = self._select_fields(arriving)
        self.data = (arriving if self.data is None
                     else pl.concat([self.data, arriving], how="diagonal_relaxed"))
        # `date` and `sid` first, because that is how every other source in the engine presents
        # itself and a strategy printing the frame should recognise it.
        leading = ["date", "sid"]
        self.data = self.data.select(
            leading + [column for column in self.data.columns if column not in leading]
        ).sort("date", "sid")
        self.mount_report["rows"] = len(self.data)
        self._logger.info("Read AlgoPack rows", **self.mount_report)

    # ------------------------------------------------------------------ shaping

    def _shape(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Published rows to engine rows: one timestamp column, one row per period."""
        frame = raw.with_columns(_moscow_day(raw.schema).alias("_day"),
                                 _moscow_instant(raw.schema).alias("_bucket"),
                                 _publication_instant(raw.schema).alias("_published"))
        frame = self._attach_knowledge(frame)
        frame = frame.drop((DROPPED_COLUMNS | {"tradedate", "tradetime", "_bucket", "_published"})
                           & set(frame.columns))

        if self.dataset.long_format is not None:
            frame = self._pivot(frame, self.dataset.long_format)
        frame = frame.drop({ENTITY_COLUMN} & set(frame.columns))

        if self.granularity is Granularity.DAILY:
            frame = self._to_sessions(frame)
        else:
            frame = frame.drop("_day").with_columns(
                pl.col("_knowledge").dt.convert_time_zone(self.session_timezone).alias("date")
            ).drop("_knowledge")
        # Clipped to the declared window. The fetch works in whole years because that is the unit
        # the cache is keyed on, so a two-week mount gets a year back and would otherwise carry
        # all of it -- six megabytes per instrument at five-minute granularity, for rows outside
        # the simulation. A strategy that wants history before its start says so with
        # `context.algopack_dataset(start_date=...)`.
        return (frame.filter(pl.col("date").is_not_null(),
                             pl.col("date") >= self.start_date,
                             pl.col("date") <= self.end_date)
                .sort("date"))

    def _attach_knowledge(self, frame: pl.DataFrame) -> pl.DataFrame:
        """When each row became knowable: the later of its bucket closing and its publication.

        ``systime`` is when MOEX wrote the row, and for a row written in the ordinary course that
        is exactly the moment a strategy could first have had it -- seconds after the bucket for
        supercandles, but **six minutes** for ``hi2``, which is longer than a bucket. Ignoring it
        would let an intraday strategy read a concentration index before it existed.

        Taking it at face value is the other mistake. MOEX rewrites tables: ``fo/orderstats`` has
        carried a systime two and a half days after its session, and the 2023 supercandles carry
        one from ten months later. A rewrite is not a publication, and believing one would make a
        2023 backtest see nothing at all until the day the rebuild happened. So a lag beyond
        :data:`~zipfinam.algopack.catalog.MAX_CREDIBLE_PUBLICATION_LAG` is
        discarded and the row falls back to the close of its own bucket, which the exchange's own
        five-minute grid already makes safe.

        Both counts reach the mount report, because "half this table was rewritten" is something
        the person reading the result should be told rather than left to infer.
        """
        if "_published" not in frame.columns or frame["_published"].null_count() == len(frame):
            return frame.with_columns(pl.col("_bucket").alias("_knowledge"))

        lag = pl.col("_published") - pl.col("_bucket")
        credible = (pl.col("_published").is_not_null()
                    & (lag > datetime.timedelta(0))
                    & (lag <= MAX_CREDIBLE_PUBLICATION_LAG))
        counts = frame.select(
            credible.sum().alias("delayed"),
            (pl.col("_published").is_not_null() & (lag > MAX_CREDIBLE_PUBLICATION_LAG))
            .sum().alias("rewritten"),
        ).row(0)
        self.mount_report["rows_held_to_publication"] = (
            self.mount_report.get("rows_held_to_publication", 0) + int(counts[0] or 0))
        self.mount_report["rows_with_a_rewritten_systime"] = (
            self.mount_report.get("rows_with_a_rewritten_systime", 0) + int(counts[1] or 0))
        return frame.with_columns(
            pl.when(credible).then(pl.col("_published")).otherwise(pl.col("_bucket"))
            .alias("_knowledge"))

    def _pivot(self, frame: pl.DataFrame, spec: LongFormat) -> pl.DataFrame:
        """Turn one-row-per-measurement into one-row-per-instrument-per-timestamp.

        ``hi2`` arrives as eight rows of ``metric``/``value`` for a single moment, and ``futoi``
        as one row per client group. Neither is addressable by a strategy asking for a field until
        the measurement names become column names.
        """
        frame = frame.drop(set(spec.drop) & set(frame.columns))
        if spec.pivot_on not in frame.columns:
            return frame
        values = [column for column in spec.values if column in frame.columns]
        if not values:
            return frame.drop(spec.pivot_on)

        # One knowledge instant per bucket before pivoting, or two rows that differ only by a
        # second of publication would pivot into two half-empty ones.
        if "_knowledge" in frame.columns:
            frame = frame.with_columns(
                pl.col("_knowledge").max().over([c for c in ("_day", ENTITY_COLUMN)
                                                 if c in frame.columns] + ["_knowledge"])
                .alias("_knowledge"))
        index = [column for column in ("_day", "_knowledge", ENTITY_COLUMN)
                 if column in frame.columns]
        wide = frame.pivot(on=spec.pivot_on, index=index, values=values, aggregate_function="last")
        # polars names a pivoted column after the pivot value alone when there is one value
        # column, and `{value}_{pivot}` when there are several. Lowercased either way, because
        # futoi's groups arrive as FIZ and YUR and a strategy should not have to shout.
        return wide.rename({column: column.lower() for column in wide.columns
                            if column not in index})

    def _to_sessions(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Collapse the day's buckets into one row, stamped when the day was over.

        The stamp is the later of the end of the trading day and the last credible publication in
        it. The first term is what normally decides -- the day's volume is not knowable before the
        day ends -- and the second only matters when the exchange published a session's last
        bucket after midnight, where honouring it is the difference between correct and a few
        minutes of look-ahead.
        """
        value_columns = [column for column in frame.columns
                         if column not in ("_day", "_knowledge", "sid")]
        aggregations, fell_back = [], []
        for column in value_columns:
            rule = self.dataset.rule(column)
            if all(name in frame.columns for name in rule.inputs(column)):
                aggregations.append(rule.expression(column))
            else:
                # A rule whose weight column was not published cannot be applied; the fallback is
                # reported rather than quietly substituted.
                aggregations.append(FALLBACK.expression(column))
                fell_back.append(column)
            if column not in self.dataset.daily:
                fell_back.append(column)
        # Only worth saying for a table that *has* rules: hi2 and futoi publish their columns
        # dynamically -- one per metric, one per client group -- and the last value of the
        # session is the intended answer for all of them, not a fallback.
        if fell_back and self.dataset.daily:
            self._logger.info(
                "Collapsed AlgoPack columns with no rule of their own by taking the day's last "
                "value", dataset=self.name, columns=sorted(set(fell_back)))

        if "_knowledge" in frame.columns:
            aggregations.append(pl.col("_knowledge").max().alias("_last_published"))
        sessions = frame.group_by("_day").agg(aggregations)
        closed = _end_of_day("_day", self.session_timezone)
        stamp = (pl.max_horizontal(closed, pl.col("_last_published")
                                   .dt.convert_time_zone(self.session_timezone))
                 if "_last_published" in sessions.columns else closed)
        return sessions.with_columns(stamp.alias("date")).drop(
            {"_day", "_last_published"} & set(sessions.columns))

    def _select_fields(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Keep ``date`` and ``sid`` plus the requested columns."""
        if self.requested_fields is None:
            return frame
        keep = ["date", "sid"] + [column for column in frame.columns
                                  if column in self.requested_fields
                                  and column not in ("date", "sid")]
        missing = set(self.requested_fields) - set(frame.columns) - {"date", "sid"}
        if missing:
            self._logger.warning("Requested fields this AlgoPack table does not publish",
                                 missing=sorted(missing), dataset=self.name,
                                 available=sorted(set(frame.columns) - {"date", "sid"}))
        return frame.select(keep)

    # ------------------------------------------------------------------ reading

    def get_dataframe(self) -> pl.DataFrame:
        """Everything fetched so far.

        Raises:
            RuntimeError: Called before any read. Reads that go through :meth:`get_data_by_limit`
                fetch on demand; this is here for the paths that do not, so the failure names the
                cause instead of surfacing as an empty result.
        """
        if self.data is None:
            raise RuntimeError(
                f"{self.name} has not read anything yet. Read it through data.history(), or call "
                f"`await source.load(assets)` first.")
        return self.data

    async def get_data_by_limit(self, fields: frozenset[str] | None, limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta | Period,
                                assets: frozenset[Asset], include_end_date: bool) -> pl.DataFrame:
        """Serve a trailing window, fetching whatever of ``assets`` is new."""
        if (await self.load(assets)).is_empty():
            return pl.DataFrame()
        return await super().get_data_by_limit(
            fields=fields, limit=limit, end_date=end_date, frequency=frequency, assets=assets,
            include_end_date=include_end_date)

    async def get_data_by_window(self, fields: frozenset[str] | None, since: datetime.timedelta,
                                 end_date: datetime.datetime,
                                 frequency: datetime.timedelta | Period,
                                 assets: frozenset[Asset], include_end_date: bool) -> pl.DataFrame:
        """Serve a calendar-time window, fetching whatever of ``assets`` is new."""
        if (await self.load(assets)).is_empty():
            return pl.DataFrame()
        return await super().get_data_by_window(
            fields=fields, since=since, end_date=end_date, frequency=frequency, assets=assets,
            include_end_date=include_end_date)

    async def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str] | None,
                             dt: datetime.datetime, frequency=None, **kwargs) -> pl.DataFrame:
        """The newest row per instrument that was observable before ``dt``.

        This is what ``data.current`` calls. AlgoPack never republishes a period, so the newest
        row is the current state and there is nothing to coalesce across revisions.
        """
        frame = await self.load(assets)
        if frame.is_empty():
            return pl.DataFrame()

        wanted = list(fields) if fields else [c for c in frame.columns if c not in ("date", "sid")]
        wanted = [c for c in wanted if c in frame.columns]
        sids = [asset.sid for asset in assets]
        # Strictly before the current moment: the rule every other read here follows, so a bucket
        # that closed during the bar being traded is not visible inside it.
        visible = frame.filter(pl.col("date") < dt, pl.col("sid").is_in(sids)).sort("date")
        if visible.is_empty():
            return pl.DataFrame()
        return visible.group_by("sid").agg(pl.col("date").last(),
                                           *[pl.col(column).last() for column in wanted])

    def get_missing_data_by_limit(self, fields: frozenset[str] | None, limit: int,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta | Period,
                                  assets: frozenset[Asset],
                                  include_end_date: bool) -> pl.DataFrame:
        """Nothing, for a window past what was mounted.

        The base class delegates here when the simulation runs past ``end_date``. An empty frame
        is the honest answer; raising would make a strategy that merely *consults* the table fail
        outright once the backtest walked past its last session.
        """
        empty = self.data if self.data is not None else pl.DataFrame()
        return empty.clear()


def _require_granularity(value: str) -> Granularity:
    granularity = _GRANULARITY_ALIASES.get(value.lower())
    if granularity is None:
        raise ValueError(f"{value!r} is not a granularity; use 1d or 5min.")
    return granularity


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    """The calendar day of a bound, whichever of the two types it arrived as."""
    return value.date() if isinstance(value, datetime.datetime) else value


def _at_zone(day: datetime.date, timezone: str, end_of_day: bool = False) -> datetime.datetime:
    """A window bound as an instant in ``timezone``, matching the mounted date column."""
    zone = ZoneInfo(timezone)
    if isinstance(day, datetime.datetime):
        return (day if day.tzinfo else day.replace(tzinfo=zone)).astimezone(zone)
    return datetime.datetime.combine(
        day, datetime.time.max if end_of_day else datetime.time.min, tzinfo=zone)


def _as_date_expr(schema: pl.Schema) -> pl.Expr:
    """``tradedate`` as a Date, whether ISS sent it as text or polars already parsed it."""
    column = pl.col("tradedate")
    return column if schema["tradedate"] == pl.Date else column.str.to_date("%Y-%m-%d")


def _moscow_day(schema: pl.Schema) -> pl.Expr:
    """The exchange's own calendar day.

    ``tradedate`` is already that day, which is why the grouping key is taken from it rather than
    from the timestamp: a bucket in the evening session is stamped near midnight, and deriving the
    day from an instant converted into another timezone would push it onto the next one.
    """
    return _as_date_expr(schema)


def _moscow_instant(schema: pl.Schema) -> pl.Expr:
    """``tradedate`` and ``tradetime`` as one instant, on the simulation's clock.

    The payload carries a bare date and a bare time and says nowhere that they are Moscow's, so
    the zone is attached here rather than guessed per row. Reading them as UTC instead would move
    every bucket three hours and put the opening auction on the previous session.
    """
    day = _as_date_expr(schema).cast(pl.Datetime("us"))
    if "tradetime" not in schema.names():
        return day.dt.replace_time_zone(EXCHANGE_TIMEZONE)
    time = pl.col("tradetime")
    if schema["tradetime"] != pl.Time:
        time = time.str.to_time("%H:%M:%S")
    return (day + time.cast(pl.Duration("us"))).dt.replace_time_zone(EXCHANGE_TIMEZONE)


def _publication_instant(schema: pl.Schema) -> pl.Expr:
    """``systime`` as an instant on the exchange's clock, or null where it is not published."""
    if PUBLICATION_COLUMN not in schema.names():
        return pl.lit(None, dtype=pl.Datetime("us", EXCHANGE_TIMEZONE))
    column = pl.col(PUBLICATION_COLUMN)
    dtype = schema[PUBLICATION_COLUMN]
    if isinstance(dtype, pl.Datetime):
        return (column.dt.replace_time_zone(EXCHANGE_TIMEZONE) if dtype.time_zone is None
                else column.dt.convert_time_zone(EXCHANGE_TIMEZONE))
    return (column.cast(pl.Utf8).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
            .dt.replace_time_zone(EXCHANGE_TIMEZONE))


def _end_of_day(column: str, session_timezone: str) -> pl.Expr:
    """The last instant of a Moscow trading day, on the simulation's clock.

    Stamping the session here rather than at its open is what keeps the aggregate out of the day
    it describes: the engine's ``date < now`` filter then makes day D readable from D+1 onwards,
    which is the earliest anyone could have computed it.
    """
    return (pl.col(column).cast(pl.Datetime("us"))
            .dt.offset_by("1d").dt.offset_by("-1us")
            .dt.replace_time_zone(EXCHANGE_TIMEZONE)
            .dt.convert_time_zone(session_timezone))
