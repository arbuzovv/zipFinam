"""Mounting MOEX AlgoPack tables.

Two things here are worth more than the rest.

**The day has to add up.** AlgoPack publishes five-minute buckets and a daily backtest reads
sessions, so every column is collapsed on the way in -- and there is no single rule that is right
for all of them. A volume that averaged instead of summing, or a VWAP taken as the mean of its
buckets, is wrong by an amount no output would show. :class:`SessionAggregationTests` fixes the
arithmetic against numbers computed by hand.

**A session must not be readable inside itself.** The day's volume is not known until the day is
over, so day D's row is stamped at the end of D and the engine's ``date < now`` filter keeps it
out of D's own bars. :class:`PointInTimeTests` walks a simulation clock across the boundary and
checks both sides of it.

Nothing here touches the network: the transport is replaced with recorded rows shaped exactly as
the exchange publishes them -- bare ``tradedate``/``tradetime`` strings, ``ticker`` for the
instrument, and the columns the documentation lists.
"""
import asyncio
import datetime
import unittest
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from zipfinam.algopack import api
from zipfinam.algopack.algopack_data_source import (
    AlgoPackDataSource, Granularity, is_address, parse_address,
)
from zipfinam.algopack.catalog import BUCKET, DATASETS

MOSCOW = ZoneInfo("Europe/Moscow")
FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)
MISX = ExchangeInfo(mic="MISX", name="MOEX", canonical_name="MOEX", country_code="RU")
RTSX = ExchangeInfo(mic="RTSX", name="FORTS", canonical_name="FORTS", country_code="RU")
XNYS = ExchangeInfo(mic="XNYS", name="NYSE", canonical_name="NYSE", country_code="US")

WINDOW = (datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))


def rouble() -> Currency:
    return Currency(id=2, isin=None, asset_name="RUB", start_date=FAR_PAST, end_date=FAR_FUTURE,
                    first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)


def make_equity(sid: int, symbol: str, exchange: ExchangeInfo = MISX) -> ExchangeAsset:
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=exchange, asset=equity, quote=rouble())


def make_future(sid: int, symbol: str) -> ExchangeAsset:
    contract = FuturesContract(
        id=sid + 20_000, isin=None, asset_name=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
        first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, root_exchange_asset=None,
        root_asset=None, root_symbol=symbol[:2], notice_date=None,
        expiration_date=datetime.date(2027, 6, 18), multiplier=1, tick_size=1.0)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=RTSX, asset=contract, quote=rouble())


def bucket(day: str, time: str, **columns) -> dict:
    """One supercandle, with the shape the exchange publishes and the noise it carries."""
    row = dict(tradedate=day, tradetime=time, ticker="SBER",
               systime="2024-08-13 03:37:28",
               sec_pr_open=1, sec_pr_high=2, sec_pr_low=3, sec_pr_close=4)
    row.update(columns)
    return row


#: Two sessions of three buckets each, with round numbers so the day's totals can be read off.
#: Volumes 100/200/300 sum to 600; the VWAPs are 100, 101 and 102 against those weights.
TRADESTATS_ROWS = [
    bucket(day, time,
           pr_open=100.0 + i, pr_high=101.0 + i, pr_low=99.0 + i, pr_close=100.5 + i,
           pr_std=0.01, vol=100 * (i + 1), val=1000.0 * (i + 1), trades=5,
           trades_b=3, trades_s=2, val_b=600.0 * (i + 1), val_s=400.0 * (i + 1),
           vol_b=60 * (i + 1), vol_s=40 * (i + 1), disb=0.2,
           pr_vwap=100.0 + i, pr_vwap_b=100.2 + i, pr_vwap_s=99.8 + i, pr_change=0.1)
    for day in ("2024-01-09", "2024-01-10")
    for i, time in enumerate(("10:05:00", "10:10:00", "10:15:00"))
]


class Recorded:
    """Stands in for the transport, and records what was asked for."""

    def __init__(self, rows: list[dict] | dict[str, list[dict]]):
        self.rows = rows
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, dataset, market, secid, start, end):
        self.calls.append((market, dataset.name, secid))
        rows = self.rows if isinstance(self.rows, list) else self.rows.get(secid, [])
        return pl.from_dicts(rows, infer_schema_length=None) if rows else pl.DataFrame()


class AlgoPackTestCase(unittest.TestCase):
    """Replaces the transport for the duration of a test and never lets a test reach MOEX."""

    def setUp(self):
        self._real_fetch = api.fetch
        self.addCleanup(lambda: setattr(api, "fetch", self._real_fetch))

    def serve(self, rows) -> Recorded:
        recorded = Recorded(rows)
        api.fetch = recorded
        return recorded

    def mount(self, address: str, **kwargs) -> AlgoPackDataSource:
        kwargs.setdefault("start_date", WINDOW[0])
        kwargs.setdefault("end_date", WINDOW[1])
        kwargs.setdefault("session_timezone", "Europe/Moscow")
        return AlgoPackDataSource.mount(address, **kwargs)

    @staticmethod
    def load(source: AlgoPackDataSource, assets) -> pl.DataFrame:
        return asyncio.run(source.load(assets))


class AddressTests(unittest.TestCase):
    def test_an_address_names_a_market_and_a_dataset(self):
        market, dataset, granularity = parse_address("algopack://eq/tradestats")
        self.assertEqual((market, dataset.name, granularity),
                         ("eq", "tradestats", Granularity.DAILY))

    def test_the_market_is_optional_and_equities_are_assumed(self):
        market, dataset, _ = parse_address("algopack://obstats")
        self.assertEqual((market, dataset.name), ("eq", "obstats"))

    def test_a_granularity_can_be_named(self):
        _, _, granularity = parse_address("algopack://eq/tradestats/5min")
        self.assertIs(granularity, Granularity.INTRADAY)

    def test_a_granularity_without_a_market(self):
        market, dataset, granularity = parse_address("algopack://tradestats/5min")
        self.assertEqual((market, dataset.name, granularity),
                         ("eq", "tradestats", Granularity.INTRADAY))

    def test_things_that_are_not_addresses(self):
        for value in ("hf://ZipLime/congress-trading", "tradestats", "", 7, None):
            self.assertFalse(is_address(value), value)

    def test_an_address_naming_nothing_is_refused(self):
        for value in ("algopack://", "algopack://eq"):
            with self.assertRaises(ValueError):
                parse_address(value)

    def test_an_unknown_dataset_lists_the_known_ones(self):
        with self.assertRaises(ValueError) as caught:
            parse_address("algopack://eq/supercandles")
        self.assertIn("tradestats", str(caught.exception))

    def test_a_dataset_the_market_does_not_serve_is_refused(self):
        # Open interest is a futures product; asking for it on FX is a mistake worth catching at
        # the address rather than at a 404 half-way into a run.
        with self.assertRaises(ValueError):
            parse_address("algopack://fx/futoi")

    def test_an_unknown_granularity_is_refused(self):
        with self.assertRaises(ValueError):
            parse_address("algopack://eq/tradestats/weekly")


class SessionAggregationTests(AlgoPackTestCase):
    """The arithmetic that turns 78 buckets into a session."""

    def setUp(self):
        super().setUp()
        self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats")
        self.frame = self.load(source, [make_equity(19771, "SBER")])
        self.day = self.frame.row(0, named=True)

    def test_one_row_per_session(self):
        self.assertEqual(len(self.frame), 2)

    def test_volume_sums(self):
        self.assertEqual(self.day["vol"], 600)
        self.assertEqual(self.day["val"], 6000.0)
        self.assertEqual(self.day["trades"], 15)

    def test_prices_take_the_session_open_high_low_and_close(self):
        self.assertEqual(self.day["pr_open"], 100.0)
        self.assertEqual(self.day["pr_high"], 103.0)
        self.assertEqual(self.day["pr_low"], 99.0)
        self.assertEqual(self.day["pr_close"], 102.5)

    def test_vwap_is_weighted_by_volume(self):
        # (100*100 + 101*200 + 102*300) / 600. The mean of the three would be 101.0, which is the
        # number a naive aggregation produces and is wrong by a third of a rouble.
        self.assertAlmostEqual(self.day["pr_vwap"], 101.3333333, places=6)
        self.assertNotAlmostEqual(self.day["pr_vwap"], 101.0, places=3)

    def test_the_buy_sell_imbalance_is_recomputed_from_the_totals(self):
        # (360 - 240) / 600, not the average of three buckets that each read 0.2 because they
        # were published that way.
        self.assertAlmostEqual(self.day["disb"], 0.2, places=9)

    def test_the_session_return_runs_open_to_close(self):
        # 102.5 / 100.0 - 1, as a percentage. Summing the buckets' own changes would give 0.3.
        self.assertAlmostEqual(self.day["pr_change"], 2.5, places=9)

    def test_the_publisher_bookkeeping_is_dropped(self):
        for column in ("systime", "sec_pr_open", "tradedate", "tradetime", "ticker"):
            self.assertNotIn(column, self.frame.columns)

    def test_date_and_sid_lead_the_frame(self):
        self.assertEqual(self.frame.columns[:2], ["date", "sid"])


class PointInTimeTests(AlgoPackTestCase):
    """When a row becomes readable, which is the only thing that can silently ruin a backtest."""

    def setUp(self):
        super().setUp()
        self.serve(TRADESTATS_ROWS)
        self.sber = make_equity(19771, "SBER")

    def history(self, source, at: datetime.datetime) -> pl.DataFrame:
        return asyncio.run(source.get_data_by_limit(
            fields=frozenset({"vol"}), limit=10, end_date=at,
            frequency=datetime.timedelta(days=1), assets=frozenset({self.sber}),
            include_end_date=False))

    def test_a_session_is_invisible_during_itself(self):
        source = self.mount("algopack://eq/tradestats")
        # Midday on the 9th: the 9th has not finished, so its volume cannot be known.
        during = datetime.datetime(2024, 1, 9, 14, 0, tzinfo=MOSCOW)
        self.assertEqual(len(self.history(source, during)), 0)

    def test_a_session_is_visible_on_the_next_one(self):
        source = self.mount("algopack://eq/tradestats")
        next_morning = datetime.datetime(2024, 1, 10, 10, 0, tzinfo=MOSCOW)
        rows = self.history(source, next_morning)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows["vol"].to_list(), [600])

    def test_a_session_is_stamped_at_its_own_end(self):
        source = self.mount("algopack://eq/tradestats")
        frame = self.load(source, [self.sber])
        stamped = frame["date"].to_list()[0]
        self.assertEqual(stamped.astimezone(MOSCOW).date(), datetime.date(2024, 1, 9))
        self.assertEqual(stamped.astimezone(MOSCOW).hour, 23)

    def test_a_bucket_is_invisible_inside_itself(self):
        # tradetime labels the end of the bucket, so the row stamped 10:05 covers 10:00-10:05 and
        # a strategy standing at 10:05 must not have it yet.
        source = self.mount("algopack://eq/tradestats/5min")
        at_close_of_first = datetime.datetime(2024, 1, 9, 10, 5, tzinfo=MOSCOW)
        self.assertEqual(len(self.history(source, at_close_of_first)), 0)
        rows = self.history(source, datetime.datetime(2024, 1, 9, 10, 6, tzinfo=MOSCOW))
        self.assertEqual(rows["vol"].to_list(), [100])

    def test_intraday_keeps_the_buckets(self):
        source = self.mount("algopack://eq/tradestats/5min")
        frame = self.load(source, [self.sber])
        self.assertEqual(len(frame), 6)
        self.assertEqual(frame["vol"].to_list()[:3], [100, 200, 300])

    def test_a_simulation_in_another_timezone_reads_the_same_instant(self):
        """The label moves, the moment does not."""
        moscow = self.mount("algopack://eq/tradestats/5min")
        new_york = self.mount("algopack://eq/tradestats/5min", session_timezone="America/New_York")
        first_moscow = self.load(moscow, [self.sber])["date"].to_list()[0]
        first_new_york = self.load(new_york, [self.sber])["date"].to_list()[0]
        self.assertEqual(first_moscow, first_new_york)
        self.assertEqual(first_new_york.astimezone(MOSCOW).hour, 10)


class PublicationLagTests(AlgoPackTestCase):
    """`systime` is when MOEX wrote the row, and it is two different things at once.

    Measured against the exchange on 2026-09-27: a row published in the ordinary course carries a
    systime seconds after its bucket -- 7 to 20 for supercandles and open interest, 90 for
    MegaAlerts, and **six minutes** for `hi2`, which is longer than a bucket and therefore a real
    source of look-ahead if it is ignored. A rewritten row carries one days or months later:
    `fo/orderstats` had one 2.5 days after its session, the 2023 supercandles one from ten months
    on. Believing that second kind would make a 2023 backtest see nothing until the rebuild.
    """

    @staticmethod
    def published(day: str, time: str, systime: str | None, **columns) -> dict:
        row = bucket(day, time, vol=100, pr_close=100.0, **columns)
        if systime is None:
            row.pop("systime", None)
        else:
            row["systime"] = systime
        return row

    def stamps(self, rows, address="algopack://eq/tradestats/5min", **mount):
        self.serve(rows)
        source = self.mount(address, **mount)
        frame = self.load(source, [make_equity(19771, "SBER")])
        return [moment.astimezone(MOSCOW) for moment in frame["date"].to_list()], source

    def test_a_row_is_held_back_to_when_it_was_published(self):
        # hi2's own lag, on a table whose columns are simple enough to assert against.
        stamps, source = self.stamps(
            [self.published("2024-01-09", "18:40:00", "2024-01-09 18:46:22")])
        self.assertEqual(stamps[0].strftime("%H:%M:%S"), "18:46:22")
        self.assertEqual(source.mount_report["rows_held_to_publication"], 1)

    def test_a_rewritten_systime_is_not_believed(self):
        # The real one: 2023-10-10 supercandles carry a systime from 2024-08-13.
        stamps, source = self.stamps(
            [self.published("2024-01-09", "10:05:00", "2024-08-13 03:37:28")])
        self.assertEqual(stamps[0].strftime("%H:%M:%S"), "10:05:00",
                         "a rewrite must fall back to the close of the bucket")
        self.assertEqual(source.mount_report["rows_with_a_rewritten_systime"], 1)
        self.assertEqual(source.mount_report["rows_held_to_publication"], 0)

    def test_a_table_without_a_publication_time_still_works(self):
        stamps, _ = self.stamps([self.published("2024-01-09", "10:05:00", None)])
        self.assertEqual(stamps[0].strftime("%H:%M:%S"), "10:05:00")

    def test_an_intraday_row_is_invisible_until_it_was_published(self):
        """The look-ahead this closes: six minutes of it, on hi2."""
        self.serve([self.published("2024-01-09", "18:40:00", "2024-01-09 18:46:22")])
        source = self.mount("algopack://eq/tradestats/5min")
        sber = make_equity(19771, "SBER")

        def history(at):
            return asyncio.run(source.get_data_by_limit(
                fields=frozenset({"vol"}), limit=5, end_date=at,
                frequency=BUCKET, assets=frozenset({sber}), include_end_date=False))

        # The bucket closed at 18:40 but was not written until 18:46:22.
        self.assertEqual(len(history(datetime.datetime(2024, 1, 9, 18, 45, tzinfo=MOSCOW))), 0)
        self.assertEqual(len(history(datetime.datetime(2024, 1, 9, 18, 47, tzinfo=MOSCOW))), 1)

    def test_a_session_is_not_moved_by_a_publication_inside_its_own_day(self):
        """Daily is stamped at the end of the day, which is later than any same-day publication."""
        stamps, _ = self.stamps(
            [self.published("2024-01-09", "18:40:00", "2024-01-09 18:46:22")],
            address="algopack://eq/tradestats")
        self.assertEqual(stamps[0].strftime("%H:%M:%S.%f"), "23:59:59.999999")

    def test_a_session_published_after_midnight_is_stamped_after_midnight(self):
        stamps, _ = self.stamps(
            [self.published("2024-01-09", "23:50:00", "2024-01-10 00:12:00")],
            address="algopack://eq/tradestats")
        self.assertEqual(stamps[0].date(), datetime.date(2024, 1, 10))
        self.assertEqual(stamps[0].strftime("%H:%M:%S"), "00:12:00")


class InstrumentTests(AlgoPackTestCase):
    """Which instruments are asked about, and which are quietly left alone."""

    def test_each_listing_is_fetched_once_and_reused(self):
        recorded = self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats")
        sber = make_equity(19771, "SBER")
        self.load(source, [sber])
        self.load(source, [sber])
        self.assertEqual(recorded.calls, [("eq", "tradestats", "SBER")])

    def test_an_instrument_the_exchange_has_nothing_for_is_not_asked_about_twice(self):
        recorded = self.serve([])
        source = self.mount("algopack://eq/tradestats")
        unknown = make_equity(999, "NOPE")
        self.load(source, [unknown])
        self.load(source, [unknown])
        self.assertEqual(len(recorded.calls), 1)
        self.assertEqual(source.mount_report["instruments_empty"], 1)

    def test_a_read_that_matches_nothing_says_so_out_loud(self):
        """The deployed image runs at WARNING, where every other line here is filtered out.

        A mount matching no instrument returns an empty frame, a strategy returns early on
        one, and the backtest then finishes successfully having traded nothing -- with the
        reason recorded nowhere. This is the one line that has to survive.
        """
        # structlog, not stdlib logging: the connector's logger writes through structlog's
        # own pipeline, so assertLogs sees nothing at all.
        from structlog.testing import capture_logs
        self.serve([])
        source = self.mount("algopack://eq/tradestats")
        with capture_logs() as caught:
            self.load(source, [make_equity(7, "AAPL", XNYS), make_equity(999, "NOPE")])
        warnings = [entry for entry in caught if entry.get("log_level") == "warning"]
        self.assertTrue(any("matched no instrument" in entry.get("event", "")
                            for entry in warnings), f"nothing warned; got {caught}")

    def test_a_successful_read_does_not_warn(self):
        from structlog.testing import capture_logs
        self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats")
        with capture_logs() as caught:
            self.load(source, [make_equity(19771, "SBER")])
        self.assertEqual([entry["event"] for entry in caught
                          if entry.get("log_level") == "warning"], [])

    def test_a_listing_outside_moscow_is_skipped_rather_than_requested(self):
        recorded = self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats")
        self.load(source, [make_equity(7, "AAPL", XNYS)])
        self.assertEqual(recorded.calls, [])
        self.assertEqual(source.mount_report["instruments_skipped"], 1)

    def test_rows_outside_the_mounted_window_are_dropped(self):
        """The fetch works in whole years; the mount must not keep one.

        Both sessions come back from the exchange, but a mount that starts on the 10th has no
        business carrying the 9th -- at five-minute granularity that is megabytes per instrument
        of rows the simulation cannot reach.
        """
        self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats", start_date=datetime.date(2024, 1, 10))
        frame = self.load(source, [make_equity(19771, "SBER")])
        days = [stamp.astimezone(MOSCOW).date() for stamp in frame["date"].to_list()]
        self.assertEqual(days, [datetime.date(2024, 1, 10)])

    def test_only_the_requested_fields_survive(self):
        self.serve(TRADESTATS_ROWS)
        source = self.mount("algopack://eq/tradestats", fields=["vol", "pr_close"])
        frame = self.load(source, [make_equity(19771, "SBER")])
        self.assertEqual(sorted(frame.columns), ["date", "pr_close", "sid", "vol"])


HI2_ROWS = [
    dict(tradedate="2024-01-09", tradetime="18:40:00", ticker="SBER", metric=metric, value=value,
         reference=None, systime="2024-01-09 18:45:46")
    for metric, value in (("hhi_volume", 115), ("hhi_sell", 194), ("hhi_agressive", 96))
]

FUTOI_ROWS = [
    dict(sess_id=7028, seqnum=57, tradedate="2024-01-09", tradetime="23:50:00", ticker="SR",
         clgroup=group, pos=position, pos_long=position + 10, pos_short=position - 10,
         pos_long_num=5, pos_short_num=9, systime="2024-01-10 16:22:46")
    for group, position in (("YUR", -5104), ("FIZ", 5104))
]


class LongFormatTests(AlgoPackTestCase):
    """Tables that publish one row per measurement have to become columns to be readable."""

    def test_hi2_metrics_become_columns(self):
        self.serve(HI2_ROWS)
        source = self.mount("algopack://eq/hi2")
        frame = self.load(source, [make_equity(19771, "SBER")])
        self.assertEqual(len(frame), 1)
        for metric in ("hhi_volume", "hhi_sell", "hhi_agressive"):
            self.assertIn(metric, frame.columns)
        self.assertEqual(frame["hhi_volume"].to_list(), [115])
        # Free text that means something different per metric, so it cannot be a column.
        self.assertNotIn("reference", frame.columns)

    def test_open_interest_splits_by_client_group(self):
        self.serve(FUTOI_ROWS)
        source = self.mount("algopack://fo/futoi")
        frame = self.load(source, [make_future(101, "SRM7")])
        self.assertEqual(frame["pos_fiz"].to_list(), [5104])
        self.assertEqual(frame["pos_yur"].to_list(), [-5104])

    def test_open_interest_is_the_root_and_reaches_every_contract_on_it(self):
        recorded = self.serve(FUTOI_ROWS)
        source = self.mount("algopack://fo/futoi")
        frame = self.load(source, [make_future(101, "SRM7"), make_future(102, "SRU7")])
        # One request for the root, and both contracts carry its answer.
        self.assertEqual(recorded.calls, [("fo", "futoi", "SR")])
        self.assertEqual(sorted(frame["sid"].to_list()), [101, 102])

    def test_the_root_code_keeps_the_exchange_own_case(self):
        # MOEX writes the dollar-rouble root as `Si`, not `SI`, and `SI` is not a code it knows.
        recorded = self.serve({"Si": FUTOI_ROWS})
        source = self.mount("algopack://fo/futoi")
        frame = self.load(source, [make_future(201, "SiM7")])
        self.assertEqual(recorded.calls, [("fo", "futoi", "Si")])
        self.assertEqual(len(frame), 1)

    def test_open_interest_ignores_anything_that_is_not_a_futures_contract(self):
        # SBER's first two letters are a real futures root. Taking them would attach Sberbank
        # futures open interest to the ordinary share, which is why the asset type is checked.
        recorded = self.serve(FUTOI_ROWS)
        source = self.mount("algopack://fo/futoi")
        self.load(source, [make_equity(19771, "SBER")])
        self.assertEqual(recorded.calls, [])


class ChunkCacheTests(unittest.TestCase):
    """Instrument-months are fetched once and kept; the month in progress never is."""

    def setUp(self):
        import contextlib
        import tempfile
        self.cache = tempfile.TemporaryDirectory()
        self.addCleanup(self.cache.cleanup)
        previous = api.os.environ.get(api.CACHE_DIR_ENV)
        api.os.environ[api.CACHE_DIR_ENV] = self.cache.name

        def restore():
            if previous is None:
                api.os.environ.pop(api.CACHE_DIR_ENV, None)
            else:
                api.os.environ[api.CACHE_DIR_ENV] = previous
        self.addCleanup(restore)

        # Chunking is what is under test, so the transport is replaced entirely -- including the
        # client, which a fully cached read must never open.
        self.real_fetch_rows, self.real_client = api._fetch_rows, api.client
        self.addCleanup(lambda: setattr(api, "_fetch_rows", self.real_fetch_rows))
        self.addCleanup(lambda: setattr(api, "client", self.real_client))
        self.requested: list[tuple[datetime.date, datetime.date]] = []
        self.clients_opened = 0

        @contextlib.contextmanager
        def counted():
            self.clients_opened += 1
            yield object()

        def recorded(dataset, market, secid, start, end, opened=None):
            self.requested.append((start, end))
            return [bucket("2024-01-09", "10:05:00", vol=1)]

        api.client = counted
        api._fetch_rows = recorded

    def test_a_finished_month_is_fetched_whole_and_then_cached(self):
        dataset = DATASETS["tradestats"]
        api.fetch(dataset, "eq", "SBER", datetime.date(2024, 1, 10), datetime.date(2024, 1, 20))
        # The whole month, although eleven days were asked for: that is what makes the chunk
        # worth keeping for the next run.
        self.assertEqual(self.requested,
                         [(datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))])
        api.fetch(dataset, "eq", "SBER", datetime.date(2024, 1, 5), datetime.date(2024, 1, 8))
        self.assertEqual(len(self.requested), 1, "the second read should come from the cache")

    def test_a_short_window_does_not_pay_for_the_year_around_it(self):
        """Reading by year meant a fortnight fetched 56 pages to use five."""
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2024, 6, 3), datetime.date(2024, 6, 14))
        self.assertEqual(self.requested,
                         [(datetime.date(2024, 6, 1), datetime.date(2024, 6, 30))])

    def test_a_cached_read_opens_no_connection(self):
        dataset = DATASETS["tradestats"]
        api.fetch(dataset, "eq", "SBER", datetime.date(2024, 2, 1), datetime.date(2024, 2, 29))
        self.assertEqual(self.clients_opened, 1)
        api.fetch(dataset, "eq", "SBER", datetime.date(2024, 2, 1), datetime.date(2024, 2, 29))
        self.assertEqual(self.clients_opened, 1, "a fully cached read must not open a socket")

    def test_the_month_still_running_is_read_for_the_window_only_and_not_cached(self):
        """The fast validity check runs in it, so paying for the whole month every time is waste.

        Reading by year was worse still: 56,000 rows re-read on every request, because a year
        that has not finished can never be cached.
        """
        today = datetime.date.today()
        asked = (today.replace(day=1), min(today, today.replace(day=28)))
        api.fetch(DATASETS["tradestats"], "eq", "SBER", *asked)
        self.assertEqual(self.requested, [(asked[0], min(asked[1], today))])
        api.fetch(DATASETS["tradestats"], "eq", "SBER", *asked)
        self.assertEqual(len(self.requested), 2, "a month still running must be re-read")

    def test_nothing_in_the_future_is_requested(self):
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2099, 1, 1), datetime.date(2099, 12, 31))
        self.assertEqual(self.requested, [])


class TransportTests(unittest.TestCase):
    """The seam with ``moexalgo``, driven for real with only the HTTP call replaced.

    Everything above this replaces :func:`api.fetch` wholesale, which leaves the part most likely
    to break on a library upgrade untested: how the REST path is assembled, how the token reaches
    the request, how pages are walked and how the exchange's columns are renamed. So these drive
    the library's own fetchers and stub one method -- the one that would open a socket.

    Skipped where ``moexalgo`` is not installed, which is every deployment outside the Russian
    contour.
    """

    def setUp(self):
        import tempfile
        __import__("pytest").importorskip(
            "moexalgo", reason="the AlgoPack transport is installed on the RU image only")

        self.seen: list[tuple[str, dict]] = []
        self.answer = self.supercandles
        self.install_stub()

        cache = tempfile.TemporaryDirectory()
        self.addCleanup(cache.cleanup)
        api.os.environ[api.CACHE_DIR_ENV] = cache.name
        self.addCleanup(lambda: api.os.environ.pop(api.CACHE_DIR_ENV, None))

        previous_token = api._TOKEN
        api.set_token("a-subscription-key")
        self.addCleanup(lambda: api.set_token(previous_token))

    def install_stub(self):
        """Replace the socket, and nothing above it.

        The connector issues requests through the library's configured client but not through
        `Session.get_objects`, whose unconditional 0.2s sleep costs eleven seconds per
        instrument-year. So the seam under test is `httpx_cli.get`, which is also the honest
        place to stand in for the network.
        """
        import httpx
        from moexalgo.session import Client

        original = Client.__init__
        outer = self

        def patched(self, sync=True, **options):
            original(self, sync=sync, **options)
            real_get = self.httpx_cli.get

            def get(url, params=None, **kwargs):
                outer.seen.append((url, dict(params or {})))
                outer.base_url = str(self.options.get("base_url"))
                outer.authorization = [value for name, value
                                       in (self.options.get("headers") or [])
                                       if name == "Authorization"]
                body = outer.answer(url, dict(params or {}))
                return httpx.Response(200, json=body,
                                      headers={"content-type": "application/json"},
                                      request=httpx.Request("GET", url))

            self.httpx_cli.get = get
            self._real_get = real_get

        Client.__init__ = patched
        self.addCleanup(lambda: setattr(Client, "__init__", original))

    @staticmethod
    def supercandles(url, params):
        columns = ["tradedate", "tradetime", "SECID", "pr_close", "vol", "SYSTIME"]
        if params.get("start", 0) >= 2:
            return {"data": {"columns": columns, "data": []}}
        return {"data": {"columns": columns,
                         "data": [["2024-01-09", "10:05:00", "SBER", 100.5, 100,
                                   "2024-08-13 03:37:28"],
                                  ["2024-01-09", "10:10:00", "SBER", 101.5, 200,
                                   "2024-08-13 03:37:28"]]}}

    def test_supercandles_are_read_from_the_market_path_with_the_key_attached(self):
        frame = api.fetch(DATASETS["tradestats"], "eq", "SBER",
                          datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        url, params = self.seen[0]
        self.assertEqual(url, "datashop/algopack/eq/tradestats/SBER.json")
        self.assertEqual((params["from"], params["till"]), ("2024-01-01", "2024-01-31"))
        # A key switches the library to the metered gateway; without one it silently reads the
        # public ISS, which serves none of this.
        self.assertEqual(self.base_url, "https://apim.moex.com/iss")
        self.assertEqual(self.authorization, ["Bearer a-subscription-key"])
        # SECID and SYSTIME come back lowercased, and SECID renamed -- which is why the shaping
        # code looks for `ticker` on every table.
        self.assertIn("ticker", frame.columns)
        self.assertEqual(len(frame), 2)

    def test_pages_are_walked_until_one_comes_back_short(self):
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        self.assertEqual([params["start"] for _, params in self.seen], [0])

    def test_a_window_is_read_one_month_at_a_time(self):
        """The month is the unit, and a short window must not pay for a year.

        Reading by year meant a fortnight's backtest fetched 56 pages to use five, and it could
        never cache the year in progress -- which is the window the fast validity check runs in.
        """
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2024, 1, 20), datetime.date(2024, 3, 10))
        windows = [(params["from"], params["till"]) for _, params in self.seen]
        self.assertEqual(windows, [("2024-01-01", "2024-01-31"),
                                   ("2024-02-01", "2024-02-29"),
                                   ("2024-03-01", "2024-03-31")],
                         "each finished month is fetched whole, so the chunk is worth caching")

    def test_open_interest_is_read_from_the_analytical_product(self):
        api.fetch(DATASETS["futoi"], "fo", "Si",
                  datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        self.assertEqual(self.seen[0][0], "analyticalproducts/futoi/securities/Si.json")

    def test_open_interest_is_sliced_by_time_because_it_cannot_be_paged(self):
        """The failure that first hung, then silently truncated, the live runs.

        The exchange answers every `start` with the same first page. Paging by offset therefore
        never terminates, and stopping at the repeat drops everything past the first thousand
        rows -- which arrive newest-first, so what disappears is the beginning of the window.
        The only safe read is one where no answer ever reaches the page ceiling.
        """
        def by_window(url, params):
            days = (datetime.date.fromisoformat(params["till"])
                    - datetime.date.fromisoformat(params["from"])).days + 1
            # 700 rows a session: one day fits inside a page, two days do not.
            rows = [["2024-01-09", "23:50:00", "Si", "FIZ", n] for n in range(700 * days)]
            return {"futoi": {"columns": ["tradedate", "tradetime", "ticker", "clgroup", "pos"],
                              "data": rows[:api._FUTOI_PAGE_ROWS]}}

        self.answer = by_window
        rows = api._fetch_rows(DATASETS["futoi"], "fo", "Si",
                               datetime.date(2024, 1, 1), datetime.date(2024, 1, 5))
        asked = [(params["from"], params["till"], params.get("start")) for _, params in self.seen]
        self.assertEqual({offset for _, _, offset in asked}, {0},
                         "every request must start at zero; the offset is not honoured")
        self.assertEqual([begins for begins, ends, _ in asked if begins == ends],
                         ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
                         "a full page must narrow the slice and the window stay covered")
        self.assertEqual(len(rows), 5 * 700, "nothing may be dropped or counted twice")

    def test_a_cached_month_never_reaches_the_transport_again(self):
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        requests = len(self.seen)
        api.fetch(DATASETS["tradestats"], "eq", "SBER",
                  datetime.date(2024, 1, 10), datetime.date(2024, 1, 20))
        self.assertEqual(len(self.seen), requests)

    def test_a_request_cannot_outlast_the_backtest_that_made_it(self):
        """The regression that returned nothing from a deployed run.

        moexalgo builds its client with `timeout=300`, which multiplied by the retries let one
        stalled request run for fifteen minutes -- past the container's own 600s execution
        timeout, so the backtest answered with neither a result nor an error.
        """
        from moexalgo.session import Client
        seen = []
        made = Client.__init__

        def noting(client, sync=True, **options):
            made(client, sync=sync, **options)
            seen.append(client.options.get("timeout"))

        Client.__init__ = noting
        self.addCleanup(lambda: setattr(Client, "__init__", made))
        with api.client():
            pass
        self.assertEqual(seen, [api.DEFAULT_HTTP_TIMEOUT])
        self.assertLessEqual(api.DEFAULT_HTTP_TIMEOUT * api._RETRIES, 120,
                             "worst case for one request has to stay well inside one backtest")

    def test_several_instruments_are_read_at_once_over_one_client(self):
        """The lever that made a universe usable: the pages cannot widen, so the names must."""
        import threading
        seen_threads = set()
        clients = []
        original = self.answer

        def note(url, params):
            seen_threads.add(threading.get_ident())
            return original(url, params)

        self.answer = note
        from moexalgo.session import Client
        made = Client.__init__

        def counting(client, sync=True, **options):
            made(client, sync=sync, **options)
            clients.append(client)

        Client.__init__ = counting
        self.addCleanup(lambda: setattr(Client, "__init__", made))

        api.os.environ[api.CONCURRENCY_ENV] = "4"
        self.addCleanup(lambda: api.os.environ.pop(api.CONCURRENCY_ENV, None))
        frames = api.fetch_many(DATASETS["tradestats"], "eq", ["SBER", "GAZP", "LKOH", "MTSS"],
                                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        self.assertEqual(sorted(frames), ["GAZP", "LKOH", "MTSS", "SBER"])
        self.assertGreater(len(seen_threads), 1, "the instruments must not be read in sequence")
        self.assertEqual(len(clients), 1, "one client for the whole read, one TLS handshake")


class TokenTests(unittest.TestCase):
    def setUp(self):
        self.previous = api._TOKEN
        self.addCleanup(lambda: api.set_token(self.previous))
        for name in api.TOKEN_ENV:
            api.os.environ.pop(name, None)

    def test_a_missing_key_says_what_to_set(self):
        api.set_token(None)
        with self.assertRaises(api.AlgoPackUnauthorized) as caught:
            api.token()
        self.assertIn("ALGOPACK_API", str(caught.exception))

    def test_a_key_handed_over_is_used_even_with_an_empty_environment(self):
        """The host pushes the key in, because by mount time the environment has been emptied."""
        api.set_token("a-key")
        self.assertEqual(api.token(), "a-key")


if __name__ == "__main__":
    unittest.main()
