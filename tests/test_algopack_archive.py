"""The history archive between the local month cache and the exchange.

What matters: inside the span the archive answers for, the exchange is not asked at all -- not
for a month the archive holds, and not for a month it says has no rows; outside the span, or for
an instrument the archive never carried, it is asked exactly as before; and an archive that cannot
be read costs time, never the read. The archive here is a directory behind a ``file://`` URL, laid
out as the real one is.
"""
import datetime
import json
import tempfile
import unittest
from pathlib import Path

import polars as pl

from zipfinam.algopack import api, archive
from zipfinam.algopack.catalog import DATASETS

TRADESTATS = DATASETS["tradestats"]


def bucket(tradedate: str, close: float) -> dict:
    return {"ticker": "SBER", "tradedate": tradedate, "tradetime": "10:05:00", "pr_close": close,
            "vol": 10, "systime": f"{tradedate} 10:05:10"}


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.store, self.cache = self.root / "archive", self.root / "cache"
        chunk = self.store / "eq" / "tradestats" / "SBER" / "2024-03.parquet"
        chunk.parent.mkdir(parents=True)
        pl.DataFrame([bucket("2024-03-04", 280.0), bucket("2024-03-29", 300.0)]).write_parquet(chunk)
        (self.store / "manifest.json").write_text(json.dumps({"version": 1, "datasets": {
            "eq/tradestats": {"from": "2024-01", "through": "2024-04",
                              "instruments": {"SBER": ["2024-03"]}}}}))
        archive.set_archive(self.store.as_uri())

        self.asked: list[tuple[str, datetime.date, datetime.date]] = []

        def exchange(dataset, market, secid, start, end, opened=None):
            self.asked.append((secid, start, end))
            return [bucket(start.isoformat(), 1.0) | {"ticker": secid}]

        self._saved = (api._fetch_rows, api.cache_dir)
        api._fetch_rows = exchange
        api.cache_dir = lambda: self.cache
        self.cache.mkdir()

    def tearDown(self):
        api._fetch_rows, api.cache_dir = self._saved
        archive.set_archive(None)

    def read(self, secid, start, end):
        return api.fetch(TRADESTATS, "eq", secid, datetime.date.fromisoformat(start),
                         datetime.date.fromisoformat(end), opened=object())

    def test_a_month_the_archive_holds_is_read_from_it(self):
        rows = self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertEqual(rows["pr_close"].to_list(), [280.0, 300.0])
        self.assertEqual(self.asked, [])

    def test_it_is_kept_in_the_local_cache(self):
        self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertTrue((self.cache / "eq" / "tradestats" / "SBER" / "2024-03.parquet").exists())

    def test_a_month_the_archive_answers_for_without_rows_asks_nobody(self):
        rows = self.read("SBER", "2024-01-01", "2024-02-29")
        self.assertEqual(rows.height, 0)
        self.assertEqual(self.asked, [])

    def test_months_after_the_archive_go_to_the_exchange(self):
        self.read("SBER", "2024-03-01", "2024-05-31")
        self.assertEqual([start.isoformat() for _, start, _ in self.asked], ["2024-05-01"])

    def test_an_instrument_the_archive_never_carried_goes_to_the_exchange(self):
        self.read("GAZP", "2024-03-01", "2024-03-31")
        self.assertEqual([secid for secid, _, _ in self.asked], ["GAZP"])

    def test_a_dataset_the_archive_lacks_goes_to_the_exchange(self):
        api.fetch(DATASETS["obstats"], "eq", "SBER", datetime.date(2024, 3, 1),
                  datetime.date(2024, 3, 31), opened=object())
        self.assertEqual(len(self.asked), 1)

    def test_an_unreadable_archive_falls_back_to_the_exchange(self):
        archive.set_archive((self.root / "nowhere").as_uri())
        rows = self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertEqual(len(self.asked), 1)
        self.assertEqual(rows.height, 1)

    def test_a_broken_chunk_falls_back_to_the_exchange(self):
        (self.store / "eq" / "tradestats" / "SBER" / "2024-03.parquet").write_bytes(b"not parquet")
        self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertEqual(len(self.asked), 1)

    def test_headers_are_asked_for_on_every_request(self):
        calls = []
        archive.set_archive(self.store.as_uri(), headers=lambda: calls.append(1) or {})
        self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertEqual(len(calls), 2)  # the manifest and the chunk

    def test_several_instruments_from_the_archive_need_no_key(self):
        def refuse():
            raise api.AlgoPackUnauthorized("no key")

        saved = api.client
        api.client = refuse
        try:
            rows = api.fetch_many(TRADESTATS, "eq", ["SBER", "SBER"], datetime.date(2024, 3, 1),
                                  datetime.date(2024, 4, 30))
        finally:
            api.client = saved
        self.assertEqual(rows["SBER"].height, 2)
        self.assertEqual(self.asked, [])

    def test_no_archive_configured_changes_nothing(self):
        archive.set_archive(None)
        self.read("SBER", "2024-03-01", "2024-03-31")
        self.assertEqual(len(self.asked), 1)


if __name__ == "__main__":
    unittest.main()
