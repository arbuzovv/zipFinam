"""The whole backtest path on ziplime 2.x, with Finam's gRPC answers faked.

Only the network is replaced: the stubs return what the Trade API would, and everything after
that -- building the instrument list, the bars, the bundle on XMOS and the simulation -- is the
real code the examples run.
"""
import asyncio
import contextlib
import datetime
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service, ingest_assets, ingest_market_data
from ziplime.core.run_simulation import run_simulation
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.calendar_utils import get_calendar

import zipfinam  # noqa: F401  (adds the algopack:// scheme)
from zipfinam.algopack import api
from zipfinam.finam import grpc_asset_data_source, grpc_data_source
from zipfinam.finam.grpc_asset_data_source import GrpcAssetDataSource
from zipfinam.finam.grpc_data_source import GrpcDataSource

MOSCOW = ZoneInfo("Europe/Moscow")
SYMBOLS = ["SBER", "GAZP", "LKOH"]
PRICES = {"SBER": 280.0, "GAZP": 130.0, "LKOH": 7000.0, "GMKN": 120.0, "ROSN": 450.0,
          "NVTK": 1000.0, "TATN": 600.0, "MTSS": 220.0, "IMOEX": 2800.0}
STRATEGY = Path(__file__).parents[1] / "examples" / "algorithms" / "buy_and_hold_moex.py"
READS_ALGOPACK = Path(__file__).parent / "strategies" / "reads_algopack.py"
FLOW_TILT = Path(__file__).parents[1] / "examples" / "algorithms" / "algopack" / "flow_tilt.py"


def _decimal(value: float):
    return types.SimpleNamespace(value=str(value))


class _AssetsStub:
    def __init__(self, channel):
        pass

    async def AllAssets(self, request, metadata=None):
        assets = [types.SimpleNamespace(ticker=s, mic="MISX", is_archived=False, isin=f"RU000{s}", type="EQUITIES",
                                        id=str(i)) for i, s in enumerate(PRICES)]
        assets.append(types.SimpleNamespace(ticker="SiZ5", mic="MISX", is_archived=False, type="FUTURES",
                                            isin="", id="99"))
        return types.SimpleNamespace(assets=assets, next_cursor=0)

    async def Exchanges(self, request, metadata=None):
        return types.SimpleNamespace(exchanges=[types.SimpleNamespace(name="Moscow Exchange",
                                                                      mic="MISX")])


class _MarketDataStub:
    def __init__(self, channel):
        pass

    async def Bars(self, request, metadata=None):
        ticker = request.symbol.split("@")[0]
        start = request.interval.start_time.ToDatetime(tzinfo=datetime.timezone.utc)
        end = request.interval.end_time.ToDatetime(tzinfo=datetime.timezone.utc)
        sessions = get_calendar("XMOS").sessions_in_range(start.date(), end.date())
        bars = []
        for i, session in enumerate(sessions):
            price = PRICES[ticker] * (1 + 0.001 * i)
            stamp = datetime.datetime.combine(session.date(), datetime.time(), MOSCOW)
            bars.append(types.SimpleNamespace(
                timestamp=types.SimpleNamespace(seconds=int(stamp.timestamp())),
                open=_decimal(price), high=_decimal(price * 1.01), low=_decimal(price * 0.99),
                close=_decimal(price), volume=_decimal(1_000_000)))
        return types.SimpleNamespace(bars=bars)


async def _token(self):
    return "token"


def fake_finam():
    """Every Finam call the sources make, answered locally."""
    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch.object(grpc_asset_data_source.assets_service_pb2_grpc,
                                          "AssetsServiceStub", _AssetsStub))
    stack.enter_context(mock.patch.object(grpc_data_source.marketdata_service_pb2_grpc,
                                          "MarketDataServiceStub", _MarketDataStub))
    stack.enter_context(mock.patch.object(GrpcAssetDataSource, "get_token", _token))
    stack.enter_context(mock.patch.object(GrpcDataSource, "get_token", _token))
    stack.enter_context(mock.patch.dict("os.environ", {"GRPC_TOKEN": "x"}))
    return stack


class BacktestPipelineTests(unittest.TestCase):

    def test_a_moscow_strategy_runs_end_to_end_on_finam_data(self):
        result = self._backtest(STRATEGY)

        self.assertFalse(result.errors, result.errors)
        perf = result.perf
        self.assertGreater(len(perf), 30)
        # Invested on the first bar, in prices that rise 0.1% a session. Some cash is left over:
        # whole shares of LKOH cost 7,000 roubles each.
        self.assertGreater(perf["portfolio_value"].iloc[-1], 100_000)
        self.assertLess(perf["ending_cash"].iloc[-1], 100_000 * 0.10)

    def test_a_strategy_reads_algopack_by_address(self):
        asked = []

        def fetch(dataset, market, secid, start, end):
            asked.append((market, dataset.name, secid))
            sessions = get_calendar("XMOS").sessions_in_range(start, end)
            return pl.from_dicts([
                dict(tradedate=str(s.date()), tradetime="10:05:00", ticker=secid,
                     systime=f"{s.date()} 10:05:20", pr_open=1.0, pr_high=1.0, pr_low=1.0,
                     pr_close=1.0, vol=100, val=100.0, disb=round(0.01 * i, 2))
                for i, s in enumerate(sessions)])

        with mock.patch.object(api, "fetch", fetch):
            result = self._backtest(READS_ALGOPACK)

        self.assertFalse(result.errors, result.errors)
        self.assertEqual({call[:3] for call in asked}, {("eq", "tradestats", "SBER")})
        disb = result.perf["disb"].dropna()
        self.assertGreater(len(disb), 30)
        # Each session reads what was published by the one before: never today's own row.
        self.assertTrue((disb.diff().dropna() > 0).all())

    def test_the_order_flow_example_buys_what_is_being_bought(self):
        buying = {"SBER", "GAZP"}

        def fetch(dataset, market, secid, start, end):
            sessions = get_calendar("XMOS").sessions_in_range(start, end)
            vol_b, vol_s = (70, 30) if secid in buying else (30, 70)
            return pl.from_dicts([
                dict(tradedate=str(s.date()), tradetime="10:05:00", ticker=secid,
                     systime=f"{s.date()} 10:05:20", pr_open=1.0, pr_high=1.0, pr_low=1.0,
                     pr_close=1.0, vol=100, val=1000.0, vol_b=vol_b, vol_s=vol_s,
                     val_b=10.0 * vol_b, val_s=10.0 * vol_s,
                     disb=(vol_b - vol_s) / (vol_b + vol_s))
                for s in sessions])

        def fetch_many(dataset, market, secids, start, end):
            return {secid: fetch(dataset, market, secid, start, end) for secid in secids}

        with mock.patch.object(api, "fetch", fetch), \
                mock.patch.object(api, "fetch_many", fetch_many):
            result = self._backtest(FLOW_TILT)

        self.assertFalse(result.errors, result.errors)
        held = {position.asset.symbol for day in result.perf["positions"] for position in day}
        self.assertEqual(held, buying)
        self.assertLess(result.perf["ending_cash"].iloc[-1], 100_000 * 0.10)

    def _backtest(self, strategy: Path):
        with tempfile.TemporaryDirectory() as tmp, fake_finam():
            return asyncio.run(self._run(Path(tmp), strategy))

    async def _run(self, tmp: Path, strategy: Path):
        asset_service = get_asset_service(db_path=str(tmp / "assets.sqlite"), clear_asset_db=True)
        await ingest_assets(asset_service=asset_service,
                            asset_data_source=GrpcAssetDataSource(authorization_token="x",
                                                                  server_url="localhost:1"))
        listings = await asset_service.get_exchange_assets_by_symbols(
            symbols=[AssetSymbol(symbol=s, mic="MISX") for s in PRICES],
            asset_type=AssetType.EQUITY)
        self.assertTrue(all(listings), listings)

        start = datetime.datetime(2025, 1, 9, tzinfo=MOSCOW)
        end = datetime.datetime(2025, 3, 31, tzinfo=MOSCOW)
        await ingest_market_data(
            start_date=start, end_date=end, symbols=[f"{s}@MISX" for s in PRICES],
            trading_calendar="XMOS", bundle_name="finam_daily",
            data_bundle_source=GrpcDataSource(authorization_token="x", server_url="localhost:1"),
            data_frequency=datetime.timedelta(days=1), asset_service=asset_service,
            bundle_storage_path=str(tmp / "data"), assets=listings)

        bundle, _missing = await get_bundle_service(str(tmp / "data")).load_bundle(
            bundle_name="finam_daily", bundle_version=None, frequency=datetime.timedelta(days=1),
            start_date=start, end_date=end, assets=listings,
            aggregations=[pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
                          pl.col("close").last(), pl.col("volume").sum(),
                          pl.col("symbol").last()])
        return await run_simulation(
            start_date=datetime.datetime(2025, 2, 3, tzinfo=MOSCOW),
            end_date=datetime.datetime(2025, 3, 28, 23, 59, tzinfo=MOSCOW),
            trading_calendar="XMOS", algorithm_file=str(strategy), total_cash=100_000.0,
            market_data_source=bundle, custom_data_sources=[],
            emission_rate=datetime.timedelta(days=1), benchmark_asset_symbol="SBER@MISX",
            stop_on_error=True, asset_service=asset_service, print_algo=False)


if __name__ == "__main__":
    unittest.main()
