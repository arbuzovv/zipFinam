"""One runner for the MOEX margined-option examples.

It assembles a single bundle: real SBER daily bars from your gRPC source, and a synthetic chain of
weekly options on top of them, written to MOEX's conventions -- margined, European, contract size
1, and natively named ``SR310CI6``.

The report **deliberately carries no return**: the option prices are generated, and a Sharpe ratio
computed from them would be a statement about the generator. What it shows instead is what the run
*did* -- how many series passed, how many contracts were opened, how much initial margin was tied
up, and how the positions settled at expiry.
"""
import datetime
import importlib.util
import os
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

from moex_config import (  # noqa: E402
    ASSET_DB_PATH, ATM_DAILY_VOLUME, EMISSION_RATE, EXPIRY_WEEKDAY, FAR_REACH, FAR_STEP,
    INITIAL_MARGIN_RATE,
    LIFE_SESSIONS, MAINTENANCE_MARGIN_RATE, NEAR_REACH, NEAR_STEP, SESSIONS_BACK,
    STARTING_CASH,
    TRADING_CALENDAR, UNDERLYING, UNDERLYING_ROOT,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.data_sources.options.ingest import build_option_bundle  # noqa: E402
from ziplime.data.data_sources.options.synthetic import (  # noqa: E402
    ChainSpec, LiquidityModel, SyntheticOptionChainSource,
)
from ziplime_grpc_data_source.moex import MOEX  # noqa: E402
from ziplime.data.services import frame_cache  # noqa: E402
from ziplime.finance.commission import PerOptionContract  # noqa: E402
from ziplime.finance.margin import FixedRateFuturesMarginModel  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import (  # noqa: E402
    FixedBasisPointsSlippage,
)
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

STRATEGY_DIR = Path(__file__).parent / "strategies"
PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_env() -> None:
    """Read the repository's ``.env`` without overwriting what the environment already sets."""
    path = PROJECT_ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def grpc_endpoint(url: str) -> str:
    """Reduce the address to the ``host:port`` form gRPC expects.

    ``.env`` carries a URL with a scheme, and gRPC does not understand one: it tries to resolve
    ``https`` as a hostname, and the failure reads as a DNS error, which points the wrong way.
    This normalisation belongs in ``GrpcDataSource.from_env``; until it lives there, it lives
    here.
    """
    url = url.strip().rstrip("/")
    for scheme in ("https://", "http://", "grpcs://", "grpc://"):
        if url.startswith(scheme):
            url = url[len(scheme):]
    return url if ":" in url else f"{url}:443"


def list_strategies() -> list[dict]:
    return [load_strategy_info(path) for path in sorted(STRATEGY_DIR.glob("m[0-9][0-9]_*.py"))]


def load_strategy_info(path: Path) -> dict:
    spec = importlib.util.spec_from_file_location(f"_moex_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info["path"] = str(path)
    return info


def window(sessions_back: int) -> tuple[datetime.date, datetime.date]:
    """The last ``sessions_back`` completed sessions of the XMOS calendar."""
    calendar = get_calendar(TRADING_CALENDAR)
    today = datetime.date.today()
    available = [s.date() for s in calendar.sessions_in_range(
        today - datetime.timedelta(days=sessions_back * 2), today) if s.date() < today]
    if len(available) < 10:
        raise SystemExit(f"Only {len(available)} XMOS sessions are available.")
    return available[-sessions_back], available[-1]


def expiries_in(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """The monthly expiries in the window: third Wednesdays, as MOEX lists them.

    Wednesday rather than Friday -- that is where the cycle ends. Monthlies only: a weekly series'
    code carries a week suffix whose rule does not follow from the data observed, and generating
    them would produce three series under one name. See :func:`format_moex_symbol`.
    """
    calendar = get_calendar(TRADING_CALENDAR)
    return [s.date() for s in calendar.sessions_in_range(start, end)
            if s.date().weekday() == EXPIRY_WEEKDAY and 15 <= s.date().day <= 21]


async def load_underlying(asset_service):
    symbol, mic = UNDERLYING
    listing = await asset_service.get_exchange_asset_by_symbol(
        symbol=AssetSymbol(symbol=symbol, mic=mic), asset_type=AssetType.EQUITY)
    if listing is None:
        raise SystemExit(f"{symbol}@{mic} is not in the asset database.")
    return listing


async def underlying_bars(listing, start: datetime.date, end: datetime.date) -> pl.DataFrame:
    """Real SBER daily bars over gRPC, stamped on the XMOS session closes."""
    load_env()
    token, server = os.environ.get("GRPC_TOKEN"), os.environ.get("GRPC_SERVER_URL")
    if not token or not server:
        raise SystemExit(
            "The MOEX examples need GRPC_TOKEN and GRPC_SERVER_URL in the repository's .env: "
            "the underlying here is real, and only the option chain is synthetic.")

    calendar = get_calendar(TRADING_CALENDAR)
    key = frame_cache.cache_key("moex-underlying", listing.sid, start, end)
    cached = frame_cache.load(key, max_age=datetime.timedelta(hours=12))
    if cached is not None:
        return cached

    from ziplime_grpc_data_source_private.grpc_data_source import GrpcDataSource

    source = GrpcDataSource(authorization_token=token, server_url=grpc_endpoint(server))
    frame = await source.get_data(
        symbols=[f"{listing.symbol}@{listing.mic}"], frequency=datetime.timedelta(days=1),
        date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
    if frame.is_empty():
        raise SystemExit(f"gRPC returned no bars for {listing.symbol}.")

    # The bars arrive stamped with a date; the simulation clock runs on session closes, so each
    # bar is moved onto the close of its own session. Without this the bars miss the grid and the
    # bundle reads as empty to the engine.
    closes = {session.date(): close.to_pydatetime() for session, close in calendar.schedule.loc[
        calendar.sessions_in_range(start, end), "close"].dt.tz_convert(calendar.tz).items()}
    frame = frame.with_columns(
        pl.col("date").dt.convert_time_zone(str(calendar.tz)).dt.date().alias("_d")
    ).filter(pl.col("_d").is_in(list(closes)))
    frame = frame.with_columns(
        pl.col("_d").map_elements(closes.__getitem__,
                                  return_dtype=pl.Datetime(time_unit="us",
                                                           time_zone=str(calendar.tz))).alias("date"),
        pl.lit(listing.sid).cast(pl.Int64).alias("sid"),
    ).drop("_d").sort("date")
    data = frame.select(["date", "sid", "symbol", "mic", "open", "high", "low", "close",
                         "price", "volume"])
    frame_cache.store(key, data)
    return data


async def build_bundle(asset_service, start: datetime.date, end: datetime.date):
    """Real SBER bars, plus a synthetic weekly MOEX chain written over them."""
    calendar = get_calendar(TRADING_CALENDAR)
    listing = await load_underlying(asset_service)
    bars = await underlying_bars(listing, start, end)

    closes = {session.date(): close.to_pydatetime() for session, close in calendar.schedule.loc[
        calendar.sessions_in_range(start, end), "close"].dt.tz_convert(calendar.tz).items()}

    source = SyntheticOptionChainSource(
        underlying_symbol=listing.symbol, mic=listing.mic,
        underlying_bars=bars, session_closes=closes,
        venue=MOEX, root=UNDERLYING_ROOT,
        # The series is weekly: listed about five sessions before its Wednesday and traded
        # throughout. Zero here would mean 0DTE, and a contract would exist for one day.
        life_sessions=LIFE_SESSIONS,
        chain=ChainSpec(near_step=NEAR_STEP, near_reach=NEAR_REACH,
                        far_step=FAR_STEP, far_reach=FAR_REACH),
        liquidity=LiquidityModel(atm_volume=ATM_DAILY_VOLUME))

    # MOEX series are weekly, while the generator lists a chain on every session by default --
    # which is a form of 0DTE and wrong for MOEX. It is handed only the Wednesdays here, so a
    # contract lives from its own Wednesday to the next, as it actually does.
    expiries = expiries_in(start, end)
    if not expiries:
        raise SystemExit(f"No expiry Wednesday falls in {start}..{end}.")

    bundle, option_listings = await build_option_bundle(
        source=source, asset_service=asset_service, underlying=listing,
        underlying_bars=bars, sessions=expiries,
        timestamps=bars["date"], trading_calendar=calendar, emission_rate=EMISSION_RATE)
    return bundle, listing, option_listings, source, expiries


async def run_strategy(info: dict):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    start, end = window(info.get("sessions_back", SESSIONS_BACK))
    bundle, underlying, option_listings, source, expiries = await build_bundle(
        asset_service, start, end)

    calendar = get_calendar(TRADING_CALENDAR)
    try:
        result = await run_simulation(
            start_date=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
            end_date=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz),
            trading_calendar=TRADING_CALENDAR, algorithm_file=info["path"],
            total_cash=STARTING_CASH, market_data_source=bundle, custom_data_sources=[],
            emission_rate=EMISSION_RATE, benchmark_returns=None, benchmark_asset_symbol=None,
            stop_on_error=True, asset_service=asset_service,
            option_commission=PerOptionContract(cost=1.0, exchange_fee=0.5),
            option_slippage=FixedBasisPointsSlippage(),
            # A margined position ties up initial margin on both sides, and without a margin
            # model the strategy would take leverage no broker would have given it.
            futures_margin_model=FixedRateFuturesMarginModel(
                initial_rate=INITIAL_MARGIN_RATE, maintenance_rate=MAINTENANCE_MARGIN_RATE),
            max_leverage=info.get("max_leverage", 5.0),
            same_bar_execution=True, price_used_in_order_execution="close", print_algo=False)
        return result, {"underlying": underlying, "contracts": len(option_listings),
                        "expiries": expiries, "source": source, "window": (start, end)}
    finally:
        await asset_service._asset_repository.engine.dispose()


def summarise(info: dict, result, context: dict) -> dict:
    """What the run did. Not what it earned -- see the module docstring."""
    perf = result.perf
    transactions = [t for row in perf["transactions"] for t in row]
    options = [t for t in transactions if hasattr(t.asset.asset, "strike")]
    start, end = context["window"]
    return {
        "name": info["name"],
        "description": info["description"],
        "window": f"{start}..{end}",
        "sessions": len(perf),
        "expiries": len(context["expiries"]),
        "contracts_listed": context["contracts"],
        "option_trades": len(options),
        "lots": sum(abs(t.amount) for t in options),
        "peak_margin": peak_margin(result),
        "errors": list(result.errors or []),
    }


def peak_margin(result) -> float:
    """The most initial margin the run tied up, if the strategy recorded it.

    That is what limits the size of a margined position -- not premium received, of which there is
    none here.
    """
    log = getattr(result.trading_algorithm, "margin_log", None) or []
    return max((row["margin"] for row in log), default=0.0)
