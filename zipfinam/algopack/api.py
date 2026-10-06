"""Reading MOEX AlgoPack over its REST API: in months, in parallel, and without the politeness.

``moexalgo`` supplies the address, the bearer token and the Russian root certificate; everything
about *how* the reading happens is here, because the library's own loop is built for a notebook
asking one question and a backtest is not that.

Three things were making a universe unusable, and each is measured rather than assumed.

**The page is 1000 rows and cannot be made bigger.** The OpenAPI spec pins ``limit`` at a maximum
of 1000, so a Moscow session -- 202 five-minute buckets, 07:00 to 23:50 -- is a fifth of a page,
and an instrument-*year* is 56 pages. Nothing here can change that; everything else is about not
making it worse.

**The library sleeps 0.2s before every request.** ``Session.get_objects`` does it unconditionally,
which is right for anonymous ISS and pure waste on a metered key: eleven seconds of sleeping per
instrument-year. So the request is issued through the library's own configured client -- same base
URL, same ``Authorization`` header, same TLS context -- but not through that method.

**Instruments were read one after another.** :func:`fetch_many` fans them out across a small
thread pool over one pooled client, so ten names cost about what two used to.

Why the month is the unit
-------------------------

AlgoPack is a live API with no commit to pin to, so reproducibility has to be built. Reads are cut
into calendar months and each finished month is cached to Parquet. The month is not an arbitrary
choice between the day and the year:

* a **year** was the first attempt and is wrong for short windows -- a two-week backtest paid for
  56 pages to use five -- and it can never cache the year in progress, which is exactly the window
  the fast validity check runs in, so that check re-read 56,000 rows on every request;
* a **day** would cache nothing reusable and pay a round trip per session;
* a **month** is about 4,400 rows, five pages: small enough that a short window fetches only what
  it needs, and coarse enough that a long one reuses whole months between runs. Only the month in
  progress is uncacheable, and it is one month rather than one year.
"""
import concurrent.futures
import datetime
import os
import re
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

import polars as pl
import structlog

from zipfinam.algopack.catalog import Dataset, path_for

_logger = structlog.get_logger(__name__)

#: Where chunks are kept. ``/tmp`` is the only writable place on the deployed containers, and the
#: images set this explicitly; the default is for a developer's machine.
CACHE_DIR_ENV = "ALGOPACK_CACHE_DIR"

#: Environment variables the subscription key is read from, in order. ``ALGOPACK_API`` is what the
#: deployment calls it; ``ALGOPACK_TOKEN`` is accepted because it is what the exchange's own
#: examples call it.
TOKEN_ENV = ("ALGOPACK_API", "ALGOPACK_TOKEN")

#: How many instruments are read at once. The exchange is the constraint, not the CPU: each worker
#: is a thread blocked on a socket, so this is set by what MOEX tolerates rather than by the
#: machine. **Four**, because that is the number actually measured end to end: 2.8x on a live
#: universe with no 429 anywhere in the log. Eight was tried on the deployed container and the
#: request never came back -- the exchange appears to stall surplus connections rather than
#: refuse them, which is far harder to see than a 429. Raise it per deployment once a live run
#: has shown the wider setting returning; :data:`DELAY_ENV` is the lever in the other direction.
CONCURRENCY_ENV = "ALGOPACK_MAX_CONCURRENCY"
DEFAULT_CONCURRENCY = 4

#: Seconds to wait between requests on one worker. Zero: the library's 0.2s was politeness for
#: anonymous ISS, and a metered key is the thing being paid for. Raise it if MOEX starts refusing.
DELAY_ENV = "ALGOPACK_REQUEST_DELAY"

#: Seconds one request may take. The library's own default is **300**, which is a sane figure for
#: a notebook and a disaster inside a backtest: multiplied by the retries below it let a single
#: stalled request consume fifteen minutes, past the container's own 600s execution timeout, and
#: a deployed run came back with nothing at all. A read that has not answered in half a minute is
#: not going to; failing it fast and loudly beats hanging the simulation.
HTTP_TIMEOUT_ENV = "ALGOPACK_HTTP_TIMEOUT"
DEFAULT_HTTP_TIMEOUT = 30.0

#: Stop writing chunks once the cache reaches this many bytes. Yandex caps a container's temporary
#: files at 512 MB and the bundles are already in the image, so a long backtest over a wide
#: universe must not be able to fill the disk out from under the run. Measured: an instrument-year
#: of supercandles is 56,296 rows and 2.7 MB of Parquet. Reads keep working; only the writing
#: stops, so the backtest gets slower rather than failing.
CACHE_BUDGET_BYTES = 256 * 1024 * 1024

#: Rows in one page. The OpenAPI spec gives ``limit`` a maximum of 1000, so this is the exchange's
#: number rather than a choice. A short page is the last one.
PAGE_ROWS = 1000

#: Pages to walk for one instrument-month before giving up. A month is about five; this exists
#: only so a server that stops advancing cannot spin forever.
_MAX_PAGES = 200

#: Attempts per request when the exchange pushes back, and the pause before a retry. Dropping the
#: library's blanket sleep means meeting 429 honestly rather than pretending it cannot happen.
#: Two rather than three, because the worst case is what matters: attempts multiply the timeout
#: above, and the product has to stay well inside one backtest.
_RETRIES = 2
_RETRY_PAUSE = 2.0

#: Rows in one page of open interest, which is a different endpoint with a different problem --
#: see :func:`_walk_open_interest`.
_FUTOI_PAGE_ROWS = 1000

#: Days of open interest to ask for at once. It is published on the same five-minute grid as
#: everything else and split across two client groups, so a session is about 400 rows and two
#: days fit inside a page with room to spare. Narrowed automatically if that stops being true.
_FUTOI_SLICE_DAYS = 2

#: Anything that is not a plain instrument code. The value reaches a filesystem path.
_UNSAFE_IN_PATH = re.compile(r"[^A-Za-z0-9_.-]")

_TOKEN: str | None = None
_CACHE_FULL_REPORTED = False


class AlgoPackUnavailable(RuntimeError):
    """``moexalgo`` is not installed, so nothing can be read from AlgoPack."""

    def __init__(self) -> None:
        super().__init__(
            "Reading MOEX AlgoPack needs the moexalgo package:\n"
            "    pip install moexalgo\n"
            "It is an optional dependency of ziplime, so installs that never read AlgoPack -- "
            "every deployment outside the Russian market -- do not carry it.")


class AlgoPackUnauthorized(RuntimeError):
    """No subscription key, or the exchange refused the one it was given."""


class AlgoPackUnreachable(RuntimeError):
    """The exchange could not be reached at all -- a route problem, not a key problem."""


# --------------------------------------------------------------------------- the key

def set_token(token: str | None) -> None:
    """Supply the AlgoPack subscription key.

    Called by the host before a simulation starts rather than read from the environment when a
    dataset is first touched, and that ordering is the point: the backtesting service empties its
    secrets out of ``os.environ`` for as long as user code is running, because a strategy may
    import and ``from os import environ as e`` walks straight past a validator. Mounting happens
    on the first read, which is inside that window -- so by then the key is only here.
    """
    global _TOKEN
    _TOKEN = token or None
    if not _TOKEN:
        return
    try:
        # The library reads this global when it builds a client, and switches to the metered
        # gateway (apim.moex.com) on the strength of it. Setting it here rather than passing it
        # per call is the only interface it offers.
        _session().TOKEN = _TOKEN
    except AlgoPackUnavailable:
        # Being handed a key on a build with no connector is not itself a failure -- the host
        # supplies it unconditionally. The key is kept, and the missing package is reported at
        # the first read, where it is actually in the way.
        _logger.debug("Kept an AlgoPack key, but moexalgo is not installed")


def token() -> str:
    """The key, from :func:`set_token` or from the environment.

    Raises:
        AlgoPackUnauthorized: Neither is set. Without a key the exchange serves only delayed
            reference data and answers every AlgoPack path with 403, so failing here -- before a
            window's worth of requests -- says what is wrong far more clearly than the HTTP error
            would.
    """
    if _TOKEN:
        return _TOKEN
    for name in TOKEN_ENV:
        value = os.environ.get(name)
        if value:
            set_token(value)
            return value
    raise AlgoPackUnauthorized(
        f"MOEX AlgoPack needs a subscription key. Set {TOKEN_ENV[0]} in the environment, or call "
        f"zipfinam.algopack.api.set_token(...). Keys are issued with a "
        f"subscription at https://data.moex.com.")


def _session():
    """``moexalgo.session``, with a clear error when the package is missing."""
    try:
        from moexalgo import session
    except ImportError as error:
        raise AlgoPackUnavailable() from error
    return session


# --------------------------------------------------------------------------- transport

@contextmanager
def client():
    """One pooled HTTP client, for a whole read rather than for a request.

    The library opens and closes one per call, which on a universe means a TLS handshake per
    instrument. Held open here instead and shared across the worker threads -- ``httpx.Client`` is
    safe for that -- so the handshake is paid once.
    """
    token()  # refuse before opening anything rather than collect a page of 403s
    with _session().Session(timeout=_http_timeout()) as opened:
        yield opened


def _http_timeout() -> float:
    try:
        return max(1.0, float(os.environ.get(HTTP_TIMEOUT_ENV, "") or DEFAULT_HTTP_TIMEOUT))
    except ValueError:
        return DEFAULT_HTTP_TIMEOUT


def _delay() -> float:
    try:
        return max(0.0, float(os.environ.get(DELAY_ENV, "") or 0.0))
    except ValueError:
        return 0.0


def _looks_like_no_route(error: Exception) -> bool:
    text = f"{type(error).__name__}: {error}"
    return any(hint in text for hint in ("ConnectError", "ConnectTimeout", "getaddrinfo",
                                         "Name or service not known", "nodename",
                                         "Temporary failure in name resolution"))


def _request(opened, path: str, params: dict, block: str = "data") -> list[dict]:
    """One request, through the library's client but not through its sleep.

    ``Session.get_objects`` waits at least 0.2 seconds before every call. That is the right
    manners for anonymous ISS and eleven seconds of nothing per instrument-year on a metered key,
    so the request goes to the same configured client directly: same base URL, same bearer header,
    same Russian root certificate, no wait.

    Returns:
        The block's rows as dicts, with the library's own column normalisation -- lowercased, and
        ``SECID`` renamed to ``ticker``, which is what every table is keyed on downstream.
    """
    from moexalgo.utils import json, normalize_data

    url = "/".join(part for part in path.split("/") if part.strip()) + ".json"
    pause = _delay()
    for attempt in range(_RETRIES):
        if pause:
            time.sleep(pause)
        try:
            response = opened.httpx_cli.get(url, params=params)
        except Exception as error:
            if _looks_like_no_route(error):
                raise AlgoPackUnreachable(
                    f"Could not reach the exchange at {opened.options.get('base_url')}. This is "
                    f"the route rather than the key -- a deployment outside Russia usually has "
                    f"none. Underlying error: {type(error).__name__}: {error}") from error
            if attempt + 1 == _RETRIES:
                raise
            _logger.warning("An AlgoPack request failed; retrying", url=url,
                            error=f"{type(error).__name__}: {error}", attempt=attempt + 1,
                            timeout_seconds=_http_timeout())
            pause = max(pause, _RETRY_PAUSE)
            continue

        if response.status_code in (401, 403):
            raise AlgoPackUnauthorized(
                f"MOEX refused the AlgoPack key for {url} ({response.status_code}). The key may "
                f"have expired, or the subscription may not cover this dataset.")
        if response.status_code == 429 or response.status_code >= 500:
            if attempt + 1 == _RETRIES:
                response.raise_for_status()
            # Backing off rather than pretending it cannot happen: this connector goes as fast as
            # the exchange allows, so it has to be the thing that notices when that is too fast.
            _logger.info("The exchange pushed back; retrying", status=response.status_code,
                         url=url, attempt=attempt + 1)
            pause = max(pause, _RETRY_PAUSE * (attempt + 1))
            continue
        if not response.is_success:
            response.raise_for_status()
        if not response.headers.get("content-type", "").startswith("application/json"):
            raise AlgoPackUnauthorized(
                f"The exchange answered {url} with {response.headers.get('content-type')!r} "
                f"rather than JSON, which is how it refuses an unsubscribed dataset.")

        payload = json.loads(response.text)
        table = payload.get(block) if payload else None
        return list(normalize_data(table)) if table else []
    return []


# --------------------------------------------------------------------------- the cache

def cache_dir() -> Path | None:
    """The chunk cache, created on demand, or ``None`` when it cannot be written to.

    A read-only filesystem is not a reason to fail: it costs the run its cache, not its data.
    """
    path = Path(os.environ.get(CACHE_DIR_ENV) or (Path.home() / ".cache" / "algopack"))
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        _logger.warning("AlgoPack cache directory is not writable; every read will hit the API",
                        path=str(path), error=str(error))
        return None
    return path


def _cache_file(market: str, dataset: str, secid: str, month: datetime.date) -> Path | None:
    base = cache_dir()
    if base is None:
        return None
    safe = _UNSAFE_IN_PATH.sub("_", secid)
    return base / market / dataset / safe / f"{month:%Y-%m}.parquet"


def _cache_has_room(base: Path) -> bool:
    global _CACHE_FULL_REPORTED
    used = sum(f.stat().st_size for f in base.rglob("*.parquet") if f.is_file())
    if used < CACHE_BUDGET_BYTES:
        return True
    if not _CACHE_FULL_REPORTED:
        _CACHE_FULL_REPORTED = True
        _logger.warning(
            "AlgoPack cache is full, so further chunks are read from the API every time",
            used_mb=round(used / 1024 / 1024), budget_mb=CACHE_BUDGET_BYTES // 1024 // 1024,
            detail="Raise ALGOPACK_CACHE_DIR to a larger volume, or narrow the window.")
    return False


def _read_chunk(path: Path | None) -> pl.DataFrame | None:
    if path is None or not path.exists():
        return None
    try:
        return pl.read_parquet(path)
    except Exception as error:  # a truncated file is a cache miss, not a failure
        _logger.warning("Discarding an unreadable AlgoPack cache file", path=str(path),
                        error=str(error))
        path.unlink(missing_ok=True)
        return None


def _write_chunk(path: Path | None, frame: pl.DataFrame) -> None:
    if path is None or not _cache_has_room(path.parents[3]):
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Written beside the target and moved into place, so a process that dies mid-write leaves
        # no half-file for the next run to read. The pid and the object id keep two workers apart.
        staging = path.with_suffix(f".{os.getpid()}.{id(frame)}.tmp")
        frame.write_parquet(staging)
        staging.replace(path)
    except Exception as error:
        _logger.warning("Could not cache an AlgoPack chunk", path=str(path), error=str(error))


# --------------------------------------------------------------------------- months

def _months(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """The first day of each calendar month the window touches."""
    months, cursor = [], start.replace(day=1)
    while cursor <= end:
        months.append(cursor)
        cursor = _month_end(cursor) + datetime.timedelta(days=1)
    return months


def _month_end(month: datetime.date) -> datetime.date:
    following = (month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return following - datetime.timedelta(days=1)


# --------------------------------------------------------------------------- reading

def _fetch_rows(dataset: Dataset, market: str, secid: str, start: datetime.date,
                end: datetime.date, opened=None) -> list[dict]:
    """One range of one instrument, straight from the exchange."""
    if opened is None:
        with client() as fresh:
            return _fetch_rows(dataset, market, secid, start, end, fresh)

    path = path_for(market, dataset)
    if dataset.name == "futoi":
        return _walk_open_interest(opened, path, secid, start, end)

    window = {"from": start.isoformat(), "till": end.isoformat()}
    rows: list[dict] = []
    for _ in range(_MAX_PAGES):
        page = _request(opened, f"{path}/{dataset.name}/{secid}", dict(window, start=len(rows)))
        rows.extend(page)
        if len(page) < PAGE_ROWS:
            return rows  # a short or empty page is the last one
    _logger.warning("An AlgoPack read hit the page limit and may be truncated",
                    dataset=f"{market}/{dataset.name}", secid=secid, rows=len(rows),
                    window=f"{start}..{end}")
    return rows


def _walk_open_interest(opened, path: str, secid: str, start: datetime.date,
                        end: datetime.date) -> list[dict]:
    """Read open interest in slices of time, because it cannot be paged by offset.

    This endpoint is the odd one out and it took a live run to find out how. ``moexalgo``'s own
    helpers always pass ``limit=-1``, which puts its fetcher into a single-page mode its author
    named ``wa`` -- a workaround. Reading that as "the caller did not want paging" and passing a
    large limit instead turns the workaround off; the loop then asks for ``start=1000``, and the
    exchange answers with **the first page again**, forever. A backtest that mounted
    ``algopack://fo/futoi`` never finished.

    Stopping at the repeat is not enough either: it silently truncates. Open interest comes back
    newest-first, so a week asked for in one request returns the most recent two and a half days
    and drops the rest, with nothing to say so.

    So the window is cut into slices small enough that one page holds each of them, and a slice
    that comes back full is retried narrower -- the only way to be sure nothing was dropped is for
    no answer ever to reach the ceiling.
    """
    rows: list[dict] = []
    span = _FUTOI_SLICE_DAYS
    slice_start = start
    while slice_start <= end:
        slice_end = min(end, slice_start + datetime.timedelta(days=span - 1))
        page = _request(opened, f"{path}/{secid}",
                        {"from": slice_start.isoformat(), "till": slice_end.isoformat(),
                         "start": 0},
                        block="futoi")
        if len(page) >= _FUTOI_PAGE_ROWS and span > 1:
            # A full page means rows were dropped. Narrow and ask again for the same days; once
            # narrowed, stay narrow for the rest of the read.
            span = 1
            continue
        if len(page) >= _FUTOI_PAGE_ROWS:
            _logger.warning(
                "A single session of AlgoPack open interest filled a page, so it may be "
                "truncated", secid=secid, session=slice_start.isoformat(), rows=len(page),
                detail="The endpoint cannot be paged by offset, so a session that does not fit "
                       "in one page cannot be read in full.")
        rows.extend(page)
        slice_start = slice_end + datetime.timedelta(days=1)
    return rows


def fetch(dataset: Dataset, market: str, secid: str, start: datetime.date,
          end: datetime.date, opened=None) -> pl.DataFrame:
    """Every row AlgoPack holds for one instrument between two dates.

    Served from the month cache where it can be, and from the API where it cannot. The frame is
    exactly what the exchange published, with the library's lowercased column names and no
    interpretation: turning it into something a simulation can read is
    :class:`~zipfinam.algopack.algopack_data_source.AlgoPackDataSource`'s job.

    Args:
        dataset: Which table, from the catalogue.
        market: ``eq``, ``fo`` or ``fx``.
        secid: The exchange's own code -- a ticker, or for ``futoi`` a two-letter futures root.
        start, end: Inclusive bounds, in Moscow calendar days.
        opened: A client to reuse. One is opened for the call when omitted.

    Returns:
        The rows, or an empty frame when the exchange holds none. An instrument AlgoPack does not
        cover is not an error: it has no supercandles, and a strategy asking about a universe that
        includes one should see nothing for it rather than fail.
    """
    today = datetime.date.today()
    chunks: list[pl.DataFrame] = []
    with ExitStack() as stack:
        # Opened on the first month that actually needs the network. A read served entirely from
        # the cache -- the second run of any backtest -- then makes no connection at all.
        def transport():
            nonlocal opened
            if opened is None:
                opened = stack.enter_context(client())
            return opened

        chunks = _read_months(dataset, market, secid, start, end, today, transport)

    chunks = [chunk for chunk in chunks if chunk.height]
    if not chunks:
        return pl.DataFrame()
    # `diagonal_relaxed`: a month in which a column was never populated comes back typed Null, and
    # a month from before the exchange added a column does not carry it at all.
    return chunks[0] if len(chunks) == 1 else pl.concat(chunks, how="diagonal_relaxed")


def _read_months(dataset: Dataset, market: str, secid: str, start: datetime.date,
                 end: datetime.date, today: datetime.date, transport) -> list[pl.DataFrame]:
    chunks: list[pl.DataFrame] = []
    for month in _months(start, end):
        month_last = _month_end(month)
        wanted_from, wanted_to = max(start, month), min(end, month_last)
        if wanted_from > wanted_to:
            continue

        path = _cache_file(market, dataset.name, secid, month)
        cached = _read_chunk(path)
        if cached is not None:
            chunks.append(cached)
            continue

        # A finished month is fetched whole even when a shorter slice was asked for, because the
        # chunk is then worth keeping. The month still running cannot be cached -- its last
        # session has not happened yet -- so it fetches the window and only the window.
        finished = month_last < today
        if finished:
            fetch_from, fetch_to = month, month_last
        else:
            fetch_from, fetch_to = wanted_from, min(wanted_to, today)
        if fetch_to < fetch_from:
            continue  # a month the exchange cannot have data for yet

        rows = _fetch_rows(dataset, market, secid, fetch_from, fetch_to, transport())
        frame = pl.from_dicts(rows, infer_schema_length=None) if rows else pl.DataFrame()
        if finished:
            _write_chunk(path, frame)
        chunks.append(frame)
    return chunks


def _concurrency() -> int:
    try:
        return max(1, int(os.environ.get(CONCURRENCY_ENV, "") or DEFAULT_CONCURRENCY))
    except ValueError:
        return DEFAULT_CONCURRENCY


def fetch_many(dataset: Dataset, market: str, secids, start: datetime.date,
               end: datetime.date) -> dict[str, pl.DataFrame]:
    """The same as :func:`fetch`, for several instruments at once.

    This is what makes a universe affordable. Every worker is a thread waiting on a socket rather
    than on a core, so the useful width is set by what the exchange tolerates and not by the
    machine -- see :data:`DEFAULT_CONCURRENCY`. One client is shared across all of them, so the
    TLS handshake is paid once for the whole read instead of once per instrument.

    An instrument that fails takes the read down with it, deliberately: a universe quietly missing
    a name because one request timed out is a worse outcome than a backtest that stops and says so.
    """
    secids = list(dict.fromkeys(secids))
    if not secids:
        return {}
    if len(secids) == 1:
        return {secids[0]: fetch(dataset, market, secids[0], start, end)}

    began = time.monotonic()
    width = min(_concurrency(), len(secids))
    results: dict[str, pl.DataFrame] = {}
    with client() as opened:
        with concurrent.futures.ThreadPoolExecutor(max_workers=width) as pool:
            futures = {pool.submit(fetch, dataset, market, secid, start, end, opened): secid
                       for secid in secids}
            for future in concurrent.futures.as_completed(futures):
                results[futures[future]] = future.result()
    _logger.info("Read AlgoPack for several instruments", dataset=f"{market}/{dataset.name}",
                 instruments=len(secids), workers=width,
                 rows=sum(frame.height for frame in results.values()),
                 seconds=round(time.monotonic() - began, 1))
    return results
