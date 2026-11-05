"""A read-only archive of AlgoPack history, consulted between the local cache and the exchange.

The exchange's API is metered, pages at 1000 rows and costs ~25 s per instrument-year, so a
backtest over years of a universe spends most of its time downloading history that never changes.
The history also exists as MOEX's own yearly exports. Converted once into the month cache's own
layout and put in object storage, they serve that history in one request per instrument-month::

    <base url>/manifest.json
    <base url>/<market>/<dataset>/<secid>/<YYYY-MM>.parquet

A chunk is exactly what :func:`zipfinam.algopack.api.fetch` would have cached after asking the
exchange for that month -- the same columns, the same spellings -- so a strategy cannot tell which
of the three a row came from, and the address stays ``algopack://...``.

The manifest says, per ``market/dataset``, which months the archive answers for (``from`` ..
``through``) and which months it holds for each instrument. Inside that span an instrument it
lists is answered by the archive alone: a month it does not hold has no rows, and the exchange is
not asked. An instrument it does not list, and every month outside the span, go to the exchange
as before. Anything that goes wrong reading the archive -- no route, a refused token, a corrupt
file -- falls back to the exchange too, with one warning: the archive saves time, it is never the
reason a backtest fails.

The host configures it with :func:`set_archive` before the simulation starts, for the same reason
the key is pushed rather than read: the backtesting service empties its environment while user
code runs. ``ALGOPACK_ARCHIVE_URL`` is read as a fallback.
"""
import datetime
import io
import json
import os
import threading
import urllib.error
import urllib.request
from typing import Callable

import polars as pl
import structlog

_logger = structlog.get_logger(__name__)

#: Where the archive lives, when the host has not called :func:`set_archive`.
ARCHIVE_URL_ENV = "ALGOPACK_ARCHIVE_URL"

#: Seconds to wait for one object. A chunk is a few hundred kilobytes; the manifest under a MB.
TIMEOUT_SECONDS = 30.0

_lock = threading.Lock()
_url: str | None = None
_headers: Callable[[], dict] | None = None
_manifest: dict | None = None
_manifest_loaded = False
_disabled_reason: str | None = None


def set_archive(url: str | None, headers: Callable[[], dict] | None = None) -> None:
    """Point the connector at an archive, or turn it off with ``None``.

    Args:
        url: The archive's base URL (``https://storage.yandexcloud.net/<bucket>/v1``, or
            ``file:///...`` for a local copy).
        headers: Called before each request for the headers to send -- a bearer token for a
            private bucket. Called per request, so a token that expires can be refreshed.
    """
    global _url, _headers, _manifest, _manifest_loaded, _disabled_reason
    with _lock:
        _url = url.rstrip("/") if url else None
        _headers = headers
        _manifest, _manifest_loaded, _disabled_reason = None, False, None


def base_url() -> str | None:
    if _url:
        return _url
    value = os.environ.get(ARCHIVE_URL_ENV, "").strip()
    return value.rstrip("/") or None


def _get(path: str) -> bytes:
    url = f"{base_url()}/{path}"
    request = urllib.request.Request(url, headers=(_headers() if _headers else {}) or {})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return response.read()


def _disable(reason: str, **details) -> None:
    global _disabled_reason
    if _disabled_reason is None:
        _disabled_reason = reason
        _logger.warning("The AlgoPack archive is unavailable; reading from the exchange instead",
                        reason=reason, url=base_url(), **details)


def manifest() -> dict | None:
    """The archive's manifest, read once per process; ``None`` when there is no archive."""
    global _manifest, _manifest_loaded
    if not base_url() or _disabled_reason:
        return None
    if _manifest_loaded:
        return _manifest
    with _lock:
        if not _manifest_loaded:
            try:
                _manifest = json.loads(_get("manifest.json"))["datasets"]
            except Exception as error:
                _manifest = None
                _disable("manifest unreadable", error=f"{type(error).__name__}: {error}")
            _manifest_loaded = True
    return _manifest


def chunk(market: str, dataset: str, secid: str, safe_secid: str,
          month: datetime.date) -> pl.DataFrame | None:
    """One instrument-month from the archive.

    Returns:
        The rows; an empty frame when the archive answers for this month and holds none; or
        ``None`` when the archive does not answer for it, or cannot be read -- the caller then
        asks the exchange.
    """
    datasets = manifest()
    if not datasets:
        return None
    entry = datasets.get(f"{market}/{dataset}")
    if entry is None:
        return None
    key = f"{month:%Y-%m}"
    if not entry["from"] <= key <= entry["through"]:
        return None
    months = entry["instruments"].get(safe_secid)
    if months is None:
        return None  # an instrument the export did not carry: the exchange may still have it
    if key not in months:
        return pl.DataFrame()
    try:
        return pl.read_parquet(io.BytesIO(_get(f"{market}/{dataset}/{safe_secid}/{key}.parquet")))
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            _disable("access refused", status=error.code)
        else:
            _logger.warning("An AlgoPack archive chunk could not be read; asking the exchange",
                            chunk=f"{market}/{dataset}/{safe_secid}/{key}", status=error.code)
        return None
    except urllib.error.URLError as error:
        _disable("no route", error=str(error.reason))
        return None
    except Exception as error:
        _logger.warning("An AlgoPack archive chunk could not be read; asking the exchange",
                        chunk=f"{market}/{dataset}/{safe_secid}/{key}",
                        error=f"{type(error).__name__}: {error}")
        return None
