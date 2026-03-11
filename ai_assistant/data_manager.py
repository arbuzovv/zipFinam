"""
Менеджер загрузки рыночных данных через Финам API.

Отслеживает загруженные инструменты и диапазоны дат, чтобы загружать
только то, что ещё не закэшировано локально.

Формат состояния (по инструментам):
    {
      "symbols": {
        "SBER@MISX": {"start_date": "2023-10-01", "end_date": "2025-01-02"},
        "GAZP@MISX": {"start_date": "2023-10-01", "end_date": "2025-01-02"}
      }
    }
"""
from __future__ import annotations

import datetime
import json
import pathlib
from typing import Callable

DEFAULT_STATE_PATH = pathlib.Path.home() / ".ziplime" / "ai_assistant_state.json"
DEFAULT_ASSETS_DB  = pathlib.Path.home() / ".ziplime" / "assets.sqlite"
BUNDLE_NAME        = "grpc_daily_data"
TRADING_CALENDAR   = "XMOS"

# Number of extra calendar days to ingest *before* the requested start date.
# This guarantees bundle_start_date < simulation start_date (avoiding the
# "Start date is before bundle start date" ValueError) and also provides
# warmup bars for indicators like moving averages.
WARMUP_BUFFER_DAYS = 30


class DataManager:
    """
    Управляет загрузкой рыночных данных через Финам API для ИИ-ассистента.

    Состояние хранится в ~/.ziplime/ai_assistant_state.json и отслеживает
    каждый инструмент независимо с его диапазоном дат.

    Загружаются только инструменты, которых нет в кэше или для которых
    диапазон дат не покрывает запрошенный период.
    """

    def __init__(
        self,
        state_path: pathlib.Path = DEFAULT_STATE_PATH,
        assets_db_path: pathlib.Path = DEFAULT_ASSETS_DB,
        on_progress: Callable[[str], None] | None = None,
    ):
        self.state_path = state_path
        self.assets_db_path = assets_db_path
        self.on_progress = on_progress or (lambda msg: None)
        self._state: dict = self._load_state()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def ensure_data(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """
        Ensure that the requested symbols are available for the given date range.
        Only downloads symbols that are missing or need a wider date range.

        The actual ingestion starts WARMUP_BUFFER_DAYS before start_date so that
        bundle_start_date is always strictly earlier than the simulation start_date.
        """
        req_start = _to_utc_midnight(start_date) - datetime.timedelta(days=WARMUP_BUFFER_DAYS)
        req_end   = _to_utc_midnight(end_date) + datetime.timedelta(days=1)

        # Determine which symbols need (re-)ingestion and their required date range
        to_ingest: dict[str, tuple[datetime.datetime, datetime.datetime]] = {}

        per_symbol = self._state.get("symbols", {})

        for symbol in symbols:
            cached = per_symbol.get(symbol)
            if cached is None:
                # Symbol never ingested
                to_ingest[symbol] = (req_start, req_end)
                continue

            cached_start = _parse_date(cached["start_date"])
            cached_end   = _parse_date(cached["end_date"])

            if cached_start > req_start or cached_end < req_end:
                # Date range needs extension — use the union so we don't lose cached data
                union_start = min(cached_start, req_start)
                union_end   = max(cached_end, req_end)
                to_ingest[symbol] = (union_start, union_end)

        if not to_ingest:
            return  # All data already available

        # Group symbols by identical date range to minimise ingestion calls
        range_groups: dict[tuple, list[str]] = {}
        for symbol, (s, e) in to_ingest.items():
            key = (s, e)
            range_groups.setdefault(key, []).append(symbol)

        for (grp_start, grp_end), grp_symbols in range_groups.items():
            self.on_progress(
                f"Загружаю данные для {', '.join(grp_symbols)} "
                f"({grp_start.date()} → {grp_end.date()})…"
            )
            # Clear state before ingestion so a failed download
            # doesn't leave stale "already ingested" markers.
            for symbol in grp_symbols:
                per_symbol.pop(symbol, None)
            self._state["symbols"] = per_symbol
            self._save_state()

            await self._ingest(grp_symbols, grp_start, grp_end)

            # Verify that the bundle actually covers grp_start before
            # marking symbols as ingested.
            ticker_only = [s.split("@")[0] for s in grp_symbols]
            if await self._bundle_covers(grp_start, grp_end, ticker_only):
                for symbol in grp_symbols:
                    per_symbol[symbol] = {
                        "start_date": grp_start.strftime("%Y-%m-%d"),
                        "end_date":   grp_end.strftime("%Y-%m-%d"),
                    }
            else:
                self.on_progress(
                    f"Предупреждение: данные для {', '.join(grp_symbols)} "
                    f"не были загружены. Повторная попытка при следующем запуске."
                )

        self._state["symbols"] = per_symbol
        self._save_state()
        self.on_progress("Загрузка данных завершена.")

    def get_bundle_name(self) -> str:
        return BUNDLE_NAME

    def get_assets_db_path(self) -> pathlib.Path:
        return self.assets_db_path

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    async def _bundle_covers(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        symbols: list[str],
    ) -> bool:
        """Return True if the latest bundle starts on or before start_date."""
        try:
            from ziplime.utils.bundle_utils import get_bundle_service
            svc = get_bundle_service()
            await svc.load_bundle(
                bundle_name=BUNDLE_NAME,
                bundle_version=None,
                frequency=datetime.timedelta(days=1),
                start_date=start_date,
                end_date=min(end_date, start_date + datetime.timedelta(days=7)),
                symbols=symbols,
            )
            return True
        except ValueError:
            return False
        except Exception:
            return True  # Don't block on unrelated errors

    async def _ingest(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """Загружает данные через Финам gRPC API только для указанных инструментов."""
        from ziplime.core.ingest_data import get_asset_service, ingest_market_data
        from ziplime_grpc_data_source.grpc_data_source import GrpcDataSource

        asset_service = get_asset_service(
            clear_asset_db=False,
            db_path=str(self.assets_db_path),
        )
        data_source = GrpcDataSource.from_env()
        await data_source.get_token()

        await ingest_market_data(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            trading_calendar=TRADING_CALENDAR,
            bundle_name=BUNDLE_NAME,
            data_bundle_source=data_source,
            data_frequency=datetime.timedelta(days=1),
            asset_service=asset_service,
        )

    def _load_state(self) -> dict:
        if self.state_path.exists():
            try:
                raw = json.loads(self.state_path.read_text())
                # Migrate old flat format → new per-symbol format
                if "symbols" in raw and isinstance(raw["symbols"], list):
                    raw = _migrate_state(raw)
                return raw
            except Exception:
                return {}
        return {}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self._state, indent=2))


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _migrate_state(old: dict) -> dict:
    """Convert old flat state {symbols: [...], start_date, end_date} to per-symbol format."""
    start_s = old.get("start_date")
    end_s   = old.get("end_date")
    if not start_s or not end_s:
        return {}
    per_symbol = {
        sym: {"start_date": start_s, "end_date": end_s}
        for sym in old.get("symbols", [])
    }
    return {"symbols": per_symbol}


def _to_utc_midnight(dt: datetime.datetime) -> datetime.datetime:
    """Convert any datetime to UTC-aware midnight."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _parse_date(s: str) -> datetime.datetime:
    return datetime.datetime.strptime(s, "%Y-%m-%d").replace(
        tzinfo=datetime.timezone.utc
    )
