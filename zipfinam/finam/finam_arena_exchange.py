import asyncio
import datetime
import time
from logging import Logger
from typing import Any, Type

import aiocache
import aiohttp
import httpx
import math
import polars as pl
import structlog
from aiocache import Cache

from exchange_calendars import ExchangeCalendar
from finam_trade_api.instruments import BarsRequest, TimeFrame
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.execution import MarketOrder, LimitOrder
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission import CommissionModel, NoCommission
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.position import Position
from ziplime.gens.domain.trading_clock import TradingClock
from finam_trade_api import Client
from finam_trade_api import TokenManager

from zipfinam.finam.allocation import Holding, allocated_cash, round_to_lots
from zipfinam.finam.errors import BrokerAuthError
from zipfinam.finam.order_status import (OUTCOME_SEVERITY, decimal_value, describe_order,
                                                   is_terminal, order_outcome)


def _raise_for_auth(response: aiohttp.ClientResponse, what: str) -> None:
    """Name a refused credential instead of failing later on a missing key.

    Without this a bad secret surfaces as ``KeyError: 'token'`` (or ``'cash'``),
    which reads like a bug in the worker rather than a token the user has to
    replace. The response body is never echoed: it can quote the request.
    """
    if response.status in (401, 403):
        raise BrokerAuthError(f"Finam refused the credentials for {what} (HTTP {response.status})")
    if response.status != 200:
        raise RuntimeError(f"Finam {what} failed (HTTP {response.status})")


# Quotes and bars come from api.finam.ru through finam_trade_api, which opens every
# request with httpx's default 5 s timeout and never retries: a slow answer failed
# the bar with httpx.ReadTimeout — once right after an order had already filled.
# These are idempotent GETs, so give them room and ask again. Orders never go
# through this client (they are POSTed to Arena with aiohttp).
MARKET_DATA_TIMEOUT = httpx.Timeout(20.0, connect=10.0)
MARKET_DATA_ATTEMPTS = 3


def _resilient_reads(exec_request, logger, *, attempts: int = MARKET_DATA_ATTEMPTS,
                     timeout: httpx.Timeout = MARKET_DATA_TIMEOUT, backoff: float = 1.0):
    """Wrap a finam_trade_api client's ``_exec_request``: a longer timeout, GETs retried."""
    async def wrapped(method, url, payload=None, **kwargs):
        kwargs.setdefault("timeout", timeout)
        tries = attempts if str(getattr(method, "value", method)).upper() == "GET" else 1
        for attempt in range(1, tries + 1):
            try:
                return await exec_request(method, url, payload, **kwargs)
            except httpx.TransportError as exc:
                if attempt == tries:
                    raise
                logger.warning("Market data request failed; retrying", url=url, attempt=attempt,
                               error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(backoff * attempt)

    return wrapped


class FinamArenaExchange(Exchange):
    def __init__(self, authorization_token: str,
                 account_id: str,
                 finam_arena_server_url: str,
                 finam_api_server_url: str,
                 finam_api_token: str,
                 name: str,
                 canonical_name: str,
                 country_code: str,
                 clock: TradingClock,
                 trading_calendar: ExchangeCalendar,
                 start_cash_balance: float,
                 asset_service: AssetService,
                 is_default: bool,
                 data_source: DataBundle | None = None,
                 commission_models: dict[Type[Asset],CommissionModel]  | None= None,
                 logger: Logger = structlog.get_logger(__name__),
                 capital_limit: float | None = None,
                 ):
        super().__init__(name=name, canonical_name=canonical_name, country_code=country_code, clock=clock,
                         is_default=is_default,
                         trading_calendar=trading_calendar, data_source=data_source, account_id=account_id)
        self._authorization_token = authorization_token
        self._finam_arena_server_url = finam_arena_server_url
        self._finam_api_server_url = finam_api_server_url
        self._finam_api_token = finam_api_token
        self._start_cash_balance = start_cash_balance
        self._asset_service = asset_service
        self._finam_client = Client(TokenManager(self._finam_api_token))
        instruments = self._finam_client.instruments
        instruments._exec_request = _resilient_reads(instruments._exec_request, logger)
        self._orders_by_external_id = {}
        # Finam Arena /trades returns the whole account trade history for the requested
        # window and carries no order id, so we track which trades we've already turned
        # into transactions and ignore anything that predates this session.
        self._processed_trade_ids: set[str] = set()
        self._session_start_utc = datetime.datetime.now(tz=datetime.timezone.utc)
        if commission_models:
            self.commission_models = commission_models
        else:
            self.commission_models = {}
        self._logger = logger
        # Capital allocation (see allocation.py): with a limit the strategy sees the
        # whole account's positions but trades its allocated capital, not the
        # account's cash.
        self._capital_limit = capital_limit

    @staticmethod
    def _open_positions(account: dict[str, Any]) -> list[dict[str, Any]]:
        """The account's open broker positions — all of them: the strategy sees the account.

        Finam keeps a symbol traded earlier in the day in `positions` with
        quantity 0. Handed to ziplime, that zero position crashes
        `position_tracker.stats` (its arrays are sized by non-zero positions,
        its loop walks all of them: "index 0 is out of bounds for axis 0 with
        size 0") before the strategy even runs — so flat positions are dropped.
        """
        return [p for p in (account.get("positions") or [])
                if float((p.get("quantity") or {}).get("value") or 0) != 0]

    async def _resolve_position_asset(self, position: dict[str, Any]) -> ExchangeAsset | None:
        """The listing behind a broker position, or None (warned) if the instrument DB lacks it.

        The strategy sees the whole account, which may hold what this worker cannot
        model (a bond, a listing never ingested). Keyed by None, such a holding
        crashed the portfolio sync on every tick; skipped, the strategy trades the
        rest — it could not have traded that one anyway.
        """
        symbol = str(position.get("symbol", ""))
        asset = None
        if "@" in symbol:
            ticker, mic = symbol.split("@", 1)
            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=AssetType.EQUITY)
        if asset is None:
            self._logger.warning("Broker position in an instrument the worker cannot model; "
                                 "left out of the strategy's portfolio", symbol=symbol,
                                 quantity=(position.get("quantity") or {}).get("value"))
        return asset

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_token(self) -> str:
        payload = {"secret": self._authorization_token}
        url = f"{self._finam_arena_server_url}/v1/sessions"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                _raise_for_auth(response, "the session")
                data = await response.json()
        return data["token"]

    def get_start_cash_balance(self) -> float:
        return self._start_cash_balance

    def get_current_cash_balance(self):
        pass

    def subscribe_to_market_data(self, asset):
        return []

    def get_subscribed_assets(self):
        return []

    async def get_positions(self) -> dict[ExchangeAsset, Position]:
        account = await self._get_account()
        positions = {}
        for p in self._open_positions(account):
            asset = await self._resolve_position_asset(p)
            if asset is None:
                continue
            positions[asset] = Position(asset=asset,
                                        amount=float(p["quantity"]["value"]),
                                        exchange_name=self.name,
                                        cost_basis=float(p["average_price"]["value"]),
                                        last_sale_price=self._current_price(p),
                                        last_sale_date=None,
                                        trading_account_id=str(account["account_id"]),
                                        )

        return positions

    @staticmethod
    def _current_price(position: dict[str, Any]) -> float:
        """Live mark for a position, falling back to its entry price.

        Positions must be marked to `current_price`, not `average_price`: the
        ledger values the portfolio from this, and `order_target_percent` sizes
        orders off that value. Marking at cost hides unrealized P&L and
        mis-sizes every live order.
        """
        price = position.get("current_price") or position.get("average_price")
        return float(price["value"])

    async def get_portfolio(self) -> Portfolio:
        account = await self._get_account()
        positions = {}
        portfolio_value = 0.00
        own = []
        for p in self._open_positions(account):
            asset = await self._resolve_position_asset(p)
            if asset is None:
                continue
            own.append(p)
            current_price = self._current_price(p)
            # The engine keys a portfolio by (exchange, trading account, asset) and
            # order_target_* looks a holding up by (exchange.name, exchange.account_id,
            # asset); keyed by asset alone, synchronize_exchange_portfolio crashed on
            # every tick that held a position. The account id is this exchange's own,
            # not Finam's echo of it, so the lookup matches however Finam formats it.
            trading_account_id = str(self.account_id)
            positions[(self.name, trading_account_id, asset)] = Position(
                asset=asset,
                amount=float(p["quantity"]["value"]),
                exchange_name=self.name,
                cost_basis=float(p["average_price"]["value"]),
                last_sale_price=current_price,
                last_sale_date=None,
                trading_account_id=trading_account_id,
            )
            # Mark to market, not to cost — otherwise portfolio_value carries the
            # unrealized P&L as phantom equity and every order is mis-sized.
            portfolio_value += float(p["quantity"]["value"]) * current_price
        total_cash = self._parse_cash(account.get("cash"))
        if self._capital_limit is not None:
            broker_cash = total_cash
            total_cash = allocated_cash(
                capital=float(self._capital_limit),
                holdings=[Holding(symbol=p["symbol"], quantity=float(p["quantity"]["value"]),
                                  average_price=float(p["average_price"]["value"])) for p in own],
            )
            self._logger.info("Capital allocation applied", capital=self._capital_limit,
                              broker_cash=broker_cash, strategy_cash=total_cash,
                              positions_value=portfolio_value, open_positions=len(own))

        portfolio = Portfolio(start_date=datetime.datetime.now(tz=self.trading_calendar.tz),
                              starting_cash=total_cash,
                              portfolio_value=total_cash + portfolio_value,
                              cash=total_cash,
                              cash_flow=0.00,
                              pnl=0.00,
                              returns=0.00,
                              positions_value=0.00,
                              positions_exposure=0.00,
                              positions=positions,
                              )
        return portfolio

    async def _get_account(self):
        token = await self.get_token()
        url = f"{self._finam_arena_server_url}/v1/accounts/{self.account_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as response:
                if response.status in (401, 403):
                    # The session opened, so the token is valid — it just does not
                    # own this account. Say which accounts it does own: that is the
                    # difference between "wrong token saved" and "wrong account".
                    available = await self._accounts_for_token(session, token)
                    raise BrokerAuthError(
                        f"Finam refused the credentials for account {self.account_id} "
                        f"(HTTP {response.status}); this token can access accounts: {available}"
                    )
                _raise_for_auth(response, f"account {self.account_id}")
                data = await response.json()
        return data

    async def _accounts_for_token(self, session: aiohttp.ClientSession, jwt: str) -> str:
        """Account ids the session token may use (read-only), or 'unknown'."""
        try:
            async with session.post(f"{self._finam_arena_server_url}/v1/sessions/details",
                                    json={"token": jwt}) as response:
                if response.status != 200:
                    return "unknown"
                return str((await response.json()).get("account_ids") or [])
        except Exception:
            return "unknown"

    @staticmethod
    def _parse_cash(cash: Any, currency_code: str = "RUB") -> float:
        """Total cash, tolerating both account shapes.

        Arena (arena.finam.ru) returns one scalar: ``{"value": "1012094.96"}``.
        The real Trade API (api.finam.ru) returns a repeated, multi-currency
        list of google.type.Money: ``[{"currency_code": "RUB", "units": "1000",
        "nanos": 0}, ...]`` — empty (``[]``) on an unfunded account. Indexing
        that list with ``["value"]`` is what used to blow up on real accounts.
        """

        def amount(entry: Any) -> float:
            if isinstance(entry, (int, float)):
                return float(entry)
            if isinstance(entry, str):
                return float(entry or 0)
            if isinstance(entry, dict):
                if entry.get("value") is not None:
                    return float(entry["value"])
                if "units" in entry or "nanos" in entry:
                    return float(entry.get("units") or 0) + float(entry.get("nanos") or 0) / 1e9
            return 0.0

        if not cash:
            return 0.0
        if isinstance(cash, list):
            # Sum only the account currency; fall back to everything if the
            # entries carry no currency_code at all.
            same_currency = [c for c in cash
                             if isinstance(c, dict) and c.get("currency_code") == currency_code]
            return sum(amount(c) for c in (same_currency or cash))
        return amount(cash)

    async def get_account(self) -> Account:
        account = await self._get_account()

        return Account(
            settled_cash=0.00,
            accrued_interest=0.00,
            buying_power=math.inf,
            equity_with_loan=0.00,
            total_positions_value=0.00,
            total_positions_exposure=0.00,
            regt_equity=0.00,
            regt_margin=math.inf,
            initial_margin_requirement=0.00,
            maintenance_margin_requirement=0.00,
            available_funds=0.00,
            excess_liquidity=0.00,
            cushion=0.00,
            day_trades_remaining=math.inf,
            leverage=0.00,
            net_leverage=0.00,
            net_liquidation=0.00
        )

    def get_time_skew(self):
        ...

    def _get_order_type_from_execution_style(self, order: Order) -> str:
        if isinstance(order.execution_style, MarketOrder):
            order_type = "ORDER_TYPE_MARKET"
        elif isinstance(order.execution_style, LimitOrder):
            order_type = "ORDER_TYPE_LIMIT"
        else:
            raise Exception(f"Unsupported order type: {order.execution_style}.")
        return order_type

    async def _lot_size(self, symbol: str) -> float:
        """Shares per lot from api.finam.ru /v1/assets (Arena has no such endpoint).

        Asked with the market-data token: the assets call needs an account id, so
        the first account that token owns is used. A lot that cannot be read
        raises -- no order goes out on a guess.
        """
        cache = self.__dict__.setdefault("_lot_sizes", {})
        if symbol in cache:
            return cache[symbol]
        host = (self._finam_api_server_url or "https://api.finam.ru").replace(":443", "").rstrip("/")
        lot = None
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(f"{host}/v1/sessions", json={"secret": self._finam_api_token}) as r:
                    _raise_for_auth(r, "the market-data session")
                    jwt = (await r.json())["token"]
                async with session.post(f"{host}/v1/sessions/details", json={"token": jwt}) as r:
                    accounts = (await r.json()).get("account_ids") or []
                async with session.get(f"{host}/v1/assets/{symbol}",
                                       params={"account_id": str(accounts[0])} if accounts else None,
                                       headers={"Authorization": f"Bearer {jwt}"}) as r:
                    if r.status == 200:
                        lot = float(((await r.json()).get("lot_size") or {}).get("value") or 1)
        except BrokerAuthError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Could not read the lot size of {symbol}: {exc}") from exc
        if lot is None:
            raise RuntimeError(f"Finam has no lot size for {symbol}")
        cache[symbol] = lot
        return lot

    async def submit_order(self, order: Order):

        symbol = f"{order.asset.symbol}@{order.asset.mic}"
        # The market's own unit: the security's lot on MOEX (GAZP trades by 10),
        # read from api.finam.ru /v1/assets.
        lot = await self._lot_size(symbol)
        quantity = round_to_lots(order.amount, lot)
        if quantity == 0:
            # A rebalance delta smaller than one lot is routine with lot sizes (GAZP
            # trades by 10): skip it with a warning instead of failing the bar, so
            # the strategy's other orders still go out and the tick is not a failure.
            reason = (f"{abs(order.amount):g} {symbol} is below one lot ({lot:g} shares); "
                      "order not sent")
            self._logger.warning("Order below one lot skipped", symbol=symbol,
                                 requested=order.amount, lot_size=lot)
            order.reject(reason=reason)
            self.__dict__.setdefault("skipped_orders", []).append(
                {"symbol": symbol, "requested": order.amount, "lot_size": lot, "reason": reason})
            return order
        if quantity != order.amount:
            self._logger.info("Order rounded to whole lots", symbol=symbol, requested=order.amount,
                              lot_size=lot, quantity=quantity)

        token = await self.get_token()
        url = f"{self._finam_arena_server_url}/v1/accounts/{self.account_id}/orders"
        async with aiohttp.ClientSession() as session:
            # Quantity/prices are google.type.Decimal messages: the real Trade API
            # (api.finam.ru) rejects a bare string with
            #   400 quantity: invalid value "1" for type google.type.Decimal
            # while Arena also accepts the {"value": ...} message form.
            body = {
                "symbol": symbol,
                "side": "SIDE_BUY" if quantity > 0 else "SIDE_SELL",
                "quantity": {"value": str(abs(quantity))},
                "type": self._get_order_type_from_execution_style(order),
            }
            if order.limit is not None:
                body["limit_price"] = {"value": str(order.limit)}
            if order.stop is not None:
                body["stop_price"] = {"value": str(order.stop)}

            async with session.post(url, headers={"Authorization": f"Bearer {token}", },
                                    json=body) as response:
                if response.status >= 400:  # not a success status
                    text = await response.text()
                    raise RuntimeError(f"API error {response.status}: {text}")
                response = await response.json()

        order.exchange_order_id = response["order_id"]

        response_order = response["order"]
        response_order["account_id"] = order.trading_account_id
        response_order["client_order_id"] = order.id
        response_order["order_id"] = order.exchange_order_id
        response_order["execution_timestamp"] = datetime.datetime.now(tz=self.clock.trading_calendar.tz)
        # Accepted is not executed: keep what the broker said at placement, the
        # post-bar check (refresh_order_states) finds out what became of it.
        response_order["status"] = response.get("status")
        self._orders_by_external_id[response["order_id"]] = response_order
        self._logger.info("ORDER_PLACED", order_id=order.exchange_order_id, symbol=symbol,
                          side=body["side"], quantity=abs(quantity), type=body["type"],
                          status=response.get("status"))
        return order

    async def _order_state(self, session: aiohttp.ClientSession, token: str,
                           order_id: str) -> dict[str, Any]:
        url = f"{self._finam_arena_server_url}/v1/accounts/{self.account_id}/orders/{order_id}"
        async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as response:
            _raise_for_auth(response, f"order {order_id}")
            return await response.json()

    async def refresh_order_states(self, timeout: float = 10.0,
                                   interval: float = 1.0) -> list[dict[str, Any]]:
        """Ask the broker what became of every order placed this tick, and log it.

        Polls ``GET /orders/{id}`` until each order reaches a terminal status or
        ``timeout`` runs out; a market order normally settles on the first read.
        Never raises: an order whose status cannot be read is reported as
        ``unknown`` with the reason, and the tick carries on.
        """
        if not self._orders_by_external_id:
            return []
        errors: dict[str, str] = {}
        waiting = set(self._orders_by_external_id)
        deadline = time.monotonic() + max(timeout, 0.0)
        try:
            token = await self.get_token()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                while True:
                    for order_id in sorted(waiting):
                        try:
                            state = await self._order_state(session, token, order_id)
                        except Exception as exc:
                            errors[order_id] = f"{type(exc).__name__}: {exc}"
                            continue
                        errors.pop(order_id, None)
                        stored = self._orders_by_external_id[order_id]
                        stored["broker_state"] = {k: v for k, v in state.items() if k != "order"}
                        stored["status"] = state.get("status") or stored.get("status")
                        if is_terminal(stored["status"]):
                            waiting.discard(order_id)
                    if not waiting or time.monotonic() >= deadline:
                        break
                    await asyncio.sleep(interval)
        except Exception as exc:
            for order_id in waiting:
                errors.setdefault(order_id, f"{type(exc).__name__}: {exc}")

        outcomes = [self._order_outcome_row(order_id, errors.get(order_id))
                    for order_id in self._orders_by_external_id]
        for row in outcomes:
            level = {"info": self._logger.info, "warning": self._logger.warning,
                     "error": self._logger.error}[OUTCOME_SEVERITY[row["outcome"]]]
            level(f"ORDER_{row['outcome'].upper()}", order_id=row["exchange_order_id"],
                  symbol=row["symbol"], side=row["side"], quantity=row["quantity"],
                  status=row["status"], executed_quantity=row["executed_quantity"],
                  remaining_quantity=row["remaining_quantity"],
                  detail=describe_order(row), status_error=row["status_error"])
        return outcomes

    def _order_outcome_row(self, order_id: str, error: str | None) -> dict[str, Any]:
        stored = self._orders_by_external_id[order_id]
        broker_state = stored.get("broker_state") or {}
        status = stored.get("status")
        executed = decimal_value(broker_state.get("executed_quantity"))
        if not broker_state and not is_terminal(status):
            # Never read back, and the placement status is not final: say so
            # rather than pass the placement's NEW off as "still working".
            outcome = "unknown"
        else:
            outcome = order_outcome(status, executed)
        return {
            "exchange_order_id": order_id,
            "client_order_id": stored.get("client_order_id"),
            "symbol": stored.get("symbol"),
            "side": stored.get("side"),
            "type": stored.get("type"),
            "quantity": decimal_value(stored.get("quantity")),
            "status": status,
            "outcome": outcome,
            "executed_quantity": executed,
            "remaining_quantity": decimal_value(broker_state.get("remaining_quantity")),
            "status_error": error,
            "broker_state": broker_state,
        }

    def is_alive(self):
        ...

    async def _finam_arena_order_to_order(self, finam_arena_order: dict[str, Any]) -> Order:
        order_status = OrderStatus.FILLED
        execution_style = MarketOrder()
        ticker, mic = finam_arena_order["symbol"].split("@")
        asset = await self._asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic),
            asset_type=AssetType.EQUITY
        )
        amount = int(float(finam_arena_order["quantity"]["value"]))
        order = Order(
            id=finam_arena_order["client_order_id"],
            dt=finam_arena_order["execution_timestamp"],
            asset=asset,
            amount=-amount if finam_arena_order["side"] == "SIDE_SELL" else amount,
            filled=int(float(finam_arena_order["quantity"]["value"])),
            commission=0,
            execution_style=execution_style,
            status=order_status,
            exchange_name=self.name,
            exchange_order_id=finam_arena_order["order_id"],
            trading_account_id=str(finam_arena_order["account_id"])
        )
        return order

    async def get_orders(self) -> dict[str, Order]:
        orders = {
            order.order.client_order_id: await self._finam_arena_order_to_order(order) for order in
            self._orders_by_external_id.values()
        }
        return orders

    async def _trades(self, account_id: str, date_from: datetime.datetime,
                      date_to: datetime.datetime) -> dict[str, Any]:
        url = f"{self._finam_arena_server_url}/v1/accounts/{self.account_id}/trades"
        token = await self.get_token()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"Authorization": f"Bearer {token}", },
                                   params={
                                       "interval.start_time": date_from.isoformat(),
                                       "interval.end_time": date_to.isoformat()
                                   }) as response:
                if response.status >= 400:  # not a success status
                    text = await response.text()
                    raise RuntimeError(f"API error {response.status}: {text}")
                trades_response = await response.json()
        return trades_response

    async def _get_trades(self, orders_by_exchange_id: dict[str, Order], date_from: datetime.datetime | None = None,
                          date_to: datetime.datetime | None = None):
        if not orders_by_exchange_id:
            return []

        date_sorted_orders = sorted([order.dt for order in orders_by_exchange_id.values()])
        if date_from is None:
            date_from = date_sorted_orders[0]
        if date_to is None:
            date_to = datetime.datetime.now(tz=datetime.timezone.utc)

        trades_response = await self._trades(
            account_id=self.account_id,
            date_from=date_from,
            date_to=date_to
        )
        # Arena trades carry no order id, so map a trade back to its order by symbol.
        orders_by_symbol = {
            f"{order.asset.symbol}@{order.asset.mic}": order
            for order in orders_by_exchange_id.values()
        }
        transactions = []
        for trade in trades_response.get("trades") or []:
            trade_id = trade["trade_id"]
            # Arena stamps a trade `created_at`; the real Trade API (AccountTrade)
            # uses `timestamp` — reading only the former failed every real fill
            # with KeyError 'created_at' after the order had already executed.
            stamp = trade.get("created_at") or trade.get("timestamp") or trade.get("transact_at")
            created_at = (datetime.datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
                          if stamp else datetime.datetime.now(tz=datetime.timezone.utc))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=datetime.timezone.utc)
            # Skip anything that predates this session or was already recorded:
            # the /trades endpoint returns the full account history for the window.
            if created_at < self._session_start_utc or trade_id in self._processed_trade_ids:
                self._processed_trade_ids.add(trade_id)
                continue
            # The real API names the order a trade filled; Arena does not, so it
            # falls back to matching by symbol.
            order = (orders_by_exchange_id.get(trade.get("order_id"))
                     or orders_by_symbol.get(trade["symbol"]))
            if order is None:
                # trade for a symbol we have no open order for -> not ours to record here
                continue
            self._processed_trade_ids.add(trade_id)
            ticker, mic = trade["symbol"].split("@")
            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            trade_amount = int(float(trade["size"]["value"]))
            transactions.append(Transaction(
                id=trade_id,
                asset=asset,
                amount=-trade_amount if trade["side"] == "SIDE_SELL" else trade_amount,
                dt=created_at.astimezone(self.clock.trading_calendar.tz),
                price=float(trade["price"]["value"]),
                order_id=order.id,
                exchange_name=self.name,
                commission=None,
                realized_pnl=0.0,
                trading_account_id=order.trading_account_id
            ))
        return transactions

    async def get_transactions(self, orders: dict[ExchangeAsset, dict[str, Order]], current_dt: datetime.datetime,
                               same_bar_execution: bool):
        if not orders:
            return [], [], []

        orders_by_exchange_id = {order.exchange_order_id: order for asset_orders in orders.values() for order in
                                 asset_orders.values() if order.exchange_order_id is not None}
        # An order skipped below one lot never reached the broker and has no id.
        order_ids = [order.exchange_order_id for orders_dict in orders.values()
                     for order in orders_dict.values() if order.exchange_order_id is not None]

        result_orders = await self.get_orders_by_ids(order_ids=order_ids)

        trades = await self._get_trades(orders_by_exchange_id=orders_by_exchange_id)

        new_trans = trades
        new_comm = []
        closed = [
            o for o in result_orders if
            o.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)
        ]

        return new_trans, new_comm, closed

    async def get_orders_by_ids(self, order_ids: list[str]) -> list[Order]:
        finam_arena_orders = [self._orders_by_external_id[order_id] for order_id in order_ids]
        orders = [await self._finam_arena_order_to_order(finam_arena_order=r) for r in finam_arena_orders]
        return orders

    async def get_transactions_by_order_ids(self, order_ids: list[str]):
        ...

    async def cancel_order(self, order_id: str) -> None:
        return None

    def get_last_traded_dt(self, asset):
        return None

    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime,
                             data_frequency: datetime.timedelta | Period = None):
        latest_quote = await self.current(assets=assets, fields=fields, dt=dt)
        return latest_quote

    async def get_realtime_bars(self, assets, frequency):
        ...

    def get_slippage_model(self, asset: ExchangeAsset):
        return NoSlippage()

    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        return self.commission_models.get(type(asset.asset), NoCommission())

    async def _lastest_quote(self, asset: ExchangeAsset, current_dt: datetime.datetime):

        response = await self._finam_client.instruments.get_last_quote(symbol=f"{asset.symbol}@{asset.mic}")

        quote = response.quote
        return {
            # temporary fix, GRPC data source always returns data in NY timezone
            "date": quote.timestamp.replace(tzinfo=current_dt.tzinfo),
            "open": float(quote.open.value) if quote.open.value else None,
            "high": float(quote.high.value) if quote.high.value else None,
            "low": float(quote.low.value) if quote.low.value else None,
            "close": float(quote.close.value) if quote.close.value else None,
            "volume": int(float(quote.volume.value)) if quote.volume.value else None,
            "mic": asset.mic,
            "price": float(quote.last.value) if quote.last.value else None,
            "symbol": asset.symbol,
            "sid": asset.sid,
            "ask": float(quote.ask.value) if quote.ask.value else None,
            "ask_size": float(quote.ask_size.value) if quote.ask_size.value else None,
            "bid": float(quote.bid.value) if quote.bid.value else None,
            "bid_size": float(quote.bid_size.value) if quote.bid_size.value else None,
            "last_size": float(quote.last_size.value) if quote.last_size.value else None,

        }

    async def current(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime = None):
        cols = list(fields.union({"date", "sid", "symbol"}))
        tasks = [self._lastest_quote(asset=asset, current_dt=dt) for asset in assets]
        result = await asyncio.gather(*tasks)

        df = pl.DataFrame(result, schema=[("open", pl.Float64()),
                                          ("close", pl.Float64()),
                                          ("price", pl.Float64()),
                                          ("high", pl.Float64()), ("low", pl.Float64()),
                                          ("volume", pl.Float64()),
                                          ("date", pl.Datetime(time_zone=dt.tzinfo)),
                                          ("mic", pl.String),
                                          ("symbol", pl.String),
                                          ("sid", pl.Int64)
                                          ])
        return df.select(pl.col(col) for col in cols)

    def _get_timeframe(self, frequency: datetime.timedelta) -> str:
        if frequency <= datetime.timedelta(minutes=1):
            return "TIME_FRAME_M1"
        elif frequency <= datetime.timedelta(minutes=5):
            return "TIME_FRAME_M5"
        elif frequency <= datetime.timedelta(minutes=15):
            return "TIME_FRAME_M15"
        elif frequency <= datetime.timedelta(minutes=30):
            return "TIME_FRAME_M30"
        elif frequency <= datetime.timedelta(hours=1):
            return "TIME_FRAME_H1"
        elif frequency <= datetime.timedelta(hours=2):
            return "TIME_FRAME_H2"
        elif frequency <= datetime.timedelta(hours=4):
            return "TIME_FRAME_H4"
        elif frequency <= datetime.timedelta(hours=8):
            return "TIME_FRAME_H8"
        elif frequency <= datetime.timedelta(days=1):
            return "TIME_FRAME_D"
        elif frequency <= datetime.timedelta(weeks=1):
            return "TIME_FRAME_W"
        elif frequency <= datetime.timedelta(days=31):
            return "TIME_FRAME_MN"
        elif frequency <= datetime.timedelta(days=95):
            return "TIME_FRAME_QR"

        raise ValueError(f"Unsupported frequency for Yahoo Finance {frequency}")

    def floor_dt(self, dt: datetime.datetime, delta: datetime.timedelta) -> datetime.datetime:
        """Round down to the nearest multiple of `delta`."""
        # Anchor to the day start to keep boundaries aligned (and tz-safe)
        anchor = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elapsed = (dt - anchor) // delta
        return anchor + elapsed * delta

    @staticmethod
    def _bar_stamp(timestamp: datetime.datetime, tz, daily: bool) -> datetime.datetime:
        """The label a Finam candle gets in the frame.

        A daily candle is labelled by its session, at local midnight — the way the
        bundles stamp theirs, so a live tick and its backtest see the same index.
        Converted, not relabelled: a candle Finam stamps at 21:00Z the evening
        before its session would otherwise land on the previous day.
        """
        if not daily:
            # temporary fix, GRPC data source always returns data in NY timezone
            return timestamp.replace(tzinfo=tz)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        return timestamp.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)

    def last_available_bar(self, sid: int | None = None) -> datetime.datetime | None:
        """A live venue has no opinion on where an instrument's data stops.

        The bundle behind this exchange is only a warm-up window. Answering with its
        end (ziplime's default) made the engine read every instrument as delisted
        the day after the bundle was built — closed at its last mark and never
        traded again.
        """
        return None

    async def _with_session_bar(self, frame: pl.DataFrame, assets,
                                session_stamp: datetime.datetime) -> pl.DataFrame:
        """Add today's in-progress daily bar from the quote when Finam's bars lack it.

        A backtest's daily bar on day D is day D's own; the live analogue during the
        session is the candle so far — seen by data.history and compute_signals alike,
        as the bundle's midnight-stamped bar is on its own day. Only today's open
        session is filled, and only for an asset the bars left without a row for it.
        """
        now = datetime.datetime.now(tz=self.trading_calendar.tz)
        today = now.date()
        if session_stamp.date() != today:
            return frame
        if not self.trading_calendar.is_session(today) or \
                now < self.trading_calendar.session_first_minute(today).to_pydatetime():
            return frame
        have = set()
        if not frame.is_empty():
            have = set(frame.filter(pl.col("date").dt.date() == today)["sid"].to_list())
        rows = []
        for asset in assets:
            if asset.sid in have:
                continue
            try:
                quote = await self._lastest_quote(asset=asset, current_dt=now)
            except Exception as exc:
                self._logger.warning("No quote for today's daily bar", symbol=asset.symbol,
                                     error=str(exc))
                continue
            price = quote.get("price") or quote.get("close")
            if price is None:
                continue
            rows.append({"open": quote.get("open") or price, "close": price, "price": price,
                         "high": quote.get("high") or price, "low": quote.get("low") or price,
                         "volume": float(quote.get("volume") or 0), "date": session_stamp,
                         "mic": asset.mic, "symbol": asset.symbol, "sid": asset.sid})
        if not rows:
            return frame
        self._logger.info("Added today's in-progress daily bar from the quote",
                          symbols=[row["symbol"] for row in rows])
        added = pl.DataFrame(rows, schema=frame.schema)
        return pl.concat([frame, added]).sort("date")

    async def fetch_historical_data(self,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    asset: ExchangeAsset,
                                    frequency: datetime.timedelta,
                                    ) -> tuple[pl.DataFrame, float, float]:
        duration_start = time.time()
        daily = frequency >= datetime.timedelta(days=1)
        timestamp_from = date_from.astimezone(datetime.timezone.utc)
        # A daily request runs to date_to itself: flooring it to midnight dropped the
        # session's own in-progress candle — the bar a backtest sees on that day.
        timestamp_to = (date_to if daily else self.floor_dt(date_to, frequency)).astimezone(
            datetime.timezone.utc)
        timeframe = self._get_timeframe(frequency=frequency)
        total_requests_time = 0
        bars = await self._finam_client.instruments.get_bars(params=BarsRequest(symbol=f"{asset.symbol}@{asset.mic}",
                                                                                timeframe=TimeFrame(timeframe),
                                                                                start_time=timestamp_from.isoformat(),
                                                                                end_time=timestamp_to.isoformat()))
        self._logger.info(f"Fetching market data for symbol {asset.symbol} - {date_from} to {date_to}",
                          symbol=asset.symbol, start_date=date_from, end_date=date_to)
        duration = time.time() - duration_start
        rows = [
            {
                "date": self._bar_stamp(candle.timestamp, date_from.tzinfo, daily=daily),
                "open": float(candle.open.value),
                "high": float(candle.high.value),
                "low": float(candle.low.value),
                "close": float(candle.close.value),
                "volume": int(float(candle.volume.value)),
                "mic": asset.mic,
                "price": float(candle.close.value),
                "symbol": asset.symbol,
                "sid": asset.sid
            } for candle in bars.bars
        ]
        self._logger.info(
            f"Fetched market data for symbol {asset.symbol} - {date_from} to {date_to} in {duration:.2f}",
            symbol=asset.symbol, start_date=date_from, end_date=date_to, duration=duration)

        df = pl.DataFrame(rows, schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                        ("price", pl.Float64()),
                                        ("high", pl.Float64()), ("low", pl.Float64()),
                                        ("volume", pl.Float64()),
                                        ("date", pl.Datetime(time_zone=date_from.tzinfo)), ("mic", pl.String),
                                        ("symbol", pl.String),
                                        ("sid", pl.Int64)
                                        ])
        duration_total = time.time() - duration_start

        self._logger.info(
            f"Retrieved {len(rows)} candles for {asset.symbol} in {duration_total:.2f}s. "
            f"Total requests time: {total_requests_time:.2f}s",
            total_duration=duration_total, requests_duration=total_requests_time
        )

        return df, duration_total, total_requests_time

    async def get_data_by_limit(self, fields: frozenset[str],
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta,
                                assets: frozenset[ExchangeAsset],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        # The engine's vectorised compute_signals asks for every field (fields=None).
        fields = frozenset(fields) if fields else frozenset(
            {"open", "high", "low", "close", "volume", "price"})
        cols = list(fields.union({"date", "sid", "symbol"}))
        if frequency >= datetime.timedelta(days=1):
            multiplier = frequency // datetime.timedelta(days=1)
            time_window = self.trading_calendar.sessions_window(session=end_date.date(), count=-limit * multiplier)
            start_dt = time_window[0].replace(tzinfo=end_date.tzinfo)
            end_dt = time_window[-1].replace(tzinfo=end_date.tzinfo)
        elif frequency <= datetime.timedelta(minutes=1):
            multiplier = frequency // datetime.timedelta(minutes=1)
            time_window = self.trading_calendar.minutes_window(minute=end_date, count=-limit * multiplier)
            start_dt = time_window[0].astimezone(end_date.tzinfo)
            end_dt = time_window[-1].astimezone(end_date.tzinfo)
        else:
            multiplier = frequency // datetime.timedelta(minutes=1)
            time_window = self.trading_calendar.minutes_window(minute=end_date, count=-limit * multiplier)
            start_dt = time_window[0].astimezone(end_date.tzinfo)
            end_dt = time_window[-1].astimezone(end_date.tzinfo)

        daily = frequency >= datetime.timedelta(days=1)
        df_raw = await self.get_data(assets=assets,
                                     frequency=frequency, date_from=start_dt,
                                     # A daily window runs through its last session's
                                     # day, so that session's own candle comes back.
                                     date_to=end_dt + datetime.timedelta(days=1) if daily else end_dt
                                     )
        if daily:
            df_raw = await self._with_session_bar(df_raw, assets, session_stamp=end_dt)
        if df_raw.is_empty():
            return df_raw
        df_raw = df_raw.filter(pl.col("date") >= start_dt, pl.col("date") <= end_dt)
        if multiplier != 1:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in cols).tail(
                limit)
            return df.select(pl.col(col) for col in cols)

        return df_raw.select(pl.col(col) for col in cols)

    async def get_data(self,
                       assets: list[ExchangeAsset],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:

        async def fetch_historical(asset: ExchangeAsset, start_date: datetime.datetime,
                                   end_date: datetime.datetime) -> tuple[pl.DataFrame | None, float, float]:
            try:
                result, duration, requests_time = await self.fetch_historical_data(date_from=start_date,
                                                                                   date_to=end_date,
                                                                                   asset=asset,
                                                                                   frequency=frequency)
                return result, duration, requests_time
            except Exception as e:
                # A missing symbol is skippable; a refused market-data token is not.
                # Skipping it hands the strategy an empty history, and the bar
                # "succeeds" having traded on no data at all.
                if "could not be verified" in str(e) or "code=16" in str(e):
                    raise BrokerAuthError(
                        "Finam market-data token could not be verified (code=16). An Arena "
                        "token does not work on api.finam.ru: set FINAM_API_TOKEN to a Finam "
                        "API token, or save market_data_token with the credential."
                    ) from e
                self._logger.exception(
                    f"Exception fetching historical data for symbol {asset.asset.asset_name}, date_from={start_date}, date_to={end_date}. Skipping."
                )
                return None, 0, 0

        final = pl.DataFrame([], schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                         ("price", pl.Float64()),
                                         ("high", pl.Float64()), ("low", pl.Float64()),
                                         ("volume", pl.Float64()),
                                         ("date", pl.Datetime(time_zone=date_from.tzinfo)), ("mic", pl.String),
                                         ("symbol", pl.String),
                                         ("sid", pl.Int64)
                                         ])

        if frequency >= datetime.timedelta(days=1):
            maximum_batch = datetime.timedelta(days=7200)
        elif frequency >= datetime.timedelta(hours=1):
            maximum_batch = datetime.timedelta(days=365)
        elif frequency >= datetime.timedelta(minutes=1):
            maximum_batch = datetime.timedelta(days=180)
        elif frequency >= datetime.timedelta(seconds=1):
            maximum_batch = datetime.timedelta(days=30)

        tasks = []
        batch_start_date = date_from
        now = datetime.datetime.now(tz=self.trading_calendar.tz)
        batch_end_date = date_to
        while batch_start_date < date_to and batch_end_date != now:

            batch_end_date = batch_start_date + maximum_batch
            if batch_end_date > date_to:
                batch_end_date = date_to
            if batch_end_date > now:
                batch_end_date = now

            tasks.extend(
                fetch_historical(
                    asset=asset, start_date=batch_start_date, end_date=batch_end_date
                ) for asset in assets
            )
            batch_start_date = batch_end_date

        total_duration = 0
        total_requests_duration = 0
        res = await asyncio.gather(*tasks)

        for item in res:
            df, duration, requests_duration = item
            total_duration += duration
            total_requests_duration += requests_duration
            if df is None:
                continue
            final = pl.concat([final, df])
        self._logger.info(
            f"Retrieved {len(final)} candles for {[asset.asset.asset_name for asset in assets]} in {total_duration:.2f}s. "
            f"Total requests time: {total_requests_duration:.2f}s",
            total_duration=total_duration, requests_duration=total_requests_duration
        )
        return final
