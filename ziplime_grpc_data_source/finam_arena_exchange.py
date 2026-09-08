import asyncio
import datetime
import time
from logging import Logger
from typing import Any

import aiocache
import aiohttp
import math
import polars as pl
import structlog
from aiocache import Cache

from exchange_calendars import ExchangeCalendar
from finam_trade_api.instruments import BarsRequest, TimeFrame
from ziplime.assets.domain.asset_type import AssetType
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
                 logger: Logger = structlog.get_logger(__name__)
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
        self._orders_by_external_id = {}
        self._logger = logger

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_token(self) -> str:
        payload = {"secret": self._authorization_token}
        url = f"{self._finam_arena_server_url}/v1/sessions"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
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
        for p in account.positions:
            ticker, mic = p.symbol.split("@")

            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            positions[asset] = Position(asset=asset,
                                        amount=float(p.quantity.value),
                                        exchange=self,
                                        cost_basis=float(p.average_price.value),
                                        last_sale_price=float(p.current_price.value),
                                        last_sale_date=None
                                        )

        return positions

    async def get_portfolio(self) -> Portfolio:
        account = await self._get_account()
        positions = {}
        portfolio_value = 0.00
        for p in account["positions"]:
            ticker, mic = p["symbol"].split("@")

            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            positions[asset] = Position(asset=asset,
                                        amount=float(p["quantity"]["value"]),
                                        exchange_name=self.name,
                                        cost_basis=float(p["average_price"]["value"]),
                                        last_sale_price=float(p["average_price"]["value"]),
                                        last_sale_date=None,
                                        trading_account_id=account["account_id"],
                                        )
            portfolio_value += float(p["quantity"]["value"]) * float(p["average_price"]["value"])
        total_cash = float(account["cash"]["value"])

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
                data = await response.json()
        return data

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

    async def submit_order(self, order: Order):

        token = await self.get_token()
        url = f"{self._finam_arena_server_url}/v1/accounts/{self.account_id}/orders"
        async with aiohttp.ClientSession() as session:
            body = {
                "symbol": f"{order.asset.symbol}@{order.asset.mic}",
                "side": "SIDE_BUY" if order.amount > 0 else "SIDE_SELL",
                "quantity": str(abs(order.amount))
            }

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
        self._orders_by_external_id[response["order_id"]] = response_order
        return order

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
        order = Order(
            id=finam_arena_order["client_order_id"],
            dt=finam_arena_order["execution_timestamp"],
            asset=asset,
            amount=int(float(finam_arena_order["quantity"]["value"])),
            filled=int(float(finam_arena_order["quantity"]["value"])),
            commission=0,
            execution_style=execution_style,
            status=order_status,
            exchange_name=self.name,
            exchange_order_id=finam_arena_order["order_id"],
            trading_account_id=finam_arena_order["account_id"]
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
        transactions = []
        for trade in trades_response["trades"]:
            ticker, mic = trade["symbol"].split("@")
            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            transactions.append(Transaction(
                id=trade["trade_id"],
                asset=asset,
                amount=int(float(trade["size"]["value"])),
                dt=datetime.datetime.fromisoformat(trade["created_at"]).astimezone(self.clock.trading_calendar.tz),
                price=float(trade["price"]["value"]),
                order_id=orders_by_exchange_id[trade["trade_id"]].id,  # trade_id and order_id are the same
                exchange_name=self.name,
                commission=None,
                realized_pnl=0.0,
                trading_account_id=orders_by_exchange_id[trade["trade_id"]].trading_account_id
            ))
        return transactions

    async def get_transactions(self, orders: dict[ExchangeAsset, dict[str, Order]], current_dt: datetime.datetime,
                               same_bar_execution: bool):
        if not orders:
            return [], [], []

        orders_by_exchange_id = {order.exchange_order_id: order for asset_orders in orders.values() for order in
                                 asset_orders.values() if order.exchange_order_id is not None}
        order_ids = [order.exchange_order_id for orders_dict in orders.values() for order in orders_dict.values()]

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
        ...

    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime,
                             data_frequency: datetime.timedelta | Period = None):
        latest_quote = await self.current(assets=assets, fields=fields, dt=dt)
        return latest_quote

    async def get_realtime_bars(self, assets, frequency):
        ...

    def get_slippage_model(self, asset: ExchangeAsset):
        return NoSlippage()

    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        return NoCommission()

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

    async def fetch_historical_data(self,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    asset: ExchangeAsset,
                                    frequency: datetime.timedelta,
                                    ) -> tuple[pl.DataFrame, float, float]:
        duration_start = time.time()
        timestamp_from = date_from.astimezone(datetime.timezone.utc)
        timestamp_to = self.floor_dt(date_to, frequency).astimezone(datetime.timezone.utc)
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
                # temporary fix, GRPC data source always returns data in NY timezone
                "date": candle.timestamp.replace(tzinfo=date_from.tzinfo),
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

        df_raw = await self.get_data(assets=assets,
                                     frequency=frequency, date_from=start_dt,
                                     date_to=end_dt
                                     )
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
