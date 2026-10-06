import asyncio
import datetime
import time
from logging import Logger
from zoneinfo import ZoneInfo

import aiocache
import grpc
import math
import polars as pl
import structlog
from aiocache import Cache

from exchange_calendars import ExchangeCalendar
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.execution import MarketOrder, LimitOrder, StopLimitOrder, StopOrder
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission import CommissionModel, NoCommission
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.position import Position
from ziplime.gens.domain.trading_clock import TradingClock
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.auth import auth_service_pb2_grpc, auth_service_pb2
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.marketdata import marketdata_service_pb2_grpc, \
    marketdata_service_pb2
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.orders import orders_service_pb2_grpc, \
    orders_service_pb2
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.accounts import accounts_service_pb2_grpc, \
    accounts_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from google.type import interval_pb2


class GrpcExchange(Exchange):
    def __init__(self, authorization_token: str,
                 account_id: str,
                 server_url: str,
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
        self._server_url = server_url
        self._start_cash_balance = start_cash_balance
        self._asset_service = asset_service
        self._logger = logger

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_token(self) -> str:
        credentials = grpc.ssl_channel_credentials()
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = auth_service_pb2_grpc.AuthServiceStub(channel)
            auth_request = auth_service_pb2.AuthRequest()
            auth_request.secret = self._authorization_token
            response = await stub.Auth(auth_request)
        return response.token

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
        positions = {}
        portfolio_value = 0.00
        # for p in ["SBER@MISX", "MGNT@MISX"]:
        #     ticker, mic = p.split("@")
        #
        #     asset = await self._asset_service.get_asset_by_symbol(
        #         symbol=ticker,
        #         exchange_name=mic,
        #         asset_type=AssetType.EQUITY
        #     )
        #     positions[asset] = Position(asset=asset,
        #                                 amount=float(2),
        #                                 exchange=self,
        #                                 cost_basis=float(15.66),
        #                                 last_sale_price=float(11.2),
        #                                 last_sale_date=None,
        #                                 trading_account_id=self.account_id
        #                                 )
        #     portfolio_value += float(2) * float(15.66)
        # portfolio = Portfolio(start_date=datetime.datetime.now(tz=datetime.timezone.utc),
        #                       starting_cash=5000,
        #                       portfolio_value=5000 + portfolio_value,
        #                       cash=5000,
        #                       cash_flow=0.00,
        #                       pnl=0.00,
        #                       returns=0.00,
        #                       positions_value=0.00,
        #                       positions_exposure=0.00,
        #                       positions=positions
        #                       )
        #
        # # TODO comment
        # return portfolio

        account = await self._get_account()
        positions = {}
        portfolio_value = 0.00
        for p in account.positions:
            ticker, mic = p.symbol.split("@")

            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            positions[asset] = Position(asset=asset,
                                        amount=float(p.quantity.value),
                                        exchange_name=self.name,
                                        cost_basis=float(p.average_price.value),
                                        last_sale_price=float(p.current_price.value),
                                        last_sale_date=None,
                                        trading_account_id=account.account_id,
                                        )
            portfolio_value += float(p.quantity.value) * float(p.average_price.value)
        total_cash = 0
        for money in account.cash:
            total_cash += money.units

        portfolio = Portfolio(start_date=account.open_account_date,
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
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = accounts_service_pb2_grpc.AccountsServiceStub(channel)
            metadata = [('authorization', token)]

            account_request = accounts_service_pb2.GetAccountRequest(account_id=self.account_id)
            total_requests_time = 0
            request_start = time.time()
            account = await stub.GetAccount(account_request, metadata=metadata)
            duration = time.time() - request_start
            total_requests_time += duration
            return account

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
        duration_start = time.time()
        token = await self.get_token()
        credentials = grpc.ssl_channel_credentials()
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = orders_service_pb2_grpc.OrdersServiceStub(channel)
            metadata = [('authorization', token)]
            ticker, mic = order.asset.symbol, order.asset.mic
            order_request = orders_service_pb2.Order()

            order_request.account_id = self.account_id
            order_request.symbol = f"{ticker}@{mic}"
            order_request.quantity.value = str(abs(order.amount))
            order_request.side = "SIDE_BUY" if order.amount > 0 else "SIDE_SELL"
            order_request.type = self._get_order_type_from_execution_style(order=order)
            order_request.time_in_force = "TIME_IN_FORCE_UNSPECIFIED"
            limit_price = order.execution_style.get_limit_price(is_buy=order.amount > 0)
            if limit_price is not None:
                order_request.limit_price.value = str(limit_price)

            stop_price = order.execution_style.get_stop_price(is_buy=order.amount > 0)
            if stop_price is not None:
                order_request.stop_price.value = str(stop_price)

            order_request.stop_condition = "STOP_CONDITION_UNSPECIFIED"
            # order_request.legs = []
            order_request.client_order_id = order.id
            order_request.valid_before = "VALID_BEFORE_UNSPECIFIED"
            order_request.comment = f"Order #{order.id} from ziplime"

            self._logger.info(f"Submitting order: {order}")
            total_requests_time = 0
            request_start = time.time()
            response = await stub.PlaceOrder(order_request, metadata=metadata)

        duration = time.time() - request_start
        total_requests_time += duration
        order.exchange_order_id = response.order_id
        return order

    def is_alive(self):
        ...

    async def _grpc_order_to_order(self, grpc_order: orders_service_pb2.OrderState) -> Order:
        match grpc_order.status:
            case orders_service_pb2.ORDER_STATUS_UNSPECIFIED:
                order_status = OrderStatus.UNKNOWN
            case orders_service_pb2.ORDER_STATUS_NEW:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_PARTIALLY_FILLED:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_FILLED:
                order_status = OrderStatus.FILLED
            case orders_service_pb2.ORDER_STATUS_DONE_FOR_DAY:
                order_status = OrderStatus.DONE_FOR_DAY
            case orders_service_pb2.ORDER_STATUS_CANCELED:
                order_status = OrderStatus.CANCELLED
            case orders_service_pb2.ORDER_STATUS_REPLACED:
                order_status = OrderStatus.REPLACED
            case orders_service_pb2.ORDER_STATUS_PENDING_CANCEL:
                order_status = OrderStatus.PENDING_CANCEL
            case orders_service_pb2.ORDER_STATUS_REJECTED:
                order_status = OrderStatus.REJECTED
            case orders_service_pb2.ORDER_STATUS_SUSPENDED:
                order_status = OrderStatus.HELD
            case orders_service_pb2.ORDER_STATUS_PENDING_NEW:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_EXPIRED:
                order_status = OrderStatus.EXPIRED
            case orders_service_pb2.ORDER_STATUS_FAILED:
                order_status = OrderStatus.REJECTED
            case orders_service_pb2.ORDER_STATUS_FORWARDING:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_WAIT:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_DENIED_BY_BROKER:
                order_status = OrderStatus.REJECTED
            case orders_service_pb2.ORDER_STATUS_REJECTED_BY_EXCHANGE:
                order_status = OrderStatus.REJECTED
            case orders_service_pb2.ORDER_STATUS_WATCHING:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_EXECUTED:
                order_status = OrderStatus.FILLED
            case orders_service_pb2.ORDER_STATUS_DISABLED:
                order_status = OrderStatus.HELD
            case orders_service_pb2.ORDER_STATUS_LINK_WAIT:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_SL_GUARD_TIME:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_SL_EXECUTED:
                order_status = OrderStatus.FILLED
            case orders_service_pb2.ORDER_STATUS_SL_FORWARDING:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_TP_GUARD_TIME:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_TP_EXECUTED:
                order_status = OrderStatus.FILLED
            case orders_service_pb2.ORDER_STATUS_TP_CORRECTION:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_TP_FORWARDING:
                order_status = OrderStatus.OPEN
            case orders_service_pb2.ORDER_STATUS_TP_CORR_GUARD_TIME:
                order_status = OrderStatus.OPEN
            case _:
                raise Exception(f"Unknown order status: {grpc_order.status}")
        match grpc_order.order.type:
            case orders_service_pb2.ORDER_TYPE_UNSPECIFIED:
                execution_style = MarketOrder()
            case orders_service_pb2.ORDER_TYPE_MARKET:
                execution_style = MarketOrder()
            case orders_service_pb2.ORDER_TYPE_LIMIT:
                execution_style = LimitOrder(limit_price=float(grpc_order.order.limit_price.value))
            case orders_service_pb2.ORDER_TYPE_STOP:
                execution_style = StopOrder(stop_price=float(grpc_order.order.stop_price.value))
            case orders_service_pb2.ORDER_TYPE_STOP_LIMIT:
                execution_style = StopLimitOrder(
                    limit_price=float(grpc_order.order.limit_price),
                    stop_price=float(grpc_order.order.stop_price),
                )
            # case orders_service_pb2.ORDER_TYPE_MULTI_LEG:
            #     execution_style = LimitOrder(limit_price=grpc_order..price)
            case _:
                raise Exception(f"Unknown order type {grpc_order.order.type}")
        ticker, mic = grpc_order.order.symbol.split("@")
        asset = await self._asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic),
            asset_type=AssetType.EQUITY
        )
        order = Order(
            id=grpc_order.order.client_order_id,
            dt=datetime.datetime.fromtimestamp(grpc_order.transact_at.seconds, tz=self.clock.trading_calendar.tz),
            asset=asset,
            amount=int(float(grpc_order.order.quantity.value)),
            filled=int(float(grpc_order.executed_quantity.value)),
            commission=0,
            execution_style=execution_style,
            status=order_status,
            exchange_name=self.name,
            exchange_order_id=grpc_order.order_id,
            trading_account_id=grpc_order.order.account_id
        )

        return order

    async def get_orders(self) -> dict[str, Order]:
        token = await self.get_token()
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = orders_service_pb2_grpc.OrdersServiceStub(channel)
            metadata = [('authorization', token)]

            order_requests = orders_service_pb2.OrdersRequest(account_id=self.account_id)
            total_requests_time = 0
            request_start = time.time()
            results = await stub.GetOrders(order_requests, metadata=metadata)
            duration = time.time() - request_start
            total_requests_time += duration

            orders = {order.order.client_order_id: await self._grpc_order_to_order(order) for order in results.orders}
            return orders

    async def _trades(self, account_id: str, date_from: datetime.datetime,
                      date_to: datetime.datetime) -> accounts_service_pb2.TradesResponse:
        token = await self.get_token()
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = accounts_service_pb2_grpc.AccountsServiceStub(channel)
            metadata = [('authorization', token)]
            timestamp_from = Timestamp()
            timestamp_from.FromDatetime(date_from.astimezone(datetime.timezone.utc))
            timestamp_to = Timestamp()
            timestamp_to.FromDatetime(date_to.astimezone(datetime.timezone.utc))
            interval = interval_pb2.Interval(start_time=timestamp_from, end_time=timestamp_to)

            order_requests = accounts_service_pb2.TradesRequest(
                account_id=account_id,
                limit=50000,
                interval=interval
            )
            trades_response = await stub.Trades(order_requests, metadata=metadata)
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
        for trade in trades_response.trades:
            ticker, mic = trade.symbol.split("@")
            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic=mic),
                asset_type=AssetType.EQUITY
            )
            transactions.append(Transaction(
                id=trade.trade_id,
                asset=asset,
                amount=int(float(trade.size.value)),
                dt=datetime.datetime.fromtimestamp(trade.timestamp.seconds, tz=self.clock.trading_calendar.tz),
                price=float(trade.price.value),
                order_id=orders_by_exchange_id[trade.order_id].id,
                exchange_name=self.name,
                commission=None,
                realized_pnl=0.0,
                trading_account_id=orders_by_exchange_id[trade.order_id].trading_account_id # TODO: get it from grpc order

            ))
        return transactions

    async def _get_orders_by_ids(self, exchange_order_ids: list[str]):
        token = await self.get_token()
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = orders_service_pb2_grpc.OrdersServiceStub(channel)
            metadata = [('authorization', token)]

            order_requests = [orders_service_pb2.GetOrderRequest(account_id=self.account_id,
                                                                 order_id=order_id) for order_id in exchange_order_ids]

            get_order_tasks = [stub.GetOrder(order_request, metadata=metadata) for order_request in order_requests]
            results = await asyncio.gather(*get_order_tasks)

        return results

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
        grpc_orders = await self._get_orders_by_ids(exchange_order_ids=order_ids)
        orders = [await self._grpc_order_to_order(grpc_order=r) for r in grpc_orders]
        return orders

    async def get_transactions_by_order_ids(self, order_ids: list[str]):
        ...

    async def cancel_order(self, order_id: str) -> None:
        token = await self.get_token()
        credentials = grpc.ssl_channel_credentials()
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = orders_service_pb2_grpc.OrdersServiceStub(channel)
            metadata = [('authorization', token)]
            cancel_request = orders_service_pb2.CancelOrderRequest(
                account_id=self.account_id,
                order_id=order_id
            )
            result = await stub.CancelOrder(cancel_request, metadata=metadata)
            # return await self._grpc_order_to_order(grpc_order=result)

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

    async def _lastest_quote(self, channel: grpc.aio.Channel, asset: ExchangeAsset, current_dt: datetime.datetime):
        duration_start = time.time()
        token = await self.get_token()

        stub = marketdata_service_pb2_grpc.MarketDataServiceStub(channel)
        metadata = [('authorization', token)]
        ticker, mic = asset.symbol, asset.mic
        quote_request = marketdata_service_pb2.QuoteRequest(symbol=f"{ticker}@{mic}")
        self._logger.info(f"Fetching current quote for symbol {asset.symbol}",
                          symbol=asset.symbol)
        total_requests_time = 0
        request_start = time.time()
        response = await stub.LastQuote(quote_request, metadata=metadata)
        duration = time.time() - request_start
        total_requests_time += duration
        quote = response.quote
        return {
            # temporary fix, GRPC data source always returns data in NY timezone
            "date": datetime.datetime.fromtimestamp(quote.timestamp.seconds,
                                                    tz=ZoneInfo("America/New_York")).replace(
                tzinfo=current_dt.tzinfo),
            "open": float(quote.open.value) if quote.open.value else None,
            "high": float(quote.high.value) if quote.high.value else None,
            "low": float(quote.low.value) if quote.low.value else None,
            "close": float(quote.close.value) if quote.close.value else None,
            "volume": int(float(quote.volume.value)) if quote.volume.value else None,
            "mic": mic,
            "price": float(quote.last.value) if quote.last.value else None,
            "symbol": ticker,
            "sid": asset.sid,
            "ask": float(quote.ask.value) if quote.ask.value else None,
            "ask_size": float(quote.ask_size.value) if quote.ask_size.value else None,
            "bid": float(quote.bid.value) if quote.bid.value else None,
            "bid_size": float(quote.bid_size.value) if quote.bid_size.value else None,
            "last_size": float(quote.last_size.value) if quote.last_size.value else None,

        }

    async def current(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime = None):
        credentials = grpc.ssl_channel_credentials()
        cols = list(fields.union({"date", "sid", "symbol"}))

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            tasks = [self._lastest_quote(channel=channel, asset=asset, current_dt=dt) for asset in assets]
            result = await asyncio.gather(*tasks)

        df = pl.DataFrame(result, schema=[("open", pl.Float64()),
                                          ("close", pl.Float64()),
                                          ("price", pl.Float64()),
                                          ("high", pl.Float64()), ("low", pl.Float64()),
                                          ("volume", pl.Float64()),
                                          ("date", pl.Datetime(time_zone=dt.tzinfo)), ("mic", pl.String),
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
                                    channel: grpc.aio.Channel,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    asset: ExchangeAsset,
                                    frequency: datetime.timedelta,
                                    ) -> tuple[pl.DataFrame, float, float]:
        duration_start = time.time()
        token = await self.get_token()

        stub = marketdata_service_pb2_grpc.MarketDataServiceStub(channel)
        metadata = [('authorization', token)]
        ticker, mic = asset.symbol, asset.mic
        timestamp_from = Timestamp()
        timestamp_to = Timestamp()
        timestamp_from.FromDatetime(date_from.astimezone(datetime.timezone.utc))
        timestamp_to.FromDatetime(self.floor_dt(date_to, frequency).astimezone(datetime.timezone.utc))
        interval = interval_pb2.Interval(start_time=timestamp_from, end_time=timestamp_to)

        bars_request = marketdata_service_pb2.BarsRequest(
            timeframe=self._get_timeframe(frequency=frequency),
            interval=interval)
        bars_request.symbol = f"{ticker}@{mic}"
        self._logger.info(f"Fetching market data for symbol {asset.symbol} - {date_from} to {date_to}",
                          symbol=asset.symbol, start_date=date_from, end_date=date_to)
        total_requests_time = 0
        request_start = time.time()
        response = await stub.Bars(bars_request, metadata=metadata)
        duration = time.time() - request_start
        total_requests_time += duration

        rows = [
            {
                # temporary fix, GRPC data source always returns data in NY timezone
                "date": datetime.datetime.fromtimestamp(candle.timestamp.seconds,
                                                        tz=ZoneInfo("America/New_York")).replace(
                    tzinfo=date_from.tzinfo),
                "open": float(candle.open.value),
                "high": float(candle.high.value),
                "low": float(candle.low.value),
                "close": float(candle.close.value),
                "volume": int(float(candle.volume.value)),
                "mic": mic,
                "price": float(candle.close.value),
                "symbol": ticker,
                "sid": asset.sid
            } for candle in response.bars
        ]
        self._logger.info(
            f"Fetched market data for symbol {asset.symbol} - {date_from} to {date_to} in {duration:.2f}",
            symbol=asset.symbol, start_date=date_from, end_date=date_to, duration=duration)

        try:

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
        except grpc.RpcError as e:
            self._logger.exception(f"Failed to get day candles for {asset.symbol}")
            raise

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
                credentials = grpc.ssl_channel_credentials()
                async with grpc.aio.secure_channel(self._server_url, credentials) as channel:

                    result, duration, requests_time = await self.fetch_historical_data(channel=channel,
                                                                                       date_from=start_date,
                                                                                       date_to=end_date,
                                                                                       asset=asset,
                                                                                       frequency=frequency)
                    return result, duration, requests_time
            except Exception as e:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {asset.symbol}, date_from={start_date}, date_to={end_date}. Skipping."
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
            f"Retrieved {len(final)} candles for {[asset.symbol for asset in assets]} in {total_duration:.2f}s. "
            f"Total requests time: {total_requests_duration:.2f}s",
            total_duration=total_duration, requests_duration=total_requests_duration
        )
        return final
