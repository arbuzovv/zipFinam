import asyncio
import datetime
import multiprocessing
import os
import sys
import time
from typing import Self
from zoneinfo import ZoneInfo

import aiocache
import grpc
import structlog

import polars as pl
from aiocache import Cache
from asyncclick import progressbar
from finam_trade_api.assets.model import Asset
from google.protobuf.timestamp_pb2 import Timestamp
from google.type import interval_pb2
from ziplime.assets.domain.assets_import import AssetsImport

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.split import Split
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime_grpc_data_source.grpc_stubs.grpc.tradeapi.v1.auth import auth_service_pb2_grpc, auth_service_pb2
from ziplime_grpc_data_source.grpc_stubs.grpc.tradeapi.v1.assets import assets_service_pb2_grpc, \
    assets_service_pb2
from ziplime.assets.entities.exchange_asset import ExchangeAsset

from ziplime_grpc_data_source.grpc_stubs.grpc.tradeapi.v1.corporateactions import corporate_actions_service_pb2_grpc, \
    corporate_actions_service_pb2


class FinamAssetDataSource(AssetDataSource):
    def __init__(self, authorization_token: str, server_url: str,
                 maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        self._server_url = server_url
        self._authorization_token = authorization_token
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_token(self) -> str:
        credentials = grpc.ssl_channel_credentials()
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = auth_service_pb2_grpc.AuthServiceStub(channel)
            auth_request = auth_service_pb2.AuthRequest()
            auth_request.secret = self._authorization_token
            response = await stub.Auth(auth_request)
        return response.token

    async def get_assets(self, exchanges: list[ExchangeInfo], **kwargs) -> AssetsImport:
        token = await self.get_token()
        metadata = [('authorization', token)]
        credentials = grpc.ssl_channel_credentials()

        assets_result = []
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = assets_service_pb2_grpc.AssetsServiceStub(channel)
            cursor = 0

            while True:
                request = assets_service_pb2.AllAssetsRequest()
                request.cursor = cursor
                response_stream = await stub.AllAssets(request, metadata=metadata)
                assets_result.extend(response_stream.assets)
                self._logger.info(f"Got {len(response_stream.assets)} assets from GRPC asset data source.")
                cursor = response_stream.next_cursor
                if cursor == 0:
                    break
                # if len(assets_result) >= 10000:
                #     break
        self._logger.info(f"Got total of {len(assets_result)} assets from GRPC asset data source.")

        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)
        current_date = datetime.datetime.now(tz=datetime.timezone.utc)
        exchanges_by_code = {exchange.mic: exchange for exchange in exchanges}

        # First fetch currencies
        currencies = []
        equities = []
        all_assets = {}
        futures = []
        exchange_assets = []
        currencies_symbol_set = set()
        currencies_symbol_set.add("RUB")
        currencies.append(Currency(
            asset_name="RUB",
            id=None,
            start_date=asset_start_date,
            end_date=asset_end_date,
            auto_close_date=asset_end_date,
            first_traded=asset_start_date,
            isin=None
        ))
        for asset in assets_result:
            if asset.mic not in exchanges_by_code:
                continue

            if asset.type == "CURRENCIES":
                print("CURRENCIES", asset.symbol, asset.name)

                try:
                    base, quote = asset.name.split("/")
                except Exception as e:
                    continue
                if base not in currencies_symbol_set:
                    currencies.append(Currency(
                        asset_name=base,
                        id=None,
                        start_date=asset_start_date,
                        end_date=asset_end_date,
                        auto_close_date=asset_end_date,
                        first_traded=asset_start_date,
                        isin=None
                    ))

                currencies_symbol_set.add(base)
                if quote not in currencies_symbol_set:
                    currencies.append(Currency(
                        asset_name=quote,
                        id=None,
                        start_date=asset_start_date,
                        end_date=asset_end_date,
                        auto_close_date=asset_end_date,
                        first_traded=asset_start_date,
                        isin=None
                    ))
                currencies_symbol_set.add(quote)
            elif asset.type == "EQUITIES":
                equity = Equity(
                    asset_name=asset.ticker,
                    id=None,
                    start_date=asset_start_date,
                    end_date=asset_end_date if not asset.is_archived else current_date,
                    auto_close_date=asset_end_date,
                    first_traded=asset_start_date,
                    isin=asset.isin
                )
                equities.append(equity)
                exchange_assets.append(ExchangeAsset(
                    sid=None,
                    symbol=asset.ticker,
                    exchange=exchanges_by_code[asset.mic],
                    start_date=equity.start_date,
                    end_date=equity.end_date,
                    auto_close_date=equity.end_date,
                    first_traded=equity.start_date,
                    asset=equity,
                    external_id=asset.id,
                    quote=currencies[0]  # base currency
                ))

        # currencies = [Currency(
        #     asset_name=currency,
        #     id=None,
        #     start_date=asset_start_date,
        #     end_date=asset_end_date,
        #     auto_close_date=asset_end_date,
        #     first_traded=asset_start_date,
        #     isin=None
        # ) for currency in ["USD", "RUB", "EUR"]]

        currency_by_name = {
            c.asset_name: c for c in currencies
        }

        for asset in assets_result:
            if asset.mic not in exchanges_by_code:
                continue

            if asset.ticker not in all_assets:
                if asset.type == "BONDS":
                    print("BONDS")
                    continue
                elif asset.type == "CURRENCIES":
                    try:
                        base, quote = asset.name.split("/")
                    except Exception as e:
                        continue
                    base_asset = currency_by_name[base]
                    quote_asset = currency_by_name[quote]
                    exchange_assets.append(
                        ExchangeAsset(
                            sid=None,
                            symbol=asset.ticker,
                            exchange=exchanges_by_code[asset.mic],
                            start_date=base_asset.start_date,
                            end_date=base_asset.end_date,
                            auto_close_date=base_asset.end_date,
                            first_traded=base_asset.start_date,
                            asset=base_asset,
                            quote=quote_asset,
                            external_id=asset.id,
                        )
                    )
                    print("CURRENCIES", asset.symbol)
                    continue
                elif asset.type == "EQUITIES":
                    continue  # already processed
                elif asset.type == "FUNDS":
                    print("FUNDS")
                    continue
                elif asset.type == "FUTURES":
                    continue
                    futures_asset = FuturesContract(
                        asset_name=asset.ticker,
                        id=None,
                        start_date=asset_start_date,
                        end_date=asset_end_date if not asset.is_archived else current_date,
                        auto_close_date=asset_end_date,
                        first_traded=asset_start_date,
                        isin=asset.isin,
                        root_asset=asset,
                        notice_date=None,
                        expiration_date=None,
                        tick_size=None,
                        multiplier=None
                    )
                    futures.append(futures_asset)
                    exchange_assets.append(ExchangeAsset(
                        sid=None,
                        symbol=asset.symbol,
                        exchange=exchanges_by_code[asset.mic],
                        start_date=futures_asset.start_date,
                        end_date=futures_asset.end_date,
                        auto_close_date=futures_asset.end_date,
                        first_traded=futures_asset.start_date,
                        asset=futures_asset,
                        external_id=asset.id,
                        quote=currency_by_name["RUB"]
                    ))
                elif asset.type == "INDICES":
                    print("INDICES")
                    continue
                elif asset.type == "OTHER":
                    print("OTHER")
                    continue
                elif asset.type == "SWAPS":
                    print("SWAPS")
                    continue

                # all_assets[asset.ticker] = Equity(
                #     asset_name=asset.ticker,
                #     id=None,
                #     start_date=asset_start_date,
                #     end_date=asset_end_date if not asset.is_archived else current_date,
                #     auto_close_date=asset_end_date,
                #     first_traded=asset_start_date,
                #     isin=asset.isin
                # )

            #
            # created_asset = all_assets[asset.symbol]
            # exchange_assets.append(
            #     ExchangeAsset(
            #         sid=None,
            #         symbol=asset.ticker,
            #         exchange=exchanges_by_code[asset.mic],
            #         start_date=created_asset.start_date,
            #         end_date=created_asset.end_date,
            #         auto_close_date=created_asset.end_date,
            #         first_traded=created_asset.start_date,
            #         asset=created_asset,
            #         external_id=asset.id,
            #         quote=currency_by_name["RUB"]
            #     )
            # )

        # exchange_currencies = [
        #     ExchangeAsset(
        #         sid=None,
        #         symbol=currency.asset_name,
        #         exchange=exchange,
        #         start_date=asset_start_date,
        #         end_date=asset_end_date,
        #         auto_close_date=asset_end_date,
        #         first_traded=asset_start_date,
        #         external_id=currency.asset_name,
        #         asset=currency
        #     )
        #     for exchange in exchanges
        #     for currency in currencies
        # ]

        # exchange_assets.extend(exchange_currencies)
        return AssetsImport(equities=equities, currencies=currencies,
                            futures=futures,
                            exchange_assets=exchange_assets)

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        request = assets_service_pb2.ExchangesRequest()
        token = await self.get_token()
        metadata = [('authorization', token)]
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = assets_service_pb2_grpc.AssetsServiceStub(channel)
            response_stream = await stub.Exchanges(request, metadata=metadata)
            exchanges = [
                ExchangeInfo(name=exchange.name, mic=exchange.mic, canonical_name=exchange.name, country_code="US")
                for exchange in response_stream.exchanges]
            self._logger.info(f"Got {len(exchanges)} exchanges from GRPC asset data source.")
        return exchanges

    async def _fetch_dividends_data(self,
                                    channel: grpc.aio.Channel,
                                    asset_service: AssetService,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    asset: ExchangeAsset,
                                    ) -> list[DividendPayout]:
        token = await self.get_token()
        stub = corporate_actions_service_pb2_grpc.CorporateActionsServiceStub(channel)
        metadata = [('authorization', token)]
        timestamp_from = Timestamp()
        timestamp_to = Timestamp()
        timestamp_from.FromDatetime(date_from)
        timestamp_to.FromDatetime(date_to)
        dividends = []
        dividends_request = corporate_actions_service_pb2.GetPastDividendsRequest(limit=1000)
        dividends_request.symbol = f"{asset.symbol}@{asset.mic}"
        self._logger.info(f"Fetching dividends for symbol {asset.symbol} - {date_from} to {date_to}",
                          symbol=asset.symbol, start_date=date_from, end_date=date_to)
        total_requests_time = 0
        request_start = time.time()
        response = await stub.GetPastDividends(dividends_request, metadata=metadata)
        duration = time.time() - request_start
        total_requests_time += duration
        for dividend in response.dividends:
            dividend_date = datetime.date(year=dividend.date.year, month=dividend.date.month,
                                          day=dividend.date.day
                                          )
            d = DividendPayout(
                asset=asset.asset,
                amount=float(dividend.amount.value),
                pay_date=dividend_date,
                ex_date=dividend_date,
                declared_date=dividend_date,
                record_date=dividend_date,
                currency=await asset_service.get_currency_by_symbol(symbol=dividend.currency)
            )
            dividends.append(d)

        self._logger.info(
            f"Fetched dividends data for symbol {asset.symbol} - {date_from} to {date_to} in {duration:.2f}",
            symbol=asset.symbol, start_date=date_from, end_date=date_to, duration=duration)

        return dividends

    async def _fetch_splits_data(self,
                                 channel: grpc.aio.Channel,
                                 asset_service: AssetService,
                                 date_from: datetime.datetime,
                                 date_to: datetime.datetime,
                                 asset: ExchangeAsset,
                                 ) -> list[Split]:
        token = await self.get_token()
        stub = corporate_actions_service_pb2_grpc.CorporateActionsServiceStub(channel)
        metadata = [('authorization', token)]
        timestamp_from = Timestamp()
        timestamp_to = Timestamp()
        timestamp_from.FromDatetime(date_from)
        timestamp_to.FromDatetime(date_to)
        splits = []
        splits_request = corporate_actions_service_pb2.GetPastSplitsRequest(limit=1000)
        splits_request.symbol = f"{asset.symbol}@{asset.mic}"
        self._logger.info(f"Fetching splits for symbol {asset.symbol} - {date_from} to {date_to}",
                          symbol=asset.symbol, start_date=date_from, end_date=date_to)
        total_requests_time = 0
        request_start = time.time()
        response = await stub.GetPastSplits(splits_request, metadata=metadata)
        duration = time.time() - request_start
        total_requests_time += duration
        for split in response.splits:
            s = Split(
                asset=asset.asset,
                ratio=float(split.old_ratio.value) / float(split.new_ratio.value),
                effective_date=datetime.date(year=split.exec_date.year, month=split.exec_date.month,
                                             day=split.exec_date.day
                                             ),
            )
            splits.append(s)

        self._logger.info(
            f"Fetched dividends data for symbol {asset.symbol} - {date_from} to {date_to} in {duration:.2f}",
            symbol=asset.symbol, start_date=date_from, end_date=date_to, duration=duration)

        return splits

    async def get_dividends(self, assets: list[ExchangeAsset],
                            asset_service: AssetService,
                            date_from: datetime.datetime,
                            date_to: datetime.datetime,
                            ) -> list[DividendPayout]:
        async def fetch_historical(asset: ExchangeAsset, asset_service: AssetService,
                                   start_date: datetime.datetime,
                                   end_date: datetime.datetime) -> list[DividendPayout]:
            try:
                credentials = grpc.ssl_channel_credentials()
                async with grpc.aio.secure_channel(self._server_url, credentials) as channel:

                    result = await self._fetch_dividends_data(channel=channel,
                                                              asset_service=asset_service,
                                                              date_from=start_date,
                                                              date_to=end_date,
                                                              asset=asset)
                    return result
            except Exception as e:
                self._logger.exception(
                    f"Exception fetching dividends data for symbol {asset.symbol}, date_from={start_date}, date_to={end_date}. Skipping."
                )
                return []

        total_days = (date_to - date_from).days
        dividend_payouts = []
        with progressbar(length=len(assets) * total_days, label="Downloading dividends data from GRPC",
                         file=sys.stdout) as pbar:
            tasks = [fetch_historical(
                asset=asset, asset_service=asset_service, start_date=date_from, end_date=date_to
            ) for asset in assets]
            total_duration = 0
            total_requests_duration = 0
            res = await asyncio.gather(*tasks)

            for item in res:
                pbar.update(total_days)
                dividend_payouts.extend(item)
            self._logger.info(
                f"Retrieved {len(dividend_payouts)} dividends for {assets} in {total_duration:.2f}s. "
                f"Total requests time: {total_requests_duration:.2f}s",
                total_duration=total_duration, requests_duration=total_requests_duration
            )

        return dividend_payouts

    async def get_splits(self, assets: list[ExchangeAsset],
                         asset_service: AssetService,
                         date_from: datetime.datetime,
                         date_to: datetime.datetime,
                         ) -> list[DividendPayout]:
        async def fetch_historical(asset: ExchangeAsset, asset_service: AssetService,
                                   start_date: datetime.datetime,
                                   end_date: datetime.datetime) -> list[DividendPayout]:
            try:
                credentials = grpc.ssl_channel_credentials()
                async with grpc.aio.secure_channel(self._server_url, credentials) as channel:

                    result = await self._fetch_splits_data(channel=channel,
                                                           asset_service=asset_service,
                                                           date_from=start_date,
                                                           date_to=end_date,
                                                           asset=asset)
                    return result
            except Exception as e:
                self._logger.exception(
                    f"Exception fetching splits data for symbol {asset.symbol}, date_from={start_date}, date_to={end_date}. Skipping."
                )
                return []

        total_days = (date_to - date_from).days
        splits = []
        with progressbar(length=len(assets) * total_days, label="Downloading splits data from GRPC",
                         file=sys.stdout) as pbar:
            tasks = [fetch_historical(
                asset=asset, asset_service=asset_service, start_date=date_from, end_date=date_to
            ) for asset in assets]
            total_duration = 0
            total_requests_duration = 0
            res = await asyncio.gather(*tasks)

            for item in res:
                pbar.update(total_days)
                splits.extend(item)
            self._logger.info(
                f"Retrieved {len(splits)} dividends for {assets} in {total_duration:.2f}s. "
                f"Total requests time: {total_requests_duration:.2f}s",
                total_duration=total_duration, requests_duration=total_requests_duration
            )

        return splits

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

    @classmethod
    def from_env(cls) -> Self:
        token = os.environ.get("GRPC_TOKEN", None)
        server_url = os.environ.get("GRPC_SERVER_URL")
        maximum_threads = os.environ.get("GRPC_MAXIMUM_THREADS", None)
        if token is None:
            raise ValueError("Missing GRPC_TOKEN environment variable.")
        return cls(server_url=server_url, authorization_token=token, maximum_threads=maximum_threads)
