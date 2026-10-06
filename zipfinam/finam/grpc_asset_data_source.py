import datetime
import multiprocessing
import os
from typing import Self

import aiocache
import grpc
import structlog

import polars as pl
from aiocache import Cache

from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.auth import auth_service_pb2_grpc, auth_service_pb2
from zipfinam.finam.grpc_stubs.grpc.tradeapi.v1.assets import assets_service_pb2_grpc, \
    assets_service_pb2
from ziplime.assets.entities.exchange_asset import ExchangeAsset


#: MICs of the Moscow Exchange markets Finam lists; everything else Finam carries is American.
RUSSIAN_MICS = frozenset({"MISX", "RTSX", "XMOS", "MCXX"})


#: Finam asset types left out of the import, matched as substrings of ``Asset.type``.
DERIVATIVE_TYPES = ("FUTURE", "OPTION", "SWAP")


def _country_code(mic: str) -> str:
    return "RU" if mic in RUSSIAN_MICS else "US"


def _quote_currency(mic: str) -> str:
    return "RUB" if mic in RUSSIAN_MICS else "USD"


class GrpcAssetDataSource(AssetDataSource):
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
        self._logger.info(f"Got total of {len(assets_result)} assets from GRPC asset data source.")

        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)
        current_date = datetime.datetime.now(tz=datetime.timezone.utc)
        exchanges_by_code = {exchange.mic: exchange for exchange in exchanges}

        currencies = {
            code: Currency(asset_name=code, id=None, start_date=asset_start_date,
                           end_date=asset_end_date, auto_close_date=asset_end_date,
                           first_traded=asset_start_date, isin=None)
            for code in ("RUB", "USD")
        }
        equities: dict[str, Equity] = {}
        exchange_assets = []
        skipped = 0
        for asset in assets_result:
            if asset.mic not in exchanges_by_code:
                continue
            if any(kind in (asset.type or "").upper() for kind in DERIVATIVE_TYPES):
                # A futures or option listed as a share would trade at a multiplier of one.
                skipped += 1
                continue
            if asset.ticker not in equities:
                equities[asset.ticker] = Equity(
                    asset_name=asset.ticker,
                    id=None,
                    start_date=asset_start_date,
                    end_date=asset_end_date if not asset.is_archived else current_date,
                    auto_close_date=asset_end_date,
                    first_traded=asset_start_date,
                    isin=asset.isin
                )
            equity = equities[asset.ticker]
            exchange_assets.append(
                ExchangeAsset(
                    sid=None,
                    symbol=asset.ticker,
                    exchange=exchanges_by_code[asset.mic],
                    start_date=equity.start_date,
                    end_date=equity.end_date,
                    auto_close_date=equity.end_date,
                    first_traded=equity.start_date,
                    asset=equity,
                    quote=currencies[_quote_currency(asset.mic)],
                    external_id=asset.id
                )
            )
        if skipped:
            self._logger.info(f"Skipped {skipped} derivatives: zipfinam imports shares and funds.")
        return AssetsImport(exchange_assets=exchange_assets, currencies=list(currencies.values()),
                            equities=list(equities.values()))

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        request = assets_service_pb2.ExchangesRequest()
        token = await self.get_token()
        metadata = [('authorization', token)]
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = assets_service_pb2_grpc.AssetsServiceStub(channel)
            response_stream = await stub.Exchanges(request, metadata=metadata)
            exchanges = [
                ExchangeInfo(name=exchange.name, mic=exchange.mic, canonical_name=exchange.name,
                             country_code=_country_code(exchange.mic))
                for exchange in response_stream.exchanges]
            self._logger.info(f"Got {len(exchanges)} exchanges from GRPC asset data source.")
        return exchanges

    async def get_constituents(self, index: str) -> pl.DataFrame:
        raise NotImplementedError("Finam Trade API does not publish index constituents")

    @classmethod
    def from_env(cls) -> Self:
        token = os.environ.get("GRPC_TOKEN", None)
        server_url = os.environ.get("GRPC_SERVER_URL", "api.finam.ru:443")
        maximum_threads_raw = os.environ.get("GRPC_MAXIMUM_THREADS", None)
        maximum_threads = int(maximum_threads_raw) if maximum_threads_raw is not None else None
        if token is None:
            raise ValueError("Missing GRPC_TOKEN environment variable.")
        return cls(server_url=server_url, authorization_token=token, maximum_threads=maximum_threads)
