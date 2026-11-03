import contextlib
import io
import unittest
from unittest.mock import AsyncMock, Mock, patch

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime_grpc_data_source.finam_asset_data_source import FinamAssetDataSource
from ziplime_grpc_data_source.grpc_stubs.grpc.tradeapi.v1.assets import assets_service_pb2


class FinamAssetDataSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.source = FinamAssetDataSource(authorization_token="secret", server_url="example:443")
        self.source._logger = Mock()

    async def _get_assets(self, *pages):
        source = self.source
        source.get_token = AsyncMock(return_value="token")
        exchanges = [
            ExchangeInfo(mic=mic, name=mic, canonical_name=mic, country_code="RU")
            for mic in ("MISX", "_NPRO")
        ]
        with (
            patch("ziplime_grpc_data_source.finam_asset_data_source.grpc.aio.secure_channel"),
            patch("ziplime_grpc_data_source.finam_asset_data_source.assets_service_pb2_grpc.AssetsServiceStub") as stub_class,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            stub = stub_class.return_value
            stub.AllAssets = AsyncMock(side_effect=pages)
            result = await source.get_assets(exchanges=exchanges)
        return result, stub

    async def test_requested_symbols_share_usd_rub_currencies_and_keep_their_tenors(self):
        tenors = [
            "ON", "TN", "TOD", "TODSPOT", "SPOT", "1M", "1W", "1Y", "2M",
            "2W", "3M", "6M", "9M", "TOM2M",
        ]
        assets = [
            assets_service_pb2.Asset(
                symbol="SBER@MISX", ticker="SBER", mic="MISX", id="3",
                type="EQUITIES", name="Sberbank",
            ),
            *[
                assets_service_pb2.Asset(
                    symbol=f"USD/RUB_{tenor}@_NPRO", ticker=f"USD/RUB_{tenor}",
                    mic="_NPRO", id=str(index), type="CURRENCIES", name=f"USD/RUB_{tenor}",
                )
                for index, tenor in enumerate(tenors, start=100)
            ],
        ]
        result, stub = await self._get_assets(
            assets_service_pb2.AllAssetsResponse(assets=assets[:7], next_cursor=7),
            assets_service_pb2.AllAssetsResponse(assets=assets[7:]),
        )

        currencies = {currency.asset_name: currency for currency in result.currencies}
        self.assertEqual(set(currencies), {"USD", "RUB"})
        self.assertEqual(len(result.currencies), 2)
        self.assertEqual(len(result.exchange_assets), len(assets))
        listings = {(listing.symbol, listing.mic): listing for listing in result.exchange_assets}
        for index, tenor in enumerate(tenors, start=100):
            with self.subTest(tenor=tenor):
                listing = listings[(f"USD/RUB_{tenor}", "_NPRO")]
                self.assertIsInstance(listing.asset, Currency)
                self.assertIs(listing.asset, currencies["USD"])
                self.assertIs(listing.quote, currencies["RUB"])
                self.assertEqual(listing.external_id, str(index))

        sber = listings[("SBER", "MISX")]
        self.assertIsInstance(sber.asset, Equity)
        self.assertEqual(sber.asset.asset_name, "SBER")
        self.assertIs(sber.quote, currencies["RUB"])
        self.assertEqual([call.args[0].cursor for call in stub.AllAssets.await_args_list], [0, 7])
        self.assertTrue(all(call.kwargs["metadata"] == [("authorization", "token")]
                            for call in stub.AllAssets.await_args_list))

    async def test_opaque_tickers_and_localized_names_still_produce_currency_pairs(self):
        assets = [
            assets_service_pb2.Asset(
                symbol="USD000UTSTOM@MISX", ticker="USD000UTSTOM", mic="MISX", id="1",
                type="CURRENCIES", name="USD/RUB_TOM",
            ),
            assets_service_pb2.Asset(
                symbol="EUR/RUB_TOD@_NPRO", ticker="EUR/RUB_TOD", mic="_NPRO", id="2",
                type="CURRENCIES", name="Евро / Российский рубль",
            ),
            assets_service_pb2.Asset(
                symbol="CNY/USD_TOM@MISX", ticker="CNYUSD_TOM", mic="MISX", id="3",
                type="CURRENCIES", name="Chinese yuan / US dollar",
            ),
        ]
        result, _ = await self._get_assets(assets_service_pb2.AllAssetsResponse(assets=assets))

        self.assertEqual({currency.asset_name for currency in result.currencies},
                         {"USD", "RUB", "EUR", "CNY"})
        self.assertEqual(
            [(listing.symbol, listing.asset.asset_name, listing.quote.asset_name)
             for listing in result.exchange_assets],
            [("USD000UTSTOM", "USD", "RUB"), ("EUR/RUB_TOD", "EUR", "RUB"),
             ("CNYUSD_TOM", "CNY", "USD")],
        )

    async def test_invalid_pairs_and_unselected_exchanges_are_skipped(self):
        invalid_pairs = ["", "USD", "USD//RUB", "USD/", "/RUB", "USD/RUB/EUR",
                         "US/RUB", "USD/Russian ruble"]
        assets = [
            assets_service_pb2.Asset(
                symbol=f"invalid{index}@_NPRO", ticker=f"invalid{index}", mic="_NPRO",
                id=str(index), type="CURRENCIES", name=name,
            )
            for index, name in enumerate(invalid_pairs)
        ]
        assets.append(assets_service_pb2.Asset(
            symbol="EUR/GBP_TOD@OTHER", ticker="EUR/GBP_TOD", mic="OTHER", id="other",
            type="CURRENCIES", name="EUR/GBP_TOD",
        ))
        result, _ = await self._get_assets(assets_service_pb2.AllAssetsResponse(assets=assets))

        self.assertEqual([currency.asset_name for currency in result.currencies], ["RUB"])
        self.assertEqual(result.exchange_assets, [])

    async def test_non_iso_currency_codes_are_skipped_without_stopping_import(self):
        pairs = ["ZZZ/RUB", "USD/ZZZ", "ZZZ/YYY", "BTC/USD", "CNH/RUB", "USD/RUB"]
        assets = [
            assets_service_pb2.Asset(
                symbol=f"{pair}_TOD@_NPRO", ticker=f"{pair}_TOD", mic="_NPRO",
                id=str(index), type="CURRENCIES", name=f"{pair}_TOD",
            )
            for index, pair in enumerate(pairs)
        ]
        result, _ = await self._get_assets(assets_service_pb2.AllAssetsResponse(assets=assets))

        self.assertEqual({currency.asset_name for currency in result.currencies}, {"USD", "RUB"})
        self.assertEqual(
            [(listing.symbol, listing.asset.asset_name, listing.quote.asset_name)
             for listing in result.exchange_assets],
            [("USD/RUB_TOD", "USD", "RUB")],
        )

    def test_valid_pair_in_name_is_used_when_ticker_has_non_iso_codes(self):
        asset = assets_service_pb2.Asset(
            ticker="ZZZ/RUB_TOD", symbol="ZZZ/RUB_TOD@_NPRO", name="USD/RUB_TOD",
        )
        self.assertEqual(self.source._parse_currency_pair(asset), ("USD", "RUB"))

    def test_pair_codes_are_normalized_and_ticker_takes_precedence_over_name(self):
        asset = assets_service_pb2.Asset(
            ticker=" usd/rub_tod ", symbol="USD/RUB_TOD@_NPRO", name="EUR/RUB_TOD",
        )
        self.assertEqual(self.source._parse_currency_pair(asset), ("USD", "RUB"))


if __name__ == "__main__":
    unittest.main()
