"""Offline checks of the Finam adapters: no token, no network.

What is covered is what goes wrong silently on a real account -- a broker status read as success,
a lot size guessed, cash read from the wrong shape of answer -- rather than the request plumbing,
which only a live account can test.
"""
import asyncio
import datetime
import unittest
from unittest import mock

import structlog

from zipfinam.finam.allocation import Holding, allocated_cash, round_to_lots, strategy_holdings
from zipfinam.finam.finam_arena_exchange import FinamArenaExchange
from zipfinam.finam.grpc_asset_data_source import _country_code
from zipfinam.finam.grpc_data_source import GrpcDataSource
from zipfinam.finam.order_status import decimal_value, describe_order, is_terminal, order_outcome


class OrderOutcomeTests(unittest.TestCase):

    def test_an_accepted_order_is_not_a_filled_one(self):
        self.assertEqual(order_outcome("ORDER_STATUS_NEW", 0), "pending")

    def test_a_broker_rejection_is_a_failure(self):
        self.assertEqual(order_outcome("ORDER_STATUS_REJECTED_BY_EXCHANGE", 0), "rejected")

    def test_a_cancel_after_a_partial_fill_keeps_the_fill(self):
        self.assertEqual(order_outcome("ORDER_STATUS_CANCELED", 10), "partially_filled")

    def test_no_status_is_unknown_rather_than_success(self):
        self.assertEqual(order_outcome(None, None), "unknown")

    def test_filled_is_terminal_and_new_is_not(self):
        self.assertTrue(is_terminal("order_status_filled"))
        self.assertFalse(is_terminal("ORDER_STATUS_NEW"))

    def test_finam_decimals_come_in_three_shapes(self):
        self.assertEqual(decimal_value({"value": "141.5"}), 141.5)
        self.assertEqual(decimal_value("10"), 10.0)
        self.assertIsNone(decimal_value({"value": ""}))

    def test_a_rejection_reads_as_not_executed(self):
        line = describe_order({"side": "SIDE_BUY", "quantity": 10, "symbol": "SBER@MISX",
                               "exchange_order_id": "1", "status": "ORDER_STATUS_REJECTED",
                               "outcome": "rejected"})
        self.assertIn("not executed", line)


class AllocationTests(unittest.TestCase):

    def test_a_delta_below_one_lot_is_not_ordered(self):
        self.assertEqual(round_to_lots(7, 10), 0)

    def test_sells_round_toward_zero_too(self):
        self.assertEqual(round_to_lots(-25, 10), -20)

    def test_cash_is_capital_less_the_cost_of_the_strategys_own_positions(self):
        holdings = [Holding("SBER@MISX", 100, 300.0), Holding("GAZP@MISX", 50, 150.0)]
        mine = strategy_holdings(holdings, {"sber@misx"})
        self.assertEqual(allocated_cash(capital=100_000, holdings=mine), 70_000)


class CashTests(unittest.TestCase):

    def test_arena_reports_one_scalar(self):
        self.assertEqual(FinamArenaExchange._parse_cash({"value": "1012094.96"}), 1012094.96)

    def test_the_trade_api_reports_money_per_currency(self):
        cash = [{"currency_code": "RUB", "units": "1000", "nanos": 500_000_000},
                {"currency_code": "USD", "units": "7", "nanos": 0}]
        self.assertEqual(FinamArenaExchange._parse_cash(cash), 1000.5)

    def test_an_unfunded_account_has_no_cash_rather_than_an_error(self):
        self.assertEqual(FinamArenaExchange._parse_cash([]), 0.0)


class LotSizeTests(unittest.TestCase):

    def _exchange(self) -> FinamArenaExchange:
        exchange = object.__new__(FinamArenaExchange)
        exchange._finam_api_server_url = "api.finam.ru:443"
        exchange._finam_api_token = "not-a-token"
        exchange._logger = structlog.get_logger(__name__)
        return exchange

    def test_an_unreadable_lot_refuses_the_order_instead_of_guessing_one(self):
        failing = mock.patch("aiohttp.ClientSession", side_effect=OSError("network is down"))
        with failing, self.assertRaisesRegex(RuntimeError, "lot size of GAZP@MISX"):
            asyncio.run(self._exchange()._lot_size("GAZP@MISX"))

    def test_a_known_lot_is_not_asked_for_again(self):
        exchange = self._exchange()
        exchange._lot_sizes = {"GAZP@MISX": 10.0}
        self.assertEqual(asyncio.run(exchange._lot_size("GAZP@MISX")), 10.0)


class MarketDataTests(unittest.TestCase):

    def test_moscow_venues_are_russian(self):
        self.assertEqual(_country_code("MISX"), "RU")
        self.assertEqual(_country_code("RTSX"), "RU")
        self.assertEqual(_country_code("XNGS"), "US")

    def test_timeframes_cover_minute_to_quarter(self):
        source = GrpcDataSource(authorization_token="x", server_url="api.finam.ru:443")
        self.assertEqual(source._get_timeframe(datetime.timedelta(minutes=1)), "TIME_FRAME_M1")
        self.assertEqual(source._get_timeframe(datetime.timedelta(hours=1)), "TIME_FRAME_H1")
        self.assertEqual(source._get_timeframe(datetime.timedelta(days=1)), "TIME_FRAME_D")
        self.assertEqual(source._get_timeframe(datetime.timedelta(days=90)), "TIME_FRAME_QR")




class ConstructionTests(unittest.TestCase):

    def test_the_exchange_builds_on_a_realtime_clock_without_the_network(self):
        from ziplime.gens.domain.realtime_clock import RealtimeClock
        from ziplime.utils.calendar_utils import get_calendar

        from zipfinam.exchanges import FinamExchange

        calendar = get_calendar("XMOS")
        clock = RealtimeClock(trading_calendar=calendar,
                              start_date=datetime.datetime.now(tz=calendar.tz),
                              emission_rate=datetime.timedelta(minutes=1))
        exchange = FinamExchange(
            name="finam", canonical_name="finam", country_code="RU", asset_service=None,
            trading_calendar=calendar, clock=clock, finam_arena_server_url="https://arena.finam.ru",
            authorization_token="t", account_id="A1", finam_api_server_url="https://api.finam.ru",
            finam_api_token="t", start_cash_balance=100_000.0, capital_limit=100_000.0,
            is_default=True)
        self.assertEqual(exchange.get_start_cash_balance(), 100_000.0)


if __name__ == "__main__":
    unittest.main()
