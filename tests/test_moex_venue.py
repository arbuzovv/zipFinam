"""The MOEX venue: its conventions, and its own way of naming a contract.

Every value asserted here was measured against the live Limex reference feed rather than looked
up: SBER options came back EUROPEAN, cash settled, listed on ``RTSX``, and named ``SR310CI6``.
"""
import datetime
import unittest

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.data.data_sources.options.venues import get_venue

from ziplime_grpc_data_source.moex import (
    MOEX, format_moex_symbol, is_monthly_expiry, parse_moex_symbol,
)


class RegistrationTests(unittest.TestCase):
    def test_importing_the_package_registers_the_venue_with_ziplime(self):
        """ziplime ships OPRA alone; this is how the Moscow Exchange row reaches it."""
        self.assertIs(get_venue("MOEX"), MOEX)

    def test_the_conventions_are_the_ones_measured_on_the_feed(self):
        self.assertIs(MOEX.premium_style, PremiumStyle.MARGINED)
        self.assertIs(MOEX.exercise_style, ExerciseStyle.EUROPEAN)
        self.assertEqual(MOEX.mic, "RTSX")

    def test_the_contract_size_is_the_specification_not_the_feed(self):
        """The feed reports a placeholder. Taking it literally makes a contract worth about three
        roubles, and a strategy sizing on risk then asks for twelve thousand lots of something
        that trades thirty a day."""
        self.assertEqual(MOEX.contract_size, 100.0)


class NamingTests(unittest.TestCase):
    def setUp(self):
        self.expiry = datetime.date(2026, 9, 16)

    def test_codes_round_trip_against_the_ones_the_feed_returned(self):
        for code, option_type in (("SR310CI6", OptionType.CALL), ("SR280CU6", OptionType.PUT),
                                  ("GZ150CI6", OptionType.CALL)):
            root, strike, parsed, year = parse_moex_symbol(code)
            self.assertIs(parsed, option_type, code)
            self.assertEqual(
                format_moex_symbol(root, strike, parsed, datetime.date(year, 9, 16)), code)

    def test_a_fractional_strike_cannot_be_named(self):
        """The scheme carries an integer strike, so rounding would land on another contract."""
        with self.assertRaises(ValueError):
            format_moex_symbol("SR", 310.5, OptionType.CALL, self.expiry)

    def test_a_weekly_needs_a_week_letter_its_date_does_not_determine(self):
        """2026-09-23 is 'SR300CI6D' and 2026-09-30 is 'SR300CJ6A' -- October's month letter. The
        rule relating the two is not derivable from the date, so this refuses rather than guesses:
        guessing collapses three series onto one name."""
        weekly = datetime.date(2026, 9, 23)
        self.assertFalse(is_monthly_expiry(weekly))
        with self.assertRaises(ValueError):
            format_moex_symbol("SR", 300.0, OptionType.CALL, weekly)

    def test_a_monthly_expiry_is_the_third_wednesday(self):
        self.assertTrue(is_monthly_expiry(datetime.date(2026, 9, 16)))
        self.assertFalse(is_monthly_expiry(datetime.date(2026, 9, 9)))

    def test_the_venue_names_its_own_contracts(self):
        """`OptionVenue.symbol_for` delegates to the venue's own formatter; ziplime no longer has
        a branch for this scheme."""
        spec = MOEX.contract("SBER", self.expiry, OptionType.CALL, 310.0, self.expiry, root="SR")
        self.assertEqual(spec.listing_symbol, "SR310CI6")
        self.assertEqual(spec.occ_symbol, "SBER260916C00310000")


if __name__ == "__main__":
    unittest.main(verbosity=2)
