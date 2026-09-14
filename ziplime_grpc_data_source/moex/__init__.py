"""Everything specific to the Moscow Exchange.

Importing this package has two side effects, both deliberate and both idempotent: it registers the
``MOEX`` option venue with ziplime, and it registers the corrected ``XMOS`` trading calendar. Both
are the kind of thing that has to be in place before anything asks for it by name, and neither has
a sensible "call this first" story for a user.
"""
from ziplime_grpc_data_source.moex import calendars  # noqa: F401  (registers XMOS)
from ziplime_grpc_data_source.moex.venue import (  # noqa: F401
    MOEX, format_moex_symbol, is_monthly_expiry, parse_moex_symbol,
)

__all__ = ["MOEX", "format_moex_symbol", "is_monthly_expiry", "parse_moex_symbol"]
