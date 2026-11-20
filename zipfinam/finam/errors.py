"""Errors shared by the exchange adapters and the code that drives them tick by tick.

Kept free of heavy imports (no ziplime, no finam-trade-api) so a caller can
classify an exception without loading the trading stack.
"""


class BrokerAuthError(PermissionError):
    """The broker refused this run's credentials (HTTP 401/403).

    Separate from every other failure because no retry and no redeploy fixes
    it: the token belongs to the user's stored provider credential, and only
    they can replace it.
    """
