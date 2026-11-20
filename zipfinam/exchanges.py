"""Order execution on Finam.

* :class:`FinamExchange` -- REST: Arena (arena.finam.ru) for paper trading, or a real account
  through the Trade API (api.finam.ru). Quotes, bars and lot sizes always come from the Trade API.
* :class:`FinamGrpcExchange` -- the same Trade API over gRPC.

Both need ``finam-trade-api``: ``pip install "zipfinam[live]"``.
"""
from zipfinam.finam.errors import BrokerAuthError
from zipfinam.finam.finam_arena_exchange import FinamArenaExchange
from zipfinam.finam.grpc_exchange import GrpcExchange

FinamExchange = FinamArenaExchange
FinamGrpcExchange = GrpcExchange

__all__ = ["BrokerAuthError", "FinamArenaExchange", "FinamExchange", "FinamGrpcExchange",
           "GrpcExchange"]
