"""zipFinam: the Russian market for ziplime.

ziplime covers American data and execution. This package adds what a Moscow strategy needs on top
of it: Moscow Exchange bars and instruments through the Finam Trade API, MOEX AlgoPack tables,
the FORTS session calendar, and order execution on Finam -- Arena for paper trading and the Trade
API for a real account.

Installing it is enough: ziplime (2.11 or later) finds the Moscow venues -- MISX on XMOS, RTSX on
FORTS -- through the ``ziplime.venue_calendars`` entry point and the ``algopack://`` scheme of
``data.history(data_source=...)`` through ``ziplime.address_schemes``. Importing it registers the
same things explicitly.
"""
from zipfinam.algopack import AlgoPackDataSource, algopack_dataset, install_address_scheme
from zipfinam.venue_calendars import FORTSExchangeCalendar, calendar_for_mic, clock_calendar_name

__version__ = "1.26.10.1"

#: The Finam sources, imported on first use. Their generated gRPC stubs check the installed grpcio
#: at import time, and ziplime imports this package to read the venue calendars and the
#: ``algopack://`` scheme -- neither of which needs Finam. Importing the stubs here eagerly made a
#: grpcio mismatch break both. ``Finam*`` are the clearer names; ``Grpc*`` stay for 0.x code.
_LAZY = {
    "GrpcDataSource": ("zipfinam.finam.grpc_data_source", "GrpcDataSource"),
    "FinamDataSource": ("zipfinam.finam.grpc_data_source", "GrpcDataSource"),
    "GrpcAssetDataSource": ("zipfinam.finam.grpc_asset_data_source", "GrpcAssetDataSource"),
    "FinamAssetDataSource": ("zipfinam.finam.grpc_asset_data_source", "GrpcAssetDataSource"),
}


def __getattr__(name: str):
    if name in _LAZY:
        import importlib

        module, attribute = _LAZY[name]
        value = getattr(importlib.import_module(module), attribute)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'zipfinam' has no attribute {name!r}")


__all__ = [
    "AlgoPackDataSource", "FORTSExchangeCalendar", "FinamAssetDataSource", "FinamDataSource",
    "GrpcAssetDataSource", "GrpcDataSource", "algopack_dataset", "calendar_for_mic",
    "clock_calendar_name",
]

install_address_scheme()
