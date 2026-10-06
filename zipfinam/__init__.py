"""zipFinam: the Russian market for ziplime.

ziplime covers American data and execution. This package adds what a Moscow strategy needs on top
of it: Moscow Exchange bars and instruments through the Finam Trade API, MOEX AlgoPack tables,
the FORTS session calendar, and order execution on Finam -- Arena for paper trading and the Trade
API for a real account.

Importing it adds the ``algopack://`` scheme to ``data.history(data_source=...)``.
"""
from zipfinam.algopack import AlgoPackDataSource, algopack_dataset, install_address_scheme
from zipfinam.finam.grpc_asset_data_source import GrpcAssetDataSource
from zipfinam.finam.grpc_data_source import GrpcDataSource
from zipfinam.venue_calendars import FORTSExchangeCalendar, calendar_for_mic, clock_calendar_name

__version__ = "1.0.0"

#: Clearer names for the Finam sources; the Grpc* ones stay for code written against 0.x.
FinamDataSource = GrpcDataSource
FinamAssetDataSource = GrpcAssetDataSource

__all__ = [
    "AlgoPackDataSource", "FORTSExchangeCalendar", "FinamAssetDataSource", "FinamDataSource",
    "GrpcAssetDataSource", "GrpcDataSource", "algopack_dataset", "calendar_for_mic",
    "clock_calendar_name",
]

install_address_scheme()
