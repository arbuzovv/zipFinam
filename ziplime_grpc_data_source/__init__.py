"""Former name of zipfinam's Finam sources, kept so code written against 0.x still imports.

New code: ``from zipfinam import FinamDataSource, FinamAssetDataSource``.
"""
from zipfinam.finam.grpc_asset_data_source import GrpcAssetDataSource
from zipfinam.finam.grpc_data_source import GrpcDataSource

__all__ = ["GrpcDataSource", "GrpcAssetDataSource"]
