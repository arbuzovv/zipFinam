"""Registers the Finam connector with ziplime.

ziplime finds it through the ``ziplime.data_providers`` entry point, so once zipfinam is installed
``finam`` is a provider name like ``yahoo``. Loading it also adds the ``algopack://`` scheme.
"""
from ziplime.assets.domain.asset_type import AssetType
from ziplime.data.data_sources.registry import DataProvider, register_provider

from zipfinam.algopack import install_address_scheme
from zipfinam.finam.grpc_asset_data_source import GrpcAssetDataSource
from zipfinam.finam.grpc_data_source import GrpcDataSource


def _asset_data_source(**kwargs) -> GrpcAssetDataSource:
    return GrpcAssetDataSource.from_env()


def _market_data_source(assets=None, **kwargs) -> GrpcDataSource:
    return GrpcDataSource.from_env()


FINAM = register_provider(DataProvider(
    name="finam",
    description="Moscow Exchange equities, bonds and futures through the Finam Trade API "
                "(GRPC_TOKEN from the Finam account)",
    asset_data_source_factory=_asset_data_source,
    market_data_source_factory=_market_data_source,
    required_env=("GRPC_TOKEN",),
    default_mic="MISX",
    default_calendar="XMOS",
    asset_types=(AssetType.EQUITY.value,),
))

install_address_scheme()
