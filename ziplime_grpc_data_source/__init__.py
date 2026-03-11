"""
ziplime-grpc-data-source — источник данных на основе gRPC для Ziplime (брокер Финам).
"""
import pathlib
import shutil

from ziplime_grpc_data_source.grpc_data_source import GrpcDataSource
from ziplime_grpc_data_source.grpc_asset_data_source import GrpcAssetDataSource

__version__ = "0.1.1"
__all__ = ["GrpcDataSource", "GrpcAssetDataSource"]


def _install_assets() -> None:
    """Копирует assets.sqlite в ~/.ziplime/ при первом импорте пакета."""
    dest_dir = pathlib.Path.home() / ".ziplime"
    dest_file = dest_dir / "assets.sqlite"
    if not dest_file.exists():
        dest_dir.mkdir(parents=True, exist_ok=True)
        src = pathlib.Path(__file__).parent / "assets.sqlite"
        if src.exists():
            shutil.copy2(src, dest_file)


_install_assets()
