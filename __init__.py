# quriousri_indications_import/__init__.py
"""Package initialization for the MONDO indications ingestion pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("quriousri_indications_import")
except PackageNotFoundError:  # pragma: no cover - package metadata absent locally
    __version__ = "0.0.0"

__all__ = ["__version__"]

