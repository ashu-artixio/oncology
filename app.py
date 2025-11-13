# quriousri_indications_import/app.py
"""CLI entry point for the MONDO disease ontology ingestion workflow."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path

    _CURRENT_DIR = _Path(__file__).resolve().parent
    if str(_CURRENT_DIR) not in _sys.path:
        _sys.path.insert(0, str(_CURRENT_DIR))

    from mondo_ingest_runner import MondoIngestRunner
else:
    from .mondo_ingest_runner import MondoIngestRunner

LOGGER = logging.getLogger("MONDO")


def _load_config(config_path: Path) -> Dict[str, Any]:
    """Load the ingestion module configuration from disk."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)

    if not isinstance(config, dict):
        raise ValueError("Configuration must be a JSON object.")

    return config


def _resolve_module_config(config: Dict[str, Any], module_name: str) -> Dict[str, Any]:
    """Fetch module-specific configuration and validate enablement."""
    module_config = config.get(module_name)
    if not module_config:
        raise KeyError(f"Module '{module_name}' is not defined in config.json.")

    if not module_config.get("enabled", False):
        raise RuntimeError(f"Module '{module_name}' is disabled in config.json.")

    return module_config


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="QuriousRI MONDO ontology ingestion orchestrator.",
    )
    parser.add_argument(
        "module",
        choices=["mondo_ingest"],
        help="Ingestion module to execute.",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Relative or absolute path to the ingestion configuration file.",
    )
    parser.add_argument(
        "--updated-since",
        dest="updated_since",
        help="ISO-8601 timestamp for incremental ingestion (e.g. 2024-01-31T00:00:00Z).",
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        help="Override batch size for database writes.",
    )
    parser.add_argument(
        "--cache-dir",
        dest="cache_dir",
        help="Override cache directory for downloaded MONDO artifacts.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments and execute the requested ingestion module."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config).expanduser().resolve()
    config = _load_config(config_path)
    module_config = _resolve_module_config(config, args.module)

    if args.batch_size:
        module_config["batch_size"] = args.batch_size
    if args.cache_dir:
        module_config["cache_dir"] = args.cache_dir

    runner = MondoIngestRunner(module_config=module_config)
    runner.run(updated_since=args.updated_since)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    try:
        main(sys.argv[1:])
    except Exception as exc:  # pragma: no cover - CLI safeguard
        LOGGER.exception("Fatal error during MONDO ingestion: %s", exc)
        raise SystemExit(1) from exc

