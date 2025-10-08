"""Command line interface for TSLA options collector."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from futu import OpenQuoteContext

from .collector import OptionCollector, OptionFilter, OptionSubscriptionManager
from .storage import DuckDBStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tsla-options-collector",
        description="Collect and export TSLA option quote/ticker streams.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=None,
        help="Optional log file path.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Start the realtime collector")
    collect.add_argument("--host", default="127.0.0.1", help="OpenD host")
    collect.add_argument("--port", type=int, default=11111, help="OpenD port")
    collect.add_argument(
        "--db-path",
        type=Path,
        default=Path("./tsla_options.duckdb"),
        help="Path to DuckDB database file.",
    )
    collect.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Optional runtime in seconds (omit to run until interrupted).",
    )

    export = subparsers.add_parser("export", help="Export stored data via DuckDB")
    export.add_argument(
        "--db-path",
        type=Path,
        required=True,
        help="Path to DuckDB database file.",
    )
    export.add_argument(
        "--table",
        choices=["quotes", "tickers"],
        required=True,
        help="Table to export.",
    )
    export.add_argument(
        "--format",
        choices=["csv", "parquet"],
        required=True,
        help="Export format.",
    )
    export.add_argument(
        "--start",
        type=_parse_datetime,
        default=None,
        help="Optional start timestamp (ISO-8601) for ingested_at filter.",
    )
    export.add_argument(
        "--end",
        type=_parse_datetime,
        default=None,
        help="Optional end timestamp (ISO-8601) for ingested_at filter.",
    )
    export.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination file path.",
    )

    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger = _configure_logging(args.log_level, args.log_path)

    if args.command == "collect":
        _run_collect(args, logger)
    elif args.command == "export":
        _run_export(args, logger)
    else:
        parser.error(f"Unknown command: {args.command}")


def _configure_logging(level: str, log_path: Optional[Path]) -> logging.Logger:
    handlers = []
    if log_path:
        handlers.append(logging.FileHandler(log_path))
    else:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=handlers,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger("tsla_options_collector")


def _run_collect(args: argparse.Namespace, logger: logging.Logger) -> None:
    quote_ctx = OpenQuoteContext(host=args.host, port=args.port)
    storage = DuckDBStorage(args.db_path, logger=logger)
    subscription_manager = OptionSubscriptionManager(quote_ctx, logger)
    option_filter = OptionFilter(quote_ctx, subscription_manager, logger)
    collector = OptionCollector(
        quote_ctx, storage, subscription_manager, option_filter, logger
    )
    try:
        collector.run(duration=args.duration)
    except Exception as exc:  # pragma: no cover - surface error to CLI
        logger.exception("Collector terminated with error: %s", exc)
        raise SystemExit(1) from exc
    finally:
        storage.close()
        quote_ctx.close()


def _run_export(args: argparse.Namespace, logger: logging.Logger) -> None:
    start = args.start
    end = args.end
    if start and end and end < start:
        raise SystemExit("--end must be greater than or equal to --start")
    storage = DuckDBStorage(args.db_path, logger=logger)
    try:
        if args.table == "quotes":
            storage.export_quotes(args.format, start, end, args.output)
        else:
            storage.export_tickers(args.format, start, end, args.output)
    finally:
        storage.close()


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - argument parsing error
        raise argparse.ArgumentTypeError(str(exc)) from exc


if __name__ == "__main__":
    main()

