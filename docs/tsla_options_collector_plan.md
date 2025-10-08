# TSLA Options Collector – Implementation Plan

## Directory Layout
- `futu/tools/tsla_options_collector/__init__.py`
- `futu/tools/tsla_options_collector/collector.py`
- `futu/tools/tsla_options_collector/storage.py`
- `futu/tools/tsla_options_collector/cli.py`
- `futu/tools/tsla_options_collector/__main__.py`
- `docs/tsla_options_collector_plan.md`
- `docs/tsla_options_collector.md`

## Core Components

### `OptionSubscriptionManager`
Handles quote/ticker subscription lifecycles, temporary quote snapshots, and reconnect recovery.
- `subscribe_quotes(codes: Sequence[str], *, push: bool = True, first_push: bool = True)`
- `unsubscribe_quotes(codes: Sequence[str])`
- `subscribe_realtime(codes: Sequence[str])`
- `unsubscribe_all()`
- `restore_subscriptions() -> int`
- `on_connection_state_change(connected: bool)`

### `OptionFilter`
Fetches the TSLA option chain and applies the fixed delta filter (Δ ∈ [+0.1,+0.9] ∪ [-0.9,-0.1]).
- `fetch_current_week_option_codes() -> list[str]`
- `filter_by_delta(codes: Sequence[str]) -> list[str]`

### `OptionCollector`
Coordinates the pipeline: fetch expiration, filter contracts, manage subscriptions, register handlers, and run the realtime loop.
- `run(duration: int | None) -> None`

### Storage (`DuckDBStorage`)
Creates `option_quotes` and `option_tickers` tables, appends quote/ticker frames, and exports via DuckDB `COPY`.
- `append_quote(frame: pd.DataFrame)`
- `append_ticker(frame: pd.DataFrame)`
- `export_quotes(fmt: str, start: datetime | None, end: datetime | None, output: Path)`
- `export_tickers(fmt: str, start: datetime | None, end: datetime | None, output: Path)`

## DuckDB Schema
- `option_quotes`: quote fields + `raw_json` + `ingested_at`.
- `option_tickers`: ticker fields + `raw_json` + `ingested_at`.

## CLI Surface
`tsla-options-collector`
- `collect --host --port --db-path --duration --log-level --log-path`
- `export --db-path --table {quotes|tickers} --format {csv|parquet} --start --end --output`

## Runtime Flow
1. Pull TSLA expirations, select current-week date.
2. Fetch the option chain for that expiration.
3. Subscribe to quotes (subscribe_push=False) and batch `get_stock_quote` for delta filtering.
4. Unsubscribe non-selected contracts, subscribe QUOTE + TICKER with push for selected contracts.
5. Stream QUOTE/TICKER callbacks into DuckDB with raw JSON backup.
6. Monitor connection notifications and call `restore_subscriptions()` after reconnect to ensure idempotent recovery.
7. Export stored data using DuckDB `COPY` to CSV/Parquet.

