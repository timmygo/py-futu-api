"""Storage backend for TSLA options collector."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd


class DuckDBStorage:
    """Persist quote and ticker streams into DuckDB."""

    QUOTE_TABLE = "option_quotes"
    TICKER_TABLE = "option_tickers"

    _QUOTE_SCHEMA: Dict[str, str] = {
        "code": "TEXT",
        "name": "TEXT",
        "data_date": "TEXT",
        "data_time": "TEXT",
        "last_price": "DOUBLE",
        "open_price": "DOUBLE",
        "high_price": "DOUBLE",
        "low_price": "DOUBLE",
        "prev_close_price": "DOUBLE",
        "volume": "DOUBLE",
        "turnover": "DOUBLE",
        "turnover_rate": "DOUBLE",
        "amplitude": "DOUBLE",
        "suspension": "BOOLEAN",
        "listing_date": "TEXT",
        "price_spread": "DOUBLE",
        "dark_status": "TEXT",
        "sec_status": "TEXT",
        "strike_price": "DOUBLE",
        "contract_size": "DOUBLE",
        "open_interest": "DOUBLE",
        "implied_volatility": "DOUBLE",
        "premium": "DOUBLE",
        "delta": "DOUBLE",
        "gamma": "DOUBLE",
        "vega": "DOUBLE",
        "theta": "DOUBLE",
        "rho": "DOUBLE",
        "net_open_interest": "DOUBLE",
        "expiry_date_distance": "DOUBLE",
        "contract_nominal_value": "DOUBLE",
        "owner_lot_multiplier": "DOUBLE",
        "option_area_type": "TEXT",
        "contract_multiplier": "DOUBLE",
        "last_settle_price": "DOUBLE",
        "position": "DOUBLE",
        "position_change": "DOUBLE",
        "index_option_type": "TEXT",
        "pre_price": "DOUBLE",
        "pre_high_price": "DOUBLE",
        "pre_low_price": "DOUBLE",
        "pre_volume": "DOUBLE",
        "pre_turnover": "DOUBLE",
        "pre_change_val": "DOUBLE",
        "pre_change_rate": "DOUBLE",
        "pre_amplitude": "DOUBLE",
        "after_price": "DOUBLE",
        "after_high_price": "DOUBLE",
        "after_low_price": "DOUBLE",
        "after_volume": "DOUBLE",
        "after_turnover": "DOUBLE",
        "after_change_val": "DOUBLE",
        "after_change_rate": "DOUBLE",
        "after_amplitude": "DOUBLE",
        "overnight_price": "DOUBLE",
        "overnight_high_price": "DOUBLE",
        "overnight_low_price": "DOUBLE",
        "overnight_volume": "DOUBLE",
        "overnight_turnover": "DOUBLE",
        "overnight_change_val": "DOUBLE",
        "overnight_change_rate": "DOUBLE",
        "overnight_amplitude": "DOUBLE",
    }

    _TICKER_SCHEMA: Dict[str, str] = {
        "code": "TEXT",
        "name": "TEXT",
        "time": "TEXT",
        "price": "DOUBLE",
        "volume": "DOUBLE",
        "turnover": "DOUBLE",
        "ticker_direction": "TEXT",
        "sequence": "DOUBLE",
        "type": "TEXT",
        "push_data_type": "TEXT",
    }

    def __init__(self, db_path: Path, *, logger=None) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._logger = logger
        self._conn = duckdb.connect(database=str(self._db_path), read_only=False)
        self._lock = threading.Lock()
        self._ensure_schema()

    def close(self) -> None:
        """Close the DuckDB connection."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ------------------------------------------------------------------
    # Write APIs
    # ------------------------------------------------------------------
    def append_quote(self, frame: pd.DataFrame) -> None:
        """Append quote rows into DuckDB."""
        if frame.empty:
            return
        columns = list(self._QUOTE_SCHEMA.keys())
        for _, row in frame.iterrows():
            self._insert_record(self.QUOTE_TABLE, columns, row)

    def append_ticker(self, frame: pd.DataFrame) -> None:
        """Append ticker rows into DuckDB."""
        if frame.empty:
            return
        columns = list(self._TICKER_SCHEMA.keys())
        for _, row in frame.iterrows():
            self._insert_record(self.TICKER_TABLE, columns, row)

    # ------------------------------------------------------------------
    # Export APIs
    # ------------------------------------------------------------------
    def export_quotes(
        self,
        fmt: str,
        start: Optional[datetime],
        end: Optional[datetime],
        output: Path,
    ) -> None:
        self._export_table(self.QUOTE_TABLE, fmt, start, end, output)

    def export_tickers(
        self,
        fmt: str,
        start: Optional[datetime],
        end: Optional[datetime],
        output: Path,
    ) -> None:
        self._export_table(self.TICKER_TABLE, fmt, start, end, output)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_schema(self) -> None:
        with self._lock:
            columns_sql = ", ".join(
                f"{name} {dtype}" for name, dtype in self._QUOTE_SCHEMA.items()
            )
            ticker_columns_sql = ", ".join(
                f"{name} {dtype}" for name, dtype in self._TICKER_SCHEMA.items()
            )
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.QUOTE_TABLE} (
                    {columns_sql},
                    raw_json TEXT NOT NULL,
                    ingested_at TIMESTAMP NOT NULL
                )
                """
            )
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TICKER_TABLE} (
                    {ticker_columns_sql},
                    raw_json TEXT NOT NULL,
                    ingested_at TIMESTAMP NOT NULL
                )
                """
            )

    def _insert_record(self, table: str, columns: List[str], row: pd.Series) -> None:
        values_map = self._normalize_row(row)
        raw_json = json.dumps(values_map, default=self._json_default)
        ingested_at = datetime.utcnow()
        placeholders = ", ".join(["?"] * (len(columns) + 2))
        column_sql = ", ".join(columns + ["raw_json", "ingested_at"])
        params = [values_map.get(col) for col in columns]
        params.extend([raw_json, ingested_at])
        sql = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
        with self._lock:
            self._conn.execute(sql, params)
        if self._logger:
            self._logger.debug(
                "Inserted %s row for %s", table, values_map.get("code")
            )

    def _normalize_row(self, row: pd.Series) -> Dict[str, object]:
        result: Dict[str, object] = {}
        for key, value in row.to_dict().items():
            result[key] = self._normalize_value(value)
        return result

    @staticmethod
    def _normalize_value(value):
        if pd.isna(value):
            return None
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return value

    @staticmethod
    def _json_default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)!r} is not JSON serializable")

    def _export_table(
        self,
        table: str,
        fmt: str,
        start: Optional[datetime],
        end: Optional[datetime],
        output: Path,
    ) -> None:
        fmt = fmt.lower()
        if fmt not in {"csv", "parquet"}:
            raise ValueError("Format must be 'csv' or 'parquet'")

        conditions: List[str] = []
        params: List[object] = []
        if start is not None:
            conditions.append("ingested_at >= ?")
            params.append(start)
        if end is not None:
            conditions.append("ingested_at <= ?")
            params.append(end)
        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
        query = f"SELECT * FROM {table}{where_clause} ORDER BY ingested_at"

        escaped_output = str(output).replace("'", "''")
        if fmt == "csv":
            options = "FORMAT 'CSV', HEADER"
        else:
            options = "FORMAT 'PARQUET'"

        output.parent.mkdir(parents=True, exist_ok=True)
        copy_sql = f"COPY ({query}) TO '{escaped_output}' ({options})"
        with self._lock:
            self._conn.execute(copy_sql, params)
        if self._logger:
            self._logger.info(
                "Exported %s rows to %s", table, output
            )

