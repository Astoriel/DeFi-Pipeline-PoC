"""
PostgresLoader — handles all database writes for the ELT pipeline.

Supports:
- Upsert (INSERT ... ON CONFLICT DO UPDATE)
- Full refresh (TRUNCATE + INSERT)
- Incremental tracking via _pipeline_runs table
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import settings


class PostgresLoader:
    """Loads DataFrames into PostgreSQL with upsert and full-refresh strategies."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or settings.database_url
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
            )
        return self._engine

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        conflict_columns: list[str] | None = None,
        schema: str = "raw",
    ) -> int:
        """
        Insert rows, update on conflict.
        
        Args:
            df: DataFrame to load
            table: Target table name (without schema)
            conflict_columns: Columns that form the unique constraint
            schema: Target schema (default: raw)
        
        Returns:
            Number of rows upserted
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping upsert to {schema}.{table}")
            return 0

        full_table = f"{schema}.{table}"
        temp_table = f"_temp_{table}_{int(datetime.utcnow().timestamp())}"

        with self.engine.begin() as conn:
            # Load to temp table
            df.to_sql(
                temp_table,
                conn,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

            if conflict_columns:
                conflict_str = ", ".join([f'"{c}"' for c in conflict_columns])
                target_cols_str = ", ".join([f'"{c}"' for c in df.columns])
                update_cols = [c for c in df.columns if c not in conflict_columns]
                if update_cols:
                    update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                    upsert_sql = f"""
                        INSERT INTO {full_table} ({target_cols_str})
                        SELECT {target_cols_str} FROM {schema}.{temp_table}
                        ON CONFLICT ({conflict_str})
                        DO UPDATE SET {update_str}
                    """
                else:
                    upsert_sql = f"""
                        INSERT INTO {full_table} ({target_cols_str})
                        SELECT {target_cols_str} FROM {schema}.{temp_table}
                        ON CONFLICT ({conflict_str}) DO NOTHING
                    """
                logger.info(f"UPSERT SQL: {upsert_sql}")
                conn.execute(text(upsert_sql))

            # Drop temp
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))

        rows = len(df)
        logger.debug(f"Upserted {rows:,} rows into {full_table}")
        return rows

    def full_refresh(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "raw",
    ) -> int:
        """Truncate table and insert all rows."""
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping full refresh of {schema}.{table}")
            return 0

        full_table = f"{schema}.{table}"
        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_table}"))
            df.to_sql(
                table,
                conn,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        rows = len(df)
        logger.debug(f"Full-refreshed {rows:,} rows into {full_table}")
        return rows

    def get_last_loaded_timestamp(
        self,
        table: str,
        timestamp_column: str = "block_timestamp",
        schema: str = "raw",
    ) -> datetime | None:
        """Get the latest timestamp in a table for incremental extraction."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX({timestamp_column}) FROM {schema}.{table}")
                )
                value = result.scalar()
                return value
        except Exception as e:
            logger.warning(f"Could not get last timestamp from {schema}.{table}: {e}")
            return None

    def log_run(
        self,
        extractor_name: str,
        status: Literal["success", "failed", "partial"],
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        started_at: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Log pipeline run metadata to raw._pipeline_runs."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw._pipeline_runs
                            (extractor_name, status, rows_extracted, rows_loaded,
                             started_at, completed_at, error_message)
                        VALUES
                            (:name, :status, :extracted, :loaded,
                             :started, NOW(), :error)
                    """),
                    {
                        "name": extractor_name,
                        "status": status,
                        "extracted": rows_extracted,
                        "loaded": rows_loaded,
                        "started": started_at or datetime.utcnow(),
                        "error": error_message,
                    },
                )
        except Exception as e:
            logger.warning(f"Could not log pipeline run: {e}")
