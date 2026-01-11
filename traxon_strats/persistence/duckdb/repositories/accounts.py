from __future__ import annotations

from datetime import datetime
from typing import Final

import pandera.polars as pa
import polars as pl
from beartype import beartype
from traxon_core.persistence.db.base import Database


class AccountsSchema(pa.DataFrameModel):
    name: pl.String
    updated_at: pl.Datetime
    equity: pl.Float64


class DuckDbAccountsRepository:
    """DuckDB implementation of the Accounts repository."""

    _TABLE_NAME: Final[str] = "accounts"

    @beartype
    def __init__(self, database: Database) -> None:
        self._database: Final[Database] = database

    @beartype
    async def init_tables(self) -> None:
        """Initialize accounts table with primary key constraint."""
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._TABLE_NAME} (
                name VARCHAR NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                equity DOUBLE NOT NULL CHECK (equity >= 0),
                PRIMARY KEY (name, updated_at)
            )
        """
        self._database.execute(create_table_sql).commit()

    @beartype
    async def store_equity(self, name: str, equity: float) -> None:
        """Store equity for account at current timestamp."""
        if len(name) == 0:
            raise ValueError("Account name cannot be empty")
        if equity < 0:
            raise ValueError(f"Equity can't be negative, got {equity}")

        insert_sql = f"""
            INSERT INTO {self._TABLE_NAME} (name, updated_at, equity)
            VALUES (?, ?, ?)
        """
        updated_at = datetime.now()
        self._database.execute(insert_sql, [name, updated_at, float(equity)]).commit()

    @beartype
    async def get_latest_equity(self, name: str) -> float | None:
        """Get most recent equity for account."""
        if len(name) == 0:
            raise ValueError("Account name cannot be empty")

        query_sql = f"""
            SELECT equity
            FROM {self._TABLE_NAME}
            WHERE name = ?
            ORDER BY updated_at DESC
            LIMIT 1
        """
        result = self._database.execute(query_sql, [name]).fetchone()

        if result is None:
            return None

        return float(str(result[0]))

    @beartype
    async def get_equity_history(self, name: str) -> pl.DataFrame:
        """Get complete equity history for account."""
        if len(name) == 0:
            raise ValueError("Account name cannot be empty")

        query_sql = f"""
            SELECT name, updated_at, equity
            FROM {self._TABLE_NAME}
            WHERE name = ?
            ORDER BY updated_at ASC
        """
        df = self._database.execute(query_sql, [name]).fetchdf()

        if df.empty:
            return pl.DataFrame(schema=AccountsSchema.to_schema().columns)

        df_pl = pl.from_pandas(df)
        df_pl = df_pl.with_columns(pl.col("updated_at").dt.cast_time_unit("us").dt.replace_time_zone(None))
        validated_df = AccountsSchema.validate(df_pl)
        return validated_df
