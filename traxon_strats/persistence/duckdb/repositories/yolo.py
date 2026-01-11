from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Final

import polars as pl
from beartype import beartype
from traxon_core import dates
from traxon_core.logs.structlog import logger
from traxon_core.persistence.db.base import Database

from traxon_strats.robotwealth.yolo.data_schemas import (
    YoloVolatilitiesSchema,
    YoloWeightsSchema,
)


class DuckDbYoloRepository:
    """DuckDB implementation of the YOLO strategy repository."""

    _WEIGHTS_TABLE_NAME: Final[str] = "weights"
    _VOLATILITIES_TABLE_NAME: Final[str] = "volatilities"

    @beartype
    def __init__(self, database: Database) -> None:
        self._database: Final[Database] = database

    @beartype
    async def init_tables(self) -> None:
        create_weights_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._WEIGHTS_TABLE_NAME} (
                symbol VARCHAR NOT NULL,
                updated_at VARCHAR NOT NULL,
                momentum_megafactor DOUBLE NOT NULL,
                trend_megafactor DOUBLE NOT NULL,
                carry_megafactor DOUBLE NOT NULL,
                combo_weight DOUBLE NOT NULL,
                arrival_price DOUBLE NOT NULL,
                PRIMARY KEY (symbol, updated_at)
            )
        """
        create_vol_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._VOLATILITIES_TABLE_NAME} (
                symbol VARCHAR NOT NULL,
                updated_at VARCHAR NOT NULL,
                ewvol DOUBLE NOT NULL,
                PRIMARY KEY (symbol, updated_at)
            )
        """

        self._database.execute(create_weights_sql)
        self._database.execute(create_vol_sql)

        # Delete old rows
        cutoff_date: str = (datetime.now() - timedelta(days=365 * 2)).strftime(dates.date_format)
        del_weights_sql = f"DELETE FROM {self._WEIGHTS_TABLE_NAME} WHERE updated_at < ?"
        del_vol_sql = f"DELETE FROM {self._VOLATILITIES_TABLE_NAME} WHERE updated_at < ?"

        self._database.execute(del_weights_sql, [cutoff_date])
        self._database.execute(del_vol_sql, [cutoff_date])
        self._database.commit()

    @beartype
    async def store_weights(self, weights: pl.DataFrame) -> None:
        """Store weights DataFrame in DB."""
        _weights = weights.to_pandas()
        with self._database.transaction():
            self._database.register_temp_table("_weights_tmp", _weights)
            self._database.execute(
                f"insert or replace into {self._WEIGHTS_TABLE_NAME} select * from _weights_tmp",
            )

    @beartype
    async def store_volatilities(self, volatilities: pl.DataFrame) -> None:
        """Store volatilities DataFrame in DB."""
        _volatilities = volatilities.to_pandas()
        with self._database.transaction():
            self._database.register_temp_table("_volatilities_tmp", _volatilities)
            self._database.execute(
                f"insert or replace into {self._VOLATILITIES_TABLE_NAME} select * from _volatilities_tmp",
            )

    @beartype
    async def get_weights(self, _date: date) -> pl.DataFrame:
        """Retrieve weights for a given date."""
        query_sql = f"""
            select * from {self._WEIGHTS_TABLE_NAME}
            where updated_at = ?
        """
        df = self._database.execute(query_sql, [_date.strftime(dates.date_format)]).fetchdf()
        logger.info(f"fetched weights for date {_date}: {df.shape[0]} rows")
        if df.empty:
            return pl.DataFrame(schema=YoloWeightsSchema.to_schema().columns)

        df_pl = pl.from_pandas(df)
        validated_df = YoloWeightsSchema.validate(df_pl)
        return validated_df

    @beartype
    async def get_volatilities(self, _date: date) -> pl.DataFrame:
        """Retrieve volatilities for a given date."""
        query_sql = f"""
            select * from {self._VOLATILITIES_TABLE_NAME} 
            where updated_at = ?
        """
        df = self._database.execute(query_sql, [_date.strftime(dates.date_format)]).fetchdf()
        logger.info(f"fetched volatilities for date {_date}: {df.shape[0]} rows")
        if df.empty:
            return pl.DataFrame(schema=YoloVolatilitiesSchema.to_schema().columns)

        df_pl = pl.from_pandas(df)
        validated_df = YoloVolatilitiesSchema.validate(df_pl)
        return validated_df
