from __future__ import annotations

import pandera.polars as pa
import polars as pl

from traxon_strats.robotwealth.api_client.yolo.data_schemas import (
    YoloVolatilitiesSchema,
    YoloWeightsSchema,
)

__all__ = [
    "YoloVolatilitiesSchema",
    "YoloWeightsSchema",
    "TargetWeightsSchema",
    "TargetPortfolioSchema",
]


class TargetWeightsSchema(pa.DataFrameModel):
    """
    Intermediate schema for the pipeline.
    Represents the target weights for each asset before sizing.
    """

    symbol: pl.String
    weight: pl.Float64
    arrival_price: pl.Float64
    updated_at: pl.String


class TargetPortfolioSchema(pa.DataFrameModel):
    """
    Final schema for the pipeline.
    Represents the target portfolio sizes and values.
    """

    symbol: pl.String
    price: pl.Float64
    target_size_signed: pl.Float64
    target_value: pl.Float64
    arrival_price: pl.Float64
    notional_size_signed: pl.Float64
    delta: pl.Float64
    delta_value: pl.Float64
    updated_at: pl.String
