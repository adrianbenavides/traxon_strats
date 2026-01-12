from __future__ import annotations

import pandera.polars as pa
import polars as pl

from traxon_strats.robotwealth.api_client.yolo.data_schemas import (
    YoloPositionsSchema,
    YoloVolatilitiesSchema,
    YoloWeightsSchema,
)

__all__ = [
    "YoloPositionsSchema",
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
    Represents the target position sizes and values.
    """

    symbol: pl.String
    target_size_signed: pl.Float64
    target_value: pl.Float64
    arrival_price: pl.Float64
    updated_at: pl.String
