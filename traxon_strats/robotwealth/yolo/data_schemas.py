from __future__ import annotations

import pandera.polars as pa
import polars as pl


class YoloWeightsSchema(pa.DataFrameModel):
    symbol: pl.String
    updated_at: pl.String
    momentum_megafactor: pl.Float64
    trend_megafactor: pl.Float64
    carry_megafactor: pl.Float64
    combo_weight: pl.Float64
    arrival_price: pl.Float64


class YoloVolatilitiesSchema(pa.DataFrameModel):
    symbol: pl.String
    updated_at: pl.String
    ewvol: pl.Float64


class YoloPositionsSchema(pa.DataFrameModel):
    symbol: pl.String
    price: pl.Float64
    arrival_price: pl.Float64
    size: pl.Float64
    notional_size_signed: pl.Float64
    target_size_signed: pl.Float64
    current_weight: pl.Float64
    target_weight: pl.Float64
    weight_diff: pl.Float64
    target_value: pl.Float64
    delta: pl.Float64
    delta_value: pl.Float64
