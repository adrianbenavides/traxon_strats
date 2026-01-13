from __future__ import annotations

from typing import NamedTuple

import pandera.polars as pa
import polars as pl


class YoloWeightsSchema(pa.DataFrameModel):
    symbol: pl.String
    updated_at: pl.String = pa.Field(str_matches=r"^\d{4}-\d{2}-\d{2}$")
    momentum_megafactor: pl.Float64
    trend_megafactor: pl.Float64
    carry_megafactor: pl.Float64
    combo_weight: pl.Float64
    arrival_price: pl.Float64


class YoloVolatilitiesSchema(pa.DataFrameModel):
    symbol: pl.String
    updated_at: pl.String = pa.Field(str_matches=r"^\d{4}-\d{2}-\d{2}$")
    ewvol: pl.Float64


class YoloFactorsSchema(pa.DataFrameModel):
    symbol: pl.String = pa.Field(str_length={"min_value": 1})
    date: pl.String = pa.Field(str_length={"min_value": 1})
    factor_name: pl.String = pa.Field(str_length={"min_value": 1})
    value: pl.Float64 = pa.Field(nullable=False)
