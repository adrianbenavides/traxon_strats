from __future__ import annotations

import pandera.polars as pa
import polars as pl


class RPWeightsSchema(pa.DataFrameModel):
    symbol: pl.String = pa.Field(str_length={"min_value": 1})
    date: pl.String = pa.Field(str_length={"min_value": 1})
    equal_vol_weight: pl.Float64 = pa.Field(nullable=False)
    pw_cor_delta: pl.Float64 = pa.Field(nullable=False)
    adj_weight: pl.Float64 = pa.Field(nullable=False)
