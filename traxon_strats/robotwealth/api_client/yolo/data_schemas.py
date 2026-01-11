from __future__ import annotations

from typing import NamedTuple

import pandera.pandas as pa
from pandera.typing import Series


class YoloWeightsSchema(pa.DataFrameModel):
    class Config:
        strict = True
        coerce = False

    symbol: pa.typing.Series[str]
    updated_at: pa.typing.Series[str] = pa.Field(str_matches=r"^\d{4}-\d{2}-\d{2}$")
    momentum_megafactor: pa.typing.Series[float]
    trend_megafactor: pa.typing.Series[float]
    carry_megafactor: pa.typing.Series[float]
    combo_weight: pa.typing.Series[float]
    arrival_price: pa.typing.Series[float]


class YoloVolatilitiesSchema(pa.DataFrameModel):
    class Config:
        strict = True
        coerce = False

    symbol: pa.typing.Series[str]
    updated_at: pa.typing.Series[str] = pa.Field(str_matches=r"^\d{4}-\d{2}-\d{2}$")
    ewvol: Series[float]


class YoloPositionsSchema(pa.DataFrameModel):
    class Config:
        strict = True
        coerce = False

    symbol: Series[str]
    price: Series[float]
    arrival_price: Series[float]
    size: Series[float]
    notional_size_signed: Series[float]
    target_size_signed: Series[float]
    current_weight: Series[float]
    target_weight: Series[float]
    weight_diff: Series[float]
    target_value: Series[float]
    delta: Series[float]
    delta_value: Series[float]


class YoloPositionsRow(NamedTuple):
    symbol: str
    price: float
    arrival_price: float
    size: float
    notional_size_signed: float
    target_size_signed: float
    current_weight: float
    target_weight: float
    weight_diff: float
    target_value: float
    delta: float
    delta_value: float


class YoloFactorsSchema(pa.DataFrameModel):
    class Config:
        strict = True

    symbol: Series[str] = pa.Field(str_length={"min_value": 1})
    date: Series[str] = pa.Field(str_length={"min_value": 1})
    factor_name: Series[str] = pa.Field(str_length={"min_value": 1})
    value: Series[float] = pa.Field(nullable=False)
