import pandera.pandas as pa
from pandera.typing import Series


class RPWeightsSchema(pa.DataFrameModel):
    class Config:
        strict = True

    symbol: Series[str] = pa.Field(str_length={"min_value": 1})
    date: Series[str] = pa.Field(str_length={"min_value": 1})
    equal_vol_weight: Series[float] = pa.Field(nullable=False)
    pw_cor_delta: Series[float] = pa.Field(nullable=False)
    adj_weight: Series[float] = pa.Field(nullable=False)
