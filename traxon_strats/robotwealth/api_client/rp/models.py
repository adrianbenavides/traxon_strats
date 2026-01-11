from pydantic import BaseModel, ConfigDict, Field


class RPWeight(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str = Field(min_length=1)
    date: str = Field(min_length=1)
    equal_vol_weight: float
    pw_cor_delta: float
    adj_weight: float


class RPWeightsResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    success: str = Field(min_length=1)
    last_updated: int
    data: list[RPWeight]
