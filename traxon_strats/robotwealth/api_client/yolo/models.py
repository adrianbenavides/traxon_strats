from pydantic import BaseModel, ConfigDict, Field


class YoloFactor(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str = Field(min_length=1)
    date: str = Field(min_length=1)
    factor_name: str = Field(min_length=1)
    value: float


class YoloFactorsResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    success: str = Field(min_length=1)
    last_updated: int
    data: list[YoloFactor]


class YoloWeight(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str = Field(min_length=1)
    date: str = Field(min_length=1)
    momentum_megafactor: float
    trend_megafactor: float
    carry_megafactor: float
    combo_weight: float
    arrival_price: float


class YoloWeightsResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    success: str = Field(min_length=1)
    last_updated: int
    data: list[YoloWeight]


class YoloVolatility(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str = Field(min_length=1)
    date: str = Field(min_length=1)
    ewvol: float


class YoloVolatilitiesResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    success: str = Field(min_length=1)
    last_updated: int
    data: list[YoloVolatility]
