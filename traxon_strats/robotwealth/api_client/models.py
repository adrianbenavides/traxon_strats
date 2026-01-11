from pydantic import BaseModel, ConfigDict, Field


class StatusResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    success: str = Field(min_length=1)
    time: str = Field(min_length=1)
