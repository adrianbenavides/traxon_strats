from __future__ import annotations

from pathlib import Path
from typing import Self

from beartype import beartype
from pydantic import BaseModel, ConfigDict, Field
from traxon_core import config
from traxon_core.config import CacheConfig, DatabaseConfig, ExchangeConfig, ExecutorConfig


@beartype
class TemporalConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    host: str = Field(pattern=r"^(?:[a-zA-Z0-9\-\.]+|\d{1,3}(?:\.\d{1,3}){3})$", min_length=1)
    port: int = Field(ge=1, le=65535)
    namespace: str
    task_queue: str


@beartype
class ServicesConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    temporal: TemporalConfig
    robot_wealth_api_key: str = Field(repr=True, min_length=1)
    database: DatabaseConfig
    cache: CacheConfig


@beartype
class YoloSettingsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    dry_run: bool
    demo: bool
    max_leverage: float = Field(ge=0.0, le=10.0)
    equity_buffer: float = Field(ge=0.0, le=1.0)
    trade_buffer: float = Field(ge=0.0, le=1.0)
    momentum_factor: float = Field(ge=0.0, le=5.0)
    trend_factor: float = Field(ge=0.0, le=5.0)
    carry_factor: float = Field(ge=0.0, le=5.0)
    executor: ExecutorConfig


@beartype
class YoloConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    settings: YoloSettingsConfig
    exchanges: list[ExchangeConfig] = Field(min_length=1, max_length=1)

    @classmethod
    def from_yaml(cls, path: str | Path) -> Self:
        data = config.load_from_yaml(path)
        return cls.model_validate(data)
