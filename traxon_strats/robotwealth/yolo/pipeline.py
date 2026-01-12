from __future__ import annotations

from datetime import date
from typing import Final, Protocol, runtime_checkable

import polars as pl

from traxon_strats.persistence.repositories.interfaces import YoloRepository
from traxon_strats.robotwealth.api_client.yolo import YoloVolatilitiesSchema, YoloWeightsSchema
from traxon_strats.robotwealth.yolo.config import YoloSettingsConfig
from traxon_strats.robotwealth.yolo.data_schemas import TargetWeightsSchema
from traxon_strats.robotwealth.yolo.errors import YoloApiDataNotUpToDateError


@runtime_checkable
class SignalStep(Protocol):
    """
    Protocol for a step in the signal generation pipeline.
    Each step takes a TargetWeightsSchema and returns a modified TargetWeightsSchema.
    """

    async def setup(self) -> None:
        """Optional setup method for initializing resources."""
        ...

    async def run(self, weights: pl.DataFrame) -> pl.DataFrame: ...


class RobotWealthSignalStep:
    """
    Base YOLO signal generation step.
    """

    def __init__(self, settings: YoloSettingsConfig, repository: YoloRepository, today: date) -> None:
        self.settings = settings
        self.repository = repository
        self.today = today
        self.api_weights: pl.DataFrame = pl.DataFrame()
        self.api_volatilities: pl.DataFrame = pl.DataFrame()

    async def setup(self) -> None:
        """Fetch RobotWealth's API signal from the repository."""
        # Load weights and volatilities from DB
        weights = await self.repository.get_weights(self.today)
        volatilities = await self.repository.get_volatilities(self.today)

        if weights.is_empty() or volatilities.is_empty():
            raise YoloApiDataNotUpToDateError()

        self.api_weights = YoloWeightsSchema.validate(weights)
        self.api_volatilities = YoloVolatilitiesSchema.validate(volatilities)

    async def run(self, weights: pl.DataFrame) -> pl.DataFrame:
        """
        Processes weights using YOLO momentum, trend, and carry factors,
        applies volatility scaling and leverage constraints.
        """
        if weights.is_empty():
            df = self.api_weights
        else:
            df = self.api_weights.join(weights, on="symbol", how="left")

        # == Calculate target weights
        df = df.with_columns(
            [
                (
                    (
                        pl.col("momentum_megafactor") * self.settings.momentum_factor
                        + pl.col("trend_megafactor") * self.settings.trend_factor
                        + pl.col("carry_megafactor") * self.settings.carry_factor
                    )
                    / 3
                )
                .round(3)
                .alias("unconstr_target_weight")
            ]
        )

        # Volatility scaling
        df = df.join(self.api_volatilities.select(["symbol", "ewvol"]), on="symbol", how="left")

        df = df.with_columns(
            [
                pl.when(pl.col("ewvol") != 0)
                .then((pl.col("unconstr_target_weight") / pl.col("ewvol")).clip(-0.25, 0.25))
                .otherwise(0.0)
                .round(3)
                .alias("vol_target_weight")
            ]
        )

        # Apply leverage constraint
        unconstr_total_weight: Final[float] = df.select(pl.col("unconstr_target_weight").abs().sum()).item()

        if unconstr_total_weight < self.settings.max_leverage:
            df = df.with_columns(pl.col("vol_target_weight").alias("weight"))
        else:
            df = df.with_columns(
                (pl.col("vol_target_weight") * self.settings.max_leverage / unconstr_total_weight)
                .round(3)
                .alias("weight")
            )

        return TargetWeightsSchema.validate(df)
