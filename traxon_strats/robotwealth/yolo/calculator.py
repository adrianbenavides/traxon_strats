from __future__ import annotations

from typing import Final

import polars as pl
from traxon_core.crypto.models import Portfolio, PositionSide
from traxon_core.floats import float_is_zero

from traxon_strats.robotwealth.yolo.config import YoloSettingsConfig
from traxon_strats.robotwealth.yolo.data_schemas import (
    YoloPositionsSchema,
    YoloVolatilitiesSchema,
    YoloWeightsSchema,
)
from traxon_strats.robotwealth.yolo.errors import YoloWeightsEmptyError


class YoloCalculator:
    """Calculate target positions and deltas for YOLO strategy."""

    def calculate(
        self,
        equity: float,
        settings: YoloSettingsConfig,
        weights: pl.DataFrame,
        volatilities: pl.DataFrame,
        portfolio: Portfolio,
    ) -> pl.DataFrame:
        """Calculate target positions and deltas."""
        if weights.is_empty():
            raise YoloWeightsEmptyError("YOLO weights are empty")

        # Validate inputs
        weights = YoloWeightsSchema.validate(weights)
        volatilities = YoloVolatilitiesSchema.validate(volatilities)

        # Convert portfolio to polars, normalizing symbols to base/quote for joining
        current_positions = self._portfolio_to_pl(portfolio)

        # Merge weights and current positions
        df = weights.join(current_positions, on="symbol", how="left")

        # Fill nulls for positions not currently held
        df = df.with_columns(
            [
                pl.col("notional_size_signed").fill_null(0.0),
                pl.col("price").fill_null(pl.col("arrival_price")),
                pl.col("size").fill_null(0.0),
            ]
        )

        df = df.with_columns(
            [
                (pl.col("notional_size_signed") * pl.col("arrival_price") / equity)
                .round(3)
                .alias("current_weight")
            ]
        )

        # == Calculate target weights
        df = df.with_columns(
            [
                (
                    (
                        pl.col("momentum_megafactor") * settings.momentum_factor
                        + pl.col("trend_megafactor") * settings.trend_factor
                        + pl.col("carry_megafactor") * settings.carry_factor
                    )
                    / 3
                )
                .round(3)
                .alias("unconstr_target_weight")
            ]
        )

        # Volatility scaling
        df = df.join(volatilities.select(["symbol", "ewvol"]), on="symbol", how="left")

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

        if unconstr_total_weight < settings.max_leverage:
            df = df.with_columns(pl.col("vol_target_weight").alias("target_weight"))
        else:
            df = df.with_columns(
                (pl.col("vol_target_weight") * settings.max_leverage / unconstr_total_weight)
                .round(3)
                .alias("target_weight")
            )

        # == Calculate target positions
        df = df.with_columns([(pl.col("target_weight") * equity).alias("target_value")])
        df = df.with_columns([(pl.col("target_value") / pl.col("arrival_price")).alias("target_size_signed")])
        df = df.with_columns([(pl.col("target_weight") - pl.col("current_weight")).alias("weight_diff")])

        # Delta calculation
        df = df.with_columns(
            [
                pl.struct(["notional_size_signed", "target_size_signed"])
                .map_elements(
                    lambda x: self.calculate_position_size(
                        x["notional_size_signed"], x["target_size_signed"], settings.trade_buffer
                    ),
                    return_dtype=pl.Float64,
                )
                .alias("delta")
            ]
        )
        df = df.with_columns([(pl.col("delta").abs() * pl.col("arrival_price")).alias("delta_value")])
        df = df.sort("symbol")
        df = df.select(
            [
                "symbol",
                "price",
                "arrival_price",
                "size",
                "notional_size_signed",
                "target_size_signed",
                "current_weight",
                "target_weight",
                "weight_diff",
                "target_value",
                "delta",
                "delta_value",
            ]
        )

        return YoloPositionsSchema.validate(df)

    def _portfolio_to_pl(self, portfolio: Portfolio) -> pl.DataFrame:
        data = []
        for bal in portfolio.balances:
            data.append(
                {
                    "symbol": f"{bal.symbol.base}/{bal.symbol.quote}",
                    "side": "long",
                    "price": float(bal.current_price),
                    "size": float(bal.size),
                    "notional_size_signed": float(bal.notional_size),
                }
            )
        for pos in portfolio.perps:
            signed_size = (
                float(pos.notional_size) if pos.side == PositionSide.LONG else -float(pos.notional_size)
            )
            data.append(
                {
                    "symbol": f"{pos.symbol.base}/{pos.symbol.quote}",
                    "side": pos.side.value,
                    "price": float(pos.current_price),
                    "size": float(pos.size),
                    "notional_size_signed": signed_size,
                }
            )

        if not data:
            return pl.DataFrame(
                schema={
                    "symbol": pl.String,
                    "side": pl.String,
                    "price": pl.Float64,
                    "size": pl.Float64,
                    "notional_size_signed": pl.Float64,
                }
            )

        # Aggregate multiple positions/balances for same symbol (though Yolo usually has one)
        df = pl.DataFrame(data)
        df = df.group_by("symbol").agg(
            [
                pl.col("notional_size_signed").sum(),
                pl.col("size").sum(),
                pl.col("price").mean(),  # price is not really aggregatable but we need something
                pl.col("side").first(),
            ]
        )
        return df

    @staticmethod
    def calculate_position_size(
        current_size: float,
        target_size: float,
        trade_buffer: float,
    ) -> float:
        """Calculate the signed delta to trade from current to target size, considering the trade buffer.

        Returns:
            Positive delta: increase position (buy for longs, cover for shorts)
            Negative delta: decrease position (sell for longs, short more for shorts)
            Zero: no trade needed (within buffer)
        """
        # Handle zero cases - return full target size
        if float_is_zero(current_size):
            return target_size
        if float_is_zero(target_size):
            return -current_size

        # Calculate bounds based on absolute target size
        abs_target = abs(target_size)
        abs_current = abs(current_size)

        lower_bound = abs_target * (1 - trade_buffer)
        upper_bound = abs_target * (1 + trade_buffer)

        # Check for direction flip (opposite signs)
        if current_size * target_size < 0:
            # Direction flip: close current position + open to lower bound of target
            # The delta is: -current_size (to close) + target_bound (to open)
            if target_size > 0:
                # Flipping to long: target the lower bound
                return -current_size + lower_bound
            else:
                # Flipping to short: target the lower bound (as negative)
                return -current_size - lower_bound

        # Same direction: use buffer logic with signed values
        # Check if current is within buffer range
        if lower_bound <= abs_current <= upper_bound:
            return 0.0

        # Calculate signed delta to reach the appropriate bound
        # Determine which bound to target based on absolute values
        if abs_current < lower_bound:
            # Need to increase absolute position size
            target_abs = lower_bound
        else:
            # Need to decrease absolute position size
            target_abs = upper_bound

        # Convert back to signed value preserving the direction
        if target_size > 0:
            # Long position: delta = target - current
            return target_abs - abs_current
        else:
            # Short position: delta = -(target - current) = current - target
            return -(target_abs - abs_current)
