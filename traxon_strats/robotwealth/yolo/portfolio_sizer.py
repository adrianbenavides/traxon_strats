from __future__ import annotations

import polars as pl
from traxon_core.crypto.models import Portfolio, PositionSide
from traxon_core.floats import float_is_zero

from traxon_strats.robotwealth.yolo.config import YoloSettingsConfig
from traxon_strats.robotwealth.yolo.data_schemas import (
    TargetPortfolioSchema,
    TargetWeightsSchema,
)


class YoloPortfolioSizer:
    """Handles the conversion of TargetWeights + Equity + Portfolio -> TargetPortfolio."""

    def size_portfolio(
        self,
        equity: float,
        target_weights: pl.DataFrame,
        portfolio: Portfolio,
        settings: YoloSettingsConfig,
    ) -> pl.DataFrame:
        """Calculate target portfolio, sizes, and deltas (orders) from weights."""
        target_weights = TargetWeightsSchema.validate(target_weights)
        current_portfolio = self._portfolio_to_pl(portfolio)

        # Merge weights and current portfolio
        df = target_weights.join(current_portfolio, on="symbol", how="left")

        # Fill nulls for portfolio not currently held
        df = df.with_columns(
            [
                pl.col("notional_size_signed").fill_null(0.0),
                pl.col("price").fill_null(pl.col("arrival_price")),
                pl.col("size").fill_null(0.0),
            ]
        )

        # Calculate current weights (optional, but good for debugging)
        df = df.with_columns(
            [
                (pl.col("notional_size_signed") * pl.col("arrival_price") / equity)
                .round(3)
                .alias("current_weight")
            ]
        )

        # Calculate target portfolio
        df = df.with_columns([(pl.col("weight") * equity).alias("target_value")])
        df = df.with_columns([(pl.col("target_value") / pl.col("arrival_price")).alias("target_size_signed")])

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

        df = df.with_columns([(pl.col("delta").abs() * pl.col("price")).alias("delta_value")])

        return TargetPortfolioSchema.validate(
            df.select(
                [
                    "symbol",
                    "price",
                    "target_size_signed",
                    "target_value",
                    "arrival_price",
                    "updated_at",
                    "notional_size_signed",
                    "delta",
                    "delta_value",
                ]
            )
        )

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

        df = pl.DataFrame(data)
        df = df.group_by("symbol").agg(
            [
                pl.col("notional_size_signed").sum(),
                pl.col("size").sum(),
                pl.col("price").mean(),
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
        """Calculate the signed delta to trade from current to target size, considering the trade buffer."""
        if float_is_zero(current_size):
            return target_size
        if float_is_zero(target_size):
            return -current_size

        abs_target = abs(target_size)
        abs_current = abs(current_size)

        lower_bound = abs_target * (1 - trade_buffer)
        upper_bound = abs_target * (1 + trade_buffer)

        if current_size * target_size < 0:
            if target_size > 0:
                return -current_size + lower_bound
            else:
                return -current_size - lower_bound

        if lower_bound <= abs_current <= upper_bound:
            return 0.0

        if abs_current < lower_bound:
            target_abs = lower_bound
        else:
            target_abs = upper_bound

        if target_size > 0:
            return target_abs - abs_current
        else:
            return -(target_abs - abs_current)
