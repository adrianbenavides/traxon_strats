from __future__ import annotations

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given
from traxon_core.config import ExecutorConfig
from traxon_core.crypto.models import (
    ExchangeId,
    Portfolio,
)
from traxon_core.floats import float_is_zero, floats_equal

from traxon_strats.robotwealth.yolo.config import YoloSettingsConfig
from traxon_strats.robotwealth.yolo.data_schemas import TargetPortfolioSchema
from traxon_strats.robotwealth.yolo.portfolio_sizer import YoloPortfolioSizer


class TestYoloPositionSizerProperties:
    """Property-based tests for YoloPositionSizer."""

    @given(
        current_size=st.one_of(
            st.floats(min_value=-1e10, max_value=-1e-2, allow_nan=False, allow_infinity=False),
            st.just(0.0),
            st.floats(min_value=1e-2, max_value=1e10, allow_nan=False, allow_infinity=False),
        ),
        target_size=st.one_of(
            st.floats(min_value=-1e10, max_value=-1e-2, allow_nan=False, allow_infinity=False),
            st.just(0.0),
            st.floats(min_value=1e-2, max_value=1e10, allow_nan=False, allow_infinity=False),
        ),
        trade_buffer=st.floats(min_value=0.0, max_value=0.5, allow_nan=False, allow_infinity=False),
    )
    def test_calculate_position_size_properties(
        self, current_size: float, target_size: float, trade_buffer: float
    ) -> None:
        """Verify invariants of position size calculation."""
        delta = YoloPortfolioSizer.calculate_position_size(current_size, target_size, trade_buffer)

        # Invariant 1: If target is 0, delta must close the position
        if float_is_zero(target_size):
            if float_is_zero(current_size):
                assert float_is_zero(delta)
            else:
                assert floats_equal(delta, -current_size)
            return

        # Invariant 2: If current is 0, delta must be target_size
        if float_is_zero(current_size):
            assert floats_equal(delta, target_size)
            return

        abs_target = abs(target_size)
        abs_current = abs(current_size)
        lower_bound = abs_target * (1 - trade_buffer)
        upper_bound = abs_target * (1 + trade_buffer)

        # Invariant 3: If within buffer and same direction, delta should be 0
        if current_size * target_size > 0 and lower_bound <= abs_current <= upper_bound:
            assert float_is_zero(delta)
            return

        # Invariant 4: New absolute size (abs(current + delta)) should be within or on the bounds
        new_size = current_size + delta
        abs_new = abs(new_size)

        # We target the bounds if we are outside or flipping
        assert (
            floats_equal(abs_new, lower_bound) or floats_equal(abs_new, upper_bound) or float_is_zero(delta)
        )

        # Invariant 5: Direction flip should always result in new_size having same sign as target_size
        if not float_is_zero(target_size):
            assert (new_size > 0) == (target_size > 0) or float_is_zero(new_size)


class TestCalculatePositionSize:
    """Test suite for YoloPositionSizer.calculate_position_size static method."""

    def test_zero_inputs(self) -> None:
        """When current or target size is zero, should handle correctly."""
        trade_buffer = 0.1
        # Zero current size returns full target size
        result = YoloPortfolioSizer.calculate_position_size(0.0, 100.0, trade_buffer)
        assert floats_equal(result, 100.0)

        # Zero current, negative target
        result = YoloPortfolioSizer.calculate_position_size(0.0, -100.0, trade_buffer)
        assert floats_equal(result, -100.0)

        # Zero target size returns negative of current (close position)
        result = YoloPortfolioSizer.calculate_position_size(100.0, 0.0, trade_buffer)
        assert floats_equal(result, -100.0)

        result = YoloPortfolioSizer.calculate_position_size(-100.0, 0.0, trade_buffer)
        assert floats_equal(result, 100.0)

        # Both zero returns zero
        result = YoloPortfolioSizer.calculate_position_size(0.0, 0.0, trade_buffer)
        assert float_is_zero(result)

    def test_direction_flip_long_to_short(self) -> None:
        """When flipping from long to short, should return full difference but target lower bound."""
        trade_buffer = 0.1
        cases = [
            # (current_size, target_size, expected_delta)
            # Long 100 -> Short 50: close 100 + short to lower bound (45)
            # delta = -100 - 45 = -145
            (100.0, -50.0, -145.0),
            # Long 50 -> Short 100: close 50 + short to lower bound (90)
            # delta = -50 - 90 = -140
            (50.0, -100.0, -140.0),
        ]
        for current_size, target_size, expected_delta in cases:
            result = YoloPortfolioSizer.calculate_position_size(current_size, target_size, trade_buffer)
            assert floats_equal(result, expected_delta), (
                f"Failed for current={current_size}, target={target_size}, expected={expected_delta}, got={result}"
            )

    def test_direction_flip_short_to_long(self) -> None:
        """When flipping from short to long, should return full difference but target lower bound."""
        trade_buffer = 0.1
        cases = [
            # (current_size, target_size, expected_delta)
            # Short -100 -> Long 50: cover 100 + long to lower bound (45)
            # delta = 100 + 45 = 145
            (-100.0, 50.0, 145.0),
            # SOL case: Short -7.9 -> Long 1.894357
            # lower_bound = 1.894357 * 0.9 = 1.704921
            # delta = 7.9 + 1.704921 = 9.604921
            (-7.9, 1.894357, 9.604921),
        ]
        for current_size, target_size, expected_delta in cases:
            result = YoloPortfolioSizer.calculate_position_size(current_size, target_size, trade_buffer)
            assert floats_equal(result, expected_delta), (
                f"Failed for current={current_size}, target={target_size}, expected={expected_delta}, got={result}"
            )

    def test_real_world_position_deltas(self) -> None:
        """Test delta calculations with real-world position data from strategy execution."""
        trade_buffer = 0.035  # 3.5%

        cases = [
            # (symbol, notional_size_signed, target_size_signed, expected_delta)
            ("ADA/USDT", -1591.0, -1679.04876, -29.273),
            ("BNB/USDT", 2.046148, 1.320514, -0.679416),
            ("BTC/USDT", 0.014402, 0.005228, -0.008991),
            ("SOL/USDT", -7.9, 1.894357, 9.728054),
        ]

        for symbol, notional_size_signed, target_size_signed, expected_delta in cases:
            result = YoloPortfolioSizer.calculate_position_size(
                notional_size_signed, target_size_signed, trade_buffer
            )
            assert floats_equal(result, expected_delta, 1e-3), (
                f"Failed for {symbol}: current={notional_size_signed}, "
                f"target={target_size_signed}, expected={expected_delta}, got={result}"
            )


@pytest.fixture
def sample_target_weights() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["BTC/USDT", "ETH/USDT"],
            "weight": [0.25, 0.25],
            "arrival_price": [50000.0, 3000.0],
            "updated_at": ["2023-01-01", "2023-01-01"],
        }
    )


@pytest.fixture
def empty_portfolio() -> Portfolio:
    return Portfolio(exchange_id=ExchangeId.BINANCE, balances=[], perps=[])


@pytest.fixture
def sample_settings() -> YoloSettingsConfig:
    return YoloSettingsConfig(
        dry_run=True,
        demo=True,
        max_leverage=2.0,
        equity_buffer=0.1,
        trade_buffer=0.05,
        momentum_factor=1.0,
        trend_factor=1.0,
        carry_factor=1.0,
        executor=ExecutorConfig(execution="fast", max_spread_pct=0.01),
    )


def test_position_sizing_logic(
    sample_target_weights: pl.DataFrame, empty_portfolio: Portfolio, sample_settings: YoloSettingsConfig
) -> None:
    sizer = YoloPortfolioSizer()
    equity = 10000.0

    output_df = sizer.size_portfolio(equity, sample_target_weights, empty_portfolio, sample_settings)
    TargetPortfolioSchema.validate(output_df)

    assert output_df.shape[0] == 2
    # BTC: 0.25 * 10000 / 50000 = 0.05
    btc_size = output_df.filter(pl.col("symbol") == "BTC/USDT").select("target_size_signed").item()
    assert btc_size == 0.05

    # ETH: 0.25 * 10000 / 3000 = 0.8333...
    eth_size = output_df.filter(pl.col("symbol") == "ETH/USDT").select("target_size_signed").item()
    assert pytest.approx(eth_size) == 0.8333333333333334

    # Since portfolio is empty, delta should be equal to target size
    btc_delta = output_df.filter(pl.col("symbol") == "BTC/USDT").select("delta").item()
    assert btc_delta == 0.05
