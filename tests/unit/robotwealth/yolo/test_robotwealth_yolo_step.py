from datetime import date
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest
from traxon_core.crypto.order_executor.config import ExecutorConfig

from traxon_strats.persistence.repositories.interfaces import YoloRepository
from traxon_strats.robotwealth.yolo.config import YoloSettingsConfig
from traxon_strats.robotwealth.yolo.data_schemas import TargetWeightsSchema
from traxon_strats.robotwealth.yolo.pipeline import RobotWealthSignalStep


@pytest.fixture
def yolo_settings() -> YoloSettingsConfig:
    return YoloSettingsConfig(
        dry_run=True,
        demo=True,
        max_leverage=1.0,
        equity_buffer=0.05,
        trade_buffer=0.1,
        momentum_factor=1.0,
        trend_factor=1.0,
        carry_factor=1.0,
        executor=ExecutorConfig(execution="fast", max_spread_pct=0.01),
    )


@pytest.fixture
def sample_weights() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["BTC/USDT", "ETH/USDT"],
            "momentum_megafactor": [0.1, 0.2],
            "trend_megafactor": [0.1, 0.2],
            "carry_megafactor": [0.1, 0.2],
            "arrival_price": [50000.0, 3000.0],
            "updated_at": ["2023-01-01", "2023-01-01"],
            "combo_weight": [0.0, 0.0],
        }
    )


@pytest.fixture
def sample_vols() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["BTC/USDT", "ETH/USDT"],
            "ewvol": [0.02, 0.03],
            "updated_at": ["2023-01-01", "2023-01-01"],
        }
    )


@pytest.mark.asyncio
async def test_robotwealth_yolo_step_calculation(
    yolo_settings: YoloSettingsConfig, sample_weights: pl.DataFrame, sample_vols: pl.DataFrame
) -> None:
    # Mock repository
    mock_repo = MagicMock(spec=YoloRepository)
    mock_repo.get_weights = AsyncMock(return_value=sample_weights)
    mock_repo.get_volatilities = AsyncMock(return_value=sample_vols)

    today = date(2023, 1, 1)
    step = RobotWealthSignalStep(settings=yolo_settings, repository=mock_repo, today=today)

    await step.setup()

    input_df = pl.DataFrame()  # Empty input for the first step
    output_df = await step.run(input_df)
    TargetWeightsSchema.validate(output_df)

    assert "weight" in output_df.columns

    btc_weight = output_df.filter(pl.col("symbol") == "BTC/USDT").select("weight").item()
    # Calculation:
    # momentum=0.1, trend=0.1, carry=0.1 -> average=0.1
    # ewvol=0.02
    # vol_target_weight = 0.1 / 0.02 = 5.0, clipped to 0.25
    assert btc_weight == 0.25
