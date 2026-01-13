import pandera.errors
import polars as pl
import pytest

from traxon_strats.robotwealth.yolo.data_schemas import TargetPortfolioSchema, TargetWeightsSchema


def test_target_weights_schema_validation() -> None:
    # Valid data
    df = pl.DataFrame(
        {
            "symbol": ["BTC-USDT", "ETH-USDT"],
            "weight": [0.5, 0.5],
            "arrival_price": [50000.0, 3000.0],
            "updated_at": ["2023-01-01T00:00:00", "2023-01-01T00:00:00"],
        }
    )
    validated = TargetWeightsSchema.validate(df)
    assert validated.shape == (2, 4)

    # Invalid data (wrong type for weight)
    invalid_df = pl.DataFrame(
        {
            "symbol": ["BTC-USDT"],
            "weight": ["bad"],
            "arrival_price": [50000.0],
            "updated_at": ["2023-01-01T00:00:00"],
        }
    )
    with pytest.raises(pandera.errors.SchemaError):
        TargetWeightsSchema.validate(invalid_df)


def test_target_portfolio_schema_validation() -> None:
    # Valid data
    df = pl.DataFrame(
        {
            "symbol": ["BTC-USDT"],
            "target_size_signed": [1.5],
            "target_value": [1500.0],
            "arrival_price": [1000.0],
            "updated_at": ["2023-01-01T00:00:00"],
            "price": [1000.0],
            "notional_size_signed": [1.0],
            "delta": [0.5],
            "delta_value": [500.0],
        }
    )
    validated = TargetPortfolioSchema.validate(df)
    assert validated.shape == (1, 9)
