from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest
from traxon_core.persistence.db.base import Database

from traxon_strats.persistence.duckdb.repositories.yolo import DuckDbYoloRepository


@pytest.fixture
def mock_db() -> MagicMock:
    db = MagicMock(spec=Database)
    db.execute.return_value = db
    db.transaction.return_value.__enter__.return_value = db
    return db


@pytest.fixture
def repository(mock_db: MagicMock) -> DuckDbYoloRepository:
    return DuckDbYoloRepository(mock_db)


@pytest.mark.asyncio
async def test_init_tables(repository: DuckDbYoloRepository, mock_db: MagicMock) -> None:
    await repository.init_tables()

    assert mock_db.execute.call_count >= 2  # At least creates tables


@pytest.mark.asyncio
async def test_store_weights(repository: DuckDbYoloRepository, mock_db: MagicMock) -> None:
    weights = pl.DataFrame(
        {
            "symbol": ["BTC-USDT"],
            "updated_at": ["2023-01-01"],
            "momentum_megafactor": [1.0],
            "trend_megafactor": [1.0],
            "carry_megafactor": [1.0],
            "combo_weight": [1.0],
            "arrival_price": [100.0],
        }
    )

    await repository.store_weights(weights)

    mock_db.register_temp_table.assert_called_once()
    mock_db.execute.assert_called()


@pytest.mark.asyncio
async def test_get_weights(repository: DuckDbYoloRepository, mock_db: MagicMock) -> None:
    expected_df = pd.DataFrame(
        {
            "symbol": ["BTC-USDT"],
            "updated_at": ["2023-01-01"],
            "momentum_megafactor": [1.0],
            "trend_megafactor": [1.0],
            "carry_megafactor": [1.0],
            "combo_weight": [1.0],
            "arrival_price": [100.0],
        }
    )
    mock_db.fetchdf.return_value = expected_df

    result = await repository.get_weights(date(2023, 1, 1))

    assert isinstance(result, pl.DataFrame)
    assert result.shape == (1, 7)
    assert result["symbol"][0] == "BTC-USDT"
