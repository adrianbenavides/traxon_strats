from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest
from traxon_core.persistence.db.base import Database

from traxon_strats.persistence.duckdb.repositories.accounts import DuckDbAccountsRepository


@pytest.fixture
def mock_db() -> MagicMock:
    db = MagicMock(spec=Database)
    db.execute.return_value = db
    return db


@pytest.fixture
def repository(mock_db: MagicMock) -> DuckDbAccountsRepository:
    return DuckDbAccountsRepository(mock_db)


@pytest.mark.asyncio
async def test_init_tables(repository: DuckDbAccountsRepository, mock_db: MagicMock) -> None:
    await repository.init_tables()
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_store_equity(repository: DuckDbAccountsRepository, mock_db: MagicMock) -> None:
    await repository.store_equity("test_account", 1000.0)
    mock_db.execute.assert_called_once()

    # Verify validation
    with pytest.raises(ValueError):
        await repository.store_equity("", 1000.0)

    with pytest.raises(ValueError):
        await repository.store_equity("test", -10.0)


@pytest.mark.asyncio
async def test_get_latest_equity(repository: DuckDbAccountsRepository, mock_db: MagicMock) -> None:
    mock_db.fetchone.return_value = (1500.0,)

    equity = await repository.get_latest_equity("test_account")
    assert equity == 1500.0

    mock_db.fetchone.return_value = None
    equity = await repository.get_latest_equity("test_account")
    assert equity is None


@pytest.mark.asyncio
async def test_get_equity_history(repository: DuckDbAccountsRepository, mock_db: MagicMock) -> None:
    data = pd.DataFrame(
        {"name": ["test_account"], "updated_at": [pd.Timestamp("2023-01-01", tz="UTC")], "equity": [1000.0]}
    )
    mock_db.fetchdf.return_value = data

    history = await repository.get_equity_history("test_account")

    assert isinstance(history, pl.DataFrame)
    assert history.shape == (1, 3)
    assert history["equity"][0] == 1000.0
