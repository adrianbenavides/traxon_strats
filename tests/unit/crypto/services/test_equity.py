from unittest.mock import AsyncMock

import pytest

from traxon_strats.crypto.services.equity import EquityService
from traxon_strats.persistence.repositories.interfaces import AccountsRepository


@pytest.fixture
def mock_repo() -> AsyncMock:
    return AsyncMock(spec=AccountsRepository)


@pytest.fixture
def service(mock_repo: AsyncMock) -> EquityService:
    return EquityService(mock_repo)


@pytest.mark.asyncio
async def test_calculate_trading_capital_initial(service: EquityService, mock_repo: AsyncMock) -> None:
    # Case: No stored equity
    mock_repo.get_latest_equity.return_value = None

    capital = await service.calculate_trading_capital(
        account="test_acc",
        max_leverage=2.0,
        equity_buffer=0.1,
        current_equity=1000.0,
    )

    assert capital == 1000.0
    mock_repo.store_equity.assert_called_once_with("test_acc", 1000.0)


@pytest.mark.asyncio
async def test_calculate_trading_capital_within_buffer(service: EquityService, mock_repo: AsyncMock) -> None:
    # Case: Usable equity (current * leverage) is within buffer of latest stored
    # Current: 1000, Leverage: 2.0 -> Usable: 2000
    # Latest stored: 2000
    # Diff: 0 -> within buffer

    mock_repo.get_latest_equity.return_value = 2000.0

    capital = await service.calculate_trading_capital(
        account="test_acc",
        max_leverage=2.0,
        equity_buffer=0.1,
        current_equity=1000.0,
    )

    assert capital == 2000.0
    mock_repo.store_equity.assert_not_called()


@pytest.mark.asyncio
async def test_calculate_trading_capital_exceeds_buffer(service: EquityService, mock_repo: AsyncMock) -> None:
    # Case: Usable equity is outside buffer
    # Current: 1200, Leverage: 2.0 -> Usable: 2400
    # Latest stored: 2000
    # Diff: |2400 - 2000| / 2000 = 0.2 >= 0.1 buffer

    mock_repo.get_latest_equity.return_value = 2000.0

    capital = await service.calculate_trading_capital(
        account="test_acc",
        max_leverage=2.0,
        equity_buffer=0.1,
        current_equity=1200.0,
    )

    assert capital == 2400.0  # Returns new usable
    mock_repo.store_equity.assert_called_once_with("test_acc", 2400.0)
