from __future__ import annotations

from beartype import beartype
from traxon_core.logs.structlog import logger

from traxon_strats.persistence.repositories.interfaces import AccountsRepository


class EquityService:
    """Service for managing account equity and trading capital smoothing."""

    @beartype
    def __init__(self, repository: AccountsRepository) -> None:
        self._repository = repository
        self._logger = logger.bind(component="EquityService")

    @beartype
    async def calculate_trading_capital(
        self,
        account: str,
        max_leverage: float,
        equity_buffer: float,
        current_equity: float,
    ) -> float:
        """
        Calculate smoothed trading capital based on current equity and buffer thresholds.

        If the difference between current usable equity (equity * leverage) and the
        previously stored trading capital exceeds the buffer, the stored value is updated.
        """
        usable = round(current_equity * max_leverage, 2)
        self._logger.info(
            f"{account} - current equity: {current_equity:.2f}, usable equity: {usable:.2f} (max leverage {max_leverage:.2f})"
        )

        latest = await self._repository.get_latest_equity(account)

        if latest is None:
            await self._repository.store_equity(account, current_equity)
            return current_equity

        diff = abs(usable - latest) / latest
        if diff >= equity_buffer:
            self._logger.info(f"{account} - updating account usable equity")
            latest = usable
            await self._repository.store_equity(account, latest)
        else:
            self._logger.info(f"{account} - account equity is within the threshold, no update needed.")

        _log = f"{account} - using equity {latest:.2f}, usable equity {usable:.2f}, max leverage {max_leverage:.2f}"
        self._logger.info(_log)
        # TODO: notifier logic removed as it's not part of the service responsibility

        return latest
