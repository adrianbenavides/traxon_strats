from __future__ import annotations

from datetime import datetime
from typing import Final

import polars as pl
from beartype import beartype
from traxon_core import dates
from traxon_core.crypto.data_fetchers.portfolio import PortfolioFetcher
from traxon_core.crypto.exchanges import Exchange, ExchangeFactory
from traxon_core.crypto.order_executor import DefaultOrderExecutor
from traxon_core.logs.notifiers import notifier
from traxon_core.logs.structlog import logger

from traxon_strats.crypto.services.equity import EquityService
from traxon_strats.persistence.repositories.interfaces import YoloRepository
from traxon_strats.robotwealth.api_client import RWApiClient
from traxon_strats.robotwealth.yolo.calculator import YoloCalculator
from traxon_strats.robotwealth.yolo.config import ServicesConfig, YoloConfig
from traxon_strats.robotwealth.yolo.errors import (
    YoloApiDataNotUpToDateError,
    YoloNoApiDataError,
    YoloStrategyError,
)
from traxon_strats.robotwealth.yolo.order_builder import YoloOrderBuilder


class YoloStrategy:
    __slots__ = (
        "_config",
        "_services_config",
        "_portfolio_fetcher",
        "_yolo_repository",
        "_equity_service",
        "_calculator",
        "_order_builder",
        "_logger",
    )

    @beartype
    def __init__(
        self,
        config: YoloConfig,
        services_config: ServicesConfig,
        portfolio_fetcher: PortfolioFetcher,
        yolo_repository: YoloRepository,
        equity_service: EquityService,
        calculator: YoloCalculator | None = None,
        order_builder: YoloOrderBuilder | None = None,
    ) -> None:
        self._config: Final[YoloConfig] = config
        self._services_config: Final[ServicesConfig] = services_config
        self._portfolio_fetcher: Final[PortfolioFetcher] = portfolio_fetcher
        self._yolo_repository = yolo_repository
        self._equity_service = equity_service
        self._calculator = calculator or YoloCalculator()
        self._order_builder = order_builder or YoloOrderBuilder()
        self._logger = logger.bind(component=self.__class__.__name__)

    @beartype
    async def fetch_strategy_params(self) -> None:
        """Fetch weights and volatilities from RWApi, store in DB."""
        today = datetime.today()
        today_str = datetime.today().strftime(dates.date_format)

        # Return early if we already have today's data
        weights_pl = await self._yolo_repository.get_weights(today)
        volatilities_pl = await self._yolo_repository.get_volatilities(today)
        if not weights_pl.is_empty() and not volatilities_pl.is_empty():
            self._logger.info("yolo strategy params already in DB for today")
            return None

        self._logger.info(f"fetching yolo strategy params for {today_str}")
        async with RWApiClient(self._services_config.robot_wealth_api_key) as client:
            # Fetch weights
            weights_pd = await client.get_yolo_weights()
            weights = pl.from_pandas(weights_pd)
            self._logger.info("yolo weights:", df=weights.sort("symbol"))
            if weights.is_empty():
                self._logger.warning("yolo weights are empty")
                raise YoloNoApiDataError()

            if str(weights["updated_at"][0]) != today_str:
                self._logger.warning("yolo weights are not from today")
                raise YoloApiDataNotUpToDateError()
            await self._yolo_repository.store_weights(weights)

            # Fetch volatilities
            volatilities_pd = await client.get_yolo_volatilities()
            volatilities = pl.from_pandas(volatilities_pd)
            self._logger.info("yolo volatilities:", df=volatilities.sort("symbol"))
            if volatilities.is_empty():
                self._logger.warning("yolo weights or volatilities are empty")
                raise YoloNoApiDataError()
            if str(volatilities["updated_at"][0]) != today_str:
                self._logger.warning("yolo volatilities are not from today")
                raise YoloApiDataNotUpToDateError()
            await self._yolo_repository.store_volatilities(volatilities)

        self._logger.info(f"fetched yolo strategy params for {today_str}")
        return None

    @beartype
    async def run_strategy(self) -> None:
        today = datetime.today().date()

        # Load weights and volatilities from DB
        self._logger.info(f"fetching yolo strategy params from DB for {today}")
        weights = await self._yolo_repository.get_weights(today)
        volatilities = await self._yolo_repository.get_volatilities(today)

        if weights.is_empty() or volatilities.is_empty():
            raise YoloApiDataNotUpToDateError()

        exchange = await self._get_exchange()
        try:
            # Get account equity
            account_equity = await exchange.fetch_account_equity()
            equity = await self._equity_service.calculate_trading_capital(
                account=f"yolo.{exchange.id}",
                max_leverage=self._config.settings.max_leverage,
                equity_buffer=self._config.settings.equity_buffer,
                current_equity=float(account_equity.total_equity),
            )

            # Calculate orders
            portfolios = await self._portfolio_fetcher.fetch_portfolios([exchange])
            portfolio = portfolios[0]  # Yolo works with a single exchange

            self._logger.info("current portfolio:", exchange=exchange.id)
            target_positions = self._calculator.calculate(
                equity, self._config.settings, weights, volatilities, portfolio
            )
            self._logger.info("target positions:", df=target_positions.sort("symbol"))
            orders = await self._order_builder.prepare_orders(exchange, target_positions)

            if not orders.is_empty():
                await orders.log_as_df("yolo orders")
            else:
                self._logger.info("no orders to place")

            # Execute orders
            if self._config.settings.dry_run:
                self._logger.info("dry run enabled, skipping order execution")
            else:
                executor = DefaultOrderExecutor(self._config.settings.executor)
                await executor.execute_orders([exchange], orders)

            # Validate positions
            if not self._config.settings.dry_run and not orders.is_empty():
                portfolios = await self._portfolio_fetcher.fetch_portfolios([exchange])
                portfolio = portfolios[0]
                target_positions = self._calculator.calculate(
                    equity, self._config.settings, weights, volatilities, portfolio
                )
                orders = await self._order_builder.prepare_orders(exchange, target_positions)
                if not orders.is_empty():
                    self._logger.warning("positions do not match target after order execution")
                    await orders.log_as_df("yolo remaining orders")
                else:
                    self._logger.info("all positions match target after order execution")

            self._logger.info("yolo strategy run complete")
            self._logger.info("==============================")

            return None
        except Exception as e:
            _log = f"error running yolo strategy: {e}"
            self._logger.error(_log, exc_info=True)
            await notifier.notify(_log)
            raise YoloStrategyError() from e
        finally:
            await Exchange.close([exchange])

    @beartype
    async def _get_exchange(self) -> Exchange:
        """Extract and validate a single exchange from config."""
        exchanges = await ExchangeFactory.from_config(self._config.settings.demo, self._config.exchanges)
        if len(exchanges) != 1:
            raise ValueError("Yolo strategy supports exactly one exchange.")
        return exchanges[0]
