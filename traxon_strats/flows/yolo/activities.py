from __future__ import annotations

from beartype import beartype
from temporalio import activity
from traxon_core.crypto.data_fetchers.portfolio import PortfolioFetcher
from traxon_core.crypto.data_fetchers.prices import PriceFetcher
from traxon_core.persistence.db.duckdb import DuckDBConfig, DuckDbDatabase

from traxon_strats.crypto.services.equity import EquityService
from traxon_strats.persistence.duckdb.repositories.accounts import DuckDbAccountsRepository
from traxon_strats.persistence.duckdb.repositories.yolo import DuckDbYoloRepository
from traxon_strats.robotwealth.yolo.config import ServicesConfig, YoloConfig
from traxon_strats.robotwealth.yolo.strategy import YoloStrategy


class YoloActivities:
    def __init__(self, config: YoloConfig, services_config: ServicesConfig, db_path: str):
        # Setup dependencies
        # TODO: inject DB or init from config
        db = DuckDbDatabase(DuckDBConfig(path=db_path))
        yolo_repo = DuckDbYoloRepository(db)
        accounts_repo = DuckDbAccountsRepository(db)
        price_fetcher = PriceFetcher()
        portfolio_fetcher = PortfolioFetcher(price_fetcher)
        equity_service = EquityService(accounts_repo)

        self.strategy = YoloStrategy(
            config=config,
            services_config=services_config,
            portfolio_fetcher=portfolio_fetcher,
            yolo_repository=yolo_repo,
            equity_service=equity_service,
        )

    @activity.defn
    @beartype
    async def init_tables(self) -> None:
        await self.strategy._yolo_repository.init_tables()
        await self.strategy._equity_service._repository.init_tables()

    @activity.defn
    @beartype
    async def fetch_strategy_params(self) -> None:
        await self.strategy.fetch_strategy_params()

    @activity.defn
    @beartype
    async def run_strategy(self) -> None:
        await self.strategy.run_strategy()
