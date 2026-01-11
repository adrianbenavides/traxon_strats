from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from traxon_core.config import (
    CacheConfig,
    DatabaseConfig,
    DiskConfig,
    DuckDBConfig,
    ExchangeConfig,
    ExecutorConfig,
)
from traxon_core.crypto.data_fetchers.portfolio import PortfolioFetcher
from traxon_core.crypto.exchanges import Exchange
from traxon_core.crypto.models import AccountEquity, ExchangeId, Portfolio, Symbol
from traxon_core.crypto.models.market_info import MarketInfo

from traxon_strats.crypto.services.equity import EquityService
from traxon_strats.persistence.repositories.interfaces import YoloRepository
from traxon_strats.robotwealth.yolo.config import (
    ServicesConfig,
    TemporalConfig,
    YoloConfig,
    YoloSettingsConfig,
)
from traxon_strats.robotwealth.yolo.strategy import YoloStrategy


class TestYoloStrategy:
    """Tests for YoloStrategy orchestration."""

    @pytest.mark.asyncio
    async def test_run_strategy_flow(self) -> None:
        # Mock dependencies
        settings = YoloSettingsConfig(
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
        config = YoloConfig(
            settings=settings,
            exchanges=[
                ExchangeConfig(
                    exchange_id="binance",
                    spot_quote_symbol="USDT",
                    leverage=1,
                    spot=True,
                    perp=True,
                    credentials={"apiKey": "key", "secret": "secret"},
                )
            ],
        )
        services_config = ServicesConfig(
            temporal=TemporalConfig(host="localhost", port=7233, namespace="default", task_queue="yolo"),
            robot_wealth_api_key="rw_key",
            database=DuckDBConfig(path="/tmp/test.db"),
            cache=DiskConfig(path="/tmp/cache"),
        )

        portfolio_fetcher = MagicMock(spec=PortfolioFetcher)
        yolo_repo = MagicMock(spec=YoloRepository)
        equity_service = MagicMock(spec=EquityService)
        equity_service.calculate_trading_capital = AsyncMock(return_value=10000.0)

        exchange = MagicMock(spec=Exchange)
        exchange.id = ExchangeId.BINANCE
        exchange.api = MagicMock()

        symbol = Symbol("BTC/USDT")
        market = MarketInfo(
            symbol=symbol,
            type="swap",
            active=True,
            precision_amount=3,
            precision_price=2,
            contract_size=Decimal("1.0"),
        )
        exchange.api.markets = {symbol: market}
        exchange.api.close = AsyncMock()

        exchange.fetch_account_equity = AsyncMock(
            return_value=AccountEquity(
                total_equity=Decimal("10000.0"),
                perps_equity=Decimal("10000.0"),
                spot_equity=Decimal("0.0"),
                available_balance=Decimal("5000.0"),
                maintenance_margin=Decimal("0.0"),
                maintenance_margin_pct=Decimal("0.0"),
            )
        )

        yolo_repo.get_weights = AsyncMock(
            return_value=pl.DataFrame(
                [
                    {
                        "symbol": "BTC/USDT",
                        "updated_at": "2026-01-05",
                        "momentum_megafactor": 0.1,
                        "trend_megafactor": 0.2,
                        "carry_megafactor": 0.3,
                        "combo_weight": 0.2,
                        "arrival_price": 50000.0,
                    }
                ]
            )
        )
        yolo_repo.get_volatilities = AsyncMock(
            return_value=pl.DataFrame(
                [
                    {
                        "symbol": "BTC/USDT",
                        "updated_at": "2026-01-05",
                        "ewvol": 0.02,
                    }
                ]
            )
        )

        strategy = YoloStrategy(
            config=config,
            services_config=services_config,
            portfolio_fetcher=portfolio_fetcher,
            yolo_repository=yolo_repo,
            equity_service=equity_service,
        )

        mock_portfolio = Portfolio(exchange_id=ExchangeId.BINANCE, balances=[], perps=[])
        portfolio_fetcher.fetch_portfolios = AsyncMock(return_value=[mock_portfolio])

        with patch.object(YoloStrategy, "_get_exchange", new_callable=AsyncMock) as mock_get_exchange:
            mock_get_exchange.return_value = exchange

            # Run
            await strategy.run_strategy()

            # Verify orchestration
            yolo_repo.get_weights.assert_called()
            mock_get_exchange.assert_called_once()
            portfolio_fetcher.fetch_portfolios.assert_called()
            equity_service.calculate_trading_capital.assert_called()
