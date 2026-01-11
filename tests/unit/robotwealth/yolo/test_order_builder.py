from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import polars as pl
import pytest
from traxon_core.crypto.exchanges import Exchange
from traxon_core.crypto.models import (
    ExchangeId,
    OrderSide,
)
from traxon_core.crypto.models.market_info import MarketInfo
from traxon_core.crypto.models.symbol import Symbol

from traxon_strats.robotwealth.yolo.order_builder import YoloOrderBuilder


class TestYoloOrderBuilder:
    """Unit tests for YoloOrderBuilder."""

    @pytest.mark.asyncio
    async def test_prepare_orders_adjustment(self) -> None:
        """Test creating an adjustment order (no flip)."""
        exchange = MagicMock(spec=Exchange)
        exchange.id = ExchangeId.BINANCE
        exchange.api = MagicMock()

        symbol = Symbol("BTC/USDT")
        market = MarketInfo(
            symbol=symbol,
            type="spot",
            active=True,
            precision_amount=3,
            precision_price=2,
            contract_size=Decimal("1.0"),
        )
        exchange.api.markets = {symbol: market}

        positions = pl.DataFrame(
            [
                {
                    "symbol": "BTC/USDT",
                    "price": 50000.0,
                    "arrival_price": 50000.0,
                    "size": 0.1,
                    "notional_size_signed": 0.1,
                    "target_size_signed": 0.15,
                    "current_weight": 0.5,
                    "target_weight": 0.75,
                    "weight_diff": 0.25,
                    "target_value": 7500.0,
                    "delta": 0.05,
                    "delta_value": 2500.0,
                }
            ]
        )

        builder = YoloOrderBuilder()
        orders = await builder.prepare_orders(exchange, positions)

        assert len(orders.updates) == 1
        assert len(orders.new) == 0

        symbol_key = list(orders.updates.keys())[0]
        order = orders.updates[symbol_key][0]
        from traxon_core.crypto.models.order.request import OrderRequest

        assert isinstance(order, OrderRequest)
        assert order.side == OrderSide.BUY
        assert order.notes == "adjustment"

    @pytest.mark.asyncio
    async def test_prepare_orders_direction_flip(self) -> None:
        """Test creating orders for a direction flip."""
        exchange = MagicMock(spec=Exchange)
        exchange.id = ExchangeId.BINANCE
        exchange.api = MagicMock()

        spot_symbol = Symbol("BTC/USDT")
        perp_symbol = Symbol("BTC/USDT:USDT")

        spot_market = MarketInfo(
            symbol=spot_symbol,
            type="spot",
            active=True,
            precision_amount=3,
            precision_price=2,
            contract_size=Decimal("1.0"),
        )
        perp_market = MarketInfo(
            symbol=perp_symbol,
            type="swap",
            active=True,
            precision_amount=3,
            precision_price=2,
            contract_size=Decimal("1.0"),
        )
        exchange.api.markets = {spot_symbol: spot_market, perp_symbol: perp_market}

        positions = pl.DataFrame(
            [
                {
                    "symbol": "BTC/USDT",
                    "price": 50000.0,
                    "arrival_price": 50000.0,
                    "size": 0.1,
                    "notional_size_signed": 0.1,
                    "target_size_signed": -0.1,
                    "current_weight": 0.5,
                    "target_weight": -0.5,
                    "weight_diff": -1.0,
                    "target_value": -5000.0,
                    "delta": -0.2,
                    "delta_value": 10000.0,
                }
            ]
        )

        builder = YoloOrderBuilder()
        orders = await builder.prepare_orders(exchange, positions)

        # Should have 1 update (close long) and 1 new (open short)
        assert len(orders.updates) == 1
        assert len(orders.new) == 1

        # Close order
        update_key = list(orders.updates.keys())[0]
        close_order = orders.updates[update_key][0]
        from traxon_core.crypto.models.order.request import OrderRequest

        assert isinstance(close_order, OrderRequest)
        assert close_order.side == OrderSide.SELL
        assert close_order.notes == "direction flip"

        # Open order
        new_key = list(orders.new.keys())[0]
        open_order = orders.new[new_key][0]
        assert isinstance(open_order, OrderRequest)
        assert open_order.side == OrderSide.SELL
        assert open_order.notes == "direction flip"
