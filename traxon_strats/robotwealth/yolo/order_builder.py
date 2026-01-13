from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

import polars as pl
from beartype import beartype
from traxon_core.crypto.exchanges import Exchange
from traxon_core.crypto.models import (
    BaseQuote,
    DynamicSizeOrderBuilder,
    OrderBuilder,
    OrderExecutionType,
    OrderSide,
    OrderSizingStrategyFixed,
    OrdersToExecute,
    SizedOrderBuilder,
    Symbol,
)
from traxon_core.crypto.utils import log_prefix
from traxon_core.floats import float_is_zero
from traxon_core.logs.structlog import logger

from traxon_strats.robotwealth.yolo.data_schemas import TargetPortfolioSchema


class YoloOrderBuilder:
    """Builder for YOLO strategy orders."""

    @beartype
    async def prepare_orders(
        self,
        exchange: Exchange,
        target_portfolio: pl.DataFrame,
    ) -> OrdersToExecute:
        """Translate target portfolio into actionable orders."""
        target_portfolio = TargetPortfolioSchema.validate(target_portfolio)

        updates_by_symbol: dict[BaseQuote, list[OrderBuilder]] = defaultdict(list)
        new_by_symbol: dict[BaseQuote, list[OrderBuilder]] = defaultdict(list)

        def _symbol_from_row(_symbol_str: str, _target_value: float) -> Symbol:
            sym = Symbol(_symbol_str)
            if _target_value > 0:
                return Symbol(f"{sym.base}/{sym.quote}")
            else:
                return Symbol(f"{sym.base}/{sym.quote}:{sym.quote}")

        for row in target_portfolio.to_dicts():
            delta = float(row["delta"])

            if float_is_zero(delta):
                continue

            symbol_str = str(row["symbol"])
            notional_size_signed = float(row["notional_size_signed"])
            target_size_signed = float(row["target_size_signed"])
            price = float(row["price"])
            arrival_price = float(row["arrival_price"])
            delta_value = float(row["delta_value"])

            # The direction is flipping if the current and target portfolio have opposite signs
            direction_flip: bool = notional_size_signed * target_size_signed < 0

            if direction_flip:
                # 1. Close current position
                current_symbol = _symbol_from_row(symbol_str, notional_size_signed)
                market_info = exchange.api.markets.get(current_symbol)
                if market_info is None:
                    logger.warning(f"{log_prefix(exchange, str(current_symbol))} - market not found")
                    continue

                order1 = SizedOrderBuilder(
                    exchange_id=exchange.id,
                    market=market_info,
                    side=OrderSide.from_size(-notional_size_signed),  # Opposite to the current position
                    execution_type=OrderExecutionType.MAKER,
                    size=Decimal(str(notional_size_signed)),  # Current position size
                    notes="direction flip",
                )
                updates_by_symbol[current_symbol.base_quote].append(order1)

                # 2. Open new position
                target_symbol = _symbol_from_row(symbol_str, target_size_signed)
                market_info2 = exchange.api.markets.get(target_symbol)
                if market_info2 is None:
                    logger.warning(f"{log_prefix(exchange, str(target_symbol))} - market not found")
                    continue

                order2 = DynamicSizeOrderBuilder(
                    exchange_id=exchange.id,
                    market=market_info2,
                    side=OrderSide.from_size(target_size_signed),
                    execution_type=OrderExecutionType.MAKER,
                    sizing_strategy=OrderSizingStrategyFixed(Decimal(str(price))),
                    value=Decimal(str(abs(row["target_value"]))),
                    notes="direction flip",
                )
                new_by_symbol[target_symbol.base_quote].append(order2)
            else:
                target_symbol = _symbol_from_row(symbol_str, target_size_signed)
                market_info = exchange.api.markets.get(target_symbol)
                if market_info is None:
                    logger.warning(f"{log_prefix(exchange, str(target_symbol))} - market not found")
                    continue

                order = DynamicSizeOrderBuilder(
                    exchange_id=exchange.id,
                    market=market_info,
                    side=OrderSide.from_size(delta),
                    execution_type=OrderExecutionType.MAKER,
                    sizing_strategy=OrderSizingStrategyFixed(Decimal(str(arrival_price))),
                    value=Decimal(str(delta_value)),
                    notes="",
                )
                if float_is_zero(notional_size_signed):
                    order.notes = "new"
                    new_by_symbol[target_symbol.base_quote].append(order)
                else:
                    order.notes = "adjustment"
                    updates_by_symbol[target_symbol.base_quote].append(order)

        return OrdersToExecute(updates=updates_by_symbol, new=new_by_symbol)
