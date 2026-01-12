from datetime import date

import polars as pl

from traxon_strats.persistence.repositories.interfaces import AccountsRepository, YoloRepository


def test_accounts_repository_exists() -> None:
    assert AccountsRepository is not None


def test_yolo_repository_exists() -> None:
    assert YoloRepository is not None


def test_accounts_repository_runtime_checkable() -> None:
    class Impl:
        async def init_tables(self) -> None:
            pass

        async def store_equity(self, name: str, equity: float) -> None:
            pass

        async def get_latest_equity(self, name: str) -> float | None:
            return None

        async def get_equity_history(self, name: str) -> pl.DataFrame:
            return pl.DataFrame()

    assert isinstance(Impl(), AccountsRepository)


def test_yolo_repository_runtime_checkable() -> None:
    class Impl:
        async def init_tables(self) -> None:
            pass

        async def store_weights(self, weights: pl.DataFrame) -> None:
            pass

        async def store_volatilities(self, volatilities: pl.DataFrame) -> None:
            pass

        async def get_weights(self, _date: date) -> pl.DataFrame:
            return pl.DataFrame()

        async def get_volatilities(self, _date: date) -> pl.DataFrame:
            return pl.DataFrame()

    assert isinstance(Impl(), YoloRepository)
