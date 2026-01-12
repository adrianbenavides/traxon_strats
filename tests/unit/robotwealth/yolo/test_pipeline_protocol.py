import polars as pl

from traxon_strats.robotwealth.yolo.pipeline import SignalStep


def test_signal_step_defined() -> None:
    assert SignalStep is not None


def test_implementation_check() -> None:
    class ValidImplementation:
        async def setup(self) -> None:
            pass

        async def run(self, weights: pl.DataFrame) -> pl.DataFrame:
            return weights

    assert isinstance(ValidImplementation(), SignalStep)
