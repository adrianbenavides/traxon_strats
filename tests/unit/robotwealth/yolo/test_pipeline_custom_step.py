import polars as pl
import pytest

from traxon_strats.robotwealth.yolo.pipeline import SignalStep


class DoubleWeightStep:
    """A custom hook that doubles all weights."""

    async def setup(self) -> None:
        pass

    async def run(self, weights: pl.DataFrame) -> pl.DataFrame:
        return weights.with_columns(pl.col("weight") * 2)


@pytest.mark.asyncio
async def test_custom_hook_execution() -> None:
    input_df = pl.DataFrame(
        {"symbol": ["BTC/USDT"], "weight": [0.1], "arrival_price": [50000.0], "updated_at": ["2023-01-01"]}
    )

    hook = DoubleWeightStep()
    assert isinstance(hook, SignalStep)

    output_df = await hook.run(input_df)

    assert output_df.filter(pl.col("symbol") == "BTC/USDT").select("weight").item() == 0.2
