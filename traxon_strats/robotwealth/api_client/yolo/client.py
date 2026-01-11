from beartype import beartype
from pandera.typing import DataFrame

from traxon_strats.robotwealth.api_client.client import RWApiClient
from traxon_strats.robotwealth.api_client.yolo.data_schemas import (
    YoloFactorsSchema,
    YoloVolatilitiesSchema,
    YoloWeightsSchema,
)
from traxon_strats.robotwealth.api_client.yolo.models import (
    YoloFactorsResponse,
    YoloVolatilitiesResponse,
    YoloWeightsResponse,
)


class RWApiYoloClient(RWApiClient):
    @beartype
    def __init__(self, api_key: str) -> None:
        super().__init__(api_key)

    @beartype
    async def get_yolo_factors(self) -> DataFrame[YoloFactorsSchema]:
        return await self._fetch_and_validate(
            f"/yolo/factors?api_key={self._api_key}",
            YoloFactorsResponse,
            YoloFactorsSchema,
        )

    @beartype
    async def get_yolo_weights(self) -> DataFrame[YoloWeightsSchema]:
        return await self._fetch_and_validate(
            f"/yolo/weights?api_key={self._api_key}",
            YoloWeightsResponse,
            YoloWeightsSchema,
        )

    @beartype
    async def get_yolo_volatilities(self) -> DataFrame[YoloVolatilitiesSchema]:
        return await self._fetch_and_validate(
            f"/yolo/volatilities?api_key={self._api_key}",
            YoloVolatilitiesResponse,
            YoloVolatilitiesSchema,
        )
