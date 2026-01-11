from beartype import beartype
from pandera.typing import DataFrame

from traxon_strats.robotwealth.api_client.client import RWApiClient
from traxon_strats.robotwealth.api_client.rp.data_schemas import RPWeightsSchema
from traxon_strats.robotwealth.api_client.rp.models import RPWeightsResponse


class RWApiYoloClient(RWApiClient):
    @beartype
    def __init__(self, api_key: str) -> None:
        super().__init__(api_key)

    @beartype
    async def get_rp_weights(self) -> DataFrame[RPWeightsSchema]:
        return await self._fetch_and_validate(
            f"/rpschteroids/weights?api_key={self._api_key}",
            RPWeightsResponse,
            RPWeightsSchema,
        )
