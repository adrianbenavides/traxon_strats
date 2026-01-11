from typing import Any, Final, Self, TypeVar

import httpx
import pandas as pd
import pandera.pandas as pa
import polars as pl
from beartype import beartype
from httpx_retry import AsyncRetryTransport, RetryPolicy
from pandera.typing import DataFrame
from pydantic import BaseModel
from traxon_core.errors import NonRecoverableError
from traxon_core.logs.structlog import logger

from traxon_strats.robotwealth.api_client.errors import RWApiError, RwApiUnsuccessfulResponse
from traxon_strats.robotwealth.api_client.models import (
    StatusResponse,
)
from traxon_strats.robotwealth.api_client.rp import RPWeightsResponse, RPWeightsSchema
from traxon_strats.robotwealth.api_client.yolo import (
    YoloFactorsResponse,
    YoloFactorsSchema,
    YoloVolatilitiesResponse,
    YoloVolatilitiesSchema,
    YoloWeightsResponse,
    YoloWeightsSchema,
)

JsonResponse = dict[str, object]
ResponseT = TypeVar("ResponseT", bound=BaseModel)
SchemaT = TypeVar("SchemaT", bound=pa.DataFrameModel)


class RWApiClient:
    __slots__ = ("_api_key", "_client", "_logger")
    _api_key: str
    _client: httpx.AsyncClient
    _logger: Any
    _BASE_URL: Final[str] = "https://api.robotwealth.com/v1"

    @beartype
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._logger = logger.bind(component=self.__class__.__name__)
        exponential_retry = (
            RetryPolicy()
            .with_max_retries(10)
            .with_min_delay(0.1)
            .with_multiplier(2)
            .with_retry_on(lambda status_code: status_code >= 500)
        )
        self._client = httpx.AsyncClient(transport=AsyncRetryTransport(policy=exponential_retry))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        await self._client.aclose()

    @staticmethod
    def _ticker_to_symbol(ticker: str) -> str:
        quotes = ["USDT", "USDC"]
        for quote in quotes:
            if ticker.endswith(quote):
                base = ticker[: -len(quote)]
                return f"{base}/{quote}"

        raise ValueError(f"Ticker {ticker} does not end with a known quote currency")

    @beartype
    async def _get(self, path: str) -> JsonResponse:
        url: str = f"{self._BASE_URL}{path}"
        self._logger.debug("sending GET request", url=url)
        try:
            response: httpx.Response = await self._client.get(url, timeout=10)
            response.raise_for_status()
            json_response: JsonResponse = response.json()
            self._logger.debug("received response", url=url, res=json_response)
            return json_response
        except httpx.HTTPStatusError as e:
            api_err = RWApiError(f"HTTP error {e.response.status_code}: {e.response.text}")
            if e.response.status_code < 500:
                raise NonRecoverableError(api_err) from e
            else:
                raise api_err from e
        except Exception as e:
            raise NonRecoverableError(RWApiError(f"Request failed: {e}")) from e

    @beartype
    async def _fetch_and_validate(
        self,
        url: str,
        response_type: type[ResponseT],
        schema_type: type[SchemaT],
    ) -> DataFrame[SchemaT]:
        json: dict[str, object] = await self._get(url)
        res: ResponseT = response_type.model_validate(json)
        if not getattr(res, "success", False):
            raise RwApiUnsuccessfulResponse()
        df = pd.DataFrame([w.model_dump() for w in getattr(res, "data")])
        self._logger.debug("raw df", df=pl.DataFrame(df))

        # Rename and sanitize columns
        if "ticker" in df.columns:
            df = df.rename(columns={"ticker": "symbol"})
            df["symbol"] = df["symbol"].apply(lambda x: self._ticker_to_symbol(str(x)))
        if "date" in df.columns:
            df = df.rename(columns={"date": "updated_at"})

        validated_df: DataFrame[SchemaT] = schema_type.validate(df)
        self._logger.debug("validated df", df=pl.DataFrame(validated_df))
        return validated_df

    @beartype
    async def get_status(self) -> StatusResponse:
        data = await self._get("/status")
        return StatusResponse.model_validate(data)

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

    @beartype
    async def get_rp_weights(self) -> DataFrame[RPWeightsSchema]:
        return await self._fetch_and_validate(
            f"/rpschteroids/weights?api_key={self._api_key}",
            RPWeightsResponse,
            RPWeightsSchema,
        )
