from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    pass


@workflow.defn
class YoloWorkflow:
    @workflow.run
    async def run(self) -> None:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=5),
            non_retryable_error_types=["NonRecoverableError"],
        )
        api_retry_policy = RetryPolicy(
            maximum_attempts=20 * 60 // 5,  # Retry for up to 20 minutes, every 5 seconds
            maximum_interval=timedelta(seconds=5),
            non_retryable_error_types=["NonRecoverableError"],
        )

        # In Temporal, we execute activities by name or reference.
        # The actual instantiation of YoloActivities happens on the Worker side.

        await workflow.execute_activity(
            "init_tables",
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry_policy,
        )
        await workflow.execute_activity(
            "fetch_strategy_params",
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=api_retry_policy,
        )
        await workflow.execute_activity(
            "run_strategy",
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=retry_policy,
        )
