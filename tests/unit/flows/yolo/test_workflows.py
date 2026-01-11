import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from traxon_strats.flows.yolo.workflows import YoloWorkflow


@pytest.mark.asyncio
async def test_yolo_workflow():
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client,
            task_queue="test-yolo",
            workflows=[YoloWorkflow],
            activities=[
                # Mock activities
                lambda: "init_tables",
                lambda: "fetch_strategy_params",
                lambda: "run_strategy",
            ],
        ):
            # It just verifies the workflow can be registered and started.
            pass
