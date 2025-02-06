import zoneinfo
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import tzlocal
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterExpectedStartTime,
    FlowRunFilterState,
)
from prefect.client.schemas.objects import Flow, FlowRun
from prefect.client.schemas.responses import DeploymentResponse
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.sorting import FlowRunSort

from prefect_cloud.client import get_prefect_cloud_client


@dataclass
class DeploymentListContext:
    deployments: list[DeploymentResponse]
    flows_by_id: dict[UUID, Flow]
    next_runs_by_deployment_id: dict[UUID, FlowRun]


async def list() -> DeploymentListContext:
    async with get_prefect_cloud_client() as client:
        deployments = await client.read_deployments()
        flows_by_id = {flow.id: flow for flow in await client.read_flows()}

        next_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(
                    any_=[deployment.id for deployment in deployments]
                ),
                state=FlowRunFilterState(any_=["SCHEDULED"]),
                expected_start_time=FlowRunFilterExpectedStartTime(
                    after_=datetime.now(timezone.utc)
                ),
            ),
            sort=FlowRunSort.EXPECTED_START_TIME_ASC,
        )
        next_runs_by_deployment_id: dict[UUID, FlowRun] = {}
        for run in next_runs:
            if run.deployment_id not in next_runs_by_deployment_id:
                next_runs_by_deployment_id[run.deployment_id] = run

    return DeploymentListContext(
        deployments=deployments,
        flows_by_id=flows_by_id,
        next_runs_by_deployment_id=next_runs_by_deployment_id,
    )


async def _get_deployment(deployment_: str) -> DeploymentResponse:
    async with get_prefect_cloud_client() as client:
        try:
            deployment_id = UUID(deployment_)
        except ValueError:
            return await client.read_deployment_by_name(deployment_)
        else:
            return await client.read_deployment(deployment_id)


async def run(deployment_: str) -> FlowRun:
    deployment = await _get_deployment(deployment_)

    async with get_prefect_cloud_client() as client:
        return await client.create_flow_run_from_deployment(deployment.id)


async def schedule(deployment_: str, schedule: str):
    deployment = await _get_deployment(deployment_)

    async with get_prefect_cloud_client() as client:
        for prior_schedule in deployment.schedules:
            await client.delete_deployment_schedule(deployment.id, prior_schedule.id)

        if schedule and schedule.lower() != "none":
            localzone = tzlocal.get_localzone()
            if isinstance(localzone, zoneinfo.ZoneInfo):
                local_tz = localzone.key
            else:  # pragma: no cover
                local_tz = "UTC"

            new_schedule = CronSchedule(cron=schedule, timezone=local_tz)
            await client.create_deployment_schedules(
                deployment.id, [(new_schedule, True)]
            )


async def pause(deployment_: str):
    deployment = await _get_deployment(deployment_)

    async with get_prefect_cloud_client() as client:
        await client.pause_deployment(deployment.id)


async def resume(deployment_: str):
    deployment = await _get_deployment(deployment_)

    async with get_prefect_cloud_client() as client:
        await client.resume_deployment(deployment.id)
