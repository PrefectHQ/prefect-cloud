import zoneinfo
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

import tzlocal

from prefect_cloud.auth import get_prefect_cloud_client
from prefect_cloud.schemas.objects import (
    CronSchedule,
    Deployment,
    DeploymentFlowRun,
    Flow,
)
from prefect_cloud.schemas.responses import DeploymentResponse


@dataclass
class DeploymentListContext:
    deployments: list[Deployment]
    flows_by_id: dict[UUID, Flow]
    next_runs_by_deployment_id: dict[UUID, DeploymentFlowRun]


async def list() -> DeploymentListContext:
    async with await get_prefect_cloud_client() as client:
        deployments = await client.read_all_deployments()
        flows_by_id = {flow.id: flow for flow in await client.read_all_flows()}

        next_runs = await client.read_next_scheduled_flow_runs_by_deployment_ids(
            deployment_ids=[deployment.id for deployment in deployments],
        )
        next_runs_by_deployment_id: dict[UUID, DeploymentFlowRun] = {}
        for run in next_runs:
            if (_id := run.deployment_id) and _id not in next_runs_by_deployment_id:
                next_runs_by_deployment_id[_id] = run

    return DeploymentListContext(
        deployments=deployments,
        flows_by_id=flows_by_id,
        next_runs_by_deployment_id=next_runs_by_deployment_id,
    )


async def get_deployment(deployment_: str) -> DeploymentResponse:
    async with await get_prefect_cloud_client() as client:
        try:
            deployment_id = UUID(deployment_)
        except ValueError:
            return await client.read_deployment_by_name(deployment_)
        else:
            return await client.read_deployment(deployment_id)


async def delete(deployment_: str):
    deployment = await get_deployment(deployment_)

    async with await get_prefect_cloud_client() as client:
        await client.delete_deployment(deployment.id)


async def run(
    deployment_: str,
    parameters: dict[str, Any] | None = None,
) -> DeploymentFlowRun:
    deployment = await get_deployment(deployment_)

    async with await get_prefect_cloud_client() as client:
        return await client.create_flow_run_from_deployment_id(
            deployment.id, parameters
        )


async def schedule(
    deployment_: str, schedule: str | None, parameters: Optional[dict[str, Any]] = None
):
    deployment = await get_deployment(deployment_)

    async with await get_prefect_cloud_client() as client:
        for prior_schedule in deployment.schedules:
            await client.delete_deployment_schedule(deployment.id, prior_schedule.id)

        if schedule and schedule.lower() != "none":
            localzone = tzlocal.get_localzone()
            if isinstance(localzone, zoneinfo.ZoneInfo):  # type: ignore[reportUnnecessaryIsInstance]
                local_tz = localzone.key
            else:  # pragma: no cover
                local_tz = "UTC"

            new_schedule = CronSchedule(cron=schedule, timezone=local_tz)
            await client.create_deployment_schedule(
                deployment.id, new_schedule, True, parameters
            )
