from uuid import UUID, uuid4

import pytest
import respx
from httpx import Response
from prefect_cloud.schemas.objects import (
    DeploymentSchedule,
    Flow,
    DeploymentFlowRun,
    CronSchedule,
)
from prefect_cloud.schemas.responses import DeploymentResponse

from prefect_cloud import deployments


@pytest.fixture
def account() -> UUID:
    return UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


@pytest.fixture
def workspace() -> UUID:
    return UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


@pytest.fixture
def api_url(account: UUID, workspace: UUID) -> str:
    return f"https://api.prefect.cloud/api/accounts/{account}/workspaces/{workspace}"


@pytest.fixture(autouse=True)
async def mock_get_cloud_urls_or_login(
    monkeypatch: pytest.MonkeyPatch, account: UUID, workspace: UUID, api_url: str
):
    async def mock_urls():
        return (
            f"https://app.prefect.cloud/account/{account}/workspace/{workspace}",
            api_url,
            "test_api_key",
        )

    monkeypatch.setattr("prefect_cloud.auth.get_cloud_urls_or_login", mock_urls)


@pytest.fixture
def mock_deployment() -> DeploymentResponse:
    return DeploymentResponse(
        id=uuid4(),
        flow_id=uuid4(),
        name="test-deployment",
        schedules=[],
    )


@pytest.fixture
def mock_deployment_with_schedule(
    mock_deployment: DeploymentResponse,
) -> DeploymentResponse:
    mock_deployment.schedules = [
        DeploymentSchedule(
            deployment_id=mock_deployment.id,
            id=uuid4(),
            schedule=CronSchedule(
                cron="0 0 * * *",
                timezone="UTC",
            ),
            active=True,
        )
    ]
    return mock_deployment


@pytest.fixture
def mock_flow():
    return Flow(
        id=uuid4(),
        name="test-flow",
    )


@pytest.fixture
def mock_flow_run():
    return DeploymentFlowRun(
        name="test-flow-run",
        id=uuid4(),
        deployment_id=uuid4(),
    )


async def test_schedule_adds_new_schedule(
    cloud_api: respx.Router, mock_deployment: DeploymentResponse, api_url: str
):
    cloud_api.get(f"{api_url}/deployments/{mock_deployment.id}").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )
    cloud_api.delete(
        f"{api_url}/deployments/{mock_deployment.id}/schedules/{mock_deployment.id}"
    ).mock(return_value=Response(204))
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/schedules").mock(
        return_value=Response(
            201,
            json=DeploymentSchedule(
                id=uuid4(),
                schedule=CronSchedule(
                    cron="0 12 * * *",
                    timezone="UTC",
                ),
                active=True,
            ).model_dump(mode="json"),
        )
    )

    await deployments.schedule(str(mock_deployment.id), "0 12 * * *")

    assert (
        cloud_api.calls.last.request.url
        == f"{api_url}/deployments/{mock_deployment.id}/schedules"
    )
    assert "0 12 * * *" in cloud_api.calls.last.request.content.decode()


async def test_schedule_removes_prior_schedules(
    cloud_api: respx.Router,
    mock_deployment_with_schedule: DeploymentResponse,
    api_url: str,
):
    cloud_api.get(f"{api_url}/deployments/{mock_deployment_with_schedule.id}").mock(
        return_value=Response(
            200, json=mock_deployment_with_schedule.model_dump(mode="json")
        )
    )
    delete_schedule = cloud_api.delete(
        f"{api_url}"
        f"/deployments/{mock_deployment_with_schedule.id}"
        f"/schedules/{mock_deployment_with_schedule.schedules[0].id}"
    ).mock(return_value=Response(204))
    cloud_api.post(
        f"{api_url}/deployments/{mock_deployment_with_schedule.id}/schedules"
    ).mock(
        return_value=Response(
            201,
            json=DeploymentSchedule(
                id=uuid4(),
                schedule=CronSchedule(
                    cron="0 12 * * *",
                    timezone="UTC",
                ),
                active=True,
            ).model_dump(mode="json"),
        )
    )

    await deployments.schedule(str(mock_deployment_with_schedule.id), "0 12 * * *")

    assert delete_schedule.called


async def test_schedule_accepts_deployment_name(
    cloud_api: respx.Router, mock_deployment: DeploymentResponse, api_url: str
):
    cloud_api.get(f"{api_url}/deployments/name/my-flow/my-deployment").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )
    cloud_api.delete(
        f"{api_url}/deployments/{mock_deployment.id}/schedules/{mock_deployment.id}"
    ).mock(return_value=Response(204))
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/schedules").mock(
        return_value=Response(
            201,
            json=DeploymentSchedule(
                id=uuid4(),
                schedule=CronSchedule(
                    cron="0 12 * * *",
                    timezone="UTC",
                ),
                active=True,
            ).model_dump(mode="json"),
        )
    )

    await deployments.schedule("my-flow/my-deployment", "0 12 * * *")

    assert (
        cloud_api.calls.last.request.url
        == f"{api_url}/deployments/{mock_deployment.id}/schedules"
    )


async def test_schedule_none_removes_all_schedules(
    cloud_api: respx.Router,
    mock_deployment_with_schedule: DeploymentResponse,
    api_url: str,
):
    cloud_api.get(f"{api_url}/deployments/{mock_deployment_with_schedule.id}").mock(
        return_value=Response(
            200, json=mock_deployment_with_schedule.model_dump(mode="json")
        )
    )
    delete_schedule = cloud_api.delete(
        f"{api_url}"
        f"/deployments/{mock_deployment_with_schedule.id}"
        f"/schedules/{mock_deployment_with_schedule.schedules[0].id}"
    ).mock(return_value=Response(204))

    await deployments.schedule(str(mock_deployment_with_schedule.id), "none")

    assert delete_schedule.called
    assert len(cloud_api.calls) == 2  # Only get and delete, no create


async def test_pause_deployment(
    cloud_api: respx.Router,
    mock_deployment_with_schedule: DeploymentResponse,
    api_url: str,
):
    cloud_api.get(f"{api_url}/deployments/{mock_deployment_with_schedule.id}").mock(
        return_value=Response(
            200, json=mock_deployment_with_schedule.model_dump(mode="json")
        )
    )
    patch_route = cloud_api.patch(
        f"{api_url}"
        f"/deployments/{mock_deployment_with_schedule.id}"
        f"/schedules/{mock_deployment_with_schedule.schedules[0].id}"
    ).mock(return_value=Response(204))

    await deployments.pause(str(mock_deployment_with_schedule.id))

    assert patch_route.called
    assert patch_route.calls.last.request.content == b'{"active":false}'


async def test_resume_deployment(
    cloud_api: respx.Router,
    mock_deployment_with_schedule: DeploymentResponse,
    api_url: str,
):
    cloud_api.get(f"{api_url}/deployments/{mock_deployment_with_schedule.id}").mock(
        return_value=Response(
            200, json=mock_deployment_with_schedule.model_dump(mode="json")
        )
    )
    patch_route = cloud_api.patch(
        f"{api_url}"
        f"/deployments/{mock_deployment_with_schedule.id}"
        f"/schedules/{mock_deployment_with_schedule.schedules[0].id}"
    ).mock(return_value=Response(204))

    await deployments.resume(str(mock_deployment_with_schedule.id))

    assert patch_route.called
    assert patch_route.calls.last.request.content == b'{"active":true}'


async def test_list_returns_empty_context_when_no_deployments(
    cloud_api: respx.Router, api_url: str
):
    cloud_api.post(f"{api_url}/deployments/filter").mock(
        return_value=Response(200, json=[])
    )
    cloud_api.post(f"{api_url}/flows/filter").mock(return_value=Response(200, json=[]))
    cloud_api.post(f"{api_url}/flow_runs/filter").mock(
        return_value=Response(200, json=[])
    )

    result = await deployments.list()

    assert len(result.deployments) == 0
    assert len(result.flows_by_id) == 0
    assert len(result.next_runs_by_deployment_id) == 0


async def test_list_returns_populated_context(
    cloud_api: respx.Router,
    api_url: str,
    mock_deployment: DeploymentResponse,
    mock_deployment_with_schedule: DeploymentResponse,
    mock_flow: Flow,
    mock_flow_run: DeploymentFlowRun,
):
    # Set up the flow run to match one of our deployments
    mock_flow_run.deployment_id = mock_deployment.id
    mock_flow.id = mock_deployment.flow_id

    cloud_api.post(f"{api_url}/deployments/filter").mock(
        return_value=Response(
            200,
            json=[
                mock_deployment.model_dump(mode="json"),
                mock_deployment_with_schedule.model_dump(mode="json"),
            ],
        )
    )
    cloud_api.post(f"{api_url}/flows/filter").mock(
        return_value=Response(200, json=[mock_flow.model_dump(mode="json")])
    )
    cloud_api.post(f"{api_url}/flow_runs/filter").mock(
        return_value=Response(200, json=[mock_flow_run.model_dump(mode="json")])
    )

    result = await deployments.list()

    assert len(result.deployments) == 2
    assert len(result.flows_by_id) == 1
    assert mock_flow.id in result.flows_by_id
    assert len(result.next_runs_by_deployment_id) == 1
    assert mock_deployment.id in result.next_runs_by_deployment_id
