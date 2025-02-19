from uuid import UUID, uuid4

import pytest
import respx
from httpx import Response

from prefect_cloud import deployments
from prefect_cloud.schemas.objects import DeploymentFlowRun
from prefect_cloud.schemas.responses import DeploymentResponse
from prefect_cloud.utilities.exception import ObjectNotFound


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
        work_pool_name="test-pool",
        schedules=[],
    )


@pytest.fixture
def mock_flow_run() -> DeploymentFlowRun:
    return DeploymentFlowRun(
        name="test-flow-run",
        id=uuid4(),
        deployment_id=uuid4(),
    )


async def test_run_deployment_by_id(
    cloud_api: respx.Router,
    mock_deployment: DeploymentResponse,
    mock_flow_run: DeploymentFlowRun,
    api_url: str,
):
    """run() should create a flow run when given a deployment ID"""
    mock_flow_run.deployment_id = mock_deployment.id

    cloud_api.get(f"{api_url}/deployments/{mock_deployment.id}").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/flow_runs").mock(
        return_value=Response(201, json=mock_flow_run.model_dump(mode="json"))
    )
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/create_flow_run").mock(
        return_value=Response(201, json=mock_flow_run.model_dump(mode="json"))
    )

    result = await deployments.run(str(mock_deployment.id))

    assert result.id == mock_flow_run.id
    assert result.deployment_id == mock_deployment.id


async def test_run_deployment_by_name(
    cloud_api: respx.Router,
    mock_deployment: DeploymentResponse,
    mock_flow_run: DeploymentFlowRun,
    api_url: str,
):
    """run() should create a flow run when given a deployment name"""
    mock_flow_run.deployment_id = mock_deployment.id
    deployment_name = "my-flow/my-deployment"

    cloud_api.get(f"{api_url}/deployments/name/{deployment_name}").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/flow_runs").mock(
        return_value=Response(201, json=mock_flow_run.model_dump(mode="json"))
    )
    cloud_api.post(f"{api_url}/deployments/{mock_deployment.id}/create_flow_run").mock(
        return_value=Response(201, json=mock_flow_run.model_dump(mode="json"))
    )

    result = await deployments.run(deployment_name)

    assert result.id == mock_flow_run.id
    assert result.deployment_id == mock_deployment.id


async def test_run_deployment_not_found(
    cloud_api: respx.Router,
    api_url: str,
):
    """run() should raise an appropriate error when the deployment is not found"""
    deployment_id = uuid4()
    cloud_api.get(f"{api_url}/deployments/{deployment_id}").mock(
        return_value=Response(404, json={"detail": "Deployment not found"})
    )

    with pytest.raises(ObjectNotFound):
        await deployments.run(str(deployment_id))
