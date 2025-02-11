from uuid import uuid4

import pytest
import respx
from httpx import Response
from prefect_cloud.client import PrefectCloudClient
from prefect_cloud.schemas.objects import (
    BlockDocument,
    BlockSchema,
    BlockType,
    CronSchedule,
    DeploymentSchedule,
    Flow,
    WorkPool,
)
from prefect_cloud.schemas.responses import DeploymentResponse
from prefect_cloud.utilities.exception import ObjectNotFound, ObjectAlreadyExists

PREFECT_API_KEY = "test_key"
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/123/workspaces/456"


@pytest.fixture
def client() -> PrefectCloudClient:
    return PrefectCloudClient(api_url=PREFECT_API_URL, api_key=PREFECT_API_KEY)


@pytest.fixture
def mock_deployment() -> DeploymentResponse:
    return DeploymentResponse(
        id=uuid4(),
        flow_id=uuid4(),
        name="test-deployment",
        schedules=[],
    )


@pytest.fixture
def mock_flow() -> Flow:
    return Flow(
        id=uuid4(),
        name="test-flow",
    )


@pytest.fixture
def mock_work_pool() -> WorkPool:
    return WorkPool(
        name="test-pool",
        type="prefect:managed",
    )


@pytest.fixture
def mock_block_type() -> BlockType:
    return BlockType(id=uuid4())


@pytest.fixture
def mock_block_schema(mock_block_type: BlockType) -> BlockSchema:
    return BlockSchema(id=uuid4())


@pytest.fixture
def mock_block_document(
    mock_block_type: BlockType, mock_block_schema: BlockSchema
) -> BlockDocument:
    return BlockDocument(
        id=uuid4(),
        name="test-secret",
        data={"value": "secret-value"},
        block_type_id=mock_block_type.id,
        block_schema_id=mock_block_schema.id,
        block_type_name="secret",
    )


async def test_read_managed_work_pools(
    client: PrefectCloudClient,
    mock_work_pool: WorkPool,
    respx_mock: respx.Router,
):
    respx_mock.post(f"{PREFECT_API_URL}/work_pools/filter").mock(
        return_value=Response(200, json=[mock_work_pool.model_dump(mode="json")])
    )

    result = await client.read_managed_work_pools()

    assert len(result) == 1
    assert result[0].name == mock_work_pool.name


async def test_create_flow_from_name(
    client: PrefectCloudClient,
    mock_flow: Flow,
    respx_mock: respx.Router,
):
    respx_mock.post(f"{PREFECT_API_URL}/flows/").mock(
        return_value=Response(201, json={"id": str(mock_flow.id)})
    )

    result = await client.create_flow_from_name("test-flow")

    assert result == mock_flow.id


async def test_create_deployment(
    client: PrefectCloudClient,
    mock_deployment: DeploymentResponse,
    respx_mock: respx.Router,
):
    respx_mock.post(f"{PREFECT_API_URL}/deployments/").mock(
        return_value=Response(201, json={"id": str(mock_deployment.id)})
    )

    result = await client.create_deployment(
        flow_id=mock_deployment.flow_id,
        name="test-deployment",
        entrypoint="flow.py:test_flow",
        work_pool_name="test-pool",
        pull_steps=None,
        parameter_openapi_schema=None,
    )

    assert result == mock_deployment.id


async def test_read_block_document_not_found(
    client: PrefectCloudClient,
    respx_mock: respx.Router,
):
    block_id = uuid4()
    respx_mock.get(f"{PREFECT_API_URL}/block_documents/{block_id}").mock(
        return_value=Response(404, json={"detail": "Block document not found"})
    )

    with pytest.raises(ObjectNotFound):
        await client.read_block_document(block_id)


async def test_create_block_document_already_exists(
    client: PrefectCloudClient,
    mock_block_document: BlockDocument,
    respx_mock: respx.Router,
):
    respx_mock.post(f"{PREFECT_API_URL}/block_documents/").mock(
        return_value=Response(409, json={"detail": "Block document already exists"})
    )

    with pytest.raises(ObjectAlreadyExists):
        await client.create_block_document(mock_block_document)


async def test_read_deployment_by_name(
    client: PrefectCloudClient,
    mock_deployment: DeploymentResponse,
    respx_mock: respx.Router,
):
    deployment_name = "test-flow/test-deployment"
    respx_mock.get(f"{PREFECT_API_URL}/deployments/name/{deployment_name}").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )

    result = await client.read_deployment_by_name(deployment_name)

    assert result.id == mock_deployment.id
    assert result.name == mock_deployment.name


async def test_create_deployment_schedule(
    client: PrefectCloudClient,
    mock_deployment: DeploymentResponse,
    respx_mock: respx.Router,
):
    schedule = CronSchedule(cron="0 0 * * *", timezone="UTC")
    deployment_schedule = DeploymentSchedule(
        id=uuid4(),
        deployment_id=mock_deployment.id,
        schedule=schedule,
        active=True,
    )

    respx_mock.post(
        f"{PREFECT_API_URL}/deployments/{mock_deployment.id}/schedules"
    ).mock(return_value=Response(201, json=deployment_schedule.model_dump(mode="json")))

    result = await client.create_deployment_schedule(
        deployment_id=mock_deployment.id,
        schedule=schedule,
        active=True,
    )

    assert result.id == deployment_schedule.id
    assert result.deployment_id == mock_deployment.id
    assert result.schedule == schedule


async def test_pause_deployment(
    client: PrefectCloudClient,
    mock_deployment: DeploymentResponse,
    respx_mock: respx.Router,
):
    schedule = DeploymentSchedule(
        id=uuid4(),
        deployment_id=mock_deployment.id,
        schedule=CronSchedule(cron="0 0 * * *", timezone="UTC"),
        active=True,
    )
    mock_deployment.schedules = [schedule]

    respx_mock.get(f"{PREFECT_API_URL}/deployments/{mock_deployment.id}").mock(
        return_value=Response(200, json=mock_deployment.model_dump(mode="json"))
    )
    patch_route = respx_mock.patch(
        f"{PREFECT_API_URL}/deployments/{mock_deployment.id}/schedules/{schedule.id}"
    ).mock(return_value=Response(204))

    await client.pause_deployment(mock_deployment.id)

    assert patch_route.called
    assert patch_route.calls.last.request.content == b'{"active":false}'


async def test_get_default_base_job_template_for_managed_work_pool(
    client: PrefectCloudClient,
    respx_mock: respx.Router,
):
    mock_template = {
        "job_configuration": {
            "command": "python {{ entrypoint }}",
            "image": "prefecthq/prefect:latest",
        }
    }

    mock_response = {
        "prefecthq": {
            "prefect-agent": {
                "type": "prefect:managed",
                "default_base_job_configuration": mock_template,
            }
        }
    }

    respx_mock.get(f"{PREFECT_API_URL}/collections/work_pool_types").mock(
        return_value=Response(200, json=mock_response)
    )

    result = await client.get_default_base_job_template_for_managed_work_pool()

    assert result == mock_template


async def test_get_default_base_job_template_for_managed_work_pool_no_template(
    client: PrefectCloudClient,
    respx_mock: respx.Router,
):
    # Mock response with no managed worker type
    mock_response = {
        "prefecthq": {
            "prefect-agent": {
                "type": "other",
            }
        }
    }

    respx_mock.get(f"{PREFECT_API_URL}/collections/work_pool_types").mock(
        return_value=Response(200, json=mock_response)
    )

    result = await client.get_default_base_job_template_for_managed_work_pool()

    assert result is None
