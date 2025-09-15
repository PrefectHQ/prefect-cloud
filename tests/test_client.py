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

PREFECT_API_KEY = "test_key"
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/123/workspaces/456"
PREFECT_ACCOUNT_URL = "https://api.prefect.cloud/api/accounts/123"


@pytest.fixture
def client() -> PrefectCloudClient:
    return PrefectCloudClient(api_url=PREFECT_API_URL, api_key=PREFECT_API_KEY)


@pytest.fixture
def mock_deployment() -> DeploymentResponse:
    return DeploymentResponse(
        id=uuid4(),
        flow_id=uuid4(),
        name="test-deployment",
        work_pool_name="test-pool",
        schedules=[],
        parameters={},
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


async def test_upsert_block_document_creates_block(
    client: PrefectCloudClient,
    mock_block_document: BlockDocument,
    respx_mock: respx.Router,
):
    respx_mock.put(f"{PREFECT_API_URL}/block_documents/").mock(
        return_value=Response(200, json=mock_block_document.model_dump(mode="json"))
    )

    result = await client.upsert_block_document(mock_block_document)

    assert result == mock_block_document


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
    ).mock(
        return_value=Response(201, json=[deployment_schedule.model_dump(mode="json")])
    )

    result = await client.create_deployment_schedule(
        deployment_id=mock_deployment.id,
        schedule=schedule,
        active=True,
    )

    assert result.id == deployment_schedule.id
    assert result.deployment_id == mock_deployment.id
    assert result.schedule == schedule


async def test_get_default_base_job_template_for_managed_work_pool(
    client: PrefectCloudClient,
    respx_mock: respx.Router,
):
    mock_template = {
        "job_configuration": {
            "command": "python {{ entrypoint }}",
            "image": "prefecthq/prefect-client:3-latest",
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


async def test_account_url(client: PrefectCloudClient):
    """Test that the account_url property correctly extracts the account URL from the workspace URL."""
    assert client.account_url == PREFECT_ACCOUNT_URL


async def test_account_request(client: PrefectCloudClient, respx_mock: respx.Router):
    """Test that account_request makes requests to the account URL."""
    test_data = {"test": "data"}

    # Setup mock to respond to the account-level endpoint
    respx_mock.post(f"{PREFECT_ACCOUNT_URL}/test/endpoint").mock(
        return_value=Response(200, json=test_data)
    )

    # Make the request
    response = await client.account_request("POST", "test/endpoint")

    # Verify the response
    assert response.status_code == 200
    assert response.json() == test_data


async def test_get_github_state_token(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that get_github_state_token correctly returns a state token."""
    test_token = "test_state_token"
    test_redirect_url = "https://example.com/callback"

    respx_mock.post(f"{PREFECT_ACCOUNT_URL}/integrations/github/state-token").mock(
        return_value=Response(200, json={"state_token": test_token})
    )

    result = await client.get_github_state_token(redirect_url=test_redirect_url)

    assert result == test_token

    # Check request was made with correct JSON data
    # The redirect_url is passed directly as the json parameter, which means
    # it gets JSON-serialized (with quotes) in the request body
    assert len(respx_mock.calls) == 1
    import json

    expected_content = json.dumps(test_redirect_url).encode()
    assert respx_mock.calls[0].request.content == expected_content


async def test_get_github_token(client: PrefectCloudClient, respx_mock: respx.Router):
    """Test that get_github_token correctly returns a GitHub token."""
    test_token = "github_access_token"
    test_repo = "test-repo"
    test_owner = "test-owner"

    respx_mock.post(f"{PREFECT_ACCOUNT_URL}/integrations/github/token").mock(
        return_value=Response(200, json={"token": test_token})
    )

    result = await client.get_github_token(repository=test_repo, owner=test_owner)
    assert result == test_token

    assert len(respx_mock.calls) == 1
    import json

    request_content = respx_mock.calls[0].request.content
    request_json = json.loads(request_content.decode("utf-8"))
    assert request_json == {"repository": test_repo, "owner": test_owner}


async def test_get_github_repositories(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that get_github_repositories correctly returns a list of repositories."""
    test_repos = ["repo1", "repo2", "repo3"]
    respx_mock.get(f"{PREFECT_ACCOUNT_URL}/integrations/github/repositories").mock(
        return_value=Response(200, json={"repositories": test_repos})
    )

    result = await client.get_github_repositories()

    assert result == test_repos


async def test_get_github_repositories_error(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that get_github_repositories returns an empty list when the request fails."""
    respx_mock.get(f"{PREFECT_ACCOUNT_URL}/integrations/github/repositories").mock(
        return_value=Response(404)
    )

    result = await client.get_github_repositories()

    assert result == []


# Retry functionality tests


async def test_request_retry_on_read_timeout(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are retried on ReadTimeout errors."""
    import httpx
    import time

    # First two requests timeout, third succeeds
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=[
            httpx.ReadTimeout("Request timeout"),
            httpx.ReadTimeout("Request timeout"),
            Response(200, json={"success": True}),
        ]
    )

    start_time = time.time()
    response = await client.request("GET", "/test")
    elapsed_time = time.time() - start_time

    assert response.status_code == 200
    assert response.json() == {"success": True}
    assert len(respx_mock.calls) == 3
    # Should have delays (approximately 1s + 2s = 3s minimum with jitter)
    assert elapsed_time >= 2.5


async def test_request_retry_on_connect_timeout(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are retried on ConnectTimeout errors."""
    import httpx

    # First request times out, second succeeds
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=[
            httpx.ConnectTimeout("Connection timeout"),
            Response(200, json={"success": True}),
        ]
    )

    response = await client.request("GET", "/test")

    assert response.status_code == 200
    assert len(respx_mock.calls) == 2


async def test_request_retry_on_network_error(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are retried on NetworkError."""
    import httpx

    # First request has network error, second succeeds
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=[
            httpx.NetworkError("Network unreachable"),
            Response(200, json={"success": True}),
        ]
    )

    response = await client.request("GET", "/test")

    assert response.status_code == 200
    assert len(respx_mock.calls) == 2


async def test_request_retry_on_server_error(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are retried on 5xx server errors."""
    # First request returns 500, second succeeds
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=[
            Response(500, json={"error": "Internal server error"}),
            Response(200, json={"success": True}),
        ]
    )

    response = await client.request("GET", "/test")

    assert response.status_code == 200
    assert len(respx_mock.calls) == 2


async def test_request_retry_on_rate_limit(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are retried on 429 rate limit errors."""
    # First request returns 429, second succeeds
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=[
            Response(429, json={"error": "Rate limit exceeded"}),
            Response(200, json={"success": True}),
        ]
    )

    response = await client.request("GET", "/test")

    assert response.status_code == 200
    assert len(respx_mock.calls) == 2


async def test_request_no_retry_on_client_error(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests are NOT retried on 4xx client errors (except 429)."""
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        return_value=Response(404, json={"error": "Not found"})
    )

    response = await client.request("GET", "/test")

    assert response.status_code == 404
    assert len(respx_mock.calls) == 1  # No retry


async def test_request_max_retries_exceeded(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that requests fail after maximum retries are exceeded."""
    import httpx

    # All requests timeout
    respx_mock.get(f"{PREFECT_API_URL}/test").mock(
        side_effect=httpx.ReadTimeout("Request timeout")
    )

    with pytest.raises(httpx.ReadTimeout):
        await client.request("GET", "/test")

    # Should have made 4 attempts (initial + 3 retries)
    assert len(respx_mock.calls) == 4


async def test_request_github_token_with_retry(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that get_github_token works with retry logic."""
    import httpx

    test_token = "github_access_token"
    test_repo = "test-repo"
    test_owner = "test-owner"

    # First request times out, second succeeds
    respx_mock.post(f"{PREFECT_ACCOUNT_URL}/integrations/github/token").mock(
        side_effect=[
            httpx.ReadTimeout("Request timeout"),
            Response(200, json={"token": test_token}),
        ]
    )

    result = await client.get_github_token(repository=test_repo, owner=test_owner)

    assert result == test_token
    assert len(respx_mock.calls) == 2


async def test_request_github_token_returns_none_on_404(
    client: PrefectCloudClient, respx_mock: respx.Router
):
    """Test that get_github_token still returns None on 404 (handled by method logic)."""
    test_repo = "test-repo"
    test_owner = "test-owner"

    # Return 404 (should not be retried)
    respx_mock.post(f"{PREFECT_ACCOUNT_URL}/integrations/github/token").mock(
        return_value=Response(404, json={"error": "Not found"})
    )

    result = await client.get_github_token(repository=test_repo, owner=test_owner)

    assert result is None
    assert len(respx_mock.calls) == 1  # No retry
