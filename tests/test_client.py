from prefect_cloud.client import PrefectCloudClient
from unittest.mock import AsyncMock

PREFECT_API_KEY = "test_key"
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/123/workspaces/456"


class TestClient:
    async def test_client_context_can_be_reentered(self):
        client = PrefectCloudClient(api_url="http://foo.test", api_key="test_key")
        client._exit_stack.__aenter__ = AsyncMock()
        client._exit_stack.__aexit__ = AsyncMock()

        assert client._exit_stack.__aenter__.call_count == 0
        assert client._exit_stack.__aexit__.call_count == 0
        async with client as c1:
            async with client as c2:
                assert c1 is c2

        # despite entering the context twice, we only ran its major logic once
        assert client._exit_stack.__aenter__.call_count == 1
        assert client._exit_stack.__aexit__.call_count == 1
