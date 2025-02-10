import json
import time
from pathlib import Path

from prefect_cloud.auth import cloud_client, get_cloud_urls_without_login

COMPLETION_CACHE = Path.home() / ".prefect" / "prefect-cloud-completions.json"
CACHE_TTL = 86400


def clear_cache():
    if COMPLETION_CACHE.exists():
        COMPLETION_CACHE.unlink()


async def complete_deployment(incomplete: str) -> list[str]:
    _, api_url, api_key = get_cloud_urls_without_login()
    if not api_url or not api_key:
        return []

    deployment_names = None
    if (
        COMPLETION_CACHE.exists()
        and time.time() - COMPLETION_CACHE.stat().st_mtime < CACHE_TTL
    ):
        try:
            with open(COMPLETION_CACHE) as f:
                cache = json.load(f)
                deployment_names = cache["deployment_names"]
        except (IOError, json.JSONDecodeError, KeyError):
            deployment_names = []

    if deployment_names is None:
        async with cloud_client(api_key) as client:
            response = await client.post(f"{api_url}/deployments/filter")
            response.raise_for_status()
            deployments = response.json()

            response = await client.post(f"{api_url}/flows/filter")
            response.raise_for_status()
            flows = response.json()

        flow_names = {flow["id"]: flow["name"] for flow in flows}

        deployment_names = [
            f"{flow_names[deployment['flow_id']]}/{deployment['name']}"
            for deployment in deployments
        ]

        COMPLETION_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with open(COMPLETION_CACHE, "w") as f:
            json.dump({"deployment_names": deployment_names}, f)

    return [name for name in deployment_names if name.startswith(incomplete)]
