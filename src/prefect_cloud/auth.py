import os
import threading
import webbrowser
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Generator, Sequence
from urllib.parse import quote
from uuid import UUID

import toml
from pydantic import BaseModel, TypeAdapter

from prefect_cloud.client import PrefectCloudClient, SyncPrefectCloudClient
from prefect_cloud.utilities.callback import CallbackServerHandler, callback_server
from prefect_cloud.utilities.tui import prompt_select_from_list
from prefect_cloud.utilities.urls import extract_account_id, extract_workspace_id


class LoginError(Exception):
    pass


def _get_cloud_urls() -> tuple[str, str]:
    """Get the appropriate Cloud UI and API URLs based on environment"""

    env = os.environ.get("CLOUD_ENV")
    from_env = os.environ.get("PREFECT_CLOUD_API_URL")

    if from_env:
        cloud_api_url = from_env
    elif env in ("prd", "prod", None):
        cloud_api_url = "https://api.prefect.cloud/api"
    elif env == "stg":
        cloud_api_url = "https://api.stg.prefect.dev/api"
    elif env == "dev":
        cloud_api_url = "https://api.prefect.dev/api"
    elif env == "lcl":
        cloud_api_url = "http://localhost:8000/api"
    else:
        raise ValueError(f"Invalid CLOUD_ENV: {env}")

    if os.environ.get("PREFECT_CLOUD_UI_URL"):
        cloud_ui_url = os.environ["PREFECT_CLOUD_UI_URL"]
    else:
        cloud_ui_url = cloud_api_url.replace("https://api.", "https://app.").replace(
            "/api", ""
        )

    return cloud_ui_url, cloud_api_url


CLOUD_UI_URL, CLOUD_API_URL = _get_cloud_urls()

PREFECT_HOME = Path.home() / ".prefect"


class Account(BaseModel):
    account_id: UUID
    account_name: str
    account_handle: str


class Workspace(BaseModel):
    account_id: UUID
    account_name: str
    account_handle: str
    workspace_id: UUID
    workspace_name: str
    workspace_handle: str

    @property
    def full_handle(self) -> str:
        return f"{self.account_handle}/{self.workspace_handle}"

    @property
    def api_url(self) -> str:
        return (
            f"{CLOUD_API_URL}/accounts/{self.account_id}/workspaces/{self.workspace_id}"
        )


class Me(BaseModel):
    id: UUID
    email: str
    first_name: str
    last_name: str
    handle: str


async def login(api_key: str | None = None, workspace_id_or_slug: str | None = None):
    """Logs the user into Prefect Cloud interactively, setting their active profile"""

    if not api_key:
        api_key = get_api_key()

    if not api_key or not await key_is_valid(api_key):
        api_key = login_interactively()
        if not api_key:
            return

    workspaces = await get_workspaces(api_key)

    # Try to use specified workspace if provided
    if workspace_id_or_slug and workspaces:
        workspace = await lookup_workspace(workspace_id_or_slug, workspaces)
        if workspace:
            set_cloud_profile(api_key, workspace)
            return

    # Select from existing workspaces or create a new one
    if workspaces:
        workspace = await prompt_for_workspace(workspaces)
        if workspace:
            set_cloud_profile(api_key, workspace)
    else:
        accounts = await get_accounts(api_key)
        account = await prompt_for_account(accounts)
        if account:
            workspace = await create_workspace(api_key, account)
            set_cloud_profile(api_key, workspace)


def logout():
    """Logs the user out of Prefect Cloud"""
    remove_cloud_profile()


async def get_account_id() -> UUID:
    """Gets the account ID from the current cloud profile"""
    _, api_url, _ = await get_cloud_urls_or_login()
    account_id = extract_account_id(api_url)
    if not account_id:
        raise ValueError("No account ID found")
    return account_id


async def get_workspace_id() -> UUID:
    """Gets the workspace ID from the current cloud profile"""
    _, api_url, _ = await get_cloud_urls_or_login()
    workspace_id = extract_workspace_id(api_url)
    if not workspace_id:
        raise ValueError("No workspace ID found")
    return workspace_id


@asynccontextmanager
async def cloud_client(api_key: str) -> AsyncGenerator[PrefectCloudClient, None]:
    """Creates a client for the Prefect Cloud API"""

    async with PrefectCloudClient(api_url=CLOUD_API_URL, api_key=api_key) as client:
        yield client


@contextmanager
def sync_cloud_client(api_key: str) -> Generator[SyncPrefectCloudClient, None, None]:
    """Creates a client for the Prefect Cloud API"""
    with SyncPrefectCloudClient(api_url=CLOUD_API_URL, api_key=api_key) as client:
        yield client


async def get_prefect_cloud_client() -> PrefectCloudClient:
    _, api_url, api_key = await get_cloud_urls_or_login()
    return PrefectCloudClient(
        api_url=api_url,
        api_key=api_key,
    )


async def get_cloud_urls_or_login() -> tuple[str, str, str]:
    """Gets the cloud UI URL, API URL, and API key"""
    ui_url, api_url, api_key = get_cloud_urls_without_login()
    if not ui_url or not api_url or not api_key:
        await login()

    ui_url, api_url, api_key = get_cloud_urls_without_login()
    if not ui_url or not api_url or not api_key:
        raise ValueError("No cloud profile found")

    return ui_url, api_url, api_key


def get_cloud_urls_without_login() -> tuple[str | None, str | None, str | None]:
    """Gets the cloud UI URL, API URL, and API key"""
    api_url: str | None = get_api_url()
    if not api_url:
        return None, None, None

    ui_url = (
        api_url.replace("https://api.", "https://app.")
        .replace("/api", "")
        .replace("/accounts/", "/account/")
        .replace("/workspaces/", "/workspace/")
    )

    api_key = get_api_key()
    if not api_key:
        return None, None, None

    return ui_url, api_url, api_key


def get_api_key_or_login() -> str:
    """Gets a validated API key or logs the user in if no API key is available"""
    api_key = get_api_key()
    if not api_key:
        api_key = login_interactively()
        if not api_key:
            raise ValueError("No API key found")
    return api_key


async def key_is_valid(api_key: str) -> bool:
    """Checks if the given API key is valid"""
    async with cloud_client(api_key) as client:
        response = await client.request("GET", "/me/")
        return response.status_code == 200


@contextmanager
def login_server() -> Generator[Any, None, None]:
    """Runs a server to handle the login callback"""

    class LoginHandler(CallbackServerHandler):
        def process_get(
            self, path: str, query_params: dict[str, list[str]]
        ) -> str | None:
            return query_params.get("key", [""])[0] or None

        def process_post(
            self, path: str, data: dict[str, Any]
        ) -> str | None | LoginError:
            if path == "/failure":
                raise LoginError("Login cancelled by user")

            return data.get("api_key") or None

    with callback_server(handler_class=LoginHandler) as callback_ctx:
        yield callback_ctx


def login_interactively() -> str | None:
    """Logs the user into Prefect Cloud interactively"""

    with login_server() as callback_ctx:
        login_url = f"{CLOUD_UI_URL}/auth/client?callback={quote(callback_ctx.url)}&source=prefect-cloud&g=true"

        threading.Thread(
            target=webbrowser.open_new_tab, args=(login_url,), daemon=True
        ).start()

        return callback_ctx.wait_for_callback()


async def me(api_key: str) -> Me:
    """Gets the current user's information"""
    async with cloud_client(api_key) as client:
        response = await client.request("GET", "/me/")
        response.raise_for_status()

    return Me.model_validate_json(response.text)


async def get_accounts(api_key: str) -> Sequence[Account]:
    """Gets the list of accounts for the current user"""
    async with cloud_client(api_key) as client:
        response = await client.request("GET", "/me/accounts")
        response.raise_for_status()

        return TypeAdapter(list[Account]).validate_json(response.text)


async def get_workspaces(api_key: str) -> Sequence[Workspace]:
    """Gets the list of workspaces for the current user"""
    async with cloud_client(api_key) as client:
        response = await client.request("GET", "/me/workspaces")
        response.raise_for_status()

        workspaces = TypeAdapter(list[Workspace]).validate_json(response.text)
        workspaces.sort(key=lambda w: w.full_handle)

        return workspaces


async def create_workspace(
    api_key: str, account: Account, name: str = "default"
) -> Workspace:
    """Create a workspace"""
    async with cloud_client(api_key) as client:
        response = await client.request(
            "POST",
            f"/accounts/{account.account_id}/workspaces/",
            json={"handle": name, "name": name},
        )
        response.raise_for_status()
        workspace = response.json()

        return Workspace(
            account_id=account.account_id,
            account_name=account.account_handle,
            account_handle=account.account_handle,
            workspace_id=UUID(workspace["id"]),
            workspace_name=workspace["name"],
            workspace_handle=workspace["handle"],
        )


async def lookup_workspace(
    workspace_id_or_slug: str, workspaces: Sequence[Workspace]
) -> Workspace | None:
    """Looks up a workspace by ID or slug"""
    workspace_id: UUID | None = None
    try:
        workspace_id = UUID(workspace_id_or_slug)
    except ValueError:
        pass

    for workspace in workspaces:
        if workspace_id and workspace.workspace_id == workspace_id:
            return workspace
        if workspace.full_handle == workspace_id_or_slug:
            return workspace
        if workspace.workspace_handle == workspace_id_or_slug:
            return workspace

    return None


async def prompt_for_workspace(workspaces: Sequence[Workspace]) -> Workspace | None:
    """Prompts the user to select a workspace from the list of available workspaces"""
    if not workspaces:
        return None

    if len(workspaces) == 1:
        return workspaces[0]

    selected = prompt_select_from_list(
        "Select a workspace",
        sorted([workspace.full_handle for workspace in workspaces]),
    )
    return next(
        workspace for workspace in workspaces if workspace.full_handle == selected
    )


async def prompt_for_account(accounts: Sequence[Account]) -> Account | None:
    """Prompts the user to select an account from the list of available account"""

    if not accounts:
        return None

    if len(accounts) == 1:
        return accounts[0]

    selected = prompt_select_from_list(
        "Select an account",
        sorted([account.account_handle for account in accounts]),
    )
    return next(account for account in accounts if account.account_handle == selected)


def get_from_env_or_profile(key: str) -> str | None:
    """Gets value from the environment or the current cloud profile"""
    profile = get_cloud_profile() or {}

    if from_env := os.environ.get(key):
        return from_env
    elif from_profile := profile.get(key):
        return from_profile
    else:
        return None


def get_api_key() -> str | None:
    return get_from_env_or_profile("PREFECT_API_KEY")


def get_api_url() -> str | None:
    return get_from_env_or_profile("PREFECT_API_URL")


def load_profiles() -> dict[str, Any]:
    """Loads the profiles from the profiles file"""
    profile_path = PREFECT_HOME / "profiles.toml"
    if profile_path.exists():
        profiles = toml.load(profile_path)
    else:
        profiles = {}

    if "profiles" not in profiles:
        profiles["profiles"] = {}

    return profiles


def cloud_profile_name() -> str:
    """Returns the name of the current cloud profile"""
    if CLOUD_API_URL == "https://api.stg.prefect.dev/api":
        return "prefect-cloud-stg"
    elif CLOUD_API_URL == "https://api.prefect.dev/api":
        return "prefect-cloud-dev"
    elif CLOUD_API_URL.startswith("http://localhost"):
        return "prefect-cloud-lcl"
    else:
        return "prefect-cloud"


def get_cloud_profile() -> dict[str, str] | None:
    """Returns the current cloud profile"""
    profile_path = PREFECT_HOME / "profiles.toml"
    if not profile_path.exists():
        return None

    profiles = toml.load(profile_path)
    return profiles.get("profiles", {}).get(cloud_profile_name())


def set_cloud_profile(api_key: str, workspace: Workspace) -> None:
    """Writes the current cloud profile"""
    profile_name = cloud_profile_name()
    profile_path = PREFECT_HOME / "profiles.toml"

    profiles = load_profiles()

    profiles["active"] = profile_name
    if profile_name not in profiles["profiles"]:
        profiles["profiles"][profile_name] = {}

    profile = {
        "PREFECT_API_URL": workspace.api_url,
        "PREFECT_API_KEY": api_key,
    }
    if os.environ.get("CLOUD_ENV") not in ("prd", "prod", None):
        profile["PREFECT_CLOUD_UI_URL"] = CLOUD_UI_URL
        profile["PREFECT_CLOUD_API_URL"] = CLOUD_API_URL

    profiles["profiles"][profile_name].update(profile)

    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(toml.dumps(profiles))


def remove_cloud_profile() -> None:
    """Removes the current cloud profile"""
    profile_path = PREFECT_HOME / "profiles.toml"
    if not profile_path.exists():
        return

    profiles = load_profiles()
    profiles["profiles"].pop(cloud_profile_name(), None)
    try:
        profiles["active"] = list(profiles["profiles"].keys())[0]
        profile_path.write_text(toml.dumps(profiles))
    except IndexError:
        profile_path.unlink()
