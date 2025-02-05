import json
import socket
import threading
import webbrowser
from contextlib import asynccontextmanager,contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from queue import Queue
from typing import AsyncGenerator, Generator, Sequence
from urllib.parse import parse_qs, quote, urlparse
from uuid import UUID

import toml
from pydantic import BaseModel, TypeAdapter
from prefect_cloud.client import PrefectCloudClient
from prefect_cloud.utilities.tui import prompt_select_from_list

# TODO: Get this configuration from the user's environment if possible
CLOUD_UI_URL = "https://app.prefect.cloud"
CLOUD_API_URL = "https://api.prefect.cloud/api"
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

    if not api_key or not key_is_valid(api_key):
        api_key = login_interactively()
        if not api_key:
            return

    workspace: Workspace | None = None
    if workspace_id_or_slug:
        workspace = await lookup_workspace(api_key, workspace_id_or_slug)

    if not workspace:
        workspace = await prompt_for_workspace(api_key)
        if not workspace:
            return

    set_cloud_profile(api_key, workspace)


def logout():
    """Logs the user out of Prefect Cloud"""
    remove_cloud_profile()


@asynccontextmanager
async def cloud_client(api_key: str) -> AsyncGenerator[PrefectCloudClient, None]:
    """Creates a client for the Prefect Cloud API"""


    async with PrefectCloudClient(api_url=CLOUD_API_URL, api_key=api_key) as client:
        yield client


def get_cloud_urls_or_login() -> tuple[str, str, str]:
    """Gets the cloud UI URL, API URL, and API key"""
    profile = get_cloud_profile()
    if not profile:
        login()

    profile = get_cloud_profile()
    if not profile:
        raise ValueError("No cloud profile found")

    api_url: str | None = profile.get("PREFECT_API_URL")
    if not api_url:
        raise ValueError("No API URL found")

    ui_url = (
        api_url.replace("https://api.", "https://app.")
        .replace("/api", "")
        .replace("/accounts/", "/account/")
        .replace("/workspaces/", "/workspace/")
    )

    api_key = profile.get("PREFECT_API_KEY")
    if not api_key:
        raise ValueError("No API key found")

    return ui_url, api_url, api_key


def get_api_key_or_login() -> str:
    """Gets a validated API key or logs the user in if no API key is available"""
    api_key = get_api_key()
    if not api_key:
        api_key = login_interactively()
        if not api_key:
            raise ValueError("No API key found")
    return api_key


def key_is_valid(api_key: str) -> bool:
    """Checks if the given API key is valid"""
    with cloud_client(api_key) as client:
        response = client.get("/me/")
        return response.status_code == 200


def login_interactively() -> str | None:
    """Logs the user into Prefect Cloud interactively"""
    result_queue: Queue[str | None] = Queue()
    with login_server(result_queue) as uri:
        login_url = f"{CLOUD_UI_URL}/auth/client?callback={quote(uri)}&g=true"

        threading.Thread(
            target=webbrowser.open_new_tab, args=(login_url,), daemon=True
        ).start()

        return result_queue.get()


@contextmanager
def login_server(result_queue: Queue[str | None]) -> Generator[str, None, None]:
    """Runs a local server to handle the callback from Prefect Cloud"""

    class LoginHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def add_cors_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")

        def do_OPTIONS(self) -> None:
            self.send_response(200)
            self.add_cors_headers()
            self.end_headers()

        def do_GET(self) -> None:
            query_params = parse_qs(urlparse(self.path).query)
            api_key = query_params.get("key", [""])[0] or None

            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
                <script>
                if (window.opener) {
                    window.opener.postMessage('auth-complete', '*')
                }
                </script>
                <p>You can close this window.</p>
            """)
            result_queue.put(api_key or None)

        def do_POST(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            api_key = data.get("api_key")

            self.send_response(200)
            self.add_cors_headers()
            self.end_headers()
            self.wfile.write(b"{}")
            result_queue.put(api_key or None)

    with socket.socket() as sock:
        sock.bind(("", 0))
        port = sock.getsockname()[1]

    server = HTTPServer(("localhost", port), LoginHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        yield f"http://localhost:{port}"
    finally:
        server.server_close()


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


async def lookup_workspace(api_key: str, workspace_id_or_slug: str) -> Workspace | None:
    """Looks up a workspace by ID or slug"""
    workspace_id: UUID | None = None
    try:
        workspace_id = UUID(workspace_id_or_slug)
    except ValueError:
        pass

    for workspace in await get_workspaces(api_key):
        if workspace_id and workspace.workspace_id == workspace_id:
            return workspace
        if workspace.full_handle == workspace_id_or_slug:
            return workspace
        if workspace.workspace_handle == workspace_id_or_slug:
            return workspace

    return None


async def prompt_for_workspace(api_key: str) -> Workspace | None:
    """Prompts the user to select a workspace from the list of available workspaces"""
    workspaces = await get_workspaces(api_key)

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


def get_api_key() -> str | None:
    """Gets the API key for the current cloud profile"""
    profile = get_cloud_profile()
    if not profile:
        return None
    return profile.get("PREFECT_API_KEY")


def load_profiles() -> dict:
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
    profile = profiles.get("profiles", {}).get(cloud_profile_name())
    if not profile:
        return None
    return profile


def set_cloud_profile(api_key: str, workspace: Workspace) -> None:
    """Writes the current cloud profile"""
    profile_name = cloud_profile_name()
    profile_path = PREFECT_HOME / "profiles.toml"

    profiles = load_profiles()

    profiles["active"] = profile_name
    if profile_name not in profiles["profiles"]:
        profiles["profiles"][profile_name] = {}

    profiles["profiles"][profile_name].update(
        {
            "PREFECT_API_KEY": api_key,
            "PREFECT_API_URL": workspace.api_url,
        }
    )

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
