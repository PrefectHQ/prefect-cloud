from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from httpx import AsyncClient


class FileNotFound(Exception):
    pass


@dataclass
class GitHubFileRef:
    """Reference to a file in a GitHub repository."""

    owner: str
    repo: str
    ref: str  # Can be either a branch name or commit SHA
    filepath: str
    ref_type: Literal["blob", "tree"]

    @classmethod
    def from_url(cls, url: str) -> "GitHubFileRef":
        """Parse a GitHub URL into its components.

        Handles various GitHub URL formats:
        - https://github.com/owner/repo/blob/branch/path/to/file.py
        - github.com/owner/repo/blob/a1b2c3d/path/to/file.py (commit SHA)
        - github.com/owner/repo/blob/path/to/file.py (uses first path component as ref)

        Also handles tree URLs:
        - https://github.com/owner/repo/tree/branch/path/to/dir
        - github.com/owner/repo/tree/a1b2c3d/path/to/dir (commit SHA)
        - github.com/owner/repo/tree/path/to/dir (uses first path component as ref)

        Args:
            url: GitHub URL to parse

        Returns:
            GitHubFileRef containing parsed components

        Raises:
            ValueError: If URL cannot be parsed into required components
        """
        # Handle URLs without protocol but with github.com
        if url.startswith("github.com"):
            url = "https://" + url

        parsed = urlparse(url)
        if parsed.netloc != "github.com":
            raise ValueError("Not a GitHub URL. Must include 'github.com' in the URL")

        # Remove leading/trailing slashes and split path
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 4:  # Need at least owner/repo/[blob|tree]/ref
            raise ValueError(
                "Invalid GitHub URL. Expected format: "
                "https://github.com/owner/repo/blob|tree/ref/path/to/file.py"
            )

        owner, repo = parts[:2]
        ref_type = cast(Literal["blob", "tree"], parts[2])

        if ref_type not in ("blob", "tree"):
            raise ValueError(
                f"Invalid reference type '{ref_type}'. Must be 'blob' or 'tree'"
            )

        # Always use the first component after blob/tree as the ref
        ref = parts[3]
        filepath = "/".join(parts[4:]) if len(parts) > 4 else ""

        if not filepath:
            raise ValueError(
                "Invalid GitHub URL. Expected format: "
                "https://github.com/owner/repo/blob|tree/ref/path/to/file.py"
            )

        return cls(
            owner=owner, repo=repo, ref=ref, filepath=filepath, ref_type=ref_type
        )

    @property
    def clone_url(self) -> str:
        """Get the HTTPS URL for cloning this repository."""
        return f"https://github.com/{self.owner}/{self.repo}.git"

    @property
    def directory(self) -> str:
        """Get the directory containing this file."""
        return str(Path(self.filepath).parent)

    @property
    def api_url(self) -> str:
        """Get the GitHub API URL for this file."""
        return f"https://api.github.com/repos/{self.owner}/{self.repo}/contents/{self.filepath}?ref={self.ref}"

    def __str__(self) -> str:
        return f"github.com/{self.owner}/{self.repo} @ {self.ref} - {self.filepath}"


def to_pull_step(
    github_ref: GitHubFileRef, credentials_block: str | None = None
) -> dict[str, Any]:
    pull_step_kwargs = {
        "repository": github_ref.clone_url,
        "branch": github_ref.ref,
    }
    if credentials_block:
        pull_step_kwargs["access_token"] = (
            "{{ prefect.blocks.secret." + credentials_block + " }}"
        )

    return {"prefect.deployments.steps.git_clone": pull_step_kwargs}


async def get_github_raw_content(
    github_ref: GitHubFileRef, credentials: str | None = None
) -> str:
    """Get content of a file from GitHub API."""
    headers = {
        "Accept": "application/vnd.github.v3.raw",
    }
    if credentials:
        headers["Authorization"] = f"Bearer {credentials}"

    async with AsyncClient() as client:
        response = await client.get(github_ref.api_url, headers=headers)
        if response.status_code == 404:
            raise FileNotFound(f"File not found: {github_ref}")
        response.raise_for_status()
        return response.text
