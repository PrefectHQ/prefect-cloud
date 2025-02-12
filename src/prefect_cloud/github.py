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
    branch: str
    filepath: str
    ref_type: Literal["blob", "tree"]

    @classmethod
    def from_url(cls, url: str) -> "GitHubFileRef":
        """Parse a GitHub URL into its components.

        Handles both blob and tree URLs:
        - https://github.com/owner/repo/blob/branch/path/to/file.py
        - https://github.com/owner/repo/tree/branch/path/to/file.py

        Args:
            url: GitHub URL to parse

        Returns:
            GitHubFileRef containing parsed components

        Raises:
            ValueError: If URL is not a valid GitHub blob/tree URL
        """
        parsed = urlparse(url)
        if parsed.netloc != "github.com":
            raise ValueError("Not a GitHub URL")

        parts = parsed.path.strip("/").split("/")
        if len(parts) < 5:  # owner/repo/[blob|tree]/branch/filepath
            raise ValueError(
                "Invalid GitHub URL. Expected format: "
                "https://github.com/owner/repo/blob|tree/branch/path/to/file.py"
            )

        owner, repo = parts[:2]
        ref_type = cast(Literal["blob", "tree"], parts[2])

        if ref_type not in ("blob", "tree"):
            raise ValueError(
                f"Invalid reference type '{ref_type}'. Must be 'blob' or 'tree'"
            )

        branch = parts[3]
        filepath = "/".join(parts[4:])

        return cls(
            owner=owner, repo=repo, branch=branch, filepath=filepath, ref_type=ref_type
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
    def raw_url(self) -> str:
        """Get the raw.githubusercontent.com URL for this file."""
        return f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/refs/heads/{self.branch}/{self.filepath}"

    def __str__(self) -> str:
        return f"github.com/{self.owner}/{self.repo} @ {self.branch} - {self.filepath}"


def to_pull_step(
    github_ref: GitHubFileRef, credentials_block: str | None = None
) -> dict[str, Any]:
    pull_step_kwargs = {
        "repository": github_ref.clone_url,
        "branch": github_ref.branch,
    }
    if credentials_block:
        pull_step_kwargs["access_token"] = (
            "{{ prefect.blocks.secret." + credentials_block + " }}"
        )

    return {"prefect.deployments.steps.git_clone": pull_step_kwargs}


async def get_github_raw_content(
    github_ref: GitHubFileRef, credentials: str | None = None
) -> str:
    """Get raw content of a file from GitHub."""
    headers: dict[str, str] = {}
    if credentials:
        headers["Authorization"] = f"Bearer {credentials}"

    async with AsyncClient() as client:
        response = await client.get(github_ref.raw_url, headers=headers)
        if response.status_code == 404:
            raise FileNotFound(f"File not found: {github_ref}")
        response.raise_for_status()
        return response.text
