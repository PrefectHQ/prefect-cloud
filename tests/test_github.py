import pytest
from httpx import Response

from prefect_cloud.github import FileNotFound, GitHubFileRef, get_github_raw_content


class TestGitHubFileRef:
    def test_from_url_blob(self):
        url = "https://github.com/PrefectHQ/prefect/blob/main/src/prefect/cli/root.py"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"
        assert ref.filepath == "src/prefect/cli/root.py"
        assert ref.ref_type == "blob"

    def test_from_url_tree(self):
        url = "https://github.com/PrefectHQ/prefect/tree/main/src/prefect/cli"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"
        assert ref.filepath == "src/prefect/cli"
        assert ref.ref_type == "tree"

    def test_from_url_invalid_github(self):
        with pytest.raises(ValueError, match="Not a GitHub URL"):
            GitHubFileRef.from_url("https://gitlab.com/owner/repo/blob/main/file.py")

    def test_from_url_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid GitHub URL"):
            GitHubFileRef.from_url("https://github.com/owner/repo")

    def test_from_url_invalid_ref_type(self):
        with pytest.raises(ValueError, match="Invalid reference type"):
            GitHubFileRef.from_url("https://github.com/owner/repo/invalid/main/file.py")

    def test_clone_url(self):
        ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="README.md",
            ref_type="blob",
        )
        assert ref.clone_url == "https://github.com/PrefectHQ/prefect.git"

    def test_directory(self):
        ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="src/prefect/cli/root.py",
            ref_type="blob",
        )
        assert ref.directory == "src/prefect/cli"

    def test_api_url(self):
        ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="README.md",
            ref_type="blob",
        )
        assert (
            ref.api_url
            == "https://api.github.com/repos/PrefectHQ/prefect/contents/README.md?ref=main"
        )

    def test_str_representation(self):
        ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="README.md",
            ref_type="blob",
        )
        assert str(ref) == "github.com/PrefectHQ/prefect @ main - README.md"

    def test_from_url_without_protocol(self):
        url = "github.com/PrefectHQ/prefect/blob/main/README.md"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"
        assert ref.filepath == "README.md"
        assert ref.ref_type == "blob"

    def test_from_url_with_http(self):
        url = "http://github.com/PrefectHQ/prefect/blob/main/README.md"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"
        assert ref.filepath == "README.md"
        assert ref.ref_type == "blob"

    def test_from_url_requires_github_domain(self):
        with pytest.raises(ValueError, match="Must include 'github.com' in the URL"):
            GitHubFileRef.from_url("PrefectHQ/prefect/blob/main/README.md")

    def test_from_url_with_multiple_path_segments(self):
        url = "github.com/PrefectHQ/prefect/blob/main/src/prefect/cli/root.py"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"
        assert ref.filepath == "src/prefect/cli/root.py"
        assert ref.ref_type == "blob"

    def test_from_url_with_commit_sha(self):
        url = "github.com/PrefectHQ/prefect/blob/a1b2c3d4e5f6/src/prefect/cli/root.py"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "a1b2c3d4e5f6"
        assert ref.filepath == "src/prefect/cli/root.py"
        assert ref.ref_type == "blob"

    def test_from_url_with_short_commit_sha(self):
        url = "github.com/PrefectHQ/prefect/blob/a1b2c3d/README.md"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "a1b2c3d"
        assert ref.filepath == "README.md"
        assert ref.ref_type == "blob"

    def test_from_url_without_ref(self):
        url = "github.com/PrefectHQ/prefect/blob/README.md"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"  # Uses default
        assert ref.filepath == "README.md"
        assert ref.ref_type == "blob"

    def test_from_url_with_custom_default_ref(self):
        url = "github.com/PrefectHQ/prefect/blob/README.md"
        ref = GitHubFileRef.from_url(url, default_ref="develop")

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "develop"  # Uses provided default
        assert ref.filepath == "README.md"
        assert ref.ref_type == "blob"

    def test_from_url_with_tree_without_ref(self):
        url = "github.com/PrefectHQ/prefect/tree/src/prefect"
        ref = GitHubFileRef.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"  # Uses default
        assert ref.filepath == "src/prefect"
        assert ref.ref_type == "tree"


class TestGitHubContent:
    @pytest.mark.asyncio
    async def test_get_github_raw_content(self, respx_mock):
        github_ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="README.md",
            ref_type="blob",
        )

        expected_content = "# Test Content"
        respx_mock.get(github_ref.api_url).mock(
            return_value=Response(status_code=200, text=expected_content)
        )

        content = await get_github_raw_content(github_ref)
        assert content == expected_content

    @pytest.mark.asyncio
    async def test_get_github_raw_content_with_credentials(self, respx_mock):
        github_ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="README.md",
            ref_type="blob",
        )

        test_token = "test-token"
        expected_content = "# Test Content"

        mock = respx_mock.get(github_ref.api_url).mock(
            return_value=Response(status_code=200, text=expected_content)
        )

        content = await get_github_raw_content(github_ref, credentials=test_token)
        assert content == expected_content

        # Verify authorization header was sent
        assert mock.calls[0].request.headers["Authorization"] == f"Bearer {test_token}"
        assert (
            mock.calls[0].request.headers["Accept"] == "application/vnd.github.v3.raw"
        )

    @pytest.mark.asyncio
    async def test_get_github_raw_content_file_not_found(self, respx_mock):
        github_ref = GitHubFileRef(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
            filepath="NONEXISTENT.md",
            ref_type="blob",
        )

        respx_mock.get(github_ref.api_url).mock(return_value=Response(status_code=404))

        with pytest.raises(FileNotFound):
            await get_github_raw_content(github_ref)
