import uuid
from prefect_cloud.utilities.urls import extract_account_id, extract_workspace_id


def test_extract_account_id():
    """Test extracting account IDs from strings."""
    # Valid account IDs
    test_uuid = uuid.uuid4()
    test_strings = [
        f"accounts/{test_uuid}",
        f"https://example.com/accounts/{test_uuid}/details",
        f"User belongs to accounts/{test_uuid} and other groups",
        f"accounts/{test_uuid}/users/12345",
    ]

    for test_str in test_strings:
        result = extract_account_id(test_str)
        assert result == test_uuid

    # Multiple account IDs
    uuid1 = uuid.uuid4()
    uuid2 = uuid.uuid4()
    test_str = f"Found multiple accounts: accounts/{uuid1} and accounts/{uuid2}"
    result = extract_account_id(test_str)
    assert result == uuid1

    # No account ID
    test_strings = [
        "No account ID here",
        "account/123e4567-e89b-12d3-a456-426614174000",  # missing 's' in accounts
        "accounts/invalid-uuid",
        "",  # empty string
    ]

    for test_str in test_strings:
        assert extract_account_id(test_str) is None


def test_extract_workspace_id():
    """Test extracting workspace IDs from strings."""
    # Valid workspace IDs
    test_uuid = uuid.uuid4()
    test_strings = [
        f"workspaces/{test_uuid}",
        f"https://example.com/workspaces/{test_uuid}/projects",
        f"User belongs to workspaces/{test_uuid} and other workspaces",
        f"workspaces/{test_uuid}/tasks/54321",
    ]

    for test_str in test_strings:
        result = extract_workspace_id(test_str)
        assert result == test_uuid

    # Multiple workspace IDs
    uuid1 = uuid.uuid4()
    uuid2 = uuid.uuid4()
    test_str = f"Found multiple workspaces: workspaces/{uuid1} and workspaces/{uuid2}"
    result = extract_workspace_id(test_str)
    assert result == uuid1

    # No workspace ID
    test_strings = [
        "No workspace ID here",
        "workspace/123e4567-e89b-12d3-a456-426614174000",  # missing 's' in workspaces
        "workspaces/invalid-uuid",
        "",  # empty string
    ]

    for test_str in test_strings:
        assert extract_workspace_id(test_str) is None

    # Mixed account and workspace
    account_uuid = uuid.uuid4()
    workspace_uuid = uuid.uuid4()
    test_str = f"accounts/{account_uuid}/workspaces/{workspace_uuid}"

    result = extract_workspace_id(test_str)
    assert result == workspace_uuid

    result = extract_account_id(test_str)
    assert result == account_uuid
