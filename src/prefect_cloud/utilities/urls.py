import re
from uuid import UUID

UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

ACCOUNTS_PREFIX = "accounts/"
ACCOUNT_ID_REGEX = f"{ACCOUNTS_PREFIX}{UUID_REGEX}"

WORKSPACES_PREFIX = "workspaces/"
WORKSPACE_ID_REGEX = f"{WORKSPACES_PREFIX}{UUID_REGEX}"


def convert_str_to_uuid(s: str) -> UUID | None:
    try:
        return UUID(s)
    except ValueError:
        return None


def extract_account_id(s: str) -> UUID | None:
    if res := re.search(ACCOUNT_ID_REGEX, s):
        return convert_str_to_uuid(res.group().removeprefix(ACCOUNTS_PREFIX))
    return None


def extract_workspace_id(s: str) -> UUID | None:
    if res := re.search(WORKSPACE_ID_REGEX, s):
        return convert_str_to_uuid(res.group().removeprefix(WORKSPACES_PREFIX))
    return None
