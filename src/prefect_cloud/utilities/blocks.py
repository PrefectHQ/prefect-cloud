def safe_block_name(name: str) -> str:
    """Sanitize a block name to conform to Prefect Cloud's naming requirements.

    Block names must only contain lowercase letters, numbers, and dashes.

    Args:
        name: The name to sanitize

    Returns:
        A sanitized name containing only lowercase letters, numbers, and dashes
    """
    # Replace any non-alphanumeric chars with dashes and ensure lowercase
    sanitized = "".join(c if c.isalnum() else "-" for c in name.lower())
    # Remove consecutive dashes and strip dashes from ends
    return "-".join(filter(None, sanitized.split("-")))
