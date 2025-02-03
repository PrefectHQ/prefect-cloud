import pytest
from pytest_asyncio import is_async_test

def pytest_collection_modifyitems(items: list[pytest.Item]):
    # Ensure that all async tests are run with the session loop scope
    pytest_asyncio_tests = [item for item in items if is_async_test(item)]
    session_scope_marker = pytest.mark.asyncio(loop_scope="session")
    for async_test in pytest_asyncio_tests:
        async_test.add_marker(session_scope_marker, append=False)
