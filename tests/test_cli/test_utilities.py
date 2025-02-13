import pytest
from prefect_cloud.cli.utilities import process_key_value_pairs
import typer


def test_process_key_value_pairs():
    # Test basic key-value pairs
    input_list = ["key1=value1", "key2=value2"]
    expected = {"key1": "value1", "key2": "value2"}
    assert process_key_value_pairs(input_list) == expected

    # Test empty list
    assert process_key_value_pairs([]) == {}
    assert process_key_value_pairs(None) == {}

    # Test single key-value pair
    assert process_key_value_pairs(["key=value"]) == {"key": "value"}

    # Test with spaces
    input_list = ["key1=value1", "key2=value2"]
    expected = {"key1": "value1", "key2": "value2"}
    assert process_key_value_pairs(input_list) == expected

    # Test with invalid format
    with pytest.raises(typer.Exit):
        process_key_value_pairs(["invalid_format"])

    # Test with missing value
    with pytest.raises(typer.Exit):
        process_key_value_pairs(["key1=value1", "key2="])

    # Test with missing key
    with pytest.raises(typer.Exit):
        process_key_value_pairs(["=value"])
