import pytest
from prefect_cloud.cli.utilities import process_key_value_pairs


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
    with pytest.raises(ValueError):
        process_key_value_pairs(["invalid_format"])

    # Test with missing value
    with pytest.raises(ValueError):
        process_key_value_pairs(["key1=value1", "key2="])

    # Test with missing key
    with pytest.raises(ValueError):
        process_key_value_pairs(["=value"])


def test_process_key_value_pairs_json():
    # Test with valid JSON values
    input_list = [
        "int=42",
        "float=3.14",
        "bool=true",
        "null=null",
        'string="hello"',
        "array=[1,2,3]",
        'object={"key":"value"}',
    ]
    expected = {
        "int": 42,
        "float": 3.14,
        "bool": True,
        "null": None,
        "string": "hello",
        "array": [1, 2, 3],
        "object": {"key": "value"},
    }
    assert process_key_value_pairs(input_list, as_json=True) == expected

    # Test mixing JSON and non-JSON values (non-JSON should remain as strings)
    input_list = ["json_num=42", "regular=not_json", "json_array=[1,2,3]"]
    expected = {"json_num": 42, "regular": "not_json", "json_array": [1, 2, 3]}
    assert process_key_value_pairs(input_list, as_json=True) == expected

    # Test invalid JSON should be treated as strings
    input_list = ["invalid_array=[1,2,", "invalid_object={key:value}", "normal=string"]
    expected = {
        "invalid_array": "[1,2,",
        "invalid_object": "{key:value}",
        "normal": "string",
    }
    assert process_key_value_pairs(input_list, as_json=True) == expected

    # Test empty values with as_json
    assert process_key_value_pairs([], as_json=True) == {}
    assert process_key_value_pairs(None, as_json=True) == {}
