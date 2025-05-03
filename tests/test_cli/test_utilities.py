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


def test_process_key_value_pairs_strips_whitespace_and_quotes():
    """Test that surrounding whitespace and quotes are stripped from values."""
    input_pairs = [
        "key1=  value1  ",  # Whitespace only
        'key2="value2"',  # Double quotes
        "key3='value3'",  # Single quotes
        'key4=  "value4"  ',  # Whitespace and double quotes
        "key5=  'value5'  ",  # Whitespace and single quotes
        "key6={block-slug}",  # Braces (should not be stripped)
        'key7="{quoted-block}"',  # Braces within quotes
        'key8=  "  value8  "  ',  # Internal whitespace should remain
    ]
    expected = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3",
        "key4": "value4",
        "key5": "value5",
        "key6": "{block-slug}",
        "key7": "{quoted-block}",
        "key8": "  value8  ",  # Internal whitespace remains
    }
    assert process_key_value_pairs(input_pairs) == expected

    # Test with as_json=True as well, ensuring stripping happens before JSON parsing attempt
    input_pairs_json = [
        'num= "42" ',  # Quoted number
        'bool=  "true" ',  # Quoted boolean
        'str="string"',  # Single-quoted string containing double quotes (value becomes "string")
    ]
    expected_json = {
        "num": 42,
        "bool": True,
        "str": "string",  # JSON parser handles internal quotes
    }
    assert process_key_value_pairs(input_pairs_json, as_json=True) == expected_json
