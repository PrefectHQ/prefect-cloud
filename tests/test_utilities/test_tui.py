import pytest
from unittest.mock import patch

from prefect_cloud.utilities.tui import prompt_select_from_list, redacted
from readchar import key


def test_redacted_short_string():
    """Test redaction of short strings (<=12 chars) are fully masked"""
    assert redacted("short") == "*****"
    assert redacted("12345678") == "********"
    assert redacted("123456789012") == "************"


def test_redacted_long_string():
    """Test redaction of long strings (>12 chars) shows first/last 4 chars"""
    assert redacted("1234567890123") == "1234*****0123"
    assert redacted("abcdefghijklmnop") == "abcd********mnop"


@pytest.mark.parametrize(
    "key_sequence,expected_selection",
    [
        # Simple enter key selects first option
        ([key.ENTER], "option1"),
        # Down arrow + enter selects second option
        ([key.DOWN, key.ENTER], "option2"),
        # Up arrow wraps to last option
        ([key.UP, key.ENTER], "option3"),
        # Multiple downs + enter
        ([key.DOWN, key.DOWN, key.ENTER], "option3"),
        # Down arrow wraps to first option
        ([key.DOWN, key.DOWN, key.DOWN, key.ENTER], "option1"),
        # Complex navigation
        ([key.DOWN, key.UP, key.DOWN, key.DOWN, key.UP, key.ENTER], "option2"),
    ],
)
def test_prompt_select_from_list(key_sequence, expected_selection):
    """Test navigation and selection in prompt_select_from_list"""
    options = ["option1", "option2", "option3"]

    with patch("prefect_cloud.utilities.tui.readchar.readkey") as mock_readkey:
        # Configure mock to return our sequence of keys
        mock_readkey.side_effect = key_sequence

        # Mock the Live context manager and console to prevent actual rendering
        with patch("prefect_cloud.utilities.tui.Live"):
            with patch("prefect_cloud.utilities.tui.Console"):
                result = prompt_select_from_list("Select an option:", options)

                assert result == expected_selection
                # Verify we consumed exactly the number of keys we expected
                assert mock_readkey.call_count == len(key_sequence)


def test_prompt_select_from_list_with_tuples():
    """Test that prompt_select_from_list handles tuple options correctly"""
    options = [
        ("key1", "First Option"),
        ("key2", "Second Option"),
        ("key3", "Third Option"),
    ]

    with patch("prefect_cloud.utilities.tui.readchar.readkey") as mock_readkey:
        mock_readkey.side_effect = [key.DOWN, key.ENTER]  # Select second option

        with patch("prefect_cloud.utilities.tui.Live"):
            with patch("prefect_cloud.utilities.tui.Console"):
                result = prompt_select_from_list("Select an option:", options)

                assert result == "key2"  # Should return the key, not the display value


def test_prompt_select_from_list_ctrl_c():
    """Test that Ctrl+C exits the program"""
    options = ["option1", "option2", "option3"]

    with patch("prefect_cloud.utilities.tui.readchar.readkey") as mock_readkey:
        mock_readkey.return_value = key.CTRL_C

        with patch("prefect_cloud.utilities.tui.Live"):
            with patch("prefect_cloud.utilities.tui.Console"):
                with pytest.raises(SystemExit):
                    prompt_select_from_list("Select an option:", options)


def test_prompt_select_from_list_carriage_return():
    """Test that carriage return works the same as enter key"""
    options = ["option1", "option2", "option3"]

    with patch("prefect_cloud.utilities.tui.readchar.readkey") as mock_readkey:
        mock_readkey.side_effect = [key.DOWN, key.CR]  # Down arrow + carriage return

        with patch("prefect_cloud.utilities.tui.Live"):
            with patch("prefect_cloud.utilities.tui.Console"):
                result = prompt_select_from_list("Select an option:", options)

                assert result == "option2"
