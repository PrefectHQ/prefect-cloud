from prefect_cloud.utilities.blocks import safe_block_name


def test_safe_block_name():
    # Test basic lowercase conversion
    assert safe_block_name("MyBlock") == "myblock"

    # Test special characters replaced with dashes
    assert safe_block_name("my@block!name") == "my-block-name"

    # Test multiple consecutive special chars become single dash
    assert safe_block_name("my!!block##name") == "my-block-name"

    # Test handling of existing dashes
    assert safe_block_name("my-block--name") == "my-block-name"

    # Test stripping dashes from ends
    assert safe_block_name("-my-block-") == "my-block"

    # Test real world GitHub examples
    assert (
        safe_block_name("ExampleOwner/example-repo-credentials")
        == "exampleowner-example-repo-credentials"
    )
    assert (
        safe_block_name("User123/My-Repo!!-credentials")
        == "user123-my-repo-credentials"
    )

    # Test empty segments are removed
    assert safe_block_name("my---block") == "my-block"

    # Test spaces handled correctly
    assert safe_block_name("my block name") == "my-block-name"

    # Test mixed case with special chars
    assert safe_block_name("My!!BLOCK@@name") == "my-block-name"
