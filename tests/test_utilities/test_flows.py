from prefect_cloud.utilities.flows import add_flow_decorator
from textwrap import dedent


class TestDecorator:
    def test_adds_flow_decorator_to_function(self):
        """Test that flow decorator is added to target function"""
        code = dedent("""
            def my_function():
                print("Hello")
        """).strip()

        expected = dedent("""
            from prefect import flow
            
            @flow(log_prints=True)
            def my_function():
                print('Hello')
        """).strip()

        result = add_flow_decorator(code, "my_function")
        assert result == expected

    def test_preserves_existing_flow_decorator(self):
        """Test that existing flow decorator is not duplicated"""
        code = dedent("""
            from prefect import flow
            
            @flow()
            def my_function():
                print('Hello')
        """).strip()

        result = add_flow_decorator(code, "my_function")
        assert result == code

    def test_preserves_existing_flow_decorator_with_params(self):
        """Test that existing flow decorator with parameters is preserved"""
        code = dedent("""
            from prefect import flow
            
            @flow(name='custom', retries=3)
            def my_function():
                print('Hello')
        """).strip()

        result = add_flow_decorator(code, "my_function")
        assert result == code

    def test_adds_import_if_missing(self):
        """Test that flow import is added if missing"""
        code = dedent("""
            def my_function():
                print('Hello')
        """).strip()

        result = add_flow_decorator(code, "my_function")
        assert "from prefect import flow" in result

    def test_preserves_existing_import(self):
        """Test that existing flow import is not duplicated"""
        code = dedent("""
            from prefect import flow
            
            def my_function():
                print('Hello')
        """)

        result = add_flow_decorator(code, "my_function")
        assert result.count("from prefect import flow") == 1

    def test_only_decorates_target_function(self):
        """Test that only the target function is decorated"""
        code = dedent("""
            def other_function():
                osprint('Other')
                
            def my_function():
                print('Hello')
                
            def another_function():
                print("Another")
        """)

        result = add_flow_decorator(code, "my_function")

        # Check that only my_function has the decorator
        assert "@flow" in result
        assert result.count("@flow") == 1
        assert "def my_function():" in result
        assert "def other_function():" in result
        assert "def another_function():" in result

    def test_preserves_other_decorators(self):
        """Test that other decorators are preserved"""
        code = dedent("""
            @other_decorator
            def my_function():
                print('Hello')
        """)

        result = add_flow_decorator(code, "my_function")
        assert "@other_decorator" in result
        assert "@flow" in result

    def test_handles_complex_function(self):
        """Test handling of complex function with docstring and type hints"""
        code = dedent("""
            def my_function(x: int, y: str = "default") -> bool:
                \"\"\"This is a docstring.\"\"\"
                print(f"{x} {y}")
                return True
        """).strip()

        expected = dedent("""
            from prefect import flow

            @flow(log_prints=True)
            def my_function(x: int, y: str='default') -> bool:
                \"\"\"This is a docstring.\"\"\"
                print(f'{x} {y}')
                return True
        """).strip()

        result = add_flow_decorator(code, "my_function")
        assert result == expected

    def test_handles_empty_file(self):
        """Test handling of empty file"""
        code = ""
        result = add_flow_decorator(code, "my_function")
        # Should add import but no function
        assert result == "from prefect import flow"
