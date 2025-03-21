import sys

import readchar
from rich.console import Console
from rich.live import Live
from rich.table import Table


def redacted(value: str) -> str:
    if len(value) <= 12:
        return "*" * len(value)
    return f"{value[:4]}{'*' * (len(value) - 8)}{value[-4:]}"


def prompt_select_from_list(prompt: str, options: list[str]) -> str:
    """
    Given a list of options, display up to 10 values to user in a table and prompt them
    to select one, with scrolling support for longer lists. After selection, collapses
    to show only the header and selected option.

    Args:
        prompt: The prompt text to display to the user
        options: A list of options to present to the user.
            A list of tuples can be passed as key value pairs. If a value is chosen, the
            key will be returned.

    Returns:
        str: the selected option
    """

    console = Console()

    current_idx = 0
    selected_option = None
    page_size = 10
    scroll_offset = 0

    def build_table(collapse: bool = False) -> Table:
        """
        Generate a table of options. The `current_idx` will be highlighted,
        showing only page_size items at a time with scrolling indicators.

        Args:
            collapse: If True, only show the header and selected option
        """
        nonlocal scroll_offset

        if current_idx >= scroll_offset + page_size:
            scroll_offset = current_idx - page_size + 1
        elif current_idx < scroll_offset:
            scroll_offset = current_idx

        visible_range_end = min(scroll_offset + page_size, len(options))
        visible_options = options[scroll_offset:visible_range_end]

        table = Table(box=None, header_style=None, padding=(0, 0))

        # For final display, remove the navigation instructions
        if collapse:
            table.add_column(
                f"? {prompt}",
                justify="left",
                no_wrap=True,
            )
        else:
            table.add_column(
                f"? {prompt} [bright_blue][Use arrows to move; enter to select;]",
                justify="left",
                no_wrap=True,
            )

        if collapse:
            # Only show the selected option
            selected_display = options[current_idx]
            if isinstance(selected_display, tuple):
                selected_display = selected_display[1]
            table.add_row("[bold][blue]❯ " + selected_display)
        else:
            # Show all visible options
            for i, option in enumerate(visible_options, start=scroll_offset):
                if isinstance(option, tuple):
                    display_option = option[1]
                else:
                    display_option = option

                if i == current_idx:
                    table.add_row("[bold][blue]❯ " + display_option)
                else:
                    table.add_row("  " + display_option)

        return table

    from prefect_cloud.cli.root import app

    with app.suppress_progress():
        with Live(build_table(), auto_refresh=False, console=console) as live:
            while selected_option is None:
                key = readchar.readkey()

                if key == readchar.key.UP:
                    current_idx = (current_idx - 1) % len(options)
                elif key == readchar.key.DOWN:
                    current_idx = (current_idx + 1) % len(options)
                elif key == readchar.key.PAGE_UP:
                    current_idx = max(0, current_idx - page_size)
                elif key == readchar.key.PAGE_DOWN:
                    current_idx = min(len(options) - 1, current_idx + page_size)
                elif key == readchar.key.HOME:
                    current_idx = 0
                elif key == readchar.key.END:
                    current_idx = len(options) - 1
                elif key == readchar.key.CTRL_C:
                    sys.exit(1)
                elif key == readchar.key.ENTER or key == readchar.key.CR:
                    selected_option = options[current_idx]
                    if isinstance(selected_option, tuple):
                        selected_option = selected_option[0]
                    # Update the live display with the collapsed view before exiting
                    live.update(build_table(collapse=True), refresh=True)
                    break

                if selected_option is None:
                    live.update(build_table(), refresh=True)

    return selected_option
