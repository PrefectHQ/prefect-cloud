import base64
import json
import zlib
from pathlib import Path

from prefect_cloud.utilities.flows import add_flow_decorator


def package_files(
    entrypoint_file: str | Path,
    flow_func: str,
    include_paths: list[str | Path] | None = None,
) -> str:
    files: dict[str, str] = {}
    root_dir = Path.cwd().resolve()
    entrypoint = Path(entrypoint_file).resolve()

    if not entrypoint.is_file():
        raise ValueError(f"Entrypoint file not found: {entrypoint}")

    content = entrypoint.read_text()
    # TODO: move this somewhere else
    content = add_flow_decorator(content, flow_func)
    files[entrypoint.name] = content

    if include_paths:
        for path in include_paths:
            path = Path(path).resolve()
            if not path.exists():
                raise ValueError(f"Included path does not exist: {path}")

            if path.is_file():
                if path.suffix == ".py":
                    content = path.read_text()
                    rel_path = path.relative_to(root_dir)
                    files[str(rel_path)] = content
            elif path.is_dir():
                for py_file in path.rglob("*.py"):
                    if py_file.is_file():
                        content = py_file.read_text()
                        rel_path = py_file.relative_to(root_dir)
                        files[str(rel_path)] = content

    json_str = json.dumps(files)
    compressed = zlib.compress(json_str.encode())
    return base64.b64encode(compressed).decode("utf-8")


def unpack_files(encoded_str: str) -> None:
    compressed = base64.b64decode(encoded_str.encode("utf-8"))
    json_bytes = zlib.decompress(compressed)

    json_str = json_bytes.decode("utf-8")

    files = json.loads(json_str)

    for file_path, content in files.items():
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        print(f"Wrote {path}")
