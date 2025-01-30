import asyncio
import base64
import json
import sys
import zlib
from pathlib import Path

from prefect import get_client


async def main():
    if len(sys.argv) != 2:
        print("Usage: retrieve_code.py <storage_id>")
        sys.exit(1)

    storage_id = sys.argv[1]

    try:
        async with get_client() as client:
            response = await client._client.get(f"/mex/storage/{storage_id}")
            encoded_str = response.json()["data"]
    except Exception:
        print(f"Error retrieving code from storage id: {storage_id}")
        raise

    compressed = base64.b64decode(encoded_str.encode("utf-8"))
    json_bytes = zlib.decompress(compressed)
    json_str = json_bytes.decode("utf-8")
    files = json.loads(json_str)

    for file_path, content in files.items():
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        print(f"Wrote {path}")

    print(f"Successfully wrote code from storage id: {storage_id}")


if __name__ == "__main__":
    asyncio.run(main())
