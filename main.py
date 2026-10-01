"""Entry point for the seed data generator service."""
from __future__ import annotations

import os

import uvicorn

from seedgen.api import create_app
from seedgen.config import Config


def main() -> None:
    data_dir = os.environ.get("SEEDGEN_DATA_DIR", "data")
    host = os.environ.get("SEEDGEN_HOST", "127.0.0.1")
    port = int(os.environ.get("SEEDGEN_PORT", "8000"))
    config = Config(data_dir=__import__("pathlib").Path(data_dir))
    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()
