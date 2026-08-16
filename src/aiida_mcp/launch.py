"""Development launcher that discovers the sibling aiida-worker checkout."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    worker_source = Path(__file__).resolve().parents[3] / "aiida-worker"
    os.environ.setdefault("AIIDA_MCP_WORKER_SOURCE", str(worker_source))
    from .server import main as serve

    serve()


if __name__ == "__main__":
    main()
