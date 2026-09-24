"""Start aiida-mcp's private worker in the selected project Python."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> None:
    source_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(source_root))
    sys.path.insert(1, str(Path.cwd()))
    runpy.run_module("aiida_mcp.worker", run_name="__main__")


if __name__ == "__main__":
    main()
