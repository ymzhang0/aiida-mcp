"""Start the centrally maintained aiida-worker in a selected project Python."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: worker_bootstrap.py <aiida-worker-source>")
    worker_root = Path(sys.argv[1]).expanduser().resolve()
    worker_src = worker_root / "src"
    if not (worker_src / "aris_aiida_worker").is_dir():
        raise SystemExit(f"invalid bundled aiida-worker source: {worker_root}")
    sys.path.insert(0, str(worker_src))
    sys.path.insert(1, str(worker_root))
    sys.path.insert(2, str(Path.cwd()))
    runpy.run_module("aris_aiida_worker", run_name="__main__")


if __name__ == "__main__":
    main()
