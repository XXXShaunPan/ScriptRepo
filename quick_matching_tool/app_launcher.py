"""Legacy launcher wrapper for the new src layout."""

from __future__ import annotations

import sys
from pathlib import Path


def get_local_code_path() -> Path:
    """Local code cache path for remote update."""

    code_path = Path(__file__).resolve().parent / "src"
    return code_path


def _ensure_src_on_path() -> None:
    src_path = get_local_code_path()
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


def main() -> None:
    _ensure_src_on_path()
    from quick_matching_tool.ui.app import main as ui_main

    ui_main()


if __name__ == "__main__":
    main()
