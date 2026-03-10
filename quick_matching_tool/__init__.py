"""Legacy package shim for src layout."""

from __future__ import annotations

import sys
from pathlib import Path


def get_local_code_path() -> Path:
    """Local code cache path for remote update."""
    if getattr(sys, "frozen", False):
        code_path = Path.home() / ".quick_matching_tool" / "src"
    else:
        code_path = Path(__file__).resolve().parent / "src"
    return code_path


def _ensure_src_on_path() -> None:
    src_path = get_local_code_path()
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


_ensure_src_on_path()

from quick_matching_tool import CURRENT_VERSION as __version__  # noqa: E402

__all__ = ["__version__"]
