"""Path helpers for local writable and cached code paths."""

from __future__ import annotations

import sys
from pathlib import Path


def get_writable_path() -> Path:
    """Get writable path for runtime data, compatible with packaged apps."""
    if getattr(sys, "frozen", False):
        app_data_dir = Path.home() / ".quick_matching_tool"
    else:
        app_data_dir = Path(
            __file__).resolve().parents[4] / "quick_matching_tool"

    try:
        app_data_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        import tempfile

        app_data_dir = Path(tempfile.gettempdir()) / "quick_matching_tool"
        app_data_dir.mkdir(parents=True, exist_ok=True)

    return app_data_dir


def get_local_code_path() -> Path:
    """Local code cache path for remote update.
    If the application is frozen, the code path is the path of the executable file.
    Otherwise, the code path is the path of the source code.
    src的上一层目录
    """
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", ""))
        code_path = base
    else:
        code_path = Path(__file__).resolve().parents[3]
    code_path.mkdir(parents=True, exist_ok=True)
    return code_path


# def get_package_current_path() -> Path:
#     return get_local_code_path() / "current"


def get_package_src_path() -> Path:
    return get_writable_path() / "src"
