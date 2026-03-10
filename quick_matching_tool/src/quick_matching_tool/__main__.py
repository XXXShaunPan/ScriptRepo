"""Module entrypoint for Quick Matching Tool."""

from __future__ import annotations

import sys
from pathlib import Path


def _prepend_meipass_src() -> None:
    base = Path(getattr(sys, "_MEIPASS", ""))
    if not base:
        return
    meipass_src = base / "src"
    if meipass_src.exists() and str(meipass_src) not in sys.path:
        sys.path.insert(0, str(meipass_src))


_prepend_meipass_src()

from quick_matching_tool.ui.app import main  # noqa: E402

if __name__ == "__main__":
    main()
