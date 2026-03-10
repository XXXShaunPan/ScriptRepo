"""PyInstaller runtime hook for credentials path."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _maybe_set_credentials_path() -> None:
    if os.environ.get("QUICK_MATCHING_CREDENTIALS_PATH"):
        return
    base = Path(getattr(sys, "_MEIPASS", ""))
    if not base:
        return
    bundled = base / "config" / "credentials.json"
    if bundled.exists():
        os.environ["QUICK_MATCHING_CREDENTIALS_PATH"] = str(bundled)


_maybe_set_credentials_path()
