"""Logging setup for the app."""

from __future__ import annotations

import logging
import sys
from typing import Optional


def setup_logging(level: int = logging.INFO,
                  handler: Optional[logging.Handler] = None) -> None:
    formatter = logging.Formatter(
        "%(asctime)s - %(lineno)d - %(filename)s - %(levelname)s - %(message)s"
    )
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]
