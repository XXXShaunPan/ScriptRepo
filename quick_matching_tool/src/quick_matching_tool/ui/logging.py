"""Qt log handler for UI output."""

from __future__ import annotations

import logging
from PyQt5.QtCore import QObject, pyqtSignal


class LogHandler(QObject, logging.Handler):
    new_log = pyqtSignal(str, int)

    def __init__(self):
        super().__init__()
        # self.setFormatter(
        #     logging.Formatter("%(asctime)s - %(lineno)d - %(levelname)s - %(message)s")
        # )

    def emit(self, record):
        log_message = self.format(record)
        self.new_log.emit(log_message, record.levelno)
