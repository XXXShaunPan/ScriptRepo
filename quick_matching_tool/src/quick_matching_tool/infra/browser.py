"""Browser session management."""

from __future__ import annotations

import logging
import os
import platform
from dataclasses import dataclass

from DrissionPage import Chromium, ChromiumOptions


@dataclass
class BrowserSession:
    user_data_path: str
    browser: Chromium | None = None
    tab = None

    def init_browser(self) -> None:
        options = ChromiumOptions()
        system = platform.system()
        if system == "Windows" and os.getlogin() == "Administrator":
            options.set_browser_path(
                r"C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"
            )
        elif system == "Darwin":
            options.set_browser_path(
                r"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
            )
        elif system == "Linux":
            options.set_browser_path("/usr/bin/microsoft-edge")
        options.set_argument("--no-sandbox")

        self.browser = Chromium(addr_or_opts=options)
        self.tab = None

    def quit(self) -> None:
        if self.browser:
            self.browser.quit()
