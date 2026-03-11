"""UI background threads."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from typing import List, Optional, Tuple

import requests
from packaging import version
from PyQt5.QtCore import QThread, pyqtSignal

from quick_matching_tool.config.settings import (
    DRIVE_FOLDER_PARENT_ID,
    ENABLE_REMOTE_CODE,
    REMOTE_PACKAGE_MANIFEST_URL,
    VERSION_CHECK_URL,
)
from quick_matching_tool.core.matching_service import QuickMatchingService
from quick_matching_tool.infra.google_sheets import gsheet_batch_update, open_gsheet_by_url, read_gsheet
from quick_matching_tool.infra.remote_update import check_code_sync_available, sync_package
from quick_matching_tool.infra.google_drive import create_folder


class CodeSyncCheckThread(QThread):
    """启动时检查是否有新版本可同步（仅拉 manifest 比对版本）。"""
    check_finished = pyqtSignal(bool, str, str)

    def __init__(self, manifest_url: str, current_version: str):
        super().__init__()
        self.manifest_url = manifest_url
        self.current_version = current_version

    def run(self):
        if not ENABLE_REMOTE_CODE:
            self.check_finished.emit(False, "", "远程代码功能未启用")
            return
        has_update, remote_version, err = check_code_sync_available(
            self.manifest_url, self.current_version
        )
        self.check_finished.emit(has_update, remote_version, err)


class CodeSyncThread(QThread):
    sync_finished = pyqtSignal(bool, str)

    def run(self):
        try:
            if not ENABLE_REMOTE_CODE:
                self.sync_finished.emit(False, "远程代码功能未启用")
                return

            success, message = sync_package(REMOTE_PACKAGE_MANIFEST_URL)
            self.sync_finished.emit(success, message)
        except Exception as exc:
            logging.error("同步代码失败: %s", exc)
            self.sync_finished.emit(False, f"同步失败: {exc}")


class UpdateCheckThread(QThread):
    check_finished = pyqtSignal(bool, str, str, str)

    def __init__(self, version_url: str, current_version: str):
        super().__init__()
        self.version_url = version_url
        self.current_version = current_version

    def run(self):
        try:
            result = self.check_update_from_gsheet()
            if result:
                has_update, latest_version, download_url, release_notes = result
                self.check_finished.emit(has_update, latest_version,
                                         download_url, release_notes)
            else:
                self.check_finished.emit(False, self.current_version, "", "")
        except Exception as exc:
            logging.error("检查更新失败: %s", exc)
            self.check_finished.emit(False, self.current_version, "",
                                     f"检查失败: {exc}")

    def check_update_from_gsheet(self) -> Optional[Tuple[bool, str, str, str]]:
        try:
            data = read_gsheet(VERSION_CHECK_URL, "update_log", "A1:C",
                               2).to_dict(orient="records")
            latest_version = data[0]["version"]
            download_url = data[0]["download_url"]
            release_notes = data[0]["release_notes"]

            has_update = version.parse(latest_version) > version.parse(
                self.current_version)
            return has_update, latest_version, download_url, release_notes
        except Exception as exc:
            logging.error("从 Google Sheet 检查更新失败: %s", exc)
            return None

    def check_update_from_github(self) -> Optional[Tuple[bool, str, str, str]]:
        try:
            response = requests.get(self.version_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            latest_version = data.get("tag_name", "").lstrip("v")
            download_url = ""
            for asset in data.get("assets", []):
                if asset["name"].endswith((".app", ".dmg", ".zip")):
                    download_url = asset["browser_download_url"]
                    break
            release_notes = data.get("body", "")
            has_update = version.parse(latest_version) > version.parse(
                self.current_version)
            return has_update, latest_version, download_url, release_notes
        except Exception as exc:
            logging.error("从 GitHub 检查更新失败: %s", exc)
            return None


class UpdateDownloadThread(QThread):
    progress = pyqtSignal(int, int, int)
    finished = pyqtSignal(bool, str, str)

    def __init__(self, download_url: str, version_str: str):
        super().__init__()
        self.download_url = download_url
        self._cancelled = False
        self.version = version_str

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            with requests.get(self.download_url, stream=True,
                              timeout=30) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", 0))
                filename = f"QuickMatchingTool_v{self.version}.zip"
                output_path = os.path.join(tempfile.gettempdir(), filename)

                downloaded = 0
                chunk_size = 1024 * 256
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if self._cancelled:
                            self.finished.emit(False, "下载已取消", "")
                            return
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        percent = int(downloaded * 100 / total) if total else 0
                        self.progress.emit(percent, downloaded, total)
            self.finished.emit(True, "下载完成", output_path)
        except Exception as exc:
            self.finished.emit(False, f"下载失败: {exc}", "")


class ToolThread(QThread):
    finished = pyqtSignal(bool, str)

    def __init__(self, batch_nos: List[dict], clear_cache: bool = False):
        super().__init__()
        self.is_running = True
        self.batch_nos = batch_nos
        self.clear_cache = clear_cache
        self.tool = None

    def run(self):
        try:
            tool = QuickMatchingService(clear_cache=self.clear_cache)
            self.tool = tool
            self.tool.batch_run(self.batch_nos)
            if self.is_running:
                self.finished.emit(True, "")
            else:
                self.finished.emit(False, "操作被用户中断")
        except Exception as exc:
            self.finished.emit(False, str(exc))

    def stop(self):
        self.is_running = False
        if self.tool:
            self.tool.stop()


class GSheetLoadThread(QThread):
    load_finished = pyqtSignal(bool, str, list)

    def run(self):
        try:
            data = read_gsheet(VERSION_CHECK_URL, "data", "A1:D",
                               2).to_dict(orient="records")
            self.load_finished.emit(True, "批次号加载完成", data)
        except Exception as exc:
            logging.error("加载批次号失败: %s", exc)
            self.load_finished.emit(False, f"加载失败: {exc}", [])


class BatchAddThread(QThread):
    add_finished = pyqtSignal(bool, str)

    def __init__(self, batch_name: str, gsheet_url: str, pic_url: str,
                 index: int):
        super().__init__()
        self.batch_name = batch_name
        self.gsheet_url = gsheet_url
        self.pic_url = pic_url
        self.index = index

    def run(self):
        try:
            drive_folder_id = create_folder(self.batch_name,
                                            DRIVE_FOLDER_PARENT_ID)
            logging.info(f'成功创建drive文件夹: {drive_folder_id}, 准备添加批次号到gsheet')
            gsheet_batch_update(open_gsheet_by_url(VERSION_CHECK_URL, "data"),
                                [{
                                    'range':
                                    f'A{self.index}:D{self.index}',
                                    'values': [[
                                        self.pic_url, self.gsheet_url,
                                        self.batch_name, drive_folder_id
                                    ]]
                                }])
            self.add_finished.emit(True, "批次号添加完成")
        except Exception as exc:
            logging.error("添加批次号失败: %s", exc)
            self.add_finished.emit(False, f"添加失败: {exc}")
