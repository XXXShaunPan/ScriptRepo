"""Remote package update utilities."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

from quick_matching_tool.utils.paths import get_local_code_path
from quick_matching_tool.config.settings import REMOTE_PACKAGE_MANIFEST_URL


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_file(url: str, target: Path) -> None:
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)


def _read_manifest(manifest_url: str) -> Dict[str, str]:
    response = requests.get(manifest_url, timeout=30)
    return json.loads(response.text)


def check_code_sync_available(
    manifest_url: str, current_version: str
) -> Tuple[bool, str, str]:
    """检查远程是否有新版本可同步。返回 (has_update, remote_version, error_msg)。"""
    try:
        from packaging import version as pkg_version

        manifest = _read_manifest(manifest_url)
        remote_version = manifest.get("version", "")
        if not remote_version:
            return False, "", "manifest 缺少 version"
        if pkg_version.parse(remote_version) > pkg_version.parse(current_version):
            return True, remote_version, ""
        return False, remote_version, ""
    except Exception as exc:
        logging.debug("检查代码同步失败: %s", exc)
        return False, "", str(exc)


def _find_package_root(extracted_dir: Path) -> Optional[Path]:
    direct = extracted_dir / "src" / "quick_matching_tool"
    if direct.exists():
        return extracted_dir
    children = [p for p in extracted_dir.iterdir() if p.is_dir()]
    for child in children:
        if (child / "src" / "quick_matching_tool").exists():
            return child
    return None


def _on_rm_error(func, path, _exc_info) -> None:
    try:
        Path(path).chmod(0o700)
        func(path)
    except Exception:
        return


def _replace_dir(src_dir: Path, dst_dir: Path) -> None:
    if not src_dir.exists():
        raise FileNotFoundError(f"源目录不存在: {src_dir}")

    backup_dir = dst_dir.with_name(dst_dir.name + "_backup")
    if backup_dir.exists():
        shutil.rmtree(backup_dir, onerror=_on_rm_error)

    if dst_dir.exists():
        dst_dir.rename(backup_dir)

    try:
        shutil.copytree(src_dir, dst_dir)
    except Exception:
        if dst_dir.exists():
            shutil.rmtree(dst_dir, onerror=_on_rm_error)
        if backup_dir.exists():
            backup_dir.rename(dst_dir)
        raise
    else:
        if backup_dir.exists():
            shutil.rmtree(backup_dir, onerror=_on_rm_error)


def _restart_app() -> None:
    exe = sys.executable
    args = [exe] + sys.argv
    try:
        subprocess.Popen(args, cwd=os.getcwd())
    except Exception:
        os.execv(exe, args)
    finally:
        os._exit(0)


def sync_package(manifest_url: str) -> Tuple[bool, str]:
    try:
        manifest = _read_manifest(manifest_url)
        pkg_url = manifest.get("url", "")
        version = manifest.get("version", "")
        sha256 = manifest.get("sha256", "")
        if not pkg_url or not version or not sha256:
            return False, "manifest 内容不完整"

        code_root = get_local_code_path()
        tmp_zip = code_root / f"package_{version}.zip"
        _download_file(pkg_url, tmp_zip)
        if _sha256(tmp_zip) != sha256:
            tmp_zip.unlink(missing_ok=True)
            return False, "包校验失败"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            with zipfile.ZipFile(tmp_zip, "r") as zf:
                zf.extractall(temp_path)
            package_root = _find_package_root(temp_path)
            if not package_root:
                return False, "包结构不符合预期"

            current_path = get_local_code_path()
            target_src = current_path / "src"

            new_src = package_root / "src"
            _replace_dir(new_src, target_src)

        tmp_zip.unlink(missing_ok=True)
        logging.info("包同步完成，版本: %s", version)
        _restart_app()
        return True, f"已更新到版本 {version}"
    except Exception as exc:
        logging.error("包同步失败: %s", exc)
        return False, f"同步失败: {exc}"
