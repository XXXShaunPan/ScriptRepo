#!/usr/bin/env python3
"""发布脚本：打包源码、生成 manifest.json，用于同步代码功能。

用法:
    python scripts/publish.py                    # 使用 settings.CURRENT_VERSION
    python scripts/publish.py 1.0.6             # 指定版本号
    python scripts/publish.py 1.0.6 --push      # 生成后执行 git push（需在 ScriptRepo 内运行）

产物:
    - dist/package_<version>.zip
    - dist/manifest.json

后续步骤:
    1. 将 package_<version>.zip 和 manifest.json 放入 ScriptRepo 的 quick_matching_tool/ 目录
    2. 或上传 zip 到 Release/对象存储，并在 manifest.json 中填对应 url
    3. push 到 GitHub
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# 项目根目录（quick_matching_tool/）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
# 需要排除的目录/文件模式
EXCLUDE = {
    "__pycache__",
    ".git",
    ".gitignore",
    "dist",
    "build",
    "upload_files",
    ".DS_Store",
    "*.pyc",
    ".pyc",
    "scripts",
}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_files_simple() -> list[tuple[Path, str]]:
    """收集需打包的路径，返回 (绝对路径, 归档内相对路径)。"""
    result = []
    # src/ 必须包含
    src_dir = PROJECT_ROOT / "src"
    if not src_dir.exists():
        raise SystemExit("未找到 src 目录")
    for f in src_dir.rglob("*"):
        if not f.is_file():
            continue
        if "__pycache__" in f.parts or f.name.endswith(
                ".pyc") or f.name == ".DS_Store":
            continue
        arc = f.relative_to(PROJECT_ROOT)
        result.append((f, str(arc)))
    # 可选：顶层配置文件
    # for name in ("pyproject.toml", "requirements.txt"):
    #     p = PROJECT_ROOT / name
    #     if p.exists():
    #         result.append((p, name))
    return result


def create_package(version: str) -> Path:
    """打包并返回 zip 路径。"""
    import zipfile

    out_dir = PROJECT_ROOT / "dist"
    out_dir.mkdir(exist_ok=True)
    zip_path = out_dir / f"package_{version}.zip"

    if zip_path.exists():
        zip_path.unlink()

    items = collect_files_simple()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for abs_path, arc_name in items:
            zf.write(abs_path, arc_name)
            logger.info("  + %s", arc_name)

    logger.info("已生成: %s", zip_path)
    return zip_path


def generate_manifest(version: str, zip_path: Path,
                      package_url: str | None) -> Path:
    """计算 sha256 并生成 manifest.json。"""
    sha = _sha256(zip_path)
    if not package_url:
        # 默认 raw.githubusercontent.com 路径（需将 zip 推送到 ScriptRepo）

        package_url = (
            "https://raw.githubusercontent.com/XXXShaunPan/ScriptRepo/main"
            f"/quick_matching_tool/dist/package_{version}.zip")
    manifest = {
        "version": version,
        "url": package_url,
        "sha256": sha,
    }
    out_path = PROJECT_ROOT / "dist" / "manifest.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info("已生成 manifest.json (sha256: %s...)", sha[:16])
    return out_path


def do_push(manifest_path: Path, zip_path: Path) -> None:
    """在 ScriptRepo 目录内执行 git add/commit/push。"""
    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            check=True,
            capture_output=True,
            cwd=PROJECT_ROOT,
        )
    except subprocess.CalledProcessError:
        logger.warning("当前目录不是 git 仓库，跳过 push")
        return
    subprocess.run(
        ["git", "add", str(manifest_path),
         str(zip_path)],
        check=True,
        cwd=PROJECT_ROOT,
    )
    subprocess.run(
        ["git", "commit", "-m", "chore: update manifest and package"],
        check=True,
        cwd=PROJECT_ROOT,
    )
    subprocess.run(["git", "push"], check=True, cwd=PROJECT_ROOT)
    logger.info("已 push 到远程")


def main() -> None:
    parser = argparse.ArgumentParser(description="发布 Quick Matching Tool 同步包")
    parser.add_argument(
        "version",
        nargs="?",
        default=None,
        help="版本号，不填则从 config.settings 读取",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="生成后执行 git add/commit/push",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="zip 包的完整下载 URL（不填则用默认 raw GitHub 路径）",
    )
    args = parser.parse_args()

    if args.version:
        version = args.version
        settings_path = PROJECT_ROOT / "src" / "quick_matching_tool" / "config" / "settings.py"
        import re

        with open(settings_path, "r", encoding="utf-8") as f:
            settings_code = f.read()
        # 替换 CURRENT_VERSION 行
        pattern = r'^CURRENT_VERSION\s*=\s*["\']([^"\']*)["\']'
        replacement = f'CURRENT_VERSION = "{version}"'
        new_settings_code = re.sub(
            pattern,
            replacement,
            settings_code,
            flags=re.MULTILINE,
        )
        if new_settings_code != settings_code:
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write(new_settings_code)
        else:
            logger.warning("未能在 settings.py 中自动替换版本号，请手动确认。")
    else:
        # 未传入 version 参数，读取当前版本号
        sys.path.insert(0, str(PROJECT_ROOT / "src"))
        from quick_matching_tool.config.settings import CURRENT_VERSION
        version = CURRENT_VERSION

    logger.info("版本: %s", version)
    zip_path = create_package(version)
    manifest_path = generate_manifest(version, zip_path, args.url)

    if args.push:
        print("pushing to github...")
        # do_push(manifest_path, zip_path)

    logger.info(
        "完成。将 manifest.json 和 %s 放入 ScriptRepo/quick_matching_tool/ 后 push。",
        zip_path.name)


if __name__ == "__main__":
    main()
