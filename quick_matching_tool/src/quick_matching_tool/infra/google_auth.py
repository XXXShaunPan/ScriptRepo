"""Load Google API credentials from external config."""

from __future__ import annotations

import json
import os
import random
from pathlib import Path
from typing import Dict

from quick_matching_tool.config.settings import credentials_path


class CredentialsError(RuntimeError):
    pass


def _load_credentials_file(path: Path) -> Dict[str, dict]:
    if not path.exists():
        raise CredentialsError(
            f"未找到凭证文件: {path}. 请复制 credentials.template.json 为 credentials.json 并填入密钥。"
        )
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    clients = payload.get("clients")
    if not isinstance(clients, dict) or not clients:
        raise CredentialsError("凭证文件格式错误，缺少 clients 字段")
    return clients


def load_client_dict() -> Dict[str, dict]:
    override = os.environ.get("QUICK_MATCHING_CREDENTIALS_PATH")
    path = Path(override) if override else credentials_path()
    return _load_credentials_file(path)


def pick_client(client: str | None) -> dict:
    client_dict = load_client_dict()
    keys = list(client_dict.keys())
    if not keys:
        raise CredentialsError("凭证文件中没有可用账号")
    if client == "random" or not client:
        return client_dict[random.choice(keys)]
    if client not in client_dict:
        raise CredentialsError(f"未找到 client: {client}")
    return client_dict[client]
