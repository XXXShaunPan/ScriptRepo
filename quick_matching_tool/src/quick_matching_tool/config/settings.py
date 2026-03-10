"""Centralized settings for Quick Matching Tool."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path, PosixPath
from typing import Dict

from quick_matching_tool.utils.paths import get_writable_path

CURRENT_VERSION = "1.0.5"
VERSION_CHECK_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1fXw9OgtWwiWE90iFdRvBdjYKXT7FTAvHIQbveBnljOc/edit?gid=0#gid=0")

REMOTE_PACKAGE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/XXXShaunPan/ScriptRepo/refs/heads/main/quick_matching_tool/manifest.json"
)

ENABLE_REMOTE_CODE = True

REGION_HOSTS = {
    "MY": "https://pricing-center.shopee.com.my",
    "PH": "https://pricing-center.shopee.ph",
    "VN": "https://pricing-center.shopee.vn",
    "TH": "https://pricing-center.shopee.co.th",
    "BR": "https://pricing-center.shopee.com.br",
}

DRIVE_FOLDER_PARENT_ID = "13iY9ncXuCc-8oF0wXLMAdiism_m1xgcL"


@dataclass(frozen=True)
class ApiHeaders:
    main: Dict[str, str]
    quick_match: Dict[str, str]


DEFAULT_HEADERS = ApiHeaders(
    main={
        "accept":
        ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
         "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
         ),
        "accept-language":
        "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cache-control":
        "no-cache",
        "pragma":
        "no-cache",
        "priority":
        "u=0, i",
        "sec-ch-ua":
        '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-arch":
        '"arm"',
        "sec-ch-ua-bitness":
        '"64"',
        "sec-ch-ua-form-factors":
        '"Desktop"',
        "sec-ch-ua-full-version":
        '"142.0.3595.94"',
        "sec-ch-ua-full-version-list":
        ('"Chromium";v="142.0.7444.176", "Microsoft Edge";v="142.0.3595.94", '
         '"Not_A Brand";v="99.0.0.0"'),
        "sec-ch-ua-mobile":
        "?0",
        "sec-ch-ua-model":
        '""',
        "sec-ch-ua-platform":
        '"macOS"',
        "sec-ch-ua-platform-version":
        '"12.5.1"',
        "sec-ch-ua-wow64":
        "?0",
        "sec-fetch-dest":
        "document",
        "sec-fetch-mode":
        "navigate",
        "sec-fetch-site":
        "same-origin",
        "sec-fetch-user":
        "?1",
        "upgrade-insecure-requests":
        "1",
        "user-agent":
        ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 "
         "Safari/537.36 Edg/142.0.0.0"),
    },
    quick_match={
        "accept":
        "*/*",
        "accept-language":
        "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cache-control":
        "no-cache",
        "pragma":
        "no-cache",
        "priority":
        "u=1, i",
        "referer":
        "https://pricing-center.shopee.ph/quick-matching",
        "sec-ch-ua":
        '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile":
        "?0",
        "sec-ch-ua-platform":
        '"macOS"',
        "sec-fetch-dest":
        "empty",
        "sec-fetch-mode":
        "cors",
        "sec-fetch-site":
        "same-origin",
        "user-agent":
        ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 "
         "Safari/537.36 Edg/143.0.0.0"),
    },
)


def repo_root() -> Path:
    repo_root = get_writable_path() / "config"
    repo_root.mkdir(parents=True, exist_ok=True)
    return repo_root


def credentials_path() -> Path:
    return repo_root() / "credentials.json"


def credentials_template_path() -> Path:
    return repo_root() / "credentials.template.json"
