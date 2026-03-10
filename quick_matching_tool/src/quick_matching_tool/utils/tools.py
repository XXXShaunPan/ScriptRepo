"""Tools."""

from __future__ import annotations

from urllib.parse import urlparse, parse_qs


def extract_gsheet_info(gsheet_url: str) -> tuple[str, str, str]:
    parsed_url = urlparse(gsheet_url)
    path_parts = parsed_url.path.split('/')
    gsheet_id = path_parts[-2]
    query_params = parse_qs(parsed_url.query)
    gid = query_params.get("gid", [None])[0]
    return gid, gsheet_id


def build_gsheet_url(gid: str, gsheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/u/0/d/e/{gsheet_id}/pubhtml/sheet?headers=false&gid={gid}"
