"""Google Sheets integration."""

from __future__ import annotations

from typing import List

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

from quick_matching_tool.infra.google_auth import pick_client
from quick_matching_tool.utils.retry import retry


def extract_sheet_id(sheet_url: str) -> str:
    return sheet_url.split("#gid=")[0].split("/edit")[0].split("/d/")[-1]


def gen_gsheet_client(client: str | None = None) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    client_secret = pick_client(client)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        client_secret, scopes)
    return gspread.authorize(credentials)


@retry(max_retry=20)
def open_gsheet(tab_name: str, sheet_name: str, client_secret: str = ""):
    client = gen_gsheet_client(client_secret or None)
    return client.open(tab_name).worksheet(sheet_name)


@retry(max_retry=20)
def open_gsheet_by_url(sheet_url: str, tab_name: str, client_secret: str = ""):
    client = gen_gsheet_client(client_secret or None)
    return client.open_by_url(sheet_url).worksheet(tab_name)


@retry(max_retry=20)
def read_gsheet(sheet_url: str,
                tab_name: str,
                cell_range: str,
                index: int = 2,
                is_only_for_sheet: bool = False):
    gc = gen_gsheet_client("random")
    if "https" in sheet_url:
        sheet_url = extract_sheet_id(sheet_url)
    gsheet = gc.open_by_key(sheet_url)
    sheet = gsheet.worksheet(tab_name)
    if is_only_for_sheet:
        return sheet
    raw_info = sheet.get_values(cell_range)
    return pd.DataFrame(raw_info[1:],
                        columns=raw_info[0],
                        index=range(index,
                                    len(raw_info) + index - 1))


@retry(max_retry=10)
def gsheet_get_value(
        sheet,
        range_name: str,
        value_render_option: str = "FORMATTED_VALUE") -> List[List[str]]:
    return sheet.get_values(range_name,
                            value_render_option=value_render_option)


def gsheet_update(sheet, range_name: str, values):
    sheet.update(range_name, values)


@retry(max_retry=10)
def gsheet_append_row(sheet, data):
    sheet.append_row(data, value_input_option="USER_ENTERED")


def gsheet_append_rows(sheet, datas):
    for _ in range(10):
        try:
            sheet.append_rows(datas, value_input_option="USER_ENTERED")
            return
        except Exception:
            import time
            import random

            time.sleep(random.random() + 1)
    raise Exception("log记录失败")


def gsheet_batch_clear(sheet, range_name):
    sheet.batch_clear(range_name)


@retry(max_retry=3)
def gsheet_batch_update(sheet, datas):
    sheet.batch_update(datas, value_input_option="USER_ENTERED")


def gsheet_add_sheet(tab_name: str,
                     sheet_name: str,
                     client_secret: str = "client1"):
    client = gen_gsheet_client(client_secret)
    gsheet = client.open(tab_name)
    existing_sheets = gsheet.worksheets()
    if any(sheet.title == sheet_name for sheet in existing_sheets):
        return gsheet.worksheet(sheet_name)
    return gsheet.add_worksheet(sheet_name, rows=1000, cols=20)


def gsheet_get_all_sheets(tab_name: str, client_secret: str = "client1"):
    client = gen_gsheet_client(client_secret)
    sheet = client.open(tab_name)
    return sheet.worksheets()


def gsheet_del_sheet(tab_name: str,
                     sheet_name: str,
                     client_secret: str = "client1"):
    client = gen_gsheet_client(client_secret)
    sheet = client.open(tab_name)
    for worksheet in sheet.worksheets():
        if worksheet.title == sheet_name:
            sheet.del_worksheet(worksheet)


def gsheet_set_with_dataframe(sheet,
                              datas,
                              col: int = 1,
                              include_column_header: bool = False):
    rows = sheet.get_values("A1:A")
    if not isinstance(datas, pd.core.frame.DataFrame):
        datas = pd.DataFrame(datas)
    set_with_dataframe(
        sheet,
        datas,
        include_column_header=include_column_header,
        row=len(rows) + 1,
        col=col,
    )


__all__ = [
    "open_gsheet",
    "open_gsheet_by_url",
    "read_gsheet",
    "gsheet_get_value",
    "gsheet_update",
    "gsheet_append_row",
    "gsheet_append_rows",
    "gsheet_batch_clear",
    "gsheet_batch_update",
    "gsheet_add_sheet",
    "gsheet_get_all_sheets",
    "gsheet_del_sheet",
    "gsheet_set_with_dataframe",
    "set_with_dataframe",
]
