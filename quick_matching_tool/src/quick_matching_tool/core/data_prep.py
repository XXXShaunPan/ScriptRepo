"""Data preparation for quick matching.
   data preparation for google drive and csv file
"""

from __future__ import annotations

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass
from datetime import datetime
import logging
import os
import re
from typing import Dict, Tuple

import pandas as pd
import requests
from pyquery import PyQuery

from quick_matching_tool.infra.google_drive import (
    download_file_from_google_drive,
    upload_file,
    gen_drive_client,
)


@dataclass
class DataPreparer:
    headers: Dict[str, str]
    source_gsheet_pic_url: str
    source_gsheet_data_url: str
    source_batch_drive_folder_id: str
    upload_file_path: str

    def extract_gsheet_data(self, res: str) -> dict:
        doc = PyQuery(res)
        rows = doc("tbody tr")
        result_dict = {}
        for i, row in enumerate(rows.items()):
            text = row("td").eq(0).text()
            img_src = row("td:lt(10)").find("img").eq(0).attr("src")
            if text and img_src:
                result_dict[text] = img_src
        logging.info("已提取%s个图片数据", len(result_dict))
        return result_dict

    def upload_file_and_get_dataframe(self, gsheet_data: dict) -> pd.DataFrame:
        drive_client = gen_drive_client()
        all_files = []
        page_token = None
        while True:
            response = drive_client.files().list(
                q=f"'{self.source_batch_drive_folder_id}' in parents",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
                pageSize=1000,
            ).execute()
            all_files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        df = pd.DataFrame(all_files).drop_duplicates(subset="name")
        logging.info("已从drive获取%s个文件，开始上传未完成项目...", len(df))
        uploaded_file_list = set(
            df["name"].tolist()) if not df.empty else set()
        results = []

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(upload_file,
                                self.source_batch_drive_folder_id,
                                item_name,
                                re.sub(r"w(\d+)-h(\d+)", r"w\g<1>0-h\g<2>0",
                                       img_src),
                                headers=self.headers)
                for item_name, img_src in gsheet_data.items()
                if item_name not in uploaded_file_list
            ]
            for future in concurrent.futures.as_completed(futures):
                file, item_name = future.result()
                if file:
                    logging.info("Upload img %s successfully", item_name)
                    results.append([item_name, file.get("id")])
        if results:
            results_df = pd.DataFrame(results, columns=["name", "id"])
            df = pd.concat([df, results_df], ignore_index=True)

        df = df.rename(columns={"name": "Product id", "id": "Product Image"})
        df["Batch id"] = datetime.now().strftime("%Y%m%dT%H%M%S")
        df["Team id"] = 109
        df["Product Image"] = df["Product Image"].apply(
            lambda x:
            f"https://drive.usercontent.google.com/download?id={x}&export=view"
        )
        df["Product Name"] = df["Product id"]
        df["Customized 1"] = ""
        df["Customized 2"] = ""
        df["Customized 3"] = ""
        df["Customized 4"] = ""
        df["Customized 5"] = ""
        df["Language"] = "zh_tw"
        df = df[[
            "Team id",
            "Batch id",
            "Product id",
            "Product Name",
            "Product Image",
            "Customized 1",
            "Customized 2",
            "Customized 3",
            "Customized 4",
            "Customized 5",
            "Language",
        ]]
        return df

    def save_csv_file_from_gsheet(self) -> str:
        csv_file_path = f"{self.upload_file_path}/source_matching_data.csv"
        logging.info("开始上传图片到drive，并转换为csv文件，请耐心等待...")
        data = {i: i for i in range(6)}  # 生成6个key，作为初始条件
        df = pd.DataFrame()
        while data.keys().__len__() > df.shape[0] + 5:
            res = requests.get(self.source_gsheet_pic_url,
                               headers=self.headers).text
            data = self.extract_gsheet_data(res)
            df = self.upload_file_and_get_dataframe(data)
        df.to_csv(csv_file_path, index=False)
        logging.info("已将source数据转换为csv文件，保存到 %s", csv_file_path)
        return csv_file_path

    def download_target_data(self) -> Tuple[str, str]:
        output_path = f"{self.upload_file_path}/target_matching_data.csv"
        drive_client = gen_drive_client()
        res = drive_client.files().list(
            q="'13iY9ncXuCc-8oF0wXLMAdiism_m1xgcL' in parents").execute(
            )["files"]
        target_file = list(
            filter(lambda x: x["name"].endswith("productList.csv"), res))[0]
        if os.path.exists(output_path):
            already_batch_id = self.get_batch_id_from_csv(output_path)
            if already_batch_id and already_batch_id == target_file[
                    "name"].split("-")[0]:
                logging.info("已检测到target数据，且batch id最新，直接返回...")
                return output_path, already_batch_id
        file_id = target_file["id"]
        download_file_from_google_drive(drive_client, file_id, output_path)
        logging.info("已将target数据下载到 %s", output_path)
        return output_path, target_file["name"].split("-")[0]

    def get_batch_id_from_csv(self, csv_file_path: str) -> str | None:
        with open(csv_file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            row = next(reader)
            idx = row.index("Batch id")
            if idx == -1:
                return None
            return next(reader)[idx]
