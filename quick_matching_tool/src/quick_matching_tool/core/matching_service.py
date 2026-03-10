"""Core quick matching orchestration."""

from __future__ import annotations

import logging
import os
import shutil
import time
from dataclasses import dataclass
from typing import Dict, List
import pandas as pd
from quick_matching_tool.config.settings import DEFAULT_HEADERS, REGION_HOSTS
from quick_matching_tool.core.data_prep import DataPreparer
from quick_matching_tool.infra.browser import BrowserSession
from quick_matching_tool.infra.google_sheets import open_gsheet, gsheet_get_value, read_gsheet, set_with_dataframe
from quick_matching_tool.infra.shopee_api import ShopeeQuickMatchClient
from quick_matching_tool.utils.paths import get_writable_path


@dataclass
class BatchInfo:
    pic_url: str
    data_url: str
    drive_folder_id: str
    gsheet_name: str | None = None


class QuickMatchingService:
    def __init__(self, clear_cache: bool = False, region: str = "PH") -> None:
        self.region = region
        self.region_host_dict = REGION_HOSTS
        self.home_path = "/bank/master-item-bank"
        self.quick_matching_path = "/quick-matching"

        self.user_data_path = os.path.join(get_writable_path(), "dp")
        if clear_cache and os.path.exists(self.user_data_path):
            try:
                shutil.rmtree(self.user_data_path)
            except Exception as exc:
                logging.warning("清除缓存失败: %s", exc)

        os.makedirs(self.user_data_path, exist_ok=True)
        logging.info("使用用户数据目录: %s", self.user_data_path)

        self.upload_file_path = os.path.join(get_writable_path(),
                                             "upload_files")
        os.makedirs(self.upload_file_path, exist_ok=True)
        logging.info("使用上传文件目录: %s", self.upload_file_path)

        cookie_sheet = open_gsheet("cookie管理",
                                   "cookie_in_one",
                                   client_secret="random")
        gsheet_cookie = gsheet_get_value(cookie_sheet, "B6")[0][0]

        self.google_headers = dict(DEFAULT_HEADERS.main)
        self.google_headers["Cookie"] = gsheet_cookie
        self.qm_headers = dict(DEFAULT_HEADERS.quick_match)

        self.browser_session = BrowserSession(
            user_data_path=self.user_data_path)
        self.qm_client = None  # ShopeeQuickMatchClient
        self.username = None  # username for quick matching

    def init_browser(self) -> None:
        self.browser_session.init_browser()

    def login(self, host: str, home_path: str) -> bool:
        self.browser_session.tab = self.browser_session.browser.new_tab(
            f"{host}{home_path}")
        self.browser_session.tab.wait(5)
        while not self.browser_session.tab.url.startswith(
                f"{host}{home_path}"):
            logging.info("Waiting for login...")
            self.browser_session.tab.wait(10)
        logging.info("Login successfully")
        return True

    def upload_file(self, host: str, csv_file_path: str,
                    job_type: str) -> None:
        tab = self.browser_session.tab
        tab.wait(5)
        tab.get(f"{host}{self.quick_matching_path}")
        tab.wait(2)
        tab.ele("@text()=Upload New Data").click()
        tab.wait(1)
        tab.ele("@text()=Select").click()
        tab.wait(1)
        tab.actions.move_to(ele_or_loc=tab.ele("@title=Cnbc"))
        tab.wait(1)
        tab.actions.scroll(delta_y=10000)
        tab.wait(1)
        tab.ele("@title=Shopee").click()
        tab.wait(1)
        tab.ele("@title=Platform").click()
        tab.wait(1)
        tab.ele("@id=job_type").click()
        tab.wait(1)
        tab.ele(f"@title={job_type}").click()
        tab.wait(1)
        tab.ele("@text()=Choose File").click.to_upload(csv_file_path)
        tab.wait(1)
        tab.ele("@text()=Create").click()
        tab.wait(1)

    def to_verify_upload_file_completed(
        self,
        host: str,
        batch_ids: List[str],
        csv_file_paths: List[str],
        job_types: List[str],
    ) -> bool:
        self.session_cookies = self.browser_session.tab.cookies().as_dict()
        self.qm_client = ShopeeQuickMatchClient(
            headers=self.qm_headers, session_cookies=self.session_cookies)

        user_token, username = self.qm_client.get_user_token(host)
        if not user_token:
            logging.error("Get user token failed")
            return False
        self.username = username.split('.')[0]
        self.qm_client.headers.update({"x-csrf-token": user_token})
        need_check_batch_ids = []
        for batch_id, csv_file_path, job_type in zip(batch_ids, csv_file_paths,
                                                     job_types):
            if not self.qm_client.verify_upload_file_completed(
                    host, [batch_id]):
                if not self.qm_client.get_loading_progress(host, [batch_id]):
                    logging.info(
                        "Region: %s, batch_id: %s, job_type: %s 未上传，开始上传...",
                        self.region,
                        batch_id,
                        job_type,
                    )
                    self.upload_file(host, csv_file_path, job_type)
                    while not self.qm_client.get_loading_progress(
                            host, [batch_id]):
                        time.sleep(10)
                need_check_batch_ids.append(batch_id)
            else:
                logging.info(
                    "Region: %s, batch_id: %s, job_type: %s 文件已上传...",
                    self.region,
                    batch_id,
                    job_type,
                )

        self.browser_session.quit()
        while need_check_batch_ids:
            if tmp_completed_batch_ids := self.qm_client.verify_upload_file_completed(
                    host, need_check_batch_ids):
                need_check_batch_ids = list(
                    set(need_check_batch_ids) - set(tmp_completed_batch_ids))
                logging.info(
                    "Region: %s, batch_ids: %s 上传处理完毕",
                    self.region,
                    tmp_completed_batch_ids,
                )
            time.sleep(10)
        logging.info("Region: %s, batch_ids: %s 所有文件上传处理完毕", self.region,
                     batch_ids)
        return True

    def to_write_down_result(self, source_gsheet_data_url: str,
                             matching_result_path: str) -> None:
        if not matching_result_path:
            return

        # 读取匹配结果并预处理
        data = (pd.read_csv(matching_result_path).sort_values(
            'Similarity Score(Image Only)', ascending=False).drop_duplicates(
                'Source Product id', keep='first').fillna('').assign(
                    **{
                        'Source Product id':
                        lambda x: x['Source Product id'].astype(str)
                    }))
        sheet_df = read_gsheet(source_gsheet_data_url, '评分表', 'A2:ZZ', 3)
        result_df = pd.merge(sheet_df,
                             data,
                             left_on='编号（必填项）',
                             right_on='Source Product id',
                             how='left')

        # 批量赋值匹配字段
        def assign_matching_fields(row):
            if customized_1 := row.get('Target Customized 1'):
                if pd.notna(customized_1):
                    prefix = f"{customized_1}相似款/同款"
                    return {
                        f'{prefix}item id': row.get('Target Product id', ''),
                        f'{prefix}image link': row.get('Target Product Image',
                                                       ''),
                        f'{prefix}图片':
                        f"=IMAGE(\"{row.get('Target Product Image', '')}\")",
                        f'{prefix}COGS': row.get('Target Customized 2', '')
                    }
            return {}

        result_df = (result_df.apply(
            lambda row: row.update(assign_matching_fields(row)) or row,
            axis=1)[[
                'Lovito相似款/同款item id', 'Lovito相似款/同款image link',
                'Lovito相似款/同款图片', 'Lovito相似款/同款COGS', 'SCS相似款/同款item id',
                'SCS相似款/同款image link', 'SCS相似款/同款图片', 'SCS相似款/同款COGS'
            ]])
        sheet = read_gsheet(source_gsheet_data_url,
                            '评分表',
                            'A2:ZZ',
                            is_only_for_sheet=True)
        col = sheet_df.columns.get_loc('Lovito相似款/同款item id') + 1
        set_with_dataframe(sheet,
                           result_df,
                           row=3,
                           col=col,
                           include_column_header=False)
        logging.info("已将匹配结果写入到评分表")

    def run(self, source_gsheet_url_info: Dict[str, str]) -> None:
        host = self.region_host_dict[self.region]
        preparer = DataPreparer(
            headers=self.google_headers,
            source_gsheet_pic_url=source_gsheet_url_info["pic_url"],
            source_gsheet_data_url=source_gsheet_url_info["data_url"],
            source_batch_drive_folder_id=source_gsheet_url_info[
                "drive_folder_id"],
            upload_file_path=self.upload_file_path,
        )
        csv_file_path = preparer.save_csv_file_from_gsheet()
        source_batch_id = preparer.get_batch_id_from_csv(csv_file_path)
        target_output_path, target_batch_id = preparer.download_target_data()

        self.init_browser()
        if not self.login(host, self.home_path):
            logging.error("Login failed")
            return

        if not self.to_verify_upload_file_completed(
                host,
            [target_batch_id, source_batch_id],
            [target_output_path, csv_file_path],
            ["Target Data for Item pairs", "Source Data for Item pairs"],
        ):
            logging.error("Upload file failed")
            return

        job_name = self.qm_client.create_quick_matching_job(
            host, self.username, source_batch_id, target_batch_id)

        if not job_name:
            logging.error("Create quick matching job failed")
            return

        logging.info(
            "Region: %s, source_batch_id: %s target_batch_id: %s 创建快速匹配任务成功，job_name: %s",
            self.region,
            source_batch_id,
            target_batch_id,
            job_name,
        )
        job_info = self.qm_client.waiting_for_matching_completed(
            host, job_name)
        matching_result_path = self.qm_client.export_and_download_matching_result(
            host, job_info, job_name)
        self.to_write_down_result(source_gsheet_url_info["data_url"],
                                  matching_result_path)
        logging.info("程序正常结束！")

    def batch_run(self, batch_infos: List[Dict[str, str]]):
        for batch_info in batch_infos:
            self.run(batch_info)

    def stop(self):
        ...

    def __del__(self):
        self.browser_session.quit()
