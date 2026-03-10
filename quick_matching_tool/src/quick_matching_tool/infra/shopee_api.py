"""Shopee quick matching API client."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests


@dataclass
class ShopeeQuickMatchClient:
    headers: Dict[str, str]
    session_cookies: Dict[str, str]

    def get_loading_progress(self, host: str, batch_ids: List[str]) -> int:
        url = (f"{host}/api/v3/quickmatch/custom-data-list?offset=0&limit=100"
               "&process_status=2&order_by=-ctime")
        response = requests.get(url,
                                headers=self.headers,
                                cookies=self.session_cookies).json()
        results = list(
            filter(
                lambda x: x.get("batch_id") in batch_ids,
                response.get("data", {}).get("data", []),
            ))
        if results:
            for result in results:
                logging.info(
                    "Batch id: %s 上传进度: %s/%s",
                    result["batch_id"],
                    result["complete_nums"],
                    result["upload_nums"],
                )
            return 1
        return 0

    def verify_upload_file_completed(self, host: str,
                                     batch_ids: List[str]) -> List[str]:
        completed_batch_ids = []
        if self.get_loading_progress(host, batch_ids):
            return completed_batch_ids
        url = (f"{host}/api/v3/quickmatch/custom-data-list?offset=0&limit=100"
               "&process_status=1&order_by=-ctime")
        response = requests.get(url,
                                headers=self.headers,
                                cookies=self.session_cookies)
        data = response.json()
        if data.get("code") == 0:
            for batch in data.get("data", {}).get("data", []):
                if batch["batch_id"] in batch_ids:
                    completed_batch_ids.append(batch["batch_id"])
        else:
            logging.error("Error: %s", data.get("message"))
        return completed_batch_ids

    def get_user_token(self, host: str) -> Optional[str]:
        url = f"{host}/api/v3/users/current"
        response = requests.get(url,
                                headers=self.headers,
                                cookies=self.session_cookies)
        if response.status_code == 200:
            data = response.json().get("data", {})
            return data.get("token"), data.get("username")
        logging.error("Error: %s, %s", response.status_code, response.text)
        return None

    def create_quick_matching_job(self, host: str, username: str,
                                  batch_id: str,
                                  target_batch_id: str) -> Optional[str]:
        url = f"{host}/api/v3/quickmatch/jobs-upload"
        current_date = time.strftime("%y%m%d_%H%M%S")
        payload = {
            "team_id": "13",
            "job_name": f"match_{current_date}_{username}",
            "product_type": "1",
            "match_type": "1",
            "data_range_type": "0",
            "source_team_id": "109",
            "source_batch_id": batch_id,
            "target_type": "4",
            "quick_match_sort_policy": "0",
            "topk": "10",
            "target_team_id": "109",
            "target_batch_id": target_batch_id,
            "version": "1",
            "frequency": "0",
            "run_time_gap": "0",
        }
        response = requests.request("POST",
                                    url,
                                    headers=self.headers,
                                    data=payload,
                                    cookies=self.session_cookies)
        logging.info("Create quick matching job response: %s", response.text)
        if response.status_code == 200 and response.json().get("code") == 0:
            return f"match_{current_date}_{username}"
        return None

    def verify_matching_completed(self, host: str, job_name: str):
        url = (
            f"{host}/api/v3/quickmatch/jobs?offset=0&limit=25&order_by=-ctime"
            "&process_status=1")
        response = requests.get(url,
                                headers=self.headers,
                                cookies=self.session_cookies)
        if response.status_code == 200:
            data = response.json()
            for job in data.get("data", {}).get("data", []):
                if job["job_name"] == job_name:
                    return job
            return None
        logging.error("Error: %s", response.status_code)
        return None

    def waiting_for_matching_completed(self, host: str, job_name: str):
        while not (job_info := self.verify_matching_completed(host, job_name)):
            logging.info("job_name: %s 正在匹配中，等待10秒查看...", job_name)
            time.sleep(10)
        logging.info("job_name: %s 匹配任务完成", job_name)
        return job_info

    def export_and_download_matching_result(self, host: str, job_info: dict,
                                            job_name: str) -> None:
        url = f"{host}/api/v3/quickmatch/jobs/{job_info['jobid']}/export"
        response = requests.post(url,
                                 headers=self.headers,
                                 cookies=self.session_cookies,
                                 json={"matched_item_count": 1})
        if response.status_code == 200 and response.json().get("code") == 0:
            task_id = response.json().get("data", {}).get("taskid")
            export_url = f"{host}/api/v3/exporttasks/{task_id}/result/url"
            for _ in range(5):  # 最多重试5次
                export_response = requests.get(export_url,
                                               headers=self.headers,
                                               cookies=self.session_cookies)
                logging.info("Export response: %s", export_response.text)
                if export_response.status_code == 200 and export_response.json(
                ).get("code") == 0:
                    file_url = export_response.json().get("data")
                    response = requests.get(file_url)
                    with open(f"{job_name}-matching_result.csv", "wb") as f:
                        f.write(response.content)
                    logging.info("已将匹配结果%s保存到本地, 在程序所在目录",
                                 f"{job_name}-matching_result.csv")
                    return f"{job_name}-matching_result.csv"
                time.sleep(2)  # 等待2秒后重试
        logging.error("Error: %s", response.status_code)
