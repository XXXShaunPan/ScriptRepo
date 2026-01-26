import csv
from datetime import datetime
import re
import sys
import time
from DrissionPage import Chromium, ChromiumOptions, SessionPage
import os
import shutil
import logging
from typing import Union, List
from pyquery import PyQuery
import pandas as pd
import requests
import asyncio

__version__ = "1.0.2"

# 兼容打包后的导入
try:
    from common import *
except ImportError:
    from quick_matching_tool.common import *

# 配置logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)])

logging.info(f"quick_matching_tool version: {__version__}")


def get_writable_path():
    """获取可写入的路径，兼容打包后的环境"""
    if getattr(sys, 'frozen', False):
        # 如果是打包后的应用，使用用户的home目录或临时目录
        user_home = os.path.expanduser("~")
        app_data_dir = os.path.join(user_home, '.quick_matching_tool')
    else:
        # 如果是开发环境，使用当前目录
        app_data_dir = os.path.dirname(os.path.abspath(__file__))

    # 确保目录存在
    if not os.path.exists(app_data_dir):
        try:
            os.makedirs(app_data_dir)
        except OSError as e:
            # 如果创建失败，使用系统临时目录
            import tempfile
            app_data_dir = os.path.join(tempfile.gettempdir(),
                                        'quick_matching_tool')
            if not os.path.exists(app_data_dir):
                os.makedirs(app_data_dir)

    return app_data_dir


class QuickMatchingTool(object):
    def __init__(self, clear_cache=False) -> None:

        # 初始化drissionpage
        self.user_data_path = os.path.join(get_writable_path(), 'dp')
        if clear_cache and os.path.exists(self.user_data_path):
            try:
                shutil.rmtree(self.user_data_path)
            except Exception as e:
                logging.warning(f"清除缓存失败: {e}")

        if not os.path.exists(self.user_data_path):
            try:
                os.makedirs(self.user_data_path)
            except Exception as e:
                logging.error(f"创建用户数据目录失败: {e}")
                # 尝试使用临时目录
                import tempfile
                self.user_data_path = os.path.join(tempfile.gettempdir(),
                                                   'quick_matching_tool_dp')
                if not os.path.exists(self.user_data_path):
                    os.makedirs(self.user_data_path)

        logging.info(f"使用用户数据目录: {self.user_data_path}")

        options = ChromiumOptions()
        # 根据操作系统设置浏览器路径
        import platform
        system = platform.system()
        if system == 'Windows' and os.getlogin() == 'Administrator':  # 我的win系统
            options.set_browser_path(
                r'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'
            )
        elif system == 'Windows' and os.getlogin() != 'Administrator':  # 业务方电脑
            # options.set_browser_path(
            #     r'C:/Program Files/Google/Chrome/Application/chrome.exe')
            pass
        elif system == 'Darwin':  # macOS
            options.set_browser_path(
                r'/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge'
            )
        elif system == 'Linux':
            options.set_browser_path('/usr/bin/microsoft-edge')
        # options.set_user_data_path('/Users/shaun.pan/edge_playwright')
        # options.incognito()
        # options.headless()
        options.set_argument('--no-sandbox')

        self.options = options
        self.region_host_dict = {
            'MY': 'https://pricing-center.shopee.com.my',
            'PH': 'https://pricing-center.shopee.ph',
            'VN': 'https://pricing-center.shopee.vn',
            'TH': 'https://pricing-center.shopee.co.th',
            'BR': 'https://pricing-center.shopee.com.br',
        }
        self.home_path = f'/bank/master-item-bank'
        self.quick_matching_path = f'/quick-matching'

        self.upload_file_path = os.path.join(get_writable_path(),
                                             'upload_files')
        if not os.path.exists(self.upload_file_path):
            try:
                os.makedirs(self.upload_file_path)
            except Exception as e:
                logging.error(f"创建上传文件目录失败: {e}")
                # 尝试使用临时目录
                import tempfile
                self.upload_file_path = os.path.join(
                    tempfile.gettempdir(), 'quick_matching_tool_upload')
                if not os.path.exists(self.upload_file_path):
                    os.makedirs(self.upload_file_path)

        logging.info(f"使用上传文件目录: {self.upload_file_path}")

        cookie_sheet = open_gsheet('cookie管理',
                                   'cookie_in_one',
                                   client_secret='random')
        gsheet_cookie = gsheet_get_value(cookie_sheet, "B6")[0][0]
        self.headers = {
            'accept':
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language':
            'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua':
            '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-arch': '"arm"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-form-factors': '"Desktop"',
            'sec-ch-ua-full-version': '"142.0.3595.94"',
            'sec-ch-ua-full-version-list':
            '"Chromium";v="142.0.7444.176", "Microsoft Edge";v="142.0.3595.94", "Not_A Brand";v="99.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"12.5.1"',
            'sec-ch-ua-wow64': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'Cookie': gsheet_cookie,
        }
        self.QM_headers = {
            'accept':
            '*/*',
            'accept-language':
            'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'cache-control':
            'no-cache',
            'pragma':
            'no-cache',
            'priority':
            'u=1, i',
            'referer':
            'https://pricing-center.shopee.ph/quick-matching',
            'sec-ch-ua':
            '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile':
            '?0',
            'sec-ch-ua-platform':
            '"macOS"',
            'sec-fetch-dest':
            'empty',
            'sec-fetch-mode':
            'cors',
            'sec-fetch-site':
            'same-origin',
            'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
        }

    def init_browser(self):
        try:
            self.browser = Chromium(addr_or_opts=self.options)
            self.tab = None  # 先不创建tab，在login时创建
        except Exception as e:
            logging.error(f"初始化浏览器失败: {e}")
            raise e

    def extract_gsheet_data(self, res: str) -> list:
        doc = PyQuery(res)
        result_dict = {}

        # 1. 在循环外只查询一次，获取所有行对象
        rows = doc('tbody tr')

        # 2. 直接遍历这些行（从第4行开始）
        # 使用 .items() 可以直接获取每一行的 pyquery 对象，效率更高
        for i, row in enumerate(rows.items()):
            if i < 4:
                continue
            text = row('td').eq(0).text()
            img_src = row('td').eq(5)('img').attr.src
            if text and img_src:
                result_dict[text] = img_src
        return result_dict

    async def upload_file_and_get_dataframe(self,
                                            gsheet_data: dict) -> pd.DataFrame:
        drive_client = gen_drive_client()
        # 数据不全，需要分页获取
        first_page = drive_client.files().list(
            q=f"'{self.source_batch_drive_folder_id}' in parents",
            fields="files(id, name), nextPageToken").execute()
        df = pd.DataFrame(first_page['files']).drop_duplicates(subset='name')
        next_page_token = first_page.get('nextPageToken')
        while next_page_token:
            next_page = drive_client.files().list(
                q=f"'{self.source_batch_drive_folder_id}' in parents",
                fields="files(id, name), nextPageToken",
                pageToken=next_page_token).execute()
            df = pd.concat([
                df,
                pd.DataFrame(next_page['files']).drop_duplicates(subset='name')
            ])
            next_page_token = next_page.get('nextPageToken')
        logging.info(
            f"already get {len(df)} files from drive, and start uploading...")
        uploaded_file_list = set(df['name'].tolist())  # 使用 set 加速查找

        # 用于收集结果
        results = []

        for item_name, img_src in gsheet_data.items():
            if item_name in uploaded_file_list:
                continue

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    modified_url = re.sub(r'w(\d+)-h(\d+)', r'w\g<1>0-h\g<2>0',
                                          img_src)
                    response = requests.get(modified_url, headers=self.headers)
                    if response.status_code != 200:
                        logging.error(f"Download img failed: {item_name}")
                        break

                    content = response.content
                    result = upload_file_with_executor(
                        drive_client, self.source_batch_drive_folder_id,
                        item_name, content)
                    drive_client.permissions().create(fileId=result.get('id'),
                                                      fields='id',
                                                      body={
                                                          "type":
                                                          "anyone",
                                                          "role":
                                                          "reader",
                                                          "allowFileDiscovery":
                                                          False
                                                      }).execute()
                    logging.info(f"Upload img {item_name} successfully")
                    results.append([item_name, result.get('id')])
                    break  # 成功则退出重试循环
                except Exception as e:
                    if attempt < max_retries - 1:
                        logging.warning(
                            f"Retry {attempt + 1}/{max_retries} for {item_name}: {e}"
                        )

        # 批量添加结果到 DataFrame
        if results:
            results_df = pd.DataFrame(results, columns=['name', 'id'])
            df = pd.concat([df, results_df], ignore_index=True)

        # Team id	Batch id	Product id	Product Name	Product Image	Customized 1	Customized 2	Customized 3	Customized 4	Customized 5	Language
        df = df.rename(columns={'name': 'Product id', 'id': 'Product Image'})
        df['Batch id'] = datetime.now().strftime('%Y%m%dT%H%M%S')
        df['Team id'] = 109
        df['Product Image'] = df['Product Image'].apply(
            lambda x:
            f'https://drive.usercontent.google.com/download?id={x}&export=view'
        )
        df['Product Name'] = df['Product id']
        df['Customized 1'] = ''
        df['Customized 2'] = ''
        df['Customized 3'] = ''
        df['Customized 4'] = ''
        df['Customized 5'] = ''
        df['Language'] = 'zh_tw'
        df = df[[
            'Team id', 'Batch id', 'Product id', 'Product Name',
            'Product Image', 'Customized 1', 'Customized 2', 'Customized 3',
            'Customized 4', 'Customized 5', 'Language'
        ]]

        return df

    def save_csv_file_from_gsheet(self) -> str:
        csv_file_path = f'{self.upload_file_path}/source_matching_data.csv'
        logging.info(f"开始提取批次表图片数据")
        res = requests.get(self.source_gsheet_pic_url,
                           headers=self.headers).text
        data = self.extract_gsheet_data(res)
        logging.info(f"开始上传图片到drive，并转换为csv文件，请耐心等待...")
        df = asyncio.run(self.upload_file_and_get_dataframe(data))
        df.to_csv(csv_file_path, index=False)
        logging.info(f"已将source数据转换为csv文件，保存到 {csv_file_path}")
        return csv_file_path

    def download_target_data(self) -> tuple[str, str]:
        output_path = f'{self.upload_file_path}/target_matching_data.csv'
        drive_client = gen_drive_client()
        res = drive_client.files().list(
            q=f"'13iY9ncXuCc-8oF0wXLMAdiism_m1xgcL' in parents").execute(
            )['files']
        target_file = list(
            filter(lambda x: x['name'].endswith('productList.csv'), res))[0]
        # 检查是否存在output_path，如果存在，则直接返回
        if os.path.exists(output_path):
            # 获取batch id
            already_batch_id = self.get_batch_id_from_csv(output_path)
            if already_batch_id:
                if already_batch_id == target_file['name'].split('-')[0]:
                    logging.info(f"已检测到target数据，且batch id最新，直接返回...")
                    return output_path, already_batch_id
        file_id = target_file['id']
        download_file_from_google_drive(drive_client, file_id, output_path)
        logging.info(f"已将target数据下载到 {output_path}")
        return output_path, target_file['name'].split('-')[0]

    def login(self, host: str) -> bool:
        self.tab = self.browser.new_tab(f'{host}{self.home_path}')
        self.tab.wait(5)
        while not self.tab.url.startswith(f'{host}{self.home_path}'):
            logging.info(f"Waiting for login...")
            self.tab.wait(10)
        logging.info(f"Login successfully")
        return True

    def upload_file(self, host: str, csv_file_path: str,
                    job_type: str) -> None:
        self.tab.wait(5)
        self.tab.get(f'{host}{self.quick_matching_path}')
        self.tab.wait(2)
        self.tab.ele('@text()=Upload New Data').click()
        self.tab.wait(1)
        self.tab.ele('@text()=Select').click()
        self.tab.wait(1)
        self.tab.actions.move_to(ele_or_loc=self.tab.ele('@title=Cnbc'))
        self.tab.wait(1)
        self.tab.actions.scroll(delta_y=10000)
        self.tab.wait(1)
        self.tab.ele('@title=Shopee').click()
        self.tab.wait(1)
        self.tab.ele('@title=Platform').click()
        self.tab.wait(1)
        self.tab.ele('@id=job_type').click()
        self.tab.wait(1)
        # self.tab.ele('@text()=Source Data for Item pairs').click()
        self.tab.ele(f'@title={job_type}').click()
        self.tab.wait(1)
        # 点击Choose File选择文件
        self.tab.ele('@text()=Choose File').click.to_upload(csv_file_path)
        self.tab.wait(1)
        # 点击Upload
        self.tab.ele('@text()=Create').click()
        self.tab.wait(1)

    def get_batch_id_from_csv(self, csv_file_path: str) -> Union[str, None]:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # 取第一行
            row = next(reader)
            idx = row.index('Batch id')
            if idx == -1:
                return None
            return next(reader)[idx]

    def get_loading_progress(self, host: str, batch_ids: list[str]) -> int:
        url = f'{host}/api/v3/quickmatch/custom-data-list?offset=0&limit=100&process_status=2&order_by=-ctime'
        response = requests.get(url,
                                headers=self.QM_headers,
                                cookies=self.session_cookies).json()
        if result := list(
                filter(lambda x: x.get('batch_id') in batch_ids,
                       response.get('data', {}).get('data', []))):
            for result in result:
                logging.info(
                    f"Batch id: {result['batch_id']} 上传进度: {result['complete_nums']}/{result['upload_nums']}"
                )
            return 1
        else:
            return 0

    def verify_upload_file_completed(self, host: str,
                                     batch_ids: list[str]) -> list[str]:
        """
        返回batch_ids中已完成的id列表
        """
        completed_batch_ids = []
        # session = SessionPage()
        try:
            if self.get_loading_progress(host, batch_ids):
                return completed_batch_ids
            headers = self.QM_headers
            url = f'{host}/api/v3/quickmatch/custom-data-list?offset=0&limit=100&process_status=1&order_by=-ctime'
            response = requests.get(url,
                                    headers=headers,
                                    cookies=self.session_cookies)
            data = response.json()
            if data['code'] == 0:
                for batch in data.get('data', {}).get('data', []):
                    if batch['batch_id'] in batch_ids:
                        completed_batch_ids.append(batch['batch_id'])
                return completed_batch_ids
            else:
                logging.error(f"Error: {data['message']}")
                return completed_batch_ids
        except Exception as e:
            logging.error(f"Error: {e}")
            return completed_batch_ids
        # finally:
        #     # session.close()

    def get_user_token(self, host: str) -> Union[str, None]:
        # 获取token，通过get请求获取
        url = f'{host}/api/v3/users/current'
        response = requests.get(url,
                                headers=self.QM_headers,
                                cookies=self.session_cookies)
        if response.status_code == 200:
            return response.json().get('data', {}).get('token', None)
        else:
            logging.error(f"Error: {response.status_code}, {response.text}")
            return None

    def to_verify_upload_file_completed(self, host: str, region: str,
                                        batch_ids: List[str],
                                        csv_file_paths: List[str],
                                        job_types: List[str]) -> bool:
        """获取浏览器cookie等信息，然后关闭浏览器tab，返回True"""
        self.session_cookies = self.tab.cookies().as_dict()
        user_token = self.get_user_token(host)
        if not user_token:
            logging.error("Get user token failed")
            return False
        self.QM_headers.update({'x-csrf-token': user_token})
        need_check_batch_ids = []
        # 遍历所有batch_id和对应的csv文件
        for batch_id, csv_file_path, job_type in zip(batch_ids, csv_file_paths,
                                                     job_types):
            # 如果没完成且不在进度中才上传
            if not self.verify_upload_file_completed(host, [batch_id]):
                if not self.get_loading_progress(host, [batch_id]):
                    logging.info(
                        f"Region: {region}, batch_id: {batch_id}, job_type: {job_type} 未上传，开始上传..."
                    )
                    self.upload_file(host, csv_file_path, job_type)
                    # 等待上传开始
                    while not self.get_loading_progress(host, [batch_id]):
                        time.sleep(10)
                need_check_batch_ids.append(batch_id)
            else:
                logging.info(
                    f"Region: {region}, batch_id: {batch_id}, job_type: {job_type} 文件已上传..."
                )

        self.browser.quit()
        while need_check_batch_ids:
            if tmp_completed_batch_ids := self.verify_upload_file_completed(
                    host, need_check_batch_ids):
                need_check_batch_ids = list(
                    set(need_check_batch_ids) - set(tmp_completed_batch_ids))
                logging.info(
                    f"Region: {region}, batch_ids: {tmp_completed_batch_ids} 上传处理完毕"
                )
            time.sleep(10)
        logging.info(f"Region: {region}, batch_ids: {batch_ids} 所有文件上传处理完毕")
        return True

    def create_quick_matching_job(self, host: str, region: str, batch_id: str,
                                  target_batch_id: str) -> Union[str, None]:
        url = f"{host}/api/v3/quickmatch/jobs-upload"
        current_date = datetime.now().strftime('%Y%m%d')[2:]
        payload = {
            "team_id": "13",
            "job_name": f"match_{current_date}_{region}",
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
            "run_time_gap": "0"
        }
        response = requests.request("POST",
                                    url,
                                    headers=self.QM_headers,
                                    data=payload,
                                    cookies=self.session_cookies)
        logging.info(f"Create quick matching job response: {response.text}")
        if response.status_code == 200 and response.json().get('code') == 0:
            return f'match_{current_date}_{region}'

    def matched_data_to_gsheet(self, matched_data: pd.DataFrame) -> None:
        origin_data = read_gsheet(self.source_gsheet_data_url,
                                  '评分表',
                                  'A2:ZZ',
                                  index=3)
        origin_columns = origin_data.columns.tolist()
        result_data = pd.merge(origin_data,
                               matched_data,
                               right_on='Product id',
                               left_on=origin_columns[0],
                               how='left')
        write_idx = origin_columns.index('Lovito相似款/同款SPU')

    def verify_matching_completed(self, host: str, job_name: str) -> bool:

        url = f'{host}/api/v3/quickmatch/jobs?offset=0&limit=25&order_by=-ctime&process_status=1'
        response = requests.get(url,
                                headers=self.QM_headers,
                                cookies=self.session_cookies)
        if response.status_code == 200:
            data = response.json()
            for job in data.get('data', {}).get('data', []):
                if job['job_name'] == job_name:
                    return job
            return None
        else:
            logging.error(f"Error: {response.status_code}")
            return None

    def waiting_for_matching_completed(self, host: str, job_name: str) -> None:
        while not (job_info := self.verify_matching_completed(host, job_name)):
            logging.info(
                f"job_name: {job_name} matching task not completed, waiting for 10 seconds..."
            )
            time.sleep(10)
        logging.info(f"job_name: {job_name} matching task completed")
        return job_info

    def export_and_download_matching_result(self, host: str,
                                            job_info: dict) -> None:
        url = f'{host}/api/v3/quickmatch/jobs/{job_info["jobid"]}/export'
        response = requests.post(url,
                                 headers=self.QM_headers,
                                 cookies=self.session_cookies,
                                 json={"matched_item_count": 1})
        if response.status_code == 200 and response.json().get('code') == 0:
            task_id = response.json().get('data', {}).get('taskid')
            export_url = f'{host}/api/v3/exporttasks/{task_id}/result/url'
            export_response = requests.get(export_url,
                                           headers=self.QM_headers,
                                           cookies=self.session_cookies)
            if export_response.status_code == 200 and export_response.json(
            ).get('code') == 0:
                file_url = export_response.json().get('data')
                # 请求file_url，保存为csv文件
                response = requests.get(file_url)
                with open(f'{job_info["jobid"]}-matching_result.csv',
                          'wb') as f:
                    f.write(response.content)
                logging.info(
                    f'已将匹配结果{job_info["jobid"]}-matching_result.csv保存到本地')
                logging.info(f"程序正常结束！")
        else:
            logging.error(f"Error: {response.status_code}")
            return None

    def __del__(self):
        self.browser.quit()

    def stop(self):
        ...

    def run(self, source_gsheet_url_info: dict) -> None:
        region = 'PH'
        self.source_gsheet_pic_url = source_gsheet_url_info['pic_url']
        self.source_gsheet_data_url = source_gsheet_url_info['data_url']
        self.source_batch_drive_folder_id = source_gsheet_url_info[
            'drive_folder_id']
        host = self.region_host_dict[region]
        csv_file_path = self.save_csv_file_from_gsheet()
        logging.info(f"Save source csv file to {csv_file_path} successfully")
        source_batch_id = self.get_batch_id_from_csv(csv_file_path)
        target_output_path, target_batch_id = self.download_target_data()
        logging.info(f"Start processing {region} quick matching job...")
        self.init_browser()
        login_success = self.login(host)
        if not login_success:
            logging.error("Login failed")
            return
        if not self.to_verify_upload_file_completed(
                host, region, [target_batch_id, source_batch_id],
            [target_output_path, csv_file_path],
            ['Target Data for Item pairs', 'Source Data for Item pairs']):
            logging.error(f"Upload file failed")
            return
        job_name = self.create_quick_matching_job(host, region,
                                                  source_batch_id,
                                                  target_batch_id)
        if not job_name:
            logging.error(f"Create quick matching job failed")
            return
        logging.info(
            f"Region: {region}, source_batch_id: {source_batch_id} target_batch_id: {target_batch_id} 创建快速匹配任务成功，job_name: {job_name}"
        )
        job_info = self.waiting_for_matching_completed(host, job_name)
        self.export_and_download_matching_result(host, job_info)

    def batch_run(self, batch_nos: List[str]):
        for batch_no in batch_nos:
            self.run(batch_no)


if __name__ == "__main__":
    tool = QuickMatchingTool()
    tool.batch_run([{
        'pic_url':
        'https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vQxQEnVPuVl0l5vJlfJ1stEuZd73o7o1lMr768LV_KfBN3xLhenPERPwq6pKBk2jOy7hhOqDbgNtmN5/pubhtml/sheet?headers=false&gid=662628708',
        'data_url': '1TXPQRygmwH5jcDrIGwfECL2ycNCi9EUmIf52Ftobr3g',
        'gsheet_name': 'b158测试',
        'drive_folder_id': '14DuidsoqTQ6BPjrfJ9TxD9s9uOCRmRVu'
    }])
