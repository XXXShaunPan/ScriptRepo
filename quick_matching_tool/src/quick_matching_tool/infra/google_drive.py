"""Google Drive integration."""

from __future__ import annotations

import io
import random

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload
from oauth2client.service_account import ServiceAccountCredentials
import requests

from quick_matching_tool.infra.google_auth import pick_client, load_client_dict
import logging

import threading

_get_quota_clients_lock = threading.Lock()

logging.getLogger('oauth2client.transport').setLevel(logging.WARNING)
logging.getLogger('oauth2client.client').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)


def gen_drive_client(client: str = "random"):
    api_name = "drive"
    api_version = "v3"
    scopes = ["https://www.googleapis.com/auth/drive"]
    client_secret = pick_client(client)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        client_secret, scopes)
    return build(api_name,
                 api_version,
                 credentials=credentials,
                 cache_discovery=False)


def get_has_quota_client_list():
    # 第一处 hasattr 判断是为了避免加锁带来的性能损失（如果已经有结果直接返回）
    if hasattr(get_has_quota_client_list, 'has_quota_client_list'):
        return get_has_quota_client_list.has_quota_client_list
    # 第二处 hasattr 判断是为了解决并发下多线程之间 race condition
    # 多线程场景下，可能通过第一层 hasattr 判断的线程不止一个，这时候都进入了 with，
    # 第一个线程会初始化 has_quota_client_list，后面线程在拿到锁后再判断一次，已被初始化就直接返回，避免重复计算
    with _get_quota_clients_lock:
        if hasattr(get_has_quota_client_list, 'has_quota_client_list'):
            return get_has_quota_client_list.has_quota_client_list
        result_list = []
        for client in load_client_dict().keys():
            try:
                logging.info(f'初始化有quota的client列表')
                drive_client = gen_drive_client(client)
                res = drive_client.about().get(fields="storageQuota").execute()
                quota = (int(res['storageQuota']['limit']) - int(
                    res['storageQuota']['usage'])) / 1024 / 1024 / 1024
                if quota >= 1:
                    result_list.append(client)
            except Exception as e:
                print(f'get {client} quota failed: {e}')
        get_has_quota_client_list.has_quota_client_list = result_list
        return result_list


def upload_file(
    folder_id,
    file_name="",
    file_path="./",
    file_content=None,
    headers={
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }):
    drive_client = gen_drive_client(random.choice(get_has_quota_client_list()))
    file_metadata = {"name": file_name, "parents": [folder_id]}
    for _ in range(3):
        try:
            if file_content is not None:
                media = MediaIoBaseUpload(io.BytesIO(file_content),
                                          mimetype="image/png",
                                          resumable=True)
            elif file_path.startswith("http"):
                response = requests.get(file_path, headers=headers)
                response.raise_for_status()
                media = MediaIoBaseUpload(io.BytesIO(response.content),
                                          mimetype="image/png",
                                          resumable=True)
            else:
                media = MediaFileUpload(file_path + file_name, resumable=True)

            file = drive_client.files().create(body=file_metadata,
                                               media_body=media,
                                               fields="id").execute()
            return file, file_name
        except Exception as e:
            logging.error(f'upload file failed: {e}')
    return None, None


def upload_file_with_executor(drive_client,
                              folder_id,
                              file_name="",
                              file_content=None):
    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(file_content),
                              mimetype="image/png",
                              resumable=True)
    return drive_client.files().create(body=file_metadata,
                                       media_body=media,
                                       fields="id").execute()


def download_file_from_google_drive(drive_client, file_id, output_path):
    request = drive_client.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    with open(output_path, "wb") as f:
        f.write(fh.getvalue())


def create_folder(folder_name, parents_id, client_secret='random'):
    """
    Args:
        folder_name: str
        parents_id: str
        client_secret: str
    """
    client = gen_drive_client(client_secret)
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parents_id]  # 上一级目录
    }
    folder = client.files().create(body=folder_metadata,
                                   fields='id',
                                   supportsAllDrives=True).execute()
    folder_id = folder.get('id')
    client.permissions().create(
        fileId=folder_id,
        supportsAllDrives=True,  # 关键：确保权限 API 能找到该文件,
        transferOwnership=False,
        body={
            "type": "anyone",
            "role": "reader",
            "allowFileDiscovery": False
        }).execute()
    return folder_id


__all__ = [
    "gen_drive_client",
    "upload_file",
    "upload_file_with_executor",
    "download_file_from_google_drive",
]
