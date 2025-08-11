from DrissionPage import Chromium, ChromiumOptions
import time
import os
import json
from string import Template
from utils.common_new import *

# 数据准备
data = read_gsheet('12SmpcD-Fm9vOXPf4IrNe9nTzNiP_-Kv1_as_qyloAiY', '评分表',
                   "A2:W", 3).assign(new_asp='', new_comment_count='')

need_to_crawl_list = list(
    filter(lambda x: 'shein' in x[1], data[['对标热销品item link',
                                            '对标热销品asp前台价']].itertuples()))
need_to_crawl_list.sort(key=lambda x: x[2])


def review_num_show(review_num):
    if review_num >= 1000:
        return '累计Reviews>=1000'
    elif review_num >= 500:
        return '累计Reviews>=500'
    elif review_num >= 100:
        return '累计Reviews>=100'
    elif review_num >= 50:
        return '累计Reviews>=50'
    elif review_num >= 20:
        return '累计Reviews>=20'
    return '累计Reviews<20'


def create_proxy_auth_extension(proxy_host,
                                proxy_port,
                                proxy_username,
                                proxy_password,
                                scheme='http',
                                plugin_path=None):
    if plugin_path is None:
        plugin_path = os.path.join(os.getcwd(), 'proxy_auth_extension')
    if os.path.exists(plugin_path):
        return plugin_path

    # Manifest V3 for latest Chrome versions
    manifest_json = {
        "manifest_version": 3,
        "name": "Proxy Auth Extension",
        "version": "1.0.0",
        "permissions": ["proxy", "webRequest", "webRequestAuthProvider"],
        "host_permissions": ["<all_urls>"],
        "background": {
            "service_worker": "background.js"
        },
        "minimum_chrome_version": "88"
    }

    # Service worker for Manifest V3
    background_js = Template("""
        chrome.proxy.settings.set({
            value: {
                mode: "fixed_servers",
                rules: {
                    singleProxy: {
                        scheme: "${scheme}",
                        host: "${host}",
                        port: ${port}
                    },
                    bypassList: ["localhost", "127.0.0.1"]
                }
            },
            scope: "regular"
        });

        chrome.webRequest.onAuthRequired.addListener(
            (details) => {
                return {
                    authCredentials: {
                        username: "${username}",
                        password: "${password}"
                    }
                };
            },
            {urls: ["<all_urls>"]},
            ["blocking"]
        );
    """).substitute(host=proxy_host,
                    port=proxy_port,
                    username=proxy_username,
                    password=proxy_password,
                    scheme=scheme)

    # Create extension directory
    os.makedirs(plugin_path, exist_ok=True)

    # Write manifest.json
    with open(os.path.join(plugin_path, "manifest.json"), "w") as f:
        json.dump(manifest_json, f, indent=2)

    # Write background.js
    with open(os.path.join(plugin_path, "background.js"), "w") as f:
        f.write(background_js)

    return plugin_path


def check_has_capcha(page):
    if page.ele('.captcha_click_confirm', timeout=1):
        print('有验证码, 请先过验')


co = ChromiumOptions()

proxy_auth_plugin_path = create_proxy_auth_extension(
    "unmetered.residential.proxyrack.net", "222", "fumbatu",
    "EPOCAS9-6KZAOJD-FFSW6MJ-SJKSQV4-NF0JH7C-KC2VXY7-QZHSRLV")
# co.add_extension(path=proxy_auth_plugin_path)

# page = ChromiumPage(co)
# page.clear_cache()

# try:
#     page.get('https://whatismyipaddress.com/', retry=0, interval=0, timeout=30)
# except Exception as e:
#     print(f"Error: {e}")
#     page.close()

# path = '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge'
path = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'
co.set_browser_path(path)
# co.set_user_data_path(r"/Users/shaun.pan/edge_playwright")
# print(co.user_data_path)
# co.set_argument('--no-sandbox')
# co.headless()
co.incognito()
page = Chromium(addr_or_opts=co).get_tab()
page.listen.start('bff-api/product/get_goods_detail_realtime_data')
# url = 'https://ph.shein.com/DAZY-Women-V-Neck-Slim-Fit-Long-Sleeve-T-Shirt-With-Bust-Ruching-And-Bowknot-Decoration-For-Summer-p-32556396.html'
item = need_to_crawl_list.pop(0)
url = item[1].split('?')[0]
page.get(url)
# check_has_capcha(page)
# page.get_screenshot(path='.', name='baidu.png')
# for i in page.cookies(all_domains=False):
#     print(i)
refresh_num = 1
for packet in page.listen.steps(timeout=60):
    # print(packet.url)  # 打印数据包json
    try:
        if 'refresh' == packet and refresh_num < 3:
            # page.refresh()
            print('刷新')
            page.get(url)
            check_has_capcha(page)
            refresh_num += 1
            continue
        res = packet.response.body['info']
        price = res['priceInfo']['salePrice']['amount']
        comment = res['comment']['comments_overview']['commentNumShow']
        data.loc[item[0],
                 ['new_asp', 'new_comment_count']] = price, review_num_show(
                     int(comment.replace('+', '')))
        print(data.loc[item[0], ['new_asp', 'new_comment_count']].values)
    except Exception as e:
        print(url)
        print(e)

    if not need_to_crawl_list:
        page.listen.stop()
        break
    while need_to_crawl_list:
        # page.set.cookies.clear()
        page.wait(1.5, 3.5)
        item = need_to_crawl_list.pop(0)
        url = item[1].split('?')[0]
        refresh_num = 1
        try:
            page.get(url)
            check_has_capcha(page)
            break
        except:
            continue

data.to_csv('result1.csv', index=False)
