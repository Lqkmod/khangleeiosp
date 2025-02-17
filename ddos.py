import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# URL chứa danh sách proxy SOCKS5
PROXY_URL = "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"
PROXY_FILE = "socks5_proxies.txt"

# Tải xuống danh sách proxy và lưu vào tệp
def download_proxies():
    try:
        response = requests.get(PROXY_URL)
        response.raise_for_status()
        with open(PROXY_FILE, 'w') as file:
            file.write(response.text)
        print(f"Đã tải xuống và lưu danh sách proxy vào {PROXY_FILE}")
    except requests.exceptions.RequestException as e:
        print(f"Đã xảy ra lỗi khi tải xuống: {e}")

# Đọc danh sách proxy từ tệp
def load_proxies():
    with open(PROXY_FILE, 'r') as file:
        proxies = file.readlines()
    return [proxy.strip() for proxy in proxies]

# Kiểm tra proxy còn hoạt động hay không
def check_proxy(proxy):
    try:
        response = requests.get("http://www.google.com", proxies={"http": proxy, "https": proxy}, timeout=5)
        if response.status_code == 200:
            print(f"Proxy {proxy} hoạt động tốt.")
            return True
        else:
            print(f"Proxy {proxy} không hoạt động.")
            return False
    except requests.RequestException:
        print(f"Proxy {proxy} không hoạt động.")
        return False

# Thiết lập Selenium với Proxy
def setup_selenium_with_proxy(proxy=None):
    chrome_options = Options()
    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')  # Cài đặt proxy nếu có
    chrome_options.add_argument('--headless')  # Chạy không giao diện
    chrome_options.add_argument('--no-sandbox')  # Khắc phục lỗi trên môi trường không đồ họa
    chrome_options.add_argument('--disable-dev-shm-usage')  # Giảm thiểu việc sử dụng bộ nhớ
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    return driver

# Truy cập trang web và xử lý Cloudflare
def bypass_cloudflare(url, proxy=None):
    driver = setup_selenium_with_proxy(proxy)
    driver.get(url)
    
    # Đợi để Cloudflare xử lý và trang web hiển thị
    time.sleep(10)

    # Kiểm tra nếu có CAPTCHA, tìm và giải quyết
    try:
        captcha_iframe = driver.find_element(By.TAG_NAME, "iframe")
        if captcha_iframe:
            print("Captcha được phát hiện!")
            # Giải quyết CAPTCHA (sử dụng 2Captcha)
            # Thêm mã giải quyết CAPTCHA ở đây nếu cần
            time.sleep(5)
            print("Captcha đã được giải quyết.")
    except Exception as e:
        print("Không tìm thấy CAPTCHA. Tiến hành vào trang web.")
    
    # Chờ một chút cho trang web load xong
    time.sleep(5)
    
    # Lấy nội dung trang
    page_source = driver.page_source
    driver.quit()
    return page_source

# Hàm truy cập trang web qua proxy và cổng tùy chọn (80 hoặc 443)
def access_page(url, proxies, port=443):
    working_proxies = [proxy for proxy in proxies if check_proxy(proxy)]  # Lọc các proxy còn hoạt động
    print(f"Sử dụng {len(working_proxies)} proxy hoạt động.")
    
    if not working_proxies:
        print("Không có proxy hoạt động. Thoát!")
        return
    
    for proxy in working_proxies:
        print(f"Đang thử với proxy {proxy}...")
        try:
            modified_url = f"{url}:{port}"  # Thêm cổng vào URL
            page_source = bypass_cloudflare(modified_url, proxy)
            print(f"Trang web đã tải thành công qua proxy {proxy}.")
            break  # Nếu truy cập thành công, thoát khỏi vòng lặp
        except Exception as e:
            print(f"Lỗi với proxy {proxy}: {e}")
            continue
    else:
        print("Không thể truy cập trang web với các proxy hiện tại.")

# Menu chức năng
def menu():
    print("Chọn chức năng:")
    print("1. Tải xuống danh sách proxy")
    print("2. Truy cập trang web")
    print("3. Thoát")
    choice = input("Nhập lựa chọn của bạn (1/2/3): ")
    return choice

# Chạy vòng lặp không giới hạn và đếm số lần đã chạy
def main():
    run_count = 0
    while True:
        choice = menu()
        if choice == '1':
            download_proxies()
        elif choice == '2':
            url = input("Nhập URL cần truy cập: ")
            port = input("Nhập cổng (80 hoặc 443): ")
            if port not in ['80', '443']:
                print("Cổng không hợp lệ. Vui lòng nhập 80 hoặc 443.")
                continue
            proxies = load_proxies()
            access_page(url, proxies, int(port))
        elif choice == '3':
            print(f"Chương trình đã chạy {run_count} lần.")
            break
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")
        run_count += 1

if __name__ == "__main__":
    main()