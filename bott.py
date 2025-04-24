
import logging
import httpx
import json
import html
import os
import time
import random
import string
import re
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

from telegram import Update, Message, InputMediaPhoto
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler # Giữ lại phòng khi cần
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (VẪN CẦN CHO LỆNH /tim)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM CỦA ADMIN
ALLOWED_GROUP_ID = -1002191171631 # <--- GROUP ID CHÍNH (Cho /getkey, /muatt, bill)
LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút (Dùng chung cho tim và fl thường)
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ
TREO_INTERVAL_SECONDS = 15 * 60 # 15 phút (Khoảng cách giữa các lần gọi API /treo)
TREO_STATS_INTERVAL_SECONDS = 2 * 3600 # 2 giờ (Khoảng cách thống kê follow tăng)
PHOTO_BILL_WINDOW_SECONDS = 1 * 3600 # 1 giờ (Thời gian nhận ảnh bill sau khi dùng /muatt)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (KHÔNG ĐỔI)
# API Follow cũ (cho key user và 1 luồng VIP)
OLD_FOLLOW_API_URL_BASE = "https://apitangfltiktok.soundcast.me/telefl.php"
# API Follow mới (chỉ cho luồng thứ 2 VIP)
NEW_FOLLOW_API_URL_BASE = "http://haigiaitrixin.great-site.net/follow.php"
NEW_FOLLOW_API_KEY = "giaitrixin" # Key cố định cho API follow mới
# API Info TikTok
INFO_API_URL_TEMPLATE = "https://guanghai.x10.mx/infott.php?username={username}"

# --- Thông tin VIP ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg"
BANK_ACCOUNT = "KHANGDINO" # <--- THAY STK CỦA BẠN
BANK_NAME = "VCB BANK" # <--- THAY TÊN NGÂN HÀNG
ACCOUNT_NAME = "LÊ QUỐC KHANG" # <--- THAY TÊN CHỦ TK
PAYMENT_NOTE_PREFIX = "VIP DinoTool ID" # Nội dung chuyển khoản sẽ là: "VIP DinoTool ID <user_id>"

# --- Lưu trữ ---
DATA_FILE = "bot_persistent_data.json"

# --- Biến toàn cục ---
user_tim_cooldown = {}
user_fl_cooldown = {} # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {}
valid_keys = {} # {key: {"user_id_generator": ..., "expiry_time": ..., "used_by": ..., "activation_time": ...}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": expiry_timestamp, "limit": user_limit}} - Người dùng VIP
active_treo_tasks = {} # {user_id_str: {target_username: asyncio.Task}} - Lưu các task /treo đang chạy
treo_stats = {} # {user_id_str: {target_username: gain_since_last_report}} - Lưu số follow tăng từ lần báo cáo trước
last_stats_report_time = 0 # Thời điểm báo cáo thống kê gần nhất
muatt_users_tracking = {} # {user_id: timestamp} - Lưu user vừa dùng /muatt để chờ ảnh bill

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("bot.log", encoding='utf-8'), logging.StreamHandler()] # Log ra file và console
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình ---
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": logger.critical("!!! BOT_TOKEN is missing !!!"); exit(1)
if not ALLOWED_GROUP_ID: logger.critical("!!! ALLOWED_GROUP_ID is missing !!!"); exit(1)
if not LINK_SHORTENER_API_KEY or LINK_SHORTENER_API_KEY == "YOUR_YEUMONEY_TOKEN": logger.critical("!!! LINK_SHORTENER_API_KEY is missing !!!"); exit(1)
if not API_KEY or API_KEY == "YOUR_TIM_API_KEY": logger.warning("!!! API_KEY (for /tim) is missing. /tim command might fail. !!!")
if not ADMIN_USER_ID: logger.critical("!!! ADMIN_USER_ID is missing !!!"); exit(1)

# --- Hàm lưu/tải dữ liệu ---
def save_data():
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    string_key_vip_users = {str(k): v for k, v in vip_users.items()}
    string_key_treo_stats = {str(uid): {uname: gain for uname, gain in udict.items()} for uid, udict in treo_stats.items()}
    # Không lưu muatt_users_tracking vì nó là trạng thái tạm thời

    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "vip_users": string_key_vip_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        },
        "treo_stats": string_key_treo_stats,
        "last_stats_report_time": last_stats_report_time
    }
    try:
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        os.replace(temp_file, DATA_FILE)
        logger.debug(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)
        if os.path.exists(temp_file):
            try: os.remove(temp_file)
            except Exception as e_rem: logger.error(f"Failed to remove temporary save file {temp_file}: {e_rem}")

def load_data():
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, treo_stats, last_stats_report_time
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                valid_keys = data.get("valid_keys", {})
                activated_users = {str(k): v for k, v in data.get("activated_users", {}).items()}
                vip_users = {str(k): v for k, v in data.get("vip_users", {}).items()}

                all_cooldowns = data.get("user_cooldowns", {})
                user_tim_cooldown = {str(k): v for k, v in all_cooldowns.get("tim", {}).items()}
                loaded_fl = all_cooldowns.get("fl", {})
                user_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in loaded_fl.items()}
                user_getkey_cooldown = {str(k): v for k, v in all_cooldowns.get("getkey", {}).items()}

                loaded_stats = data.get("treo_stats", {})
                # Chuyển đổi lại thành defaultdict khi tải
                treo_stats_temp = {str(uid): targets for uid, targets in loaded_stats.items()}
                treo_stats = {uid: defaultdict(int, targets) for uid, targets in treo_stats_temp.items()}

                last_stats_report_time = data.get("last_stats_report_time", 0)

                logger.info(f"Data loaded successfully from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}
            treo_stats = {}
            last_stats_report_time = 0
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}
        treo_stats = {}
        last_stats_report_time = 0
    # Khởi tạo muatt_users_tracking là dict rỗng mỗi khi bot khởi động
    global muatt_users_tracking
    muatt_users_tracking = {}
    logger.info("Initialized empty muatt_users_tracking.")

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    # Không cần check group ID ở đây nữa vì hàm này có thể được gọi từ nhiều context
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except (BadRequest, Forbidden) as e:
        if "Message to delete not found" in str(e) or "message can't be deleted" in str(e) or "MESSAGE_ID_INVALID" in str(e) or "message to delete not found" in str(e).lower():
            logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted or no permission): {e}")
        else:
            logger.warning(f"Error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job được lên lịch để xóa tin nhắn."""
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except (BadRequest, Forbidden) as e:
            if "Message to delete not found" in str(e).lower() or "message can't be deleted" in str(e):
                logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else:
                 logger.warning(f"Job '{job_name}' error deleting message {message_id}: {e}")
        except TelegramError as e:
             logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e:
            logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else:
        logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Gửi tin nhắn và tự động xóa sau một khoảng thời gian."""
    if not update or not update.effective_chat: return
    # Có thể gửi ở bất kỳ đâu
    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if reply and update.message else None
        if reply_to_msg_id:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
        else:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True)

        if sent_message and context.job_queue:
            job_name = f"del_temp_{chat_id}_{sent_message.message_id}"
            context.job_queue.run_once(
                delete_message_job,
                duration,
                data={'chat_id': chat_id, 'message_id': sent_message.message_id},
                name=job_name
            )
            logger.debug(f"Scheduled job '{job_name}' to delete message {sent_message.message_id} in {duration}s")
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending temporary message to {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in send_temporary_message to {chat_id}: {e}", exc_info=True)

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên dạng Dinotool-xxxx."""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"Dinotool-{random_part}"

async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng một task treo cụ thể. Trả về True nếu dừng thành công, False nếu không tìm thấy hoặc đã dừng."""
    task = None
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str][target_username]

    if task and not task.done():
        task.cancel()
        logger.info(f"[Treo Task Stop] Attempting to cancel task for user {user_id_str} -> @{target_username}. Reason: {reason}")
        try:
            await asyncio.wait_for(task, timeout=1.0)
            logger.info(f"[Treo Task Stop] Task {user_id_str} -> @{target_username} finished after cancellation.")
        except asyncio.CancelledError:
            logger.info(f"[Treo Task Stop] Task {user_id_str} -> @{target_username} confirmed cancelled.")
            pass
        except asyncio.TimeoutError:
             logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled task {user_id_str}->{target_username} to finish. Assuming stopped.")
        except Exception as e:
             logger.error(f"[Treo Task Stop] Error awaiting cancelled task for {user_id_str}->{target_username}: {e}")

        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            del active_treo_tasks[user_id_str][target_username]
            if not active_treo_tasks[user_id_str]:
                del active_treo_tasks[user_id_str]
            logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active tasks.")
            return True
        else:
             logger.warning(f"[Treo Task Stop] Task entry for {user_id_str} -> {target_username} already removed after cancellation attempt.")
             return True
    elif task and task.done():
         logger.info(f"[Treo Task Stop] Task for {user_id_str} -> @{target_username} was already done. Removing entry.")
         if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
             del active_treo_tasks[user_id_str][target_username]
             if not active_treo_tasks[user_id_str]:
                 del active_treo_tasks[user_id_str]
             return True
         return False
    else:
         logger.info(f"[Treo Task Stop] No active task found for user {user_id_str} -> @{target_username} to stop.")
         return False

async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng tất cả các task treo của một user."""
    stopped_count = 0
    if user_id_str in active_treo_tasks:
        targets_to_stop = list(active_treo_tasks[user_id_str].keys())
        logger.info(f"Stopping all {len(targets_to_stop)} treo tasks for user {user_id_str}. Reason: {reason}")
        for target_username in targets_to_stop:
            if await stop_treo_task(user_id_str, target_username, context, reason):
                stopped_count += 1
        if user_id_str in active_treo_tasks and not active_treo_tasks[user_id_str]:
             del active_treo_tasks[user_id_str]
        logger.info(f"Finished stopping tasks for user {user_id_str}. Stopped: {stopped_count}/{len(targets_to_stop)}")
    else:
        logger.info(f"No active treo tasks found for user {user_id_str} to stop.")

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs, muatt tracking)."""
    global valid_keys, activated_users, vip_users, muatt_users_tracking
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    muatt_users_to_remove = []
    data_changed = False

    logger.info("[Cleanup] Starting cleanup job...")

    # Check expired keys
    for key, data in list(valid_keys.items()):
        try:
            expiry = float(data.get("expiry_time", 0))
            if data.get("used_by") is None and current_time > expiry:
                keys_to_remove.append(key)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry_time '{data.get('expiry_time')}' for key {key}, removing.")
            keys_to_remove.append(key)

    # Check expired key activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp):
                users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid activation timestamp '{expiry_timestamp}' for user {user_id_str} (key system), removing.")
            users_to_deactivate_key.append(user_id_str)

    # Check expired VIP activations
    vip_users_to_stop_tasks = []
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            expiry = float(vip_data.get("expiry", 0))
            if current_time > expiry:
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry timestamp '{vip_data.get('expiry')}' for VIP user {user_id_str}, removing.")
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str)

    # Check expired muatt tracking entries
    for user_id, timestamp in list(muatt_users_tracking.items()):
        try:
            if current_time > float(timestamp) + PHOTO_BILL_WINDOW_SECONDS:
                muatt_users_to_remove.append(user_id)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid timestamp '{timestamp}' for muatt tracking user {user_id}, removing.")
            muatt_users_to_remove.append(user_id)

    # Perform deletions
    if keys_to_remove:
        logger.info(f"[Cleanup] Removing {len(keys_to_remove)} expired unused keys.")
        for key in keys_to_remove:
            if key in valid_keys: del valid_keys[key]; data_changed = True
    if users_to_deactivate_key:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_key)} users (key system).")
         for user_id_str in users_to_deactivate_key:
             if user_id_str in activated_users: del activated_users[user_id_str]; data_changed = True
    if users_to_deactivate_vip:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_vip)} VIP users.")
         for user_id_str in users_to_deactivate_vip:
             if user_id_str in vip_users: del vip_users[user_id_str]; data_changed = True
    if muatt_users_to_remove:
        logger.info(f"[Cleanup] Removing {len(muatt_users_to_remove)} expired muatt tracking entries.")
        for user_id in muatt_users_to_remove:
            if user_id in muatt_users_tracking: del muatt_users_tracking[user_id] # No need to set data_changed as this isn't saved

    # Stop tasks for expired/invalid VIPs
    if vip_users_to_stop_tasks:
         logger.info(f"[Cleanup] Stopping tasks for {len(vip_users_to_stop_tasks)} expired/invalid VIP users.")
         app = context.application
         for user_id_str in vip_users_to_stop_tasks:
             app.create_task(stop_all_treo_tasks_for_user(user_id_str, context, reason="VIP Expired/Removed during Cleanup"))

    # Save if data changed
    if data_changed:
        logger.info("[Cleanup] Data changed, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No persistent data changed during cleanup.")
    logger.info("[Cleanup] Cleanup job finished.")


def is_user_vip(user_id: int) -> bool:
    """Kiểm tra trạng thái VIP."""
    user_id_str = str(user_id)
    vip_data = vip_users.get(user_id_str)
    if vip_data:
        try:
            expiry_time = float(vip_data.get("expiry", 0))
            if time.time() < expiry_time:
                return True
            else:
                logger.debug(f"VIP check for {user_id_str}: Expired")
        except (ValueError, TypeError):
             logger.warning(f"VIP check for {user_id_str}: Invalid expiry data '{vip_data.get('expiry')}'. Treating as not VIP.")
    return False

def get_vip_limit(user_id: int) -> int:
    """Lấy giới hạn treo user của VIP."""
    user_id_str = str(user_id)
    if is_user_vip(user_id):
        vip_data = vip_users.get(user_id_str, {})
        return vip_data.get("limit", 0)
    return 0

def is_user_activated_by_key(user_id: int) -> bool:
    """Kiểm tra trạng thái kích hoạt bằng key."""
    user_id_str = str(user_id)
    expiry_time_str = activated_users.get(user_id_str)
    if expiry_time_str:
        try:
            if time.time() < float(expiry_time_str):
                return True
            else:
                 logger.debug(f"Key activation check for {user_id_str}: Expired")
        except (ValueError, TypeError):
             logger.warning(f"Key activation check for {user_id_str}: Invalid expiry data '{expiry_time_str}'. Treating as not activated.")
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl) không (VIP hoặc đã kích hoạt key)."""
    is_vip = is_user_vip(user_id)
    is_key = is_user_activated_by_key(user_id)
    logger.debug(f"Feature check for {user_id}: VIP={is_vip}, KeyActivated={is_key}")
    return is_vip or is_key

# --- Logic API Follow ---
async def call_single_follow_api(api_url: str, params: dict, api_name: str, bot_token: str | None = None) -> dict:
    """Gọi một API follow duy nhất và trả về kết quả."""
    result = {"success": False, "message": f"Lỗi không xác định khi gọi API {api_name}.", "data": None, "api_name": api_name}
    request_params = params.copy()
    if bot_token and 'tokenbot' in request_params: # Chỉ thêm token nếu API yêu cầu
        request_params['tokenbot'] = bot_token

    log_params = request_params.copy()
    if 'tokenbot' in log_params: log_params["tokenbot"] = f"...{log_params['tokenbot'][-6:]}" if len(log_params.get('tokenbot','')) > 6 else "***"
    if 'key' in log_params: log_params["key"] = "***" # Giấu key cố định
    logger.info(f"[API Call - {api_name}] Calling {api_url} with params: {log_params}")

    try:
        async with httpx.AsyncClient(verify=False, timeout=60.0) as client: # verify=False nếu API mới dùng HTTP hoặc SSL tự ký
            resp = await client.get(api_url, params=request_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                response_text_for_debug = await resp.aread()
                response_text_for_debug = response_text_for_debug.decode('utf-8', errors='replace')[:1000]
            except Exception as e_read:
                 logger.warning(f"[API Call - {api_name}] Error reading response body: {e_read}")

            logger.debug(f"[API Call - {api_name}] Status: {resp.status_code}, Content-Type: {content_type}")

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    logger.debug(f"[API Call - {api_name}] JSON Data: {data}")
                    result["data"] = data

                    # API cũ dùng "status": true/false, API mới không rõ response, giả định tương tự hoặc chỉ check message
                    api_status = data.get("status") # True/False hoặc None
                    api_message = data.get("message", "Không có thông báo.")

                    # Logic xác định thành công: status=true HOẶC message chứa từ khóa thành công (linh hoạt hơn)
                    success_keywords = ["success", "thành công", "đã tăng", "ok"]
                    is_success_status = api_status is True
                    is_success_message = any(keyword in api_message.lower() for keyword in success_keywords)
                    # Ưu tiên status nếu có, nếu không thì dựa vào message
                    is_api_reported_success = is_success_status if api_status is not None else is_success_message

                    if is_api_reported_success:
                        result["success"] = True
                        result["message"] = api_message or f"{api_name} thành công."
                    else:
                        result["success"] = False
                        result["message"] = api_message or f"{api_name} thất bại (API status={api_status})."

                except json.JSONDecodeError:
                    # Xử lý trường hợp API mới trả về text thay vì JSON khi thành công?
                    if "success" in response_text_for_debug.lower() or "thành công" in response_text_for_debug.lower():
                         logger.info(f"[API Call - {api_name}] Response 200 OK, not JSON, but text indicates success: {response_text_for_debug[:100]}...")
                         result["success"] = True
                         result["message"] = f"{api_name} thành công (phản hồi text)."
                         result["data"] = {"message": response_text_for_debug} # Lưu text vào data
                    else:
                        logger.error(f"[API Call - {api_name}] Response 200 OK but not valid JSON or success text. Text: {response_text_for_debug}...")
                        result["message"] = f"Lỗi: API {api_name} không trả về JSON/Text hợp lệ."
                except Exception as e_proc:
                    logger.error(f"[API Call - {api_name}] Error processing API data: {e_proc}", exc_info=True)
                    result["message"] = f"Lỗi xử lý dữ liệu từ API {api_name}."
            else: # Lỗi HTTP
                 logger.error(f"[API Call - {api_name}] HTTP Error Status: {resp.status_code}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi từ API {api_name} (Code: {resp.status_code})."

    except httpx.TimeoutException:
        logger.warning(f"[API Call - {api_name}] API timeout.")
        result["message"] = f"Lỗi: API {api_name} timeout."
    except httpx.ConnectError as e_connect:
        logger.error(f"[API Call - {api_name}] Connection error: {e_connect}", exc_info=False)
        result["message"] = f"Lỗi kết nối đến API {api_name}."
    except httpx.RequestError as e_req:
        logger.error(f"[API Call - {api_name}] Network error: {e_req}", exc_info=False)
        result["message"] = f"Lỗi mạng khi kết nối API {api_name}."
    except Exception as e_unexp:
        logger.error(f"[API Call - {api_name}] Unexpected error during API call: {e_unexp}", exc_info=True)
        result["message"] = f"Lỗi hệ thống Bot khi xử lý API {api_name}."

    logger.debug(f"[API Call - {api_name}] Final result: Success={result['success']}, Message='{result['message']}'")
    return result

async def execute_follow_request(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """
    Thực hiện yêu cầu follow, chạy 2 luồng cho VIP, 1 luồng cho user thường.
    Returns: {"success": bool, "message": str, "data": dict | None}
    'data' sẽ ưu tiên từ API cũ nếu thành công.
    """
    user_id = int(user_id_str)
    is_vip = is_user_vip(user_id)

    combined_result = {"success": False, "message": "Lỗi không xác định.", "data": None}
    results = []

    if is_vip:
        logger.info(f"[Follow VIP] User {user_id_str} is VIP. Calling 2 APIs for @{target_username}.")
        # Chuẩn bị params
        old_api_params = {"user": target_username, "userid": user_id_str} # tokenbot được thêm trong call_single
        new_api_params = {"username": target_username, "key": NEW_FOLLOW_API_KEY}

        # Gọi đồng thời
        tasks = [
            call_single_follow_api(OLD_FOLLOW_API_URL_BASE, old_api_params, "OldAPI", bot_token),
            call_single_follow_api(NEW_FOLLOW_API_URL_BASE, new_api_params, "NewAPI", None) # API mới ko cần token bot
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    else: # User thường (đã kích hoạt key)
        logger.info(f"[Follow KeyUser] User {user_id_str} is KeyUser. Calling Old API for @{target_username}.")
        old_api_params = {"user": target_username, "userid": user_id_str}
        # Gọi chỉ API cũ
        result_old = await call_single_follow_api(OLD_FOLLOW_API_URL_BASE, old_api_params, "OldAPI", bot_token)
        results.append(result_old)

    # Xử lý kết quả
    successful_calls = []
    failed_calls = []
    old_api_success_result = None

    for res in results:
        if isinstance(res, Exception):
            logger.error(f"[Follow Combine] API call failed with exception: {res}")
            failed_calls.append({"message": f"Lỗi hệ thống: {res}", "api_name": "Unknown"})
        elif isinstance(res, dict):
            if res.get("success"):
                successful_calls.append(res)
                if res.get("api_name") == "OldAPI":
                    old_api_success_result = res # Ưu tiên lưu kết quả thành công của API cũ
            else:
                failed_calls.append(res)
        else:
             logger.error(f"[Follow Combine] Unexpected result type from gather: {type(res)}")
             failed_calls.append({"message": f"Lỗi hệ thống: Kiểu dữ liệu không mong đợi {type(res)}", "api_name": "Unknown"})


    if successful_calls:
        combined_result["success"] = True
        # Ưu tiên data và message từ API cũ nếu nó thành công
        if old_api_success_result:
            combined_result["data"] = old_api_success_result.get("data")
            # Tạo message tổng hợp
            success_msgs = [f"{c['api_name']}: {c['message']}" for c in successful_calls]
            fail_msgs = [f"{c['api_name']}: {c['message']}" for c in failed_calls]
            combined_result["message"] = "✅ Thành công! " + " | ".join(success_msgs)
            if fail_msgs: combined_result["message"] += " | ⚠️ Thất bại: " + " | ".join(fail_msgs)
        else:
            # Nếu chỉ API mới thành công, lấy message từ nó
            first_success = successful_calls[0]
            combined_result["data"] = first_success.get("data") # Data từ API mới có thể là text
            combined_result["message"] = f"✅ Thành công! ({first_success['api_name']}: {first_success['message']})"
            fail_msgs = [f"{c['api_name']}: {c['message']}" for c in failed_calls]
            if fail_msgs: combined_result["message"] += " | ⚠️ Thất bại: " + " | ".join(fail_msgs)

    else: # Tất cả đều thất bại
        combined_result["success"] = False
        fail_msgs = [f"{c['api_name']}: {c['message']}" for c in failed_calls]
        combined_result["message"] = "❌ Thất bại! " + " | ".join(fail_msgs) if fail_msgs else "Tất cả API đều lỗi."
        # Có thể lấy data từ lỗi đầu tiên nếu có
        if failed_calls and failed_calls[0].get("data"):
             combined_result["data"] = failed_calls[0].get("data")


    logger.info(f"[Follow Combined Result @{target_username}] Success: {combined_result['success']}, Message: {combined_result['message']}")
    return combined_result

# --- Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start (Hoạt động mọi nơi)."""
    if not update or not update.message: return
    user = update.effective_user
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id

    act_h = ACTIVATION_DURATION_SECONDS // 3600
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
           f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n"
           f"<i>Một số lệnh như /getkey, /muatt chỉ hoạt động trong nhóm chính.</i>\n\n"
           f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
           f"   1️⃣ Vào <a href='https://t.me/'>Nhóm Chính</a>, dùng <code>/getkey</code> để nhận link.\n" # Cần link group chính
           f"   2️⃣ Truy cập link, làm theo các bước để lấy Key.\n"
           f"       (Ví dụ: <code>Dinotool-ABC123XYZ</code>).\n"
           f"   3️⃣ Quay lại Nhóm Chính, dùng <code>/nhapkey &lt;key_cua_ban&gt;</code>.\n"
           f"   4️⃣ Sau khi kích hoạt, bạn có thể dùng <code>/tim</code> và <code>/fl</code> trong <b>{act_h} giờ</b> (ở bất kỳ nhóm nào bot có mặt hoặc chat riêng).\n\n"
           f"👑 <b>Nâng cấp VIP:</b>\n"
           f"   » Xem chi tiết và hướng dẫn với lệnh <code>/muatt</code> (chỉ trong Nhóm Chính).\n"
           f"   » Thành viên VIP có thể dùng <code>/treo</code>, <code>/dungtreo</code>, không cần lấy key và có nhiều ưu đãi khác.\n\n"
           f"ℹ️ <b>Danh sách lệnh:</b>\n"
           f"   » Gõ <code>/lenh</code> để xem tất cả các lệnh và trạng thái của bạn.\n\n"
           f"💬 Cần hỗ trợ? Liên hệ Admin trong nhóm chính.\n"
           f"<i>Bot được phát triển bởi <a href='https://t.me/dinotool'>DinoTool</a></i>") # Sửa link nếu cần

    try:
        await update.message.reply_html(msg, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start message to {user.id} in chat {chat_id}: {e}")

async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user (Hoạt động mọi nơi)."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type

    user_id = user.id
    user_id_str = str(user_id)
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60

    is_vip = is_user_vip(user_id)
    is_key_active = is_user_activated_by_key(user_id)
    can_use_std_features = is_vip or is_key_active

    # --- Thông tin User ---
    status_lines = []
    status_lines.append(f"👤 <b>Người dùng:</b> {user.mention_html()} (<code>{user_id}</code>)")

    if is_vip:
        vip_data = vip_users.get(user_id_str, {})
        expiry_ts = vip_data.get("expiry")
        limit = vip_data.get("limit", "?")
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError): pass
        status_lines.append(f"👑 <b>Trạng thái:</b> VIP ✨ (Hết hạn: {expiry_str}, Giới hạn treo: {limit} users)")
    elif is_key_active:
        expiry_ts = activated_users.get(user_id_str)
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError): pass
        status_lines.append(f"🔑 <b>Trạng thái:</b> Đã kích hoạt (Key) (Hết hạn: {expiry_str})")
    else:
        status_lines.append("▫️ <b>Trạng thái:</b> Thành viên thường")

    status_lines.append(f"⚡️ <b>Quyền dùng /tim, /fl:</b> {'✅ Có thể' if can_use_std_features else '❌ Chưa thể (Cần VIP/Key)'}")
    if is_vip:
        current_treo_count = len(active_treo_tasks.get(user_id_str, {}))
        vip_limit = get_vip_limit(user_id)
        status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có thể (Đang treo: {current_treo_count}/{vip_limit} users)")
    else:
         status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Chỉ dành cho VIP")

    # --- Danh sách lệnh ---
    cmd_lines = ["\n\n📜=== <b>DANH SÁCH LỆNH</b> ===📜"]

    cmd_lines.append("\n<b><u>🔑 Lệnh Miễn Phí (Kích hoạt Key - Chỉ trong Nhóm Chính):</u></b>")
    cmd_lines.append(f"  <code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần, Key hiệu lực {key_exp_h}h)")
    cmd_lines.append(f"  <code>/nhapkey &lt;key&gt;</code> - Kích hoạt tài khoản (Sử dụng {act_h}h)")

    cmd_lines.append("\n<b><u>❤️ Lệnh Tăng Tương Tác (Cần VIP/Key - Hoạt động mọi nơi):</u></b>")
    cmd_lines.append(f"  <code>/tim &lt;link_video&gt;</code> - Tăng tim cho video TikTok (⏳ {tf_cd_m}p/lần)")
    cmd_lines.append(f"  <code>/fl &lt;username&gt;</code> - Tăng follow cho tài khoản TikTok (⏳ {tf_cd_m}p/user)")

    cmd_lines.append("\n<b><u>👑 Lệnh VIP:</u></b>")
    cmd_lines.append(f"  <code>/muatt</code> - Thông tin và hướng dẫn mua VIP (Chỉ trong Nhóm Chính)")
    cmd_lines.append(f"  <code>/treo &lt;username&gt;</code> - Tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút (Hoạt động mọi nơi, dùng slot)")
    cmd_lines.append(f"  <code>/dungtreo &lt;username&gt;</code> - Dừng treo cho một tài khoản (Hoạt động mọi nơi)")

    cmd_lines.append("\n<b><u>ℹ️ Lệnh Chung (Hoạt động mọi nơi):</u></b>")
    cmd_lines.append(f"  <code>/start</code> - Tin nhắn chào mừng")
    cmd_lines.append(f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này")
    cmd_lines.append(f"  <code>/tt &lt;username&gt;</code> - Xem thông tin tài khoản TikTok")

    # Chỉ hiển thị lệnh Admin cho Admin
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin (Hoạt động mọi nơi):</u></b>")
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;days&gt;</code> - Thêm ngày VIP (VD: /addtt 12345 30)")
        cmd_lines.append(f"  <code>/removett &lt;user_id&gt;</code> - Xóa VIP (Chưa implement)") # Ví dụ
        cmd_lines.append(f"  <code>/stats</code> - Xem thống kê bot (chưa implement)") # Ví dụ

    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng. Một số lệnh chỉ dùng được trong nhóm chính.</i>")

    help_text = "\n".join(status_lines + cmd_lines)

    try:
        # Không xóa lệnh gốc của user ở đây vì có thể dùng ở chat riêng
        # await delete_user_message(update, context)
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim (Hoạt động mọi nơi, cần VIP/Key)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # --- Check quyền sử dụng ---
    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt tài khoản bằng key</b> để sử dụng lệnh <code>/tim</code>!\n\n"
                   f"➡️ Vào <a href='https://t.me/'>Nhóm Chính</a>, dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n" # Cần link group chính
                   f"👑 Hoặc: <code>/muatt</code> trong Nhóm Chính để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        # Không xóa lệnh gốc ở đây vì có thể ở chat riêng
        # await delete_user_message(update, context, original_message_id)
        return

    # --- Check Cooldown ---
    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage = float(last_usage_str)
            elapsed = current_time - last_usage
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, bạn cần đợi <b>{rem_time:.0f}</b> giây nữa để tiếp tục dùng <code>/tim</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                # await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /tim user {user_id}. Resetting.")
             if user_id_str in user_tim_cooldown:
                 del user_tim_cooldown[user_id_str]
                 save_data()

    # --- Parse Arguments ---
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Bạn chưa nhập link video.\n"
                   "<b>Cú pháp đúng:</b> <code>/tim https://tiktok.com/...</code>")
    elif "tiktok.com" not in args[0] or not args[0].startswith(("http://", "https://")):
        err_txt = f"⚠️ Link <code>{html.escape(args[0])}</code> không hợp lệ. Phải là link video TikTok."
    else:
        video_url = args[0]

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        # await delete_user_message(update, context, original_message_id)
        return

    # --- API Key Check ---
    if not API_KEY:
        logger.error(f"Missing API_KEY for /tim command triggered by user {user_id}")
        # await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key cho chức năng này. Vui lòng báo Admin.", duration=20)
        return

    # --- Call API ---
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")

    processing_msg = None
    final_response_text = ""
    is_success = False

    try:
        # Gửi tin nhắn chờ (không xóa lệnh gốc nếu ở chat riêng)
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
        # if update.effective_chat.id == ALLOWED_GROUP_ID: # Chỉ xóa lệnh gốc trong group
        #     await delete_user_message(update, context, original_message_id)

        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim Caller'})
            content_type = resp.headers.get("content-type","").lower()
            response_text_for_debug = ""
            try:
                response_text_for_debug = await resp.aread()
                response_text_for_debug = response_text_for_debug.decode('utf-8', errors='replace')[:500]
            except Exception: pass

            logger.debug(f"/tim API response status: {resp.status_code}, content-type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/tim API response data: {data}")
                    if data.get("success"):
                        user_tim_cooldown[user_id_str] = time.time()
                        save_data()
                        is_success = True
                        d = data.get("data", {})
                        a = html.escape(str(d.get("author", "?")))
                        ct = html.escape(str(d.get("create_time", "?")))
                        v = html.escape(str(d.get("video_url", video_url)))
                        db = html.escape(str(d.get('digg_before', '?')))
                        di = html.escape(str(d.get('digg_increased', '?')))
                        da = html.escape(str(d.get('digg_after', '?')))

                        final_response_text = (
                            f"🎉 <b>Tăng Tim Thành Công!</b> ❤️\n"
                            f"👤 Cho: {user.mention_html()}\n\n"
                            f"📊 <b>Thông tin Video:</b>\n"
                            f"🎬 <a href='{v}'>Link Video</a>\n"
                            f"✍️ Tác giả: <code>{a}</code>\n"
                            f"👍 Trước: <code>{db}</code> ➜ 💖 Tăng: <code>+{di}</code> ➜ ✅ Sau: <code>{da}</code>"
                        )
                    else:
                        api_msg = data.get('message', 'Không rõ lý do từ API')
                        logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
                        final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {user.mention_html()}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"
                except json.JSONDecodeError as e_json:
                    logger.error(f"/tim API response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API</b>\n👤 Cho: {user.mention_html()}\nℹ️ API không trả về JSON hợp lệ."
            else:
                logger.error(f"/tim API call HTTP error or wrong content type. Status: {resp.status_code}, Type: {content_type}. Text: {response_text_for_debug}...")
                final_response_text = f"❌ <b>Lỗi Kết Nối API Tăng Tim</b>\n👤 Cho: {user.mention_html()}\nℹ️ Mã lỗi: {resp.status_code}, Loại: {html.escape(content_type)}. Vui lòng thử lại sau."

    except httpx.TimeoutException:
        logger.warning(f"/tim API call timeout for user {user_id}")
        final_response_text = f"❌ <b>Lỗi Timeout</b>\n👤 Cho: {user.mention_html()}\nℹ️ API tăng tim không phản hồi kịp thời. Vui lòng thử lại sau."
    except httpx.RequestError as e_req:
        logger.error(f"/tim API call network error for user {user_id}: {e_req}", exc_info=False)
        final_response_text = f"❌ <b>Lỗi Mạng</b>\n👤 Cho: {user.mention_html()}\nℹ️ Không thể kết nối đến API tăng tim. Kiểm tra lại mạng hoặc thử lại sau."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /tim command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ <b>Lỗi Hệ Thống Bot</b>\n👤 Cho: {user.mention_html()}\nℹ️ Đã xảy ra lỗi không mong muốn. Vui lòng báo Admin."
    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: Message not found (maybe deleted?)")
                 else: logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except Forbidden as e_edit: logger.warning(f"Bot lacks permission to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except TelegramError as e_edit: logger.error(f"Telegram error editing /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             logger.warning(f"Processing message for /tim user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /tim message for user {user_id} after processing msg was None: {e_send}")


# --- /fl Command ---
async def process_fl_request_background(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id_str: str,
    target_username: str,
    processing_msg_id: int,
    invoking_user_mention: str
):
    """Hàm chạy nền xử lý API follow và cập nhật kết quả."""
    logger.info(f"[BG Task /fl] Starting for user {user_id_str} -> @{target_username}")
    # Gọi hàm execute_follow_request mới
    api_result = await execute_follow_request(user_id_str, target_username, context.bot.token)
    success = api_result["success"]
    api_message = api_result["message"]
    api_data = api_result["data"] # Ưu tiên data từ API cũ nếu thành công
    final_response_text = ""

    # --- Xây dựng khối thông tin người dùng (chỉ dùng data từ API cũ nếu có) ---
    user_info_block = ""
    if api_data and isinstance(api_data, dict) and "name" in api_data: # Kiểm tra là dict và có trường của API cũ
        name = html.escape(str(api_data.get("name", "?")))
        tt_username_from_api = api_data.get("username")
        tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
        tt_user_id = html.escape(str(api_data.get("user_id", "?")))
        khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
        avatar = api_data.get("avatar", "")
        create_time = html.escape(str(api_data.get("create_time", "?")))

        user_info_lines = []
        user_info_lines.append(f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)")
        if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
        if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
        if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
        if avatar and avatar.startswith("http"): user_info_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>")

        if user_info_lines: user_info_block = "\n".join(user_info_lines) + "\n"

    # --- Xây dựng khối thông tin follower (chỉ dùng data từ API cũ nếu có) ---
    follower_info_block = ""
    if api_data and isinstance(api_data, dict) and "followers_before" in api_data:
        f_before = html.escape(str(api_data.get("followers_before", "?")))
        f_add = html.escape(str(api_data.get("followers_add", "?")))
        f_after = html.escape(str(api_data.get("followers_after", "?")))

        if f_before != "?" or f_add != "?" or f_after != "?":
            follower_lines = ["📈 <b>Số lượng Follower:</b>"]
            if f_before != "?": follower_lines.append(f"   Trước: <code>{f_before}</code>")
            if f_add != "?" and f_add != "0": follower_lines.append(f"   Tăng:   <b><code>+{f_add}</code></b> ✨")
            elif f_add == "0": follower_lines.append(f"   Tăng:   <code>+{f_add}</code>")
            if f_after != "?": follower_lines.append(f"   Sau:    <code>{f_after}</code>")
            follower_info_block = "\n".join(follower_lines)

    # --- Tạo nội dung phản hồi cuối cùng ---
    if success:
        current_time = time.time()
        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = current_time
        save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")

        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n"
            f"✨ Cho: {invoking_user_mention}\n"
            f"💬 Kết quả API: <i>{html.escape(api_message)}</i>\n\n" # Hiển thị message tổng hợp
            f"{user_info_block}"
            f"{follower_info_block}"
        )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n"
            f"👤 Cho: {invoking_user_mention}\n"
            f"🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message)}</i>\n\n"
            f"{user_info_block}" # Vẫn hiển thị thông tin user nếu API cũ trả về
        )
        if "đợi" in api_message.lower() and ("phút" in api_message.lower() or "giây" in api_message.lower()):
            final_response_text += f"\n\n<i>ℹ️ API yêu cầu chờ đợi. Vui lòng thử lại sau khoảng thời gian được nêu.</i>"

    # --- Chỉnh sửa tin nhắn chờ ---
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=processing_msg_id, text=final_response_text,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
        logger.info(f"[BG Task /fl] Edited message {processing_msg_id} for user {user_id_str} -> @{target_username}")
    except BadRequest as e:
        if "Message is not modified" in str(e): pass
        elif "message to edit not found" in str(e).lower(): logger.warning(f"[BG Task /fl] Message {processing_msg_id} not found for editing.")
        elif "Can't parse entities" in str(e) or "nested" in str(e).lower():
             logger.warning(f"[BG Task /fl] HTML parse error editing {processing_msg_id}. Falling back to plain text.")
             try:
                 plain_text = re.sub('<[^<]+?>', '', final_response_text)
                 plain_text = html.unescape(plain_text)
                 plain_text += "\n\n(Lỗi hiển thị định dạng)"
                 await context.bot.edit_message_text(chat_id, processing_msg_id, plain_text[:4096], disable_web_page_preview=True)
             except Exception as pt_edit_err: logger.error(f"[BG Task /fl] Failed plain text fallback edit for {processing_msg_id}: {pt_edit_err}")
        else: logger.error(f"[BG Task /fl] BadRequest editing msg {processing_msg_id}: {e}")
    except Forbidden as e: logger.error(f"[BG Task /fl] Bot lacks permission to edit msg {processing_msg_id}: {e}")
    except TelegramError as e: logger.error(f"[BG Task /fl] Telegram error editing msg {processing_msg_id}: {e}")
    except Exception as e: logger.error(f"[BG Task /fl] Unexpected error editing msg {processing_msg_id}: {e}", exc_info=True)

async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /fl (Hoạt động mọi nơi, cần VIP/Key)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    invoking_user_mention = user.mention_html()
    current_time = time.time()
    original_message_id = update.message.message_id

    # --- Check quyền sử dụng ---
    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để sử dụng lệnh <code>/fl</code>!\n\n"
                   f"➡️ Vào <a href='https://t.me/'>Nhóm Chính</a>, dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n" # Cần link group chính
                   f"👑 Hoặc: <code>/muatt</code> trong Nhóm Chính để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$"

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập username TikTok.\n"
                   "<b>Cú pháp đúng:</b> <code>/fl username</code> (không cần @)")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"(Chứa chữ, số, '.', '_'; dài 2-24 ký tự; không bắt đầu/kết thúc bằng '.')")
        else:
            target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        return

    # --- Check Cooldown cho target cụ thể ---
    if target_username:
        user_cds = user_fl_cooldown.get(user_id_str, {})
        last_usage_str = user_cds.get(target_username)
        if last_usage_str:
            try:
                last_usage = float(last_usage_str)
                elapsed = current_time - last_usage
                if elapsed < TIM_FL_COOLDOWN_SECONDS:
                     rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                     cd_msg = f"⏳ {invoking_user_mention}, bạn cần đợi <b>{rem_time:.0f} giây</b> nữa để tiếp tục dùng <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                     await send_temporary_message(update, context, cd_msg, duration=15)
                     return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /fl user {user_id} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]
                     save_data()

    # --- Gửi tin nhắn chờ và chạy nền ---
    processing_msg = None
    try:
        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý..."
        )
        # if update.effective_chat.id == ALLOWED_GROUP_ID: # Chỉ xóa lệnh gốc trong group chính
        #     await delete_user_message(update, context, original_message_id)

        if processing_msg and target_username:
            logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
            context.application.create_task(
                process_fl_request_background(
                    context=context, chat_id=chat_id, user_id_str=user_id_str,
                    target_username=target_username, processing_msg_id=processing_msg.message_id,
                    invoking_user_mention=invoking_user_mention
                ),
                name=f"fl_bg_{user_id_str}_{target_username}"
            )
        elif not target_username:
             logger.error(f"Target username became None before scheduling background task for /fl user {user_id}.")
             if processing_msg: await context.bot.edit_message_text(chat_id, processing_msg.message_id, "❌ Lỗi: Username không hợp lệ.")
        elif not processing_msg:
             logger.error(f"Could not send processing message for /fl @{target_username}, cannot schedule background task.")

    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Failed to send processing message or schedule task for /fl @{target_username}: {e}")
        # if update.effective_chat.id == ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id)
    except Exception as e:
         logger.error(f"Unexpected error in fl_command for user {user_id} target @{target_username}: {e}", exc_info=True)
         # if update.effective_chat.id == ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id)


# --- Lệnh /getkey (Chỉ trong Group Chính)---
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/getkey command used outside allowed group ({chat_id}) by user {user_id}. Sending temporary message.")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh sai chỗ
        await send_temporary_message(update, context, "Lệnh <code>/getkey</code> chỉ có thể sử dụng trong nhóm chính.", duration=15, reply=False)
        return

    # --- Check Cooldown ---
    last_usage_str = user_getkey_cooldown.get(user_id_str)
    if last_usage_str:
         try:
             last_usage = float(last_usage_str)
             elapsed = current_time - last_usage
             if elapsed < GETKEY_COOLDOWN_SECONDS:
                remaining = GETKEY_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, bạn cần đợi <b>{remaining:.0f} giây</b> nữa để tiếp tục dùng <code>/getkey</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
         except (ValueError, TypeError):
              logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /getkey user {user_id}. Resetting.")
              if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]; save_data()

    # --- Tạo Key và Link ---
    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY) > 6 else "***", "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_saved_to_dict = False

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id)

        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {"user_id_generator": user_id, "generation_time": generation_time, "expiry_time": expiry_time, "used_by": None, "activation_time": None}
        key_saved_to_dict = True
        logger.info(f"Key {generated_key} temporarily stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)
            response_content_type = response.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                 response_text_for_debug = await response.aread()
                 response_text_for_debug = response_text_for_debug.decode('utf-8', errors='replace')[:500]
            except Exception: pass

            logger.debug(f"Shortener API response status: {response.status_code}, content-type: {response_content_type}")

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time()
                        save_data() # Lưu key và cooldown mới
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}. Key {generated_key} confirmed.")
                        final_response_text = (
                            f"🚀 <b>Link Lấy Key Của Bạn ({user.mention_html()}):</b>\n\n"
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"📝 <b>Hướng dẫn:</b>\n"
                            f"   1️⃣ Click vào link trên.\n"
                            f"   2️⃣ Làm theo các bước trên trang web để nhận Key (VD: <code>Dinotool-ABC123XYZ</code>).\n"
                            f"   3️⃣ Copy Key đó và quay lại đây.\n"
                            f"   4️⃣ Gửi lệnh: <code>/nhapkey &lt;key_ban_vua_copy&gt;</code>\n\n"
                            f"⏳ <i>Key chỉ có hiệu lực để nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>. Hãy nhập sớm!</i>"
                        )
                    else:
                        api_message = response_data.get("message", "Lỗi không xác định từ API rút gọn link.")
                        logger.error(f"Shortener API returned error for user {user_id}. Status: {status}, Message: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Khi Tạo Link:</b>\n<code>{html.escape(str(api_message))}</code>\nVui lòng thử lại sau hoặc báo Admin."
                        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to shortener API error.")
                except json.JSONDecodeError:
                    logger.error(f"Shortener API Status 200 but JSON decode failed. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API:</b> Máy chủ rút gọn link trả về dữ liệu không hợp lệ. Vui lòng thử lại sau."
                    if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to JSON decode error.")
            else:
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Mã: {response.status_code}). Vui lòng thử lại sau hoặc báo Admin."
                 if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to HTTP error {response.status_code}.")

    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout during /getkey for user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> Máy chủ tạo link không phản hồi kịp thời. Vui lòng thử lại sau."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to timeout.")
    except httpx.ConnectError as e_connect:
        logger.error(f"Shortener API connection error during /getkey for user {user_id}: {e_connect}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối đến máy chủ tạo link. Vui lòng kiểm tra mạng hoặc thử lại sau."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to connection error.")
    except httpx.RequestError as e_req:
        logger.error(f"Shortener API network error during /getkey for user {user_id}: {e_req}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link. Vui lòng thử lại sau."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to network error.")
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /getkey command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key. Vui lòng báo Admin."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to unexpected error.")
    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=False
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: Message not found.")
                 else: logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except Forbidden as e_edit: logger.warning(f"Bot lacks permission to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except TelegramError as e_edit: logger.error(f"Telegram error editing /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
             except Exception as e_send: logger.error(f"Failed to send final /getkey message for user {user_id} after processing msg was None: {e_send}")

# --- Lệnh /nhapkey (Chỉ trong Group Chính)---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/nhapkey command used outside allowed group ({chat_id}) by user {user_id}. Sending temporary message.")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "Lệnh <code>/nhapkey</code> chỉ có thể sử dụng trong nhóm chính.", duration=15, reply=False)
        return

    # --- Parse Input ---
    args = context.args
    submitted_key = None
    err_txt = ""
    key_prefix = "Dinotool-"
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]+$")

    if not args: err_txt = ("⚠️ Bạn chưa nhập key.\n<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1: err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        if not key_format_regex.match(key_input):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\n"
                        f"Key phải bắt đầu bằng <code>{key_prefix}</code> và theo sau là chữ IN HOA hoặc số.")
        else: submitted_key = key_input

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Validate Key Logic ---
    logger.info(f"User {user_id} attempting key activation with: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key)
    final_response_text = ""
    activation_success = False

    if not key_data:
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' not found.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại. Vui lòng kiểm tra lại hoặc dùng <code>/getkey</code> để lấy key mới."
    elif key_data.get("used_by") is not None:
        used_by_id = key_data["used_by"]
        activation_time_ts = key_data.get("activation_time")
        used_time_str = "không rõ thời gian"
        if activation_time_ts:
            try: used_time_str = f"lúc {datetime.fromtimestamp(float(activation_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass
        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by themself {used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi ({used_time_str})."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by user ({used_by_id}) {used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng {used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = "không rõ thời gian"
        if expiry_time_ts:
            try: expiry_time_str = f"vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired {expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng {expiry_time_str}. Vui lòng dùng <code>/getkey</code> để lấy key mới."
        if submitted_key in valid_keys: del valid_keys[submitted_key]; save_data(); logger.info(f"Removed expired key {submitted_key} upon activation attempt.")
    else:
        try:
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts
            save_data()
            expiry_dt = datetime.fromtimestamp(activation_expiry_ts)
            expiry_str = expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
            activation_success = True
            logger.info(f"Key '{submitted_key}' successfully activated by user {user_id}. Expires at {expiry_str}.")
            final_response_text = (f"✅ <b>Kích Hoạt Key Thành Công!</b>\n\n"
                                   f"👤 Người dùng: {user.mention_html()}\n"
                                   f"🔑 Key đã nhập: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Bạn có thể sử dụng <code>/tim</code> và <code>/fl</code> ở mọi nơi.\n"
                                   f"⏳ Quyền lợi sẽ hết hạn vào lúc: <b>{expiry_str}</b> (sau {ACTIVATION_DURATION_SECONDS // 3600} giờ)."
                                 )
        except Exception as e_activate:
             logger.error(f"Unexpected error during key activation for user {user_id} key {submitted_key}: {e_activate}", exc_info=True)
             final_response_text = f"❌ Lỗi hệ thống khi kích hoạt key <code>{html.escape(submitted_key)}</code>. Vui lòng báo Admin."
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None; valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users: del activated_users[user_id_str]

    # --- Gửi phản hồi cuối cùng ---
    await delete_user_message(update, context, original_message_id)
    try:
        await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
         logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")

# --- Lệnh /muatt (Chỉ trong Group Chính) ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP và bắt đầu theo dõi ảnh bill."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    original_message_id = update.message.message_id

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/muatt command used outside allowed group ({chat_id}) by user {user.id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "Lệnh <code>/muatt</code> chỉ có thể sử dụng trong nhóm chính.", duration=15, reply=False)
        return

    user_id = user.id
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

    # --- Xây dựng nội dung tin nhắn ---
    text_lines = []
    text_lines.append("👑 <b>Thông Tin Nâng Cấp VIP - DinoTool</b> 👑")
    text_lines.append("\nTrở thành VIP để mở khóa các tính năng độc quyền như <code>/treo</code>, không cần lấy key và nhiều hơn nữa!")
    text_lines.append("\n💎 <b>Các Gói VIP Hiện Có:</b>")
    for days, info in VIP_PRICES.items():
        text_lines.append(f"\n⭐️ <b>Gói {info['duration_days']} Ngày:</b>")
        text_lines.append(f"   - 💰 Giá: <b>{info['price']}</b>")
        text_lines.append(f"   - ⏳ Thời hạn: {info['duration_days']} ngày")
        text_lines.append(f"   - 🚀 Treo tối đa: <b>{info['limit']} tài khoản</b> TikTok cùng lúc")
    text_lines.append("\n🏦 <b>Thông tin thanh toán:</b>")
    text_lines.append(f"   - Ngân hàng: <b>{BANK_NAME}</b>")
    text_lines.append(f"   - STK: <code>{BANK_ACCOUNT}</code> (👈 Click để copy)")
    text_lines.append(f"   - Tên chủ TK: <b>{ACCOUNT_NAME}</b>")
    text_lines.append("\n📝 <b>Nội dung chuyển khoản (Quan trọng!):</b>")
    text_lines.append(f"   » Chuyển khoản với nội dung <b>CHÍNH XÁC</b> là:")
    text_lines.append(f"   » <code>{payment_note}</code> (👈 Click để copy)")
    text_lines.append(f"   <i>(Sai nội dung có thể khiến giao dịch xử lý chậm)</i>")
    text_lines.append("\n📸 <b>Sau Khi Chuyển Khoản Thành Công:</b>")
    text_lines.append(f"   1️⃣ Chụp ảnh màn hình biên lai (bill) giao dịch.")
    text_lines.append(f"   2️⃣ Gửi ảnh đó <b>vào nhóm chat này</b> trong vòng <b>{PHOTO_BILL_WINDOW_SECONDS // 3600} giờ</b> tới.")
    text_lines.append(f"   3️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin để xác nhận (ảnh của bạn sẽ bị xóa khỏi nhóm).")
    text_lines.append(f"   4️⃣ Admin sẽ kiểm tra và kích hoạt VIP cho bạn.")
    text_lines.append("\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️")
    text = "\n".join(text_lines)

    # --- Gửi tin nhắn kèm ảnh QR và Bắt đầu theo dõi ---
    await delete_user_message(update, context, original_message_id)
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=text, parse_mode=ParseMode.HTML)
        # Bắt đầu theo dõi user này
        muatt_users_tracking[user_id] = time.time()
        logger.info(f"User {user_id} used /muatt. Started tracking for photo bill for {PHOTO_BILL_WINDOW_SECONDS} seconds.")
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending /muatt photo+caption to chat {chat_id}: {e}")
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            # Vẫn bắt đầu theo dõi nếu gửi text thành công
            muatt_users_tracking[user_id] = time.time()
            logger.info(f"User {user_id} used /muatt (text fallback). Started tracking for photo bill.")
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")
    except Exception as e_unexp:
        logger.error(f"Unexpected error sending /muatt command to chat {chat_id}: {e_unexp}", exc_info=True)

# --- Xử lý nhận ảnh bill (Chỉ trong Group Chính và user đang được theo dõi) ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh trong group chính từ user đang được theo dõi."""
    if not update or not update.message: return
    # Chỉ xử lý trong group chính
    if update.effective_chat.id != ALLOWED_GROUP_ID: return
    # Bỏ qua command
    if update.message.text and update.message.text.startswith('/'): return

    is_photo = bool(update.message.photo)
    is_image_document = bool(update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return

    user = update.effective_user
    chat = update.effective_chat
    message_id = update.message.message_id
    if not user or not chat: return

    user_id = user.id
    current_time = time.time()

    # --- Kiểm tra xem user có đang được theo dõi không ---
    tracked_time = muatt_users_tracking.get(user_id)
    if tracked_time and (current_time - tracked_time < PHOTO_BILL_WINDOW_SECONDS):
        logger.info(f"Bill received from tracked user {user_id} in group {chat.id}. Forwarding to admin {ADMIN_USER_ID}.")

        # --- Dừng theo dõi user này ---
        del muatt_users_tracking[user_id]
        logger.info(f"Stopped tracking user {user_id} for photo bill.")

        # --- Tạo caption cho admin ---
        forward_caption_lines = []
        forward_caption_lines.append(f"📄 <b>Bill VIP Nhận Được</b>")
        forward_caption_lines.append(f"👤 <b>Từ User:</b> {user.mention_html()} (<code>{user.id}</code>)")
        forward_caption_lines.append(f"👥 <b>Trong Group:</b> {html.escape(chat.title or str(chat.id))} (<code>{chat.id}</code>)")
        try:
             message_link = update.message.link
             if message_link: forward_caption_lines.append(f"🔗 <b>Link Tin Nhắn Gốc:</b> <a href='{message_link}'>Click vào đây</a>")
        except AttributeError: pass
        original_caption = update.message.caption or update.message.text
        if original_caption: forward_caption_lines.append(f"\n💬 <b>Caption gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")
        forward_caption = "\n".join(forward_caption_lines)

        # --- Chuyển tiếp và xóa tin nhắn gốc ---
        try:
            # 1. Chuyển tiếp tin nhắn gốc
            await context.bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=chat.id, message_id=message_id)
            # 2. Gửi thông tin chi tiết
            await context.bot.send_message(chat_id=ADMIN_USER_ID, text=forward_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            logger.info(f"Successfully forwarded bill message {message_id} and sent info to admin {ADMIN_USER_ID}.")

            # 3. Xóa tin nhắn ảnh gốc khỏi nhóm
            await delete_user_message(update, context, message_id)
            logger.info(f"Deleted original bill message {message_id} from group {chat.id}.")

        except Forbidden:
            logger.error(f"Bot cannot forward/send message to admin {ADMIN_USER_ID} or delete message in group {chat.id}.")
            try:
                 error_admin_msg = f"⚠️ {user.mention_html()}, không thể gửi ảnh của bạn đến Admin hoặc xóa ảnh gốc (Bot thiếu quyền). Vui lòng liên hệ Admin trực tiếp."
                 await send_temporary_message(update, context, error_admin_msg, duration=60)
            except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
            # Nếu lỗi, đặt lại tracking để user thử gửi lại? Hoặc không? -> Không đặt lại
        except TelegramError as e_fwd:
             logger.error(f"Telegram error processing bill message {message_id}: {e_fwd}")
             try:
                 error_admin_msg = f"⚠️ {user.mention_html()}, đã xảy ra lỗi khi xử lý ảnh của bạn. Vui lòng thử lại hoặc báo Admin."
                 await send_temporary_message(update, context, error_admin_msg, duration=60)
             except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
        except Exception as e:
            logger.error(f"Unexpected error processing bill: {e}", exc_info=True)
            try:
                 error_admin_msg = f"⚠️ {user.mention_html()}, lỗi hệ thống khi xử lý ảnh của bạn. Vui lòng báo Admin."
                 await send_temporary_message(update, context, error_admin_msg, duration=60)
            except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
    else:
        # Ảnh được gửi bởi user không được theo dõi hoặc ngoài thời gian chờ
        logger.debug(f"Ignoring photo from user {user_id} in group {chat.id} (not tracked or expired).")
        pass # Ảnh sẽ tồn tại bình thường trong nhóm

# --- Lệnh /addtt (Admin, hoạt động mọi nơi) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin, hoạt động mọi nơi)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat: return

    # --- Check Admin ---
    if admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id} in chat {chat.id}.")
        return

    # --- Parse Arguments ---
    args = context.args
    err_txt = None
    target_user_id = None
    days_to_add_input = None
    limit = None
    duration_days = None
    valid_days = list(VIP_PRICES.keys())

    if len(args) != 2:
        err_txt = f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;số_ngày&gt;</code>\n<b>Ví dụ:</b> <code>/addtt 123456789 {valid_days[0]}</code>"
    else:
        try: target_user_id = int(args[0]); assert target_user_id > 0
        except (ValueError, AssertionError): err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."
        if not err_txt:
            try:
                days_to_add_input = int(args[1])
                if days_to_add_input not in VIP_PRICES:
                    err_txt = f"⚠️ Số ngày không hợp lệ. Chỉ chấp nhận: <b>{', '.join(map(str, valid_days))}</b>."
                else:
                    vip_info = VIP_PRICES[days_to_add_input]
                    limit = vip_info["limit"]
                    duration_days = vip_info["duration_days"]
            except ValueError: err_txt = f"⚠️ Số ngày '<code>{html.escape(args[1])}</code>' không hợp lệ."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    # --- Cập nhật dữ liệu VIP ---
    target_user_id_str = str(target_user_id)
    current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time
    operation_type = "Nâng cấp lên"

    if current_vip_data:
         try:
             current_expiry = float(current_vip_data.get("expiry", 0))
             if current_expiry > current_time:
                 start_time = current_expiry
                 operation_type = "Gia hạn thêm"
                 logger.info(f"User {target_user_id_str} already VIP. Extending from {datetime.fromtimestamp(start_time).isoformat()}.")
             else: logger.info(f"User {target_user_id_str} was VIP but expired. Treating as new.")
         except (ValueError, TypeError): logger.warning(f"Invalid expiry data for user {target_user_id_str}. Treating as new.")

    new_expiry_ts = start_time + duration_days * 86400
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')

    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data()
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days} days. New expiry: {new_expiry_str}, Limit: {limit}")

    # --- Gửi thông báo ---
    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days} ngày VIP</b>!\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n"
                 f"✨ Gói: {duration_days} ngày\n"
                 f"⏳ Hạn mới: <b>{new_expiry_str}</b>\n"
                 f"🚀 Limit: <b>{limit} users</b>")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation to admin {admin_user.id} in chat {chat.id}: {e}")

    # Gửi thông báo cho người dùng vào group chính
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        user_mention = target_user_info.mention_html() if target_user_info else f"User ID <code>{target_user_id}</code>"
    except Exception as e_get_chat:
        logger.warning(f"Could not get chat info for target user {target_user_id}: {e_get_chat}. Using ID.")
        user_mention = f"User ID <code>{target_user_id}</code>"

    group_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\n"
                 f"Bạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"✨ Gói VIP: <b>{duration_days} ngày</b>\n"
                 f"⏳ Hạn sử dụng đến: <b>{new_expiry_str}</b>\n"
                 f"🚀 Giới hạn treo: <b>{limit} tài khoản</b>\n\n"
                 f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n"
                 f"(Dùng <code>/lenh</code> để xem lại trạng thái)")
    try:
        await context.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=group_msg, parse_mode=ParseMode.HTML)
        logger.info(f"Sent VIP notification to group {ALLOWED_GROUP_ID} for user {target_user_id}")
    except Exception as e_send_group:
        logger.error(f"Failed to send VIP notification to group {ALLOWED_GROUP_ID} for user {target_user_id}: {e_send_group}")
        try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} vào group {ALLOWED_GROUP_ID}. Lỗi: {e_send_group}")
        except Exception: pass

# --- Logic Treo ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE):
    """Vòng lặp chạy nền cho lệnh /treo."""
    user_id = int(user_id_str)
    task_name = f"treo_{user_id_str}_{target_username}"
    logger.info(f"[Treo Task Start] Task '{task_name}' started.")

    try:
        while True:
            # Check 1: Task còn active?
            current_task_in_dict = active_treo_tasks.get(user_id_str, {}).get(target_username)
            if current_task_in_dict is not asyncio.current_task():
                logger.warning(f"[Treo Task Stop] Task '{task_name}' seems replaced or removed. Stopping.")
                break

            # Check 2: User còn VIP?
            if not is_user_vip(user_id):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired")
                break

            # Thực hiện gọi API Follow (dùng hàm execute_follow_request mới)
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await execute_follow_request(user_id_str, target_username, context.bot.token)

            if api_result["success"]:
                gain = 0
                # Chỉ cập nhật stats nếu API cũ thành công và trả về data hợp lệ
                if api_result["data"] and isinstance(api_result["data"], dict) and "followers_add" in api_result["data"]:
                    try:
                        gain_str = str(api_result["data"].get("followers_add", "0"))
                        gain = int(gain_str)
                        if gain > 0:
                            # Sử dụng defaultdict đã được load
                            treo_stats.setdefault(user_id_str, defaultdict(int))[target_username] += gain
                            logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Current gain: {treo_stats[user_id_str][target_username]}")
                        else:
                             logger.info(f"[Treo Task Success] Task '{task_name}' successful but gain was {gain}. API Msg: {api_result['message']}")
                    except (ValueError, TypeError) as e_gain:
                         logger.warning(f"[Treo Task Stats] Task '{task_name}' could not parse gain '{api_result['data'].get('followers_add')}' from API data: {e_gain}")
                    except Exception as e_stats:
                         logger.error(f"[Treo Task Stats] Task '{task_name}' unexpected error processing stats: {e_stats}", exc_info=True)
                else:
                    logger.info(f"[Treo Task Success] Task '{task_name}' successful but no valid data for stats. API Msg: {api_result['message']}")
            else: # API Follow thất bại
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed. API Msg: {api_result['message']}")
                # Có thể dừng task nếu lỗi nghiêm trọng (VD: invalid username)
                # if "invalid username" in api_result['message'].lower(): # Logic ví dụ
                #    logger.error(f"[Treo Task Stop] Stopping task '{task_name}' due to invalid username.")
                #    await stop_treo_task(user_id_str, target_username, context, reason="Invalid Username from API")
                #    break

            # Chờ đợi
            sleep_duration = TREO_INTERVAL_SECONDS
            logger.debug(f"[Treo Task Sleep] Task '{task_name}' sleeping for {sleep_duration} seconds...")
            await asyncio.sleep(sleep_duration)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled.")
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Đảm bảo xóa khỏi dict nếu vẫn còn
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
             task_in_dict = active_treo_tasks[user_id_str].get(target_username)
             if task_in_dict is asyncio.current_task() and task_in_dict.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished task '{task_name}' from active tasks.")

# --- Lệnh /treo (VIP, hoạt động mọi nơi) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow (chỉ VIP, hoạt động mọi nơi)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # --- Check VIP ---
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho thành viên <b>VIP</b>.\nDùng <code>/muatt</code> trong nhóm chính để xem thông tin nâng cấp."
        await send_temporary_message(update, context, err_msg, duration=20)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$"

    if not args: err_txt = ("⚠️ Bạn chưa nhập username TikTok cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"(Chứa chữ, số, '.', '_'; dài 2-24; không bắt đầu/kết thúc bằng '.')")
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        return

    # --- Check Giới Hạn và Trạng Thái Treo ---
    if target_username:
        vip_limit = get_vip_limit(user_id)
        user_tasks = active_treo_tasks.get(user_id_str, {})
        current_treo_count = len(user_tasks)

        existing_task = user_tasks.get(target_username)
        if existing_task and not existing_task.done():
            logger.info(f"User {user_id} tried /treo @{target_username} (already running).")
            await send_temporary_message(update, context, f"⚠️ Đã đang treo cho <code>@{html.escape(target_username)}</code>.\nDùng <code>/dungtreo {target_username}</code> để dừng.", duration=20)
            return
        elif existing_task and existing_task.done():
             logger.warning(f"Found finished task for {user_id_str}->{target_username}. Cleaning up.")
             await stop_treo_task(user_id_str, target_username, context, reason="Cleanup before new /treo")
             user_tasks = active_treo_tasks.get(user_id_str, {}) # Lấy lại dict sau khi xóa
             current_treo_count = len(user_tasks)

        if current_treo_count >= vip_limit:
             logger.warning(f"User {user_id} reached treo limit ({current_treo_count}/{vip_limit}).")
             limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit}).\n"
                         f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot.")
             await send_temporary_message(update, context, limit_msg, duration=30)
             return

        # --- Bắt đầu Task Treo Mới ---
        try:
            app = context.application
            task = app.create_task(run_treo_loop(user_id_str, target_username, context), name=f"treo_{user_id_str}_{target_username}")
            active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
            logger.info(f"Successfully created treo task '{task.get_name()}' for user {user_id}")

            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n"
                           f"👤 Cho: {invoking_user_mention}\n"
                           f"🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {TREO_INTERVAL_SECONDS // 60} phút\n"
                           f"📊 Slot đã dùng: {current_treo_count + 1}/{vip_limit}")
            await update.message.reply_html(success_msg)
            # if update.effective_chat.id == ALLOWED_GROUP_ID: # Chỉ xóa lệnh gốc trong group chính
            #    await delete_user_message(update, context, original_message_id)

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>.", duration=20)
             # if update.effective_chat.id == ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id)

    else:
        logger.error(f"/treo command for user {user_id}: target_username None.")
        await send_temporary_message(update, context, "❌ Lỗi username.", duration=15)
        # if update.effective_chat.id == ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (VIP, hoạt động mọi nơi) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow (chỉ VIP, hoạt động mọi nơi)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # --- Check VIP (Cần VIP để dùng lệnh này chủ động) ---
    # Mặc dù task tự dừng khi hết VIP, lệnh này cần user là VIP để gọi
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/dungtreo</code> chỉ dành cho thành viên <b>VIP</b>."
        await send_temporary_message(update, context, err_msg, duration=20)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username_input = None
    target_username_clean = None
    err_txt = None

    if not args:
        user_tasks = active_treo_tasks.get(user_id_str, {})
        if not user_tasks: err_txt = ("⚠️ Bạn chưa nhập username cần dừng.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n<i>(Bạn không có tài khoản nào đang treo.)</i>")
        else:
             running_targets = [f"<code>@{html.escape(t)}</code>" for t in user_tasks.keys()]
             err_txt = (f"⚠️ Bạn cần chỉ định username muốn dừng.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                        f"<b>Đang treo:</b> {', '.join(running_targets)}")
    else:
        target_username_input = args[0].strip()
        target_username_clean = target_username_input.lstrip("@")
        if not target_username_clean: err_txt = "⚠️ Username không được để trống."

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=30)
        return

    # --- Dừng Task ---
    if target_username_clean:
        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        # --- Gửi Phản Hồi ---
        # if update.effective_chat.id == ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id)
        if stopped:
            vip_limit = get_vip_limit(user_id)
            current_treo_count = len(active_treo_tasks.get(user_id_str, {}))
            await update.message.reply_html(f"✅ Đã dừng treo follow tự động cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {current_treo_count}/{vip_limit})")
        else:
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy tác vụ treo nào đang chạy cho <code>@{html.escape(target_username_clean)}</code>.", duration=20)


# --- Job Thống Kê Follow Tăng (Gửi vào Group Chính) ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    logger.info(f"[Stats Job] Starting statistics report job. Current time: {current_time}, Last report: {last_stats_report_time}")

    # Dùng deepcopy để tránh ảnh hưởng đến dict gốc đang được cập nhật bởi các task treo
    try:
        stats_snapshot = {uid: targets.copy() for uid, targets in treo_stats.items() if targets}
    except Exception as e_copy:
        logger.error(f"[Stats Job] Error creating stats snapshot: {e_copy}. Aborting job run.", exc_info=True)
        return # Bỏ qua lần chạy này nếu không tạo được snapshot an toàn

    # Reset dữ liệu gốc *sau khi* đã có snapshot
    users_to_clear = list(treo_stats.keys())
    for user_id_str in users_to_clear:
         treo_stats[user_id_str] = defaultdict(int)
    last_stats_report_time = current_time
    save_data()
    logger.info(f"[Stats Job] Cleared current stats and updated last report time. Processing snapshot...")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data in snapshot. Skipping report.")
        return

    top_gainers = [] # (gain, user_id_str, target_username)
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        for target_username, gain in targets.items():
            if gain > 0:
                top_gainers.append((gain, user_id_str, target_username))
                total_gain_all += gain

    if not top_gainers:
        logger.info("[Stats Job] No positive gains in snapshot. Skipping report.")
        return

    top_gainers.sort(key=lambda x: x[0], reverse=True)

    report_lines = []
    interval_hours = TREO_STATS_INTERVAL_SECONDS / 3600
    report_lines.append(f"📊 <b>Thống Kê Tăng Follow (Trong {interval_hours:.0f} Giờ Qua)</b> 📊")
    report_lines.append(f"<i>(Tổng cộng: {total_gain_all} follow được tăng bởi các tài khoản đang treo)</i>")
    report_lines.append("\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>")

    num_top_to_show = 3
    displayed_count = 0
    user_mentions_cache = {}

    for gain, user_id_str, target_username in top_gainers[:num_top_to_show]:
        user_mention = user_mentions_cache.get(user_id_str)
        if not user_mention:
            try:
                user_info = await context.bot.get_chat(int(user_id_str))
                user_mention = user_info.mention_html() if user_info else f"User ID <code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed get mention for user {user_id_str}: {e_get_chat}")
                user_mention = f"User ID <code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention

        report_lines.append(f"  🏅 <b>+{gain} follow</b> cho <code>@{html.escape(target_username)}</code> (Treo bởi: {user_mention})")
        displayed_count += 1

    if not displayed_count: report_lines.append("  <i>Không có dữ liệu tăng follow đáng kể.</i>")
    report_lines.append(f"\n🕒 <i>Cập nhật mỗi {interval_hours:.0f} giờ.</i>")

    report_text = "\n".join(report_lines)
    try:
        # Luôn gửi vào group chính
        await context.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=report_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"[Stats Job] Sent statistics report to group {ALLOWED_GROUP_ID}.")
    except Exception as e:
        logger.error(f"[Stats Job] Failed to send statistics report to group {ALLOWED_GROUP_ID}: {e}")

    logger.info("[Stats Job] Statistics report job finished.")

# --- API Info TikTok ---
async def get_tiktok_info(username: str) -> dict | None:
    """Lấy thông tin user TikTok từ API."""
    api_url = INFO_API_URL_TEMPLATE.format(username=username)
    logger.info(f"[Info API] Requesting info for @{username} from {api_url}")
    try:
        async with httpx.AsyncClient(timeout=20.0, verify=True) as client: # Timeout ngắn hơn cho API info
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Info Caller'})
            resp.raise_for_status() # Ném lỗi nếu status code là 4xx hoặc 5xx

            content_type = resp.headers.get("content-type", "").lower()
            if "application/json" not in content_type:
                 logger.error(f"[Info API @{username}] Response OK but not JSON. Type: {content_type}")
                 return None # Hoặc trả về lỗi cụ thể

            data = resp.json()
            logger.debug(f"[Info API @{username}] Received data: {data}")
            return data

    except httpx.HTTPStatusError as e:
        logger.error(f"[Info API @{username}] HTTP Error: {e.response.status_code} for URL: {e.request.url}. Response: {e.response.text[:200]}")
        # API có thể trả về JSON lỗi, thử parse xem
        try: return e.response.json()
        except json.JSONDecodeError: return None
    except httpx.TimeoutException:
        logger.warning(f"[Info API @{username}] API timeout.")
        return None
    except httpx.RequestError as e:
        logger.error(f"[Info API @{username}] Network error: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"[Info API @{username}] Failed to decode JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"[Info API @{username}] Unexpected error: {e}", exc_info=True)
        return None

# --- Lệnh /tt (Hoạt động mọi nơi) ---
async def tt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lấy và hiển thị thông tin tài khoản TikTok."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return
    original_message_id = update.message.message_id

    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$" # Regex username TikTok

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập username TikTok.\n"
                   "<b>Cú pháp:</b> <code>/tt username</code> (không cần @)")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ."
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        return

    if target_username:
        processing_msg = None
        try:
            processing_msg = await update.message.reply_html(f"⏳ Đang lấy thông tin cho <code>@{html.escape(target_username)}</code>...")

            info_data = await get_tiktok_info(target_username)

            if not info_data:
                final_text = f"❌ Không thể lấy thông tin cho <code>@{html.escape(target_username)}</code>. API không phản hồi hoặc lỗi mạng."
            # Kiểm tra lỗi "Không tìm thấy user_id" mà API trả về
            elif isinstance(info_data, dict) and info_data.get("user_id") == "Không tìm thấy user_id":
                 logger.info(f"/tt: User @{target_username} not found by API.")
                 final_text = f"❌ Không tìm thấy tài khoản TikTok nào có username là <code>@{html.escape(target_username)}</code>."
            elif isinstance(info_data, dict):
                 logger.info(f"/tt: Successfully retrieved info for @{target_username}.")
                 # Format thông tin
                 uid = html.escape(str(info_data.get("unique_id", target_username)))
                 nick = html.escape(str(info_data.get("nickname", "N/A")))
                 fol = html.escape(str(info_data.get("followers", "N/A")))
                 flg = html.escape(str(info_data.get("following", "N/A")))
                 lik = html.escape(str(info_data.get("likes", "N/A"))) # Hoặc dùng 'heart' nếu API trả về cái đó
                 vid = html.escape(str(info_data.get("videos", "N/A")))
                 ver = "✅ Có" if info_data.get("verified") == "true" else "❌ Không"
                 reg = html.escape(str(info_data.get("region", "N/A")))
                 sig = html.escape(str(info_data.get("signature", "")))
                 pic = info_data.get("profile_pic", "")
                 link = info_data.get("tiktok_link", f"https://tiktok.com/@{uid}")

                 info_lines = []
                 info_lines.append(f"<b>Thông Tin Tài Khoản TikTok</b>")
                 if pic: info_lines.append(f"<a href='{html.escape(pic)}'>🖼️</a> <b>{nick}</b> (<code>@{uid}</code>)")
                 else: info_lines.append(f"👤 <b>{nick}</b> (<code>@{uid}</code>)")
                 info_lines.append(f"🔗 <a href='{html.escape(link)}'>Link TikTok</a>")
                 info_lines.append(f"👥 Follower: <b>{fol}</b> | Đang Follow: <b>{flg}</b>")
                 info_lines.append(f"❤️ Tổng tim: <b>{lik}</b> | 🎬 Video: <b>{vid}</b>")
                 info_lines.append(f"🌍 Khu vực: {reg} | Tích xanh: {ver}")
                 if sig: info_lines.append(f"📝 Tiểu sử: <i>{sig}</i>")

                 final_text = "\n".join(info_lines)
            else: # Trường hợp không mong muốn
                 logger.error(f"/tt: Unexpected data type received from get_tiktok_info for @{target_username}: {type(info_data)}")
                 final_text = f"❌ Lỗi không mong muốn khi xử lý thông tin cho <code>@{html.escape(target_username)}</code>."

            # Chỉnh sửa tin nhắn chờ
            if processing_msg:
                 await context.bot.edit_message_text(chat_id, processing_msg.message_id, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            else: # Nếu gửi tin nhắn chờ lỗi, gửi tin nhắn mới
                 await update.message.reply_html(final_text, disable_web_page_preview=True)

        except Exception as e:
            logger.error(f"Error during /tt command for @{target_username}: {e}", exc_info=True)
            error_text = f"❌ Đã xảy ra lỗi khi thực hiện lệnh /tt cho <code>@{html.escape(target_username)}</code>."
            if processing_msg:
                 try: await context.bot.edit_message_text(chat_id, processing_msg.message_id, error_text, parse_mode=ParseMode.HTML)
                 except Exception: pass
            else: await send_temporary_message(update, context, error_text, duration=15)


# --- Handler cho các lệnh không xác định trong group chính ---
async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xóa các lệnh không xác định trong group chính."""
    # Chỉ hoạt động trong group chính
    if update.message and update.message.text and update.message.text.startswith('/') and update.effective_chat.id == ALLOWED_GROUP_ID:
        command_entity = next((e for e in update.message.entities if e.type == 'bot_command' and e.offset == 0), None)
        if command_entity:
            command = update.message.text[1:command_entity.length].split('@')[0]
            # Lấy danh sách các lệnh đã đăng ký
            known_commands = [h.command[0] for h in context.application.handlers.get(0, []) if isinstance(h, CommandHandler)]

            if command not in known_commands:
                logger.info(f"Unknown command '/{command}' in allowed group {ALLOWED_GROUP_ID}. Deleting message {update.message.message_id}.")
                await delete_user_message(update, context)
            # else: logger.debug(f"Command '/{command}' is known.")
        # else: logger.debug("Message starts with / but not a command entity at offset 0.")

# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task]):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        logger.info("No active treo tasks found to cancel during shutdown.")
        return
    logger.info(f"Attempting to gracefully cancel {len(tasks_to_cancel)} active treo tasks...")
    for task in tasks_to_cancel:
        if not task.done(): task.cancel()
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    logger.info("Finished waiting for treo task cancellations during shutdown.")
    cancelled_count, errors_count, finished_normally_count = 0, 0, 0
    for i, result in enumerate(results):
        task_name = tasks_to_cancel[i].get_name() if hasattr(tasks_to_cancel[i], 'get_name') else f"Task_{i}"
        if isinstance(result, asyncio.CancelledError): cancelled_count += 1
        elif isinstance(result, Exception): errors_count += 1; logger.error(f"Error in task '{task_name}' during shutdown: {result}", exc_info=isinstance(result, BaseException))
        else: finished_normally_count += 1
    logger.info(f"Shutdown task summary: {cancelled_count} cancelled, {errors_count} errors, {finished_normally_count} finished normally.")

# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    start_time = time.time()
    print("--- Bot DinoTool Starting ---")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"Bot Token: {'Loaded' if BOT_TOKEN else 'Missing!'}")
    print(f"Allowed Group ID (for key/vip/bill): {ALLOWED_GROUP_ID}")
    print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Follow API (Old): {OLD_FOLLOW_API_URL_BASE}")
    print(f"Follow API (New VIP): {NEW_FOLLOW_API_URL_BASE} (Key: {'Set' if NEW_FOLLOW_API_KEY else 'Missing!'})")
    print(f"Info API: {INFO_API_URL_TEMPLATE.format(username='...')}")
    print(f"Data File: {DATA_FILE}")
    print(f"Photo Bill Window: {PHOTO_BILL_WINDOW_SECONDS / 60:.0f}m")
    print("-" * 30)

    print("Loading persistent data...")
    load_data()
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Initial Treo Stats Users: {len(treo_stats)}")
    print(f"Initial Muatt Tracking: {len(muatt_users_tracking)} (should be 0)")

    application = (
        Application.builder().token(BOT_TOKEN).job_queue(JobQueue())
        .pool_timeout(120).connect_timeout(60).read_timeout(90).write_timeout(90)
        .build()
    )

    # --- Schedule Jobs ---
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_job")
    application.job_queue.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="stats_report_job")
    logger.info("Scheduled cleanup and stats report jobs.")

    # --- Register Handlers ---
    # Filter cho lệnh chỉ hoạt động trong group chính
    group_only_filter = filters.Chat(chat_id=ALLOWED_GROUP_ID)
    # Filter cho lệnh hoạt động mọi nơi (mặc định hoặc dùng filters.ALL)
    # Filter cho admin
    admin_filter = filters.User(user_id=ADMIN_USER_ID)

    # Lệnh hoạt động mọi nơi
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("tim", tim_command)) # Check VIP/Key bên trong
    application.add_handler(CommandHandler("fl", fl_command))   # Check VIP/Key bên trong
    application.add_handler(CommandHandler("treo", treo_command)) # Check VIP bên trong
    application.add_handler(CommandHandler("dungtreo", dungtreo_command)) # Check VIP bên trong
    application.add_handler(CommandHandler("tt", tt_command))   # Không check quyền đặc biệt

    # Lệnh chỉ trong group chính
    application.add_handler(CommandHandler("getkey", getkey_command, filters=group_only_filter))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=group_only_filter))
    application.add_handler(CommandHandler("muatt", muatt_command, filters=group_only_filter))

    # Lệnh Admin (hoạt động mọi nơi)
    application.add_handler(CommandHandler("addtt", addtt_command, filters=admin_filter))
    # Thêm các lệnh admin khác ở đây nếu cần

    # Handler cho ảnh/bill (chỉ trong group chính, ưu tiên cao)
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & group_only_filter & (~filters.COMMAND)
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=1)

    # Handler cho lệnh không xác định (chỉ trong group chính, ưu tiên thấp)
    application.add_handler(MessageHandler(filters.COMMAND & group_only_filter, unknown_command_handler), group=10)

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time
    print(f"(Initialization took {run_duration:.2f} seconds)")

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt: print("\nCtrl+C detected. Stopping..."); logger.info("KeyboardInterrupt detected.")
    except Exception as e: print(f"\nCRITICAL ERROR: {e}"); logger.critical(f"CRITICAL ERROR: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown..."); logger.info("Initiating shutdown...")
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("Collecting active treo tasks for shutdown...")
            for targets in active_treo_tasks.values():
                for task in targets.values():
                    if task and not task.done(): tasks_to_stop_on_shutdown.append(task)
        if tasks_to_stop_on_shutdown:
            print(f"Found {len(tasks_to_stop_on_shutdown)} active tasks. Cancelling...")
            try:
                # Sử dụng asyncio.run để đảm bảo chạy trong loop nếu cần
                asyncio.run(shutdown_async_tasks(tasks_to_stop_on_shutdown))
            except RuntimeError: # Loop already running? Try direct cancel.
                 logger.warning("Event loop running during final shutdown. Attempting direct cancellation.")
                 for task in tasks_to_stop_on_shutdown:
                     if not task.done(): task.cancel()
            except Exception as e_shutdown: logger.error(f"Error during async task shutdown: {e_shutdown}", exc_info=True)
        else: print("No active treo tasks found.")

        print("Attempting final data save..."); logger.info("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")
        print("Bot stopped."); logger.info("Bot stopped.")
        print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: Could not execute main function: {e}")
        logger.critical(f"FATAL ERROR preventing main execution: {e}", exc_info=True)
        with open("fatal_error.log", "a", encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - FATAL ERROR: {e}\n")
            import traceback
            traceback.print_exc(file=f)
