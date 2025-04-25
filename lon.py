
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

# Thêm import cho Inline Keyboard
from telegram import Update, Message, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler # Giữ lại
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (VẪN CẦN CHO LỆNH /tim)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM CỦA ADMIN
ALLOWED_GROUP_ID = -1002191171631 # <--- GIỮ LẠI CHO VIỆC GỬI BILL, THỐNG KÊ, RESUME TREO. CÁC LỆNH KHÁC HOẠT ĐỘNG MỌI NƠI
# HOẶC BỎ HẲN DÒNG TRÊN (xóa hoặc comment): ALLOWED_GROUP_ID = None

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
TREO_STATS_INTERVAL_SECONDS = 24 * 3600 # 24 giờ (Khoảng cách thống kê follow tăng)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (KHÔNG ĐỔI)
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/telefl.php" # <-- API FOLLOW MỚI (BASE URL)
# --- YÊU CẦU: Thêm API Check Info TikTok ---
INFO_TT_API_URL_TEMPLATE = "https://guanghai.x10.mx/infott.php?username={username}" # API Check Info

# --- Thông tin VIP ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg"
BANK_ACCOUNT = "KHANGDINO" # <--- THAY STK CỦA BẠN
BANK_NAME = "VCB BANK" # <--- THAY TÊN NGÂN HÀNG
ACCOUNT_NAME = "LE QUOC KHANG" # <--- THAY TÊN CHỦ TK
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
treo_stats = defaultdict(lambda: defaultdict(int)) # {user_id_str: {target_username: gain_since_last_report}}
last_stats_report_time = 0 # Thời điểm báo cáo thống kê gần nhất
# --- YÊU CẦU: Lưu trạng thái tin nhắn treo ---
treo_status_messages = {} # {(user_id_str, target_username): (message_id, chat_id)}
# --- YÊU CẦU: Lưu trạng thái treo cần resume ---
pending_treo_resumptions = {} # {user_id_str: {target_username: chat_id}}

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
if 'ALLOWED_GROUP_ID' in globals() and ALLOWED_GROUP_ID is None and not isinstance(ALLOWED_GROUP_ID, int):
    logger.warning("!!! ALLOWED_GROUP_ID is not defined or set to None. Bill forwarding, Stats reporting and Treo Resumption might behave unexpectedly or send to Admin. !!!")
elif 'ALLOWED_GROUP_ID' in globals() and ALLOWED_GROUP_ID:
     logger.info(f"Bill forwarding, Stats reporting and Treo Resumption restricted to Group ID: {ALLOWED_GROUP_ID}")
else:
     logger.warning("!!! ALLOWED_GROUP_ID is not defined. Bill forwarding, Stats reporting and Treo Resumption disabled/needs review. !!!")
     ALLOWED_GROUP_ID = None

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
    string_key_treo_stats = {str(uid): dict(targets) for uid, targets in treo_stats.items()}

    # --- YÊU CẦU: Lưu trạng thái treo đang chạy ---
    persisted_treo_state = {}
    for user_id_str, targets in active_treo_tasks.items():
        user_persisted = {}
        for target_username, task in targets.items():
            if task and not task.done():
                # Cố gắng lấy chat_id từ tên task hoặc nơi khác nếu cần
                # Hiện tại run_treo_loop nhận chat_id, ta cần lưu nó
                # Giả sử run_treo_loop có thể truy cập chat_id (đã được thêm vào)
                # Ta cần cách lấy chat_id đó ở đây. Cách đơn giản là lưu nó khi tạo task
                # Sửa đổi: Lưu trực tiếp vào data_to_save từ active_treo_tasks và treo_status_messages
                # Lấy chat_id từ treo_status_messages nếu có
                status_info = treo_status_messages.get((user_id_str, target_username))
                if status_info:
                    chat_id = status_info[1]
                    user_persisted[target_username] = chat_id
                else:
                    # Fallback: Nếu không có trong status_messages, cố gắng lấy từ tên task nếu định dạng chuẩn
                    try:
                        task_name = task.get_name() # VD: treo_7193749511_tiktokuser_in_-1002191171631
                        match = re.search(r"_in_(-?\d+)$", task_name)
                        if match:
                            chat_id = int(match.group(1))
                            user_persisted[target_username] = chat_id
                            logger.debug(f"Extracted chat_id {chat_id} from task name '{task_name}' for persistence.")
                        else:
                             logger.warning(f"Could not find chat_id for persisting task {user_id_str} -> {target_username}. Skipping persistence for this target.")
                    except Exception as e_name:
                         logger.warning(f"Error getting/parsing task name for persistence ({user_id_str}->{target_username}): {e_name}. Skipping.")

        if user_persisted: # Chỉ thêm user vào state nếu có ít nhất 1 target được lưu chat_id
            persisted_treo_state[user_id_str] = user_persisted

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
        "last_stats_report_time": last_stats_report_time,
        # --- YÊU CẦU: Thêm state treo vào file save ---
        "persisted_treo_state": persisted_treo_state
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
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, treo_stats, last_stats_report_time, pending_treo_resumptions
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
                treo_stats = defaultdict(lambda: defaultdict(int))
                for uid_str, targets in loaded_stats.items():
                    for target, gain in targets.items():
                         treo_stats[str(uid_str)][target] = gain

                last_stats_report_time = data.get("last_stats_report_time", 0)

                # --- YÊU CẦU: Tải trạng thái treo cần resume ---
                # Đảm bảo key user là string khi tải
                loaded_persisted_treo = data.get("persisted_treo_state", {})
                pending_treo_resumptions = {str(uid): {target: chat_id for target, chat_id in targets.items()}
                                           for uid, targets in loaded_persisted_treo.items()}

                logger.info(f"Data loaded successfully from {DATA_FILE}. Found {len(pending_treo_resumptions)} users with persisted treo tasks.")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users = {}, {}, {}
            user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}
            treo_stats = defaultdict(lambda: defaultdict(int))
            last_stats_report_time = 0
            pending_treo_resumptions = {} # Khởi tạo trống
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}
        treo_stats = defaultdict(lambda: defaultdict(int))
        last_stats_report_time = 0
        pending_treo_resumptions = {} # Khởi tạo trống

# --- Hàm trợ giúp ---
# ... (Các hàm delete_user_message, delete_message_job, send_temporary_message giữ nguyên) ...
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return

    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except Forbidden:
         logger.debug(f"Cannot delete message {msg_id_to_delete} in chat {original_chat_id}. Bot might not be admin or message too old.")
    except BadRequest as e:
        if "Message to delete not found" in str(e).lower() or "message can't be deleted" in str(e).lower() or "message_id_invalid" in str(e).lower():
            logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted?): {e}")
        else:
            logger.warning(f"BadRequest error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}")
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
        except Forbidden:
             logger.info(f"Job '{job_name}' cannot delete message {message_id}. Bot might not be admin or message too old.")
        except BadRequest as e:
            if "Message to delete not found" in str(e).lower() or "message can't be deleted" in str(e):
                logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else:
                 logger.warning(f"Job '{job_name}' BadRequest deleting message {message_id}: {e}")
        except TelegramError as e:
             logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e:
            logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else:
        logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Gửi tin nhắn và tự động xóa sau một khoảng thời gian."""
    if not update or not update.effective_chat: return

    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if update.message else None
        if reply and reply_to_msg_id:
            try:
                sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
            except BadRequest as e:
                if "reply message not found" in str(e).lower():
                     logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                     sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True)
                else:
                     raise
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

# --- YÊU CẦU: Sửa stop_treo_task để xóa status message ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng một task treo cụ thể, xóa status message và trả về True nếu dừng thành công."""
    task_stopped = False
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
        except asyncio.TimeoutError:
             logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled task {user_id_str}->{target_username} to finish. Assuming stopped.")
        except Exception as e:
             logger.error(f"[Treo Task Stop] Error awaiting cancelled task for {user_id_str}->{target_username}: {e}")
        task_stopped = True # Dừng hoặc cố gắng dừng thành công

    elif task and task.done():
         logger.info(f"[Treo Task Stop] Task for {user_id_str} -> @{target_username} was already done.")
         task_stopped = True # Coi như đã dừng
    else:
         logger.info(f"[Treo Task Stop] No active task found for user {user_id_str} -> @{target_username} to stop.")
         task_stopped = False # Không tìm thấy task

    # Xóa khỏi danh sách active tasks nếu task đã dừng hoặc không tìm thấy (để dọn dẹp)
    if task_stopped and user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]:
            del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active tasks.")

    # --- YÊU CẦU: Xóa tin nhắn trạng thái cuối cùng ---
    status_info = treo_status_messages.pop((user_id_str, target_username), None)
    if status_info:
        message_id, chat_id = status_info
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"[Treo Task Stop] Deleted last status message {message_id} in chat {chat_id} for {user_id_str} -> @{target_username}.")
        except Forbidden:
             logger.warning(f"[Treo Task Stop] Cannot delete status message {message_id} in chat {chat_id}. Bot might not be admin or message too old.")
        except BadRequest as e:
            if "Message to delete not found" in str(e).lower():
                logger.info(f"[Treo Task Stop] Status message {message_id} in chat {chat_id} already deleted.")
            else:
                logger.warning(f"[Treo Task Stop] BadRequest deleting status message {message_id} in chat {chat_id}: {e}")
        except Exception as e:
            logger.error(f"[Treo Task Stop] Unexpected error deleting status message {message_id} in chat {chat_id}: {e}", exc_info=True)

    return task_stopped

# ... (stop_all_treo_tasks_for_user giữ nguyên, nó dùng stop_treo_task đã sửa) ...
async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng tất cả các task treo của một user."""
    stopped_count = 0
    if user_id_str in active_treo_tasks:
        targets_to_stop = list(active_treo_tasks[user_id_str].keys())
        logger.info(f"Stopping all {len(targets_to_stop)} treo tasks for user {user_id_str}. Reason: {reason}")
        for target_username in targets_to_stop:
            if await stop_treo_task(user_id_str, target_username, context, reason): # Gọi hàm đã sửa
                stopped_count += 1
        # Không cần xóa active_treo_tasks[user_id_str] ở đây nữa, stop_treo_task tự xử lý
        logger.info(f"Finished stopping tasks for user {user_id_str}. Stopped: {stopped_count}/{len(targets_to_stop)}")
    else:
        logger.info(f"No active treo tasks found for user {user_id_str} to stop.")

# ... (cleanup_expired_data, is_user_vip, get_vip_limit, is_user_activated_by_key, can_use_feature giữ nguyên) ...
async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs)."""
    global valid_keys, activated_users, vip_users
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
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

    # Perform deletions
    if keys_to_remove:
        logger.info(f"[Cleanup] Removing {len(keys_to_remove)} expired unused keys: {keys_to_remove}")
        for key in keys_to_remove:
            if key in valid_keys:
                 del valid_keys[key]; data_changed = True
    if users_to_deactivate_key:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_key)} users (key system): {users_to_deactivate_key}")
         for user_id_str in users_to_deactivate_key:
             if user_id_str in activated_users:
                  del activated_users[user_id_str]; data_changed = True
    if users_to_deactivate_vip:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_vip)} VIP users: {users_to_deactivate_vip}")
         for user_id_str in users_to_deactivate_vip:
             if user_id_str in vip_users:
                  del vip_users[user_id_str]; data_changed = True

    # Stop tasks for expired VIPs
    if vip_users_to_stop_tasks:
         logger.info(f"[Cleanup] Stopping tasks for {len(vip_users_to_stop_tasks)} expired/invalid VIP users: {vip_users_to_stop_tasks}")
         app = context.application
         for user_id_str in vip_users_to_stop_tasks:
             app.create_task(
                 stop_all_treo_tasks_for_user(user_id_str, context, reason="VIP Expired/Removed during Cleanup"),
            )

    if data_changed:
        logger.info("[Cleanup] Data changed, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No expired data found.")
    logger.info("[Cleanup] Cleanup job finished.")

def is_user_vip(user_id: int) -> bool:
    """Kiểm tra trạng thái VIP."""
    user_id_str = str(user_id)
    vip_data = vip_users.get(user_id_str)
    if vip_data:
        try:
            expiry_time = float(vip_data.get("expiry", 0))
            return time.time() < expiry_time
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
            return time.time() < float(expiry_time_str)
        except (ValueError, TypeError):
             logger.warning(f"Key activation check for {user_id_str}: Invalid expiry data '{expiry_time_str}'. Treating as not activated.")
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl) không (VIP hoặc đã kích hoạt key)."""
    return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# ... (call_follow_api giữ nguyên) ...
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow và trả về kết quả."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")
    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None}
    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                response_text_for_debug = (await resp.aread()).decode('utf-8', errors='replace')[:1000]
            except Exception as e_read:
                 logger.warning(f"[API Call @{target_username}] Error reading response body: {e_read}")

            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                    result["data"] = data
                    api_status = data.get("status")
                    api_message = data.get("message", "Không có thông báo từ API.")
                    result["success"] = api_status is True
                    result["message"] = api_message or (f"Follow thành công." if result["success"] else f"Follow thất bại (API status={api_status}).")
                except json.JSONDecodeError as e_json:
                    logger.error(f"[API Call @{target_username}] Response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    result["message"] = f"Lỗi: API không trả về JSON hợp lệ (Code: {resp.status_code})."
                except Exception as e_proc:
                    logger.error(f"[API Call @{target_username}] Error processing API JSON data: {e_proc}", exc_info=True)
                    result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
            elif resp.status_code == 200:
                 logger.error(f"[API Call @{target_username}] Response 200 OK but wrong Content-Type: {content_type}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type}, Code: {resp.status_code})."
            else:
                 logger.error(f"[API Call @{target_username}] HTTP Error Status: {resp.status_code}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi từ API follow (Code: {resp.status_code})."

    except httpx.TimeoutException:
        logger.warning(f"[API Call @{target_username}] API timeout.")
        result["message"] = f"Lỗi: API timeout khi follow @{html.escape(target_username)}."
    except httpx.ConnectError as e_connect:
        logger.error(f"[API Call @{target_username}] Connection error: {e_connect}", exc_info=False)
        result["message"] = f"Lỗi kết nối đến API follow @{html.escape(target_username)}."
    except httpx.RequestError as e_req:
        logger.error(f"[API Call @{target_username}] Network error: {e_req}", exc_info=False)
        result["message"] = f"Lỗi mạng khi kết nối API follow @{html.escape(target_username)}."
    except Exception as e_unexp:
        logger.error(f"[API Call @{target_username}] Unexpected error during API call: {e_unexp}", exc_info=True)
        result["message"] = f"Lỗi hệ thống Bot khi xử lý follow @{html.escape(target_username)}."

    logger.debug(f"[API Call @{target_username}] Final result: Success={result['success']}, Message='{result['message']}'")
    return result

# --- Handlers ---

# ... (start_command giữ nguyên) ...
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id

    act_h = ACTIVATION_DURATION_SECONDS // 3600
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
           f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n\n"
           f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
           f"   1️⃣ Dùng <code>/getkey</code> để nhận link.\n"
           f"   2️⃣ Truy cập link, làm theo các bước để lấy Key.\n"
           f"       (Ví dụ: <code>Dinotool-ABC123XYZ</code>).\n"
           f"   3️⃣ Quay lại chat này hoặc nhóm, dùng <code>/nhapkey &lt;key_cua_ban&gt;</code>.\n"
           f"   4️⃣ Sau khi kích hoạt, bạn có thể dùng <code>/tim</code> và <code>/fl</code> trong <b>{act_h} giờ</b>.\n\n"
           f"👑 <b>Nâng cấp VIP:</b>\n"
           f"   » Xem chi tiết và hướng dẫn với lệnh <code>/muatt</code>.\n"
           f"   » Thành viên VIP có thể dùng <code>/treo</code>, <code>/dungtreo</code>, không cần lấy key và có nhiều ưu đãi khác.\n\n"
           f"ℹ️ <b>Danh sách lệnh:</b>\n"
           f"   » Gõ <code>/lenh</code> để xem tất cả các lệnh và trạng thái của bạn.\n\n"
           f"💬 Cần hỗ trợ? Liên hệ Admin @{context.bot.username} (nếu bạn là admin) hoặc theo thông tin được cung cấp.\n" # Cập nhật cách liên hệ admin
           f"<i>Bot được phát triển bởi <a href='https://t.me/dinotool'>DinoTool</a></i>")

    try:
        await update.message.reply_html(msg, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start message to {user.id} in chat {chat_id}: {e}")

# --- YÊU CẦU: Cập nhật /lenh ---
async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
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

    cmd_lines.append("\n<b><u>🔑 Lệnh Miễn Phí (Kích hoạt Key):</u></b>")
    cmd_lines.append(f"  <code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần, Key hiệu lực {key_exp_h}h)")
    cmd_lines.append(f"  <code>/nhapkey &lt;key&gt;</code> - Kích hoạt tài khoản (Sử dụng {act_h}h)")

    cmd_lines.append("\n<b><u>❤️ Lệnh Tăng Tương Tác (Cần VIP/Key):</u></b>")
    cmd_lines.append(f"  <code>/tim &lt;link_video&gt;</code> - Tăng tim cho video TikTok (⏳ {tf_cd_m}p/lần)")
    cmd_lines.append(f"  <code>/fl &lt;username&gt;</code> - Tăng follow cho tài khoản TikTok (⏳ {tf_cd_m}p/user)")

    cmd_lines.append("\n<b><u>👑 Lệnh VIP:</u></b>")
    cmd_lines.append(f"  <code>/muatt</code> - Thông tin và hướng dẫn mua VIP")
    cmd_lines.append(f"  <code>/treo &lt;username&gt;</code> - Tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút (Dùng slot)")
    cmd_lines.append(f"  <code>/dungtreo &lt;username&gt;</code> - Dừng treo cho một tài khoản")

    cmd_lines.append("\n<b><u>✨ Lệnh Tiện Ích:</u></b>") # Thêm mục mới
    cmd_lines.append(f"  <code>/tt &lt;username&gt;</code> - Check thông tin tài khoản TikTok (Miễn phí)") # Thêm lệnh /tt

    # Chỉ hiển thị lệnh Admin cho Admin
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code> - Thêm ngày VIP (VD: /addtt 12345 30)")
        cmd_lines.append(f"  <code>/removett &lt;user_id&gt;</code> - Xóa VIP của user") # Thêm lệnh /removett
        cmd_lines.append(f"  <code>/stats</code> - Xem thống kê bot (chưa implement đầy đủ)") # Ví dụ

    cmd_lines.append("\n<b><u>ℹ️ Lệnh Chung:</u></b>")
    cmd_lines.append(f"  <code>/start</code> - Tin nhắn chào mừng")
    cmd_lines.append(f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này")

    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng.</i>")

    help_text = "\n".join(status_lines + cmd_lines)

    try:
        await delete_user_message(update, context)
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")


# ... (tim_command, process_fl_request_background, fl_command giữ nguyên) ...
async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt tài khoản bằng key</b> để sử dụng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » nhận link » lấy key » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage = float(last_usage_str)
            elapsed = current_time - last_usage
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, bạn cần đợi <b>{rem_time:.0f}</b> giây nữa để tiếp tục dùng <code>/tim</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /tim user {user_id}. Resetting.")
             if user_id_str in user_tim_cooldown:
                 del user_tim_cooldown[user_id_str]
                 save_data()

    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Bạn chưa nhập link video.\n"
                   "<b>Cú pháp đúng:</b> <code>/tim https://tiktok.com/...</code>")
    elif "tiktok.com/" not in args[0] or not args[0].startswith(("http://", "https://")):
        err_txt = f"⚠️ Link <code>{html.escape(args[0])}</code> không hợp lệ. Phải là link video TikTok."
    else:
        match = re.search(r"(https?://.*tiktok\.com/.*video/\d+)", args[0])
        if match:
            video_url = match.group(1)
        else:
            logger.warning(f"Could not extract standard TikTok video URL from: {args[0]}. Using as is.")
            video_url = args[0]

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return
    elif not video_url:
        await send_temporary_message(update, context, "⚠️ Không thể xử lý link video. Vui lòng cung cấp link chuẩn.", duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    if not API_KEY:
        logger.error(f"Missing API_KEY for /tim command triggered by user {user_id}")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key cho chức năng này. Vui lòng báo Admin.", duration=20)
        return

    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")

    processing_msg = None
    final_response_text = ""

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
        await delete_user_message(update, context, original_message_id)

        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim Caller'})
            content_type = resp.headers.get("content-type","").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = (await resp.aread()).decode('utf-8', errors='replace')[:500]
            except Exception: pass
            logger.debug(f"/tim API response status: {resp.status_code}, content-type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/tim API response data: {data}")
                    if data.get("success"):
                        user_tim_cooldown[user_id_str] = time.time()
                        save_data()
                        d = data.get("data", {})
                        a = html.escape(str(d.get("author", "?")))
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
            except Forbidden: logger.warning(f"Bot lacks permission to edit /tim msg {processing_msg.message_id}")
            except TelegramError as e_edit: logger.error(f"Telegram error editing /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             logger.warning(f"Processing message for /tim user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /tim message for user {user_id} after processing msg was None: {e_send}")

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
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]
    api_message = api_result["message"]
    api_data = api_result["data"]
    final_response_text = ""

    user_info_block = ""
    if api_data:
        name = html.escape(str(api_data.get("name", "?")))
        tt_username_from_api = api_data.get("username")
        tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
        tt_user_id = html.escape(str(api_data.get("user_id", "?")))
        khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
        avatar = api_data.get("avatar", "")
        create_time = html.escape(str(api_data.get("create_time", "?")))
        user_info_lines = [f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)"]
        if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
        if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
        if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
        if avatar and avatar.startswith("http"): user_info_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>")
        if len(user_info_lines) > 1: user_info_block = "\n".join(user_info_lines) + "\n"
        else: user_info_block = user_info_lines[0] + "\n"

    follower_info_block = ""
    if api_data:
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

    if success:
        current_time_ts = time.time()
        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = current_time_ts
        save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")
        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n"
            f"✨ Cho: {invoking_user_mention}\n\n"
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
            f"{user_info_block}"
        )
        if "đợi" in api_message.lower() and ("phút" in api_message.lower() or "giây" in api_message.lower()):
            final_response_text += f"\n\n<i>ℹ️ API yêu cầu chờ đợi. Vui lòng thử lại sau khoảng thời gian được nêu.</i>"

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
    except Forbidden: logger.error(f"[BG Task /fl] Bot lacks permission to edit msg {processing_msg_id}")
    except TelegramError as e: logger.error(f"[BG Task /fl] Telegram error editing msg {processing_msg_id}: {e}")
    except Exception as e: logger.error(f"[BG Task /fl] Unexpected error editing msg {processing_msg_id}: {e}", exc_info=True)

async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /fl - Check quyền, cooldown, gửi tin chờ và chạy task nền."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    invoking_user_mention = user.mention_html()
    current_time = time.time()
    original_message_id = update.message.message_id

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để sử dụng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

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
        if not uname:
            err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"Username chỉ chứa chữ cái, số, dấu chấm (.), dấu gạch dưới (_), dài 2-24 ký tự và không bắt đầu/kết thúc bằng dấu chấm.")
        else:
            target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

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
                     await delete_user_message(update, context, original_message_id)
                     return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /fl user {user_id} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]
                     save_data()

    processing_msg = None
    try:
        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý..."
        )
        await delete_user_message(update, context, original_message_id)

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
        await delete_user_message(update, context, original_message_id)
    except Exception as e:
         logger.error(f"Unexpected error in fl_command for user {user_id} target @{target_username}: {e}", exc_info=True)
         await delete_user_message(update, context, original_message_id)


# ... (getkey_command, nhapkey_command giữ nguyên) ...
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

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
              if user_id_str in user_getkey_cooldown:
                  del user_getkey_cooldown[user_id_str]
                  save_data()

    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster

    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY) > 6 else "***",
                           "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_saved_to_dict = False

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id)

        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id,
            "generation_time": generation_time,
            "expiry_time": expiry_time,
            "used_by": None,
            "activation_time": None
        }
        key_saved_to_dict = True
        logger.info(f"Key {generated_key} temporarily stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)
            response_content_type = response.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = (await response.aread()).decode('utf-8', errors='replace')[:500]
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
                        save_data()
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
            except Forbidden: logger.warning(f"Bot lacks permission to edit /getkey msg {processing_msg.message_id}")
            except TelegramError as e_edit: logger.error(f"Telegram error editing /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
             except Exception as e_send: logger.error(f"Failed to send final /getkey message for user {user_id} after processing msg was None: {e_send}")

async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    args = context.args
    submitted_key = None
    err_txt = ""
    key_prefix = "Dinotool-"
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]+$")

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập key.\n"
                   "<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1:
        err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        if not key_format_regex.match(key_input):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\n"
                        f"Key phải bắt đầu bằng <code>{key_prefix}</code> và theo sau là các chữ cái IN HOA hoặc số.")
        else:
            submitted_key = key_input

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    logger.info(f"User {user_id} attempting key activation with: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key)
    final_response_text = ""

    if not key_data:
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' not found in valid_keys.")
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
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by another user ({used_by_id}) {used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng {used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = "không rõ thời gian"
        if expiry_time_ts:
             try: expiry_time_str = f"vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
             except (ValueError, TypeError, OSError): pass
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired {expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng {expiry_time_str}. Vui lòng dùng <code>/getkey</code> để lấy key mới."
        if submitted_key in valid_keys: del valid_keys[submitted_key]; save_data(); logger.info(f"Removed expired key {submitted_key} from valid_keys upon activation attempt.")
    else:
        try:
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts
            save_data()
            expiry_dt = datetime.fromtimestamp(activation_expiry_ts)
            expiry_str = expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
            logger.info(f"Key '{submitted_key}' successfully activated by user {user_id}. Activation expires at {expiry_str}.")
            final_response_text = (f"✅ <b>Kích Hoạt Key Thành Công!</b>\n\n"
                                   f"👤 Người dùng: {user.mention_html()}\n"
                                   f"🔑 Key đã nhập: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Bạn có thể sử dụng các lệnh <code>/tim</code> và <code>/fl</code>.\n"
                                   f"⏳ Quyền lợi sẽ hết hạn vào lúc: <b>{expiry_str}</b> (sau {ACTIVATION_DURATION_SECONDS // 3600} giờ)."
                                 )
        except Exception as e_activate:
             logger.error(f"Unexpected error during key activation process for user {user_id} key {submitted_key}: {e_activate}", exc_info=True)
             final_response_text = f"❌ Đã xảy ra lỗi hệ thống trong quá trình kích hoạt key <code>{html.escape(submitted_key)}</code>. Vui lòng thử lại hoặc báo Admin."
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None
                 valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users: del activated_users[user_id_str]

    await delete_user_message(update, context, original_message_id)
    try:
        await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
         logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")


# ... (muatt_command, prompt_send_bill_callback, handle_photo_bill, addtt_command giữ nguyên) ...
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP và nút gửi bill."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    original_message_id = update.message.message_id

    user_id = user.id
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

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
    text_lines.append(f"   2️⃣ Nhấn nút 'Gửi Bill Thanh Toán' bên dưới.")
    text_lines.append(f"   3️⃣ Bot sẽ yêu cầu bạn gửi ảnh bill.")
    text_lines.append(f"   4️⃣ Gửi ảnh bill vào cuộc trò chuyện.")
    text_lines.append(f"   5️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin để xác nhận.")
    text_lines.append(f"   6️⃣ Admin sẽ kiểm tra và kích hoạt VIP cho bạn trong thời gian sớm nhất.")
    text_lines.append("\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️")
    text = "\n".join(text_lines)

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data="prompt_send_bill")]])

    await delete_user_message(update, context, original_message_id)
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending /muatt photo+caption to chat {chat_id}: {e}")
        logger.info(f"Falling back to sending text only for /muatt in chat {chat_id}")
        try: await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=keyboard)
        except Exception as e_text: logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")
    except Exception as e_unexp:
        logger.error(f"Unexpected error sending /muatt command to chat {chat_id}: {e_unexp}", exc_info=True)

async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = query.from_user
    chat_id = query.message.chat_id
    await query.answer()
    logger.info(f"User {user.id} clicked 'prompt_send_bill' button in chat {chat_id}.")
    prompt_text = f"📸 {user.mention_html()}, vui lòng gửi ảnh chụp màn hình biên lai thanh toán của bạn vào cuộc trò chuyện này ngay bây giờ."
    try:
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML)
    except (BadRequest, Forbidden, TelegramError) as e: logger.error(f"Error sending bill prompt message to {user.id} in chat {chat_id}: {e}")
    except Exception as e: logger.error(f"Unexpected error sending bill prompt message to {user.id} in chat {chat_id}: {e}", exc_info=True)

async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message or not ALLOWED_GROUP_ID: return
    if update.effective_chat.id != ALLOWED_GROUP_ID or (update.message.text and update.message.text.startswith('/')): return
    is_photo = bool(update.message.photo)
    is_image_document = bool(update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return
    user = update.effective_user
    chat = update.effective_chat
    message_id = update.message.message_id
    if not user or not chat: return
    logger.info(f"Potential bill received in ALLOWED_GROUP {chat.id} from user {user.id}. Forwarding to admin {ADMIN_USER_ID}.")
    forward_caption_lines = [f"📄 <b>Bill/Ảnh Nhận Được (Tự Động)</b>", f"👤 <b>Từ User:</b> {user.mention_html()} (<code>{user.id}</code>)", f"👥 <b>Trong Group:</b> {html.escape(chat.title or str(chat.id))} (<code>{chat.id}</code>)"]
    try:
         message_link = update.message.link
         if message_link: forward_caption_lines.append(f"🔗 <b>Link Tin Nhắn Gốc:</b> <a href='{message_link}'>Click vào đây</a>")
         else: raise AttributeError # Simulate link failure if None
    except AttributeError:
         logger.debug(f"Could not get message link for message {message_id} in chat {chat.id}")
         forward_caption_lines.append(f"🔗 <b>Link Tin Nhắn Gốc:</b> (Không thể tạo)")
    original_caption = update.message.caption or update.message.text
    if original_caption: forward_caption_lines.append(f"\n💬 <b>Caption/Nội dung gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")
    forward_caption = "\n".join(forward_caption_lines)
    try:
        await context.bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=chat.id, message_id=message_id)
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=forward_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Successfully forwarded bill message {message_id} and sent info to admin {ADMIN_USER_ID}.")
        reply_text = f"✅ Đã nhận và gửi ảnh của {user.mention_html()} cho Admin xem xét."
        await send_temporary_message(update, context, reply_text, duration=60, reply=True)
    except Forbidden:
        logger.error(f"Bot cannot forward/send message to admin {ADMIN_USER_ID}. Check permissions/block status.")
        try:
             error_admin_msg = f"⚠️ {user.mention_html()}, không thể gửi ảnh của bạn đến Admin lúc này (Bot bị chặn hoặc thiếu quyền). Vui lòng liên hệ Admin trực tiếp."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
        except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
    except TelegramError as e_fwd:
         logger.error(f"Telegram error forwarding bill message {message_id} to admin: {e_fwd}")
         try:
             error_admin_msg = f"⚠️ {user.mention_html()}, đã xảy ra lỗi khi gửi ảnh của bạn đến Admin. Vui lòng thử lại hoặc báo Admin."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
         except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
    except Exception as e:
        logger.error(f"Unexpected error forwarding/sending bill to admin: {e}", exc_info=True)
        try:
             error_admin_msg = f"⚠️ {user.mention_html()}, lỗi hệ thống khi xử lý ảnh của bạn. Vui lòng báo Admin."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
        except Exception as e_reply: logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")

async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat: return
    if admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id} ({admin_user.username}) in chat {chat.id}.")
        return

    args = context.args
    err_txt = None
    target_user_id = None
    days_to_add_input = None
    limit = None
    duration_days = None
    valid_day_keys = list(VIP_PRICES.keys())
    valid_days_str = ', '.join(map(str, valid_day_keys))

    if len(args) != 2:
        err_txt = (f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code>\n"
                   f"<b>Các gói hợp lệ:</b> {valid_days_str}\n<b>Ví dụ:</b> <code>/addtt 123456789 {valid_day_keys[0]}</code>")
    else:
        try: target_user_id = int(args[0])
        except ValueError: err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."
        if not err_txt:
            try:
                days_to_add_input = int(args[1])
                if days_to_add_input not in VIP_PRICES: err_txt = f"⚠️ Gói ngày không hợp lệ. Chỉ chấp nhận: <b>{valid_days_str}</b>."
                else:
                    vip_info = VIP_PRICES[days_to_add_input]
                    limit = vip_info["limit"]
                    duration_days = vip_info["duration_days"]
            except ValueError: err_txt = f"⚠️ Gói ngày '<code>{html.escape(args[1])}</code>' không phải là số hợp lệ."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

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
             else: logger.info(f"User {target_user_id_str} was VIP but expired. Treating as new activation.")
         except (ValueError, TypeError): logger.warning(f"Invalid expiry data for user {target_user_id_str}. Treating as new activation.")

    new_expiry_ts = start_time + duration_days * 86400
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data()
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days} days. New expiry: {new_expiry_str}, Limit: {limit}")

    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n✨ Gói: {duration_days} ngày\n"
                 f"⏳ Hạn sử dụng mới: <b>{new_expiry_str}</b>\n🚀 Giới hạn treo: <b>{limit} users</b>")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation message to admin {admin_user.id} in chat {chat.id}: {e}")

    user_mention = f"User ID <code>{target_user_id}</code>"
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info and hasattr(target_user_info, 'mention_html'): user_mention = target_user_info.mention_html()
    except Exception as e_get_chat: logger.warning(f"Could not get chat info for target user {target_user_id}: {e_get_chat}. Using ID instead.")

    group_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\n"
                 f"Bạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"✨ Gói VIP: <b>{duration_days} ngày</b>\n⏳ Hạn sử dụng đến: <b>{new_expiry_str}</b>\n"
                 f"🚀 Giới hạn treo: <b>{limit} tài khoản</b>\n\n"
                 f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n(Dùng <code>/lenh</code> để xem lại trạng thái)")

    target_chat_id_for_notification = ADMIN_USER_ID
    if ALLOWED_GROUP_ID:
        target_chat_id_for_notification = ALLOWED_GROUP_ID
        logger.info(f"Sending VIP notification for {target_user_id} to group {ALLOWED_GROUP_ID}")
    else:
         logger.info(f"ALLOWED_GROUP_ID not set. Sending VIP notification for {target_user_id} to admin {ADMIN_USER_ID}")
    try:
        await context.bot.send_message(chat_id=target_chat_id_for_notification, text=group_msg, parse_mode=ParseMode.HTML)
    except Exception as e_send_notify:
        logger.error(f"Failed to send VIP notification for user {target_user_id} to chat {target_chat_id_for_notification}: {e_send_notify}")
        if admin_user.id != target_chat_id_for_notification:
             try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} vào chat {target_chat_id_for_notification}. Lỗi: {e_send_notify}")
             except Exception: pass

# --- YÊU CẦU: Lệnh /removett (Admin) ---
async def removett_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xóa VIP của người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat: return

    # --- Check Admin ---
    if admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /removett attempt by {admin_user.id} ({admin_user.username}) in chat {chat.id}.")
        return

    # --- Parse Arguments ---
    args = context.args
    err_txt = None
    target_user_id = None

    if len(args) != 1:
        err_txt = ("⚠️ Sai cú pháp.\n"
                   "<b>Dùng:</b> <code>/removett &lt;user_id&gt;</code>\n"
                   "<b>Ví dụ:</b> <code>/removett 123456789</code>")
    else:
        try:
            target_user_id = int(args[0])
        except ValueError:
            err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    # --- Xóa VIP và dừng task ---
    target_user_id_str = str(target_user_id)
    removed = False
    user_mention = f"User ID <code>{target_user_id}</code>" # Default mention

    # Cố gắng lấy mention trước khi xóa dữ liệu
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info and hasattr(target_user_info, 'mention_html'):
             user_mention = target_user_info.mention_html()
    except Exception as e_get_chat:
        logger.warning(f"Could not get chat info for target user {target_user_id} before removing VIP: {e_get_chat}. Using ID instead.")

    if target_user_id_str in vip_users:
        del vip_users[target_user_id_str]
        removed = True
        logger.info(f"Admin {admin_user.id} removed VIP status for user {target_user_id_str}.")

        # Dừng tất cả task treo của user này
        await stop_all_treo_tasks_for_user(target_user_id_str, context, reason=f"VIP removed by Admin {admin_user.id}")

        save_data() # Lưu lại thay đổi

        admin_msg = f"✅ Đã xóa VIP thành công cho {user_mention} (<code>{target_user_id}</code>)."
        try: await update.message.reply_html(admin_msg)
        except Exception as e: logger.error(f"Failed to send confirmation message to admin {admin_user.id} in chat {chat.id}: {e}")

        # --- Thông báo cho người dùng (nếu có thể và có group) ---
        group_msg = f"ℹ️ {user_mention}, trạng thái VIP của bạn đã được gỡ bỏ bởi Admin."
        if ALLOWED_GROUP_ID:
             target_chat_id_for_notification = ALLOWED_GROUP_ID
             logger.info(f"Sending VIP removal notification for {target_user_id} to group {ALLOWED_GROUP_ID}")
             try:
                 await context.bot.send_message(chat_id=target_chat_id_for_notification, text=group_msg, parse_mode=ParseMode.HTML)
             except Exception as e_send_notify:
                 logger.error(f"Failed to send VIP removal notification for user {target_user_id} to chat {target_chat_id_for_notification}: {e_send_notify}")
                 if admin_user.id != target_chat_id_for_notification:
                      try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo xóa VIP cho user {target_user_id} vào chat {target_chat_id_for_notification}. Lỗi: {e_send_notify}")
                      except Exception: pass
        else:
            logger.info(f"ALLOWED_GROUP_ID not set. Skipping VIP removal notification for {target_user_id}.")

    else:
        # User không có trong danh sách VIP
        err_msg = f"ℹ️ Người dùng {user_mention} (<code>{target_user_id}</code>) không có trong danh sách VIP."
        logger.info(f"Admin {admin_user.id} tried to remove VIP for non-VIP user {target_user_id_str}.")
        try: await update.message.reply_html(err_msg)
        except Exception as e: logger.error(f"Failed to send 'not found' message to admin {admin_user.id} in chat {chat.id}: {e}")


# --- YÊU CẦU: Lệnh /tt Check Info ---
async def tt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tt - Check thông tin tài khoản TikTok."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # --- Parse Arguments ---
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$" # Regex kiểm tra username hợp lệ

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập username TikTok.\n"
                   "<b>Cú pháp đúng:</b> <code>/tt username</code> (không cần @)")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname:
            err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"(Chỉ chứa chữ, số, '.', '_'; dài 2-24 ký tự; không bắt đầu/kết thúc bằng '.')")
        else:
            target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Call API ---
    api_url = INFO_TT_API_URL_TEMPLATE.format(username=target_username)
    logger.info(f"User {user.id} calling /tt API for @{target_username}: {api_url}")

    processing_msg = None
    final_response_text = ""

    try:
        processing_msg = await update.message.reply_html(f"<b><i>⏳ Đang kiểm tra thông tin @{html.escape(target_username)}...</i></b>")
        await delete_user_message(update, context, original_message_id)

        async with httpx.AsyncClient(verify=False, timeout=30.0) as client: # verify=False nếu API dùng cert tự ký
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot TT Info Checker'})
            content_type = resp.headers.get("content-type","").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = (await resp.aread()).decode('utf-8', errors='replace')[:500]
            except Exception: pass

            logger.debug(f"/tt API response status: {resp.status_code}, content-type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/tt API response data: {data}")

                    # Kiểm tra cấu trúc JSON trả về có khớp không (dựa trên ví dụ)
                    # Nếu API trả về lỗi bên trong JSON (ví dụ: {"error": "User not found"}) thì cần xử lý
                    if isinstance(data, dict) and "error" in data:
                         error_msg = data.get("error", "Lỗi không rõ từ API.")
                         logger.warning(f"/tt API call for @{target_username} failed. API error: {error_msg}")
                         final_response_text = f"❌ <b>Không thể lấy thông tin @{html.escape(target_username)}</b>\nℹ️ Lý do: <code>{html.escape(error_msg)}</code>"
                    elif isinstance(data, dict) and "username" in data: # Giả sử thành công nếu có username
                        # Trích xuất dữ liệu an toàn
                        tt_name = html.escape(data.get("name", "Không có"))
                        tt_username = html.escape(data.get("username", target_username)) # Dùng username từ API nếu có
                        tt_user_id = html.escape(str(data.get("user_id", "N/A")))
                        followers = data.get("followers", 0)
                        following = data.get("following", 0)
                        hearts = data.get("hearts", 0) # Tổng lượt thích các video
                        # digg_count = data.get("digg_count", 0) # Lượt thích đã nhận (có thể trùng hearts?)
                        videos = data.get("videos", 0)
                        signature = html.escape(data.get("signature", ""))
                        avatar_url = data.get("profile_picture", "")
                        profile_url = f"https://tiktok.com/@{tt_username}"

                        # Định dạng kết quả
                        info_lines = [f"📊 <b>Thông Tin TikTok: <a href='{profile_url}'>@{tt_username}</a></b>"]
                        info_lines.append(f"📛 Tên: <b>{tt_name}</b>")
                        info_lines.append(f"🆔 User ID: <code>{tt_user_id}</code>")
                        info_lines.append(f"👥 Followers: <b>{followers:,}</b>") # Định dạng số
                        info_lines.append(f"👣 Following: <b>{following:,}</b>")
                        info_lines.append(f"❤️ Tổng Tim: <b>{hearts:,}</b>")
                        info_lines.append(f"🎬 Videos: <b>{videos}</b>")
                        if signature: info_lines.append(f"📝 Bio: <i>{signature}</i>")
                        if avatar_url: info_lines.append(f"🖼️ <a href='{html.escape(avatar_url)}'>Xem Avatar</a>")

                        final_response_text = "\n".join(info_lines)
                    else:
                        # JSON hợp lệ nhưng cấu trúc không mong đợi
                        logger.warning(f"/tt API response for @{target_username} has unexpected structure: {response_text_for_debug}...")
                        final_response_text = f"❌ <b>Lỗi Phản Hồi API</b>\nℹ️ Dữ liệu trả về từ API không đúng định dạng mong đợi."

                except json.JSONDecodeError as e_json:
                    logger.error(f"/tt API response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API</b>\nℹ️ API không trả về JSON hợp lệ."
            else:
                logger.error(f"/tt API call HTTP error or wrong content type. Status: {resp.status_code}, Type: {content_type}. Text: {response_text_for_debug}...")
                final_response_text = f"❌ <b>Lỗi Kết Nối API</b>\nℹ️ Không thể kết nối đến máy chủ check info (Mã: {resp.status_code}). Vui lòng thử lại sau."

    except httpx.TimeoutException:
        logger.warning(f"/tt API call timeout for @{target_username}")
        final_response_text = f"❌ <b>Lỗi Timeout</b>\nℹ️ API check info không phản hồi kịp thời. Vui lòng thử lại sau."
    except httpx.RequestError as e_req:
        logger.error(f"/tt API call network error for @{target_username}: {e_req}", exc_info=False)
        final_response_text = f"❌ <b>Lỗi Mạng</b>\nℹ️ Không thể kết nối đến API check info. Kiểm tra lại mạng hoặc thử lại sau."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /tt command for @{target_username}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ <b>Lỗi Hệ Thống Bot</b>\nℹ️ Đã xảy ra lỗi không mong muốn. Vui lòng báo Admin."
    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True # Tắt preview cho link avatar/profile
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Failed to edit /tt msg {processing_msg.message_id}: Message not found.")
                 else: logger.warning(f"Failed to edit /tt msg {processing_msg.message_id}: {e_edit}")
            except Forbidden: logger.warning(f"Bot lacks permission to edit /tt msg {processing_msg.message_id}")
            except TelegramError as e_edit: logger.error(f"Telegram error editing /tt msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /tt msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             logger.warning(f"Processing message for /tt @{target_username} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /tt message for @{target_username} after processing msg was None: {e_send}")


# --- Logic Treo (Sửa run_treo_loop) ---
# --- YÊU CẦU: Sửa run_treo_loop để xóa status message cũ ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Vòng lặp chạy nền cho lệnh /treo, gửi và quản lý thông báo trạng thái."""
    user_id = int(user_id_str)
    task_name = f"treo_{user_id_str}_{target_username}_in_{chat_id}"
    logger.info(f"[Treo Task Start] Task '{task_name}' started in chat {chat_id}.")
    invoking_user_mention = f"User ID <code>{user_id_str}</code>"
    try:
        user_info = await context.bot.get_chat(user_id)
        if user_info and hasattr(user_info, 'mention_html'):
             invoking_user_mention = user_info.mention_html()
    except Exception: pass

    # Lấy bot context một lần
    bot = context.bot

    try:
        while True:
            # Check 1: Task còn trong danh sách active không?
            current_task_in_dict = active_treo_tasks.get(user_id_str, {}).get(target_username)
            if current_task_in_dict is not asyncio.current_task(loop=asyncio.get_running_loop()):
                 logger.warning(f"[Treo Task Stop] Task '{task_name}' seems replaced or removed from active_treo_tasks dict. Stopping.")
                 break

            # Check 2: User còn VIP không?
            if not is_user_vip(user_id):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                # stop_treo_task sẽ tự động xóa status message cuối cùng
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired")
                try: await bot.send_message(chat_id, f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã dừng do VIP hết hạn.", parse_mode=ParseMode.HTML, disable_notification=True)
                except Exception: pass
                break

            # Thực hiện gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, bot.token)
            success = api_result["success"]
            api_message = api_result["message"]
            gain = 0

            if success and api_result["data"]:
                try:
                    gain_str = str(api_result["data"].get("followers_add", "0"))
                    gain = int(gain_str)
                    if gain > 0:
                        treo_stats[user_id_str][target_username] += gain
                        logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Current gain in this cycle: {treo_stats[user_id_str][target_username]}")
                    else:
                         logger.info(f"[Treo Task Success] Task '{task_name}' successful but gain was {gain}. API Msg: {api_message}")
                except (ValueError, TypeError) as e_gain: logger.warning(f"[Treo Task Stats] Task '{task_name}' could not parse gain '{api_result['data'].get('followers_add')}' from API data: {e_gain}")
                except Exception as e_stats: logger.error(f"[Treo Task Stats] Task '{task_name}' unexpected error processing stats: {e_stats}", exc_info=True)
            elif success: logger.info(f"[Treo Task Success] Task '{task_name}' successful but no data returned. API Msg: {api_message}")
            else: logger.warning(f"[Treo Task Fail] Task '{task_name}' failed. API Msg: {api_message}")

            # --- YÊU CẦU: Gửi thông báo trạng thái mới, xóa cái cũ ---
            status_lines = []
            if success:
                status_lines.append(f"✅ {invoking_user_mention}: Treo <code>@{html.escape(target_username)}</code> thành công!")
                status_lines.append(f"➕ Thêm: <b>{gain}</b>")
                if api_message and api_message != "Follow thành công.":
                     status_lines.append(f"💬 <i>{html.escape(api_message)}</i>")
                # Nếu không có thông báo từ API và không phải "Follow thành công.", thêm dòng mặc định (Tùy chọn)
                # elif not api_message:
                #      status_lines.append(f"💬 Không có thông báo từ API.")
            else: # Thất bại
                status_lines.append(f"❌ {invoking_user_mention}: Treo <code>@{html.escape(target_username)}</code> thất bại!")
                status_lines.append(f"➕ Thêm: 0")
                status_lines.append(f"💬 Lý do: <i>{html.escape(api_message)}</i>")

            status_msg = "\n".join(status_lines)

            # --- Xóa tin nhắn cũ ---
            old_status_info = treo_status_messages.pop((user_id_str, target_username), None)
            if old_status_info:
                old_message_id, old_chat_id = old_status_info
                try:
                    await bot.delete_message(chat_id=old_chat_id, message_id=old_message_id)
                    logger.debug(f"Deleted previous status message {old_message_id} in chat {old_chat_id} for '{task_name}'.")
                except Forbidden: logger.warning(f"Cannot delete previous status message {old_message_id} in chat {old_chat_id} (Forbidden).")
                except BadRequest as e_del:
                    if "Message to delete not found" in str(e_del).lower(): logger.debug(f"Previous status message {old_message_id} not found for deletion.")
                    else: logger.warning(f"BadRequest deleting previous status message {old_message_id}: {e_del}")
                except Exception as e_del: logger.error(f"Error deleting previous status message {old_message_id}: {e_del}", exc_info=True)

            # --- Gửi tin nhắn mới và lưu lại ---
            new_sent_message = None
            try:
                new_sent_message = await bot.send_message(
                    chat_id=chat_id,
                    text=status_msg,
                    parse_mode=ParseMode.HTML,
                    disable_notification=True
                )
                # Lưu thông tin tin nhắn mới
                treo_status_messages[(user_id_str, target_username)] = (new_sent_message.message_id, chat_id)
                logger.debug(f"Sent new treo status update ({new_sent_message.message_id}) for '{task_name}' to chat {chat_id}")
            except Forbidden: logger.warning(f"Could not send new treo status for '{task_name}' to chat {chat_id} (Forbidden). Task continues.")
            except TelegramError as e_send: logger.error(f"Error sending new treo status for '{task_name}' to chat {chat_id}: {e_send}")
            except Exception as e_unexp: logger.error(f"Unexpected error sending new treo status for '{task_name}': {e_unexp}", exc_info=True)

            # Chờ đợi
            sleep_duration = TREO_INTERVAL_SECONDS
            logger.debug(f"[Treo Task Sleep] Task '{task_name}' sleeping for {sleep_duration} seconds...")
            await asyncio.sleep(sleep_duration)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
        # Không cần xóa message ở đây, stop_treo_task sẽ xử lý khi được gọi từ bên ngoài
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try: await bot.send_message(chat_id, f"💥 {invoking_user_mention}: Lỗi nghiêm trọng xảy ra khi treo <code>@{html.escape(target_username)}</code>. Tác vụ đã dừng.", parse_mode=ParseMode.HTML, disable_notification=True)
        except Exception: pass
        # Dừng task và xóa message
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Đảm bảo entry status message được xóa nếu task kết thúc mà không qua stop_treo_task (hiếm khi xảy ra)
        if (user_id_str, target_username) in treo_status_messages:
             status_info = treo_status_messages.pop((user_id_str, target_username), None)
             if status_info:
                 m_id, c_id = status_info
                 logger.warning(f"Task '{task_name}' ended unexpectedly without explicit stop. Attempting cleanup of status message {m_id} in chat {c_id}.")
                 try: await context.bot.delete_message(chat_id=c_id, message_id=m_id)
                 except Exception: pass # Bỏ qua lỗi khi cleanup cuối cùng

        # Đảm bảo task được xóa khỏi dict active_treo_tasks
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            task_in_dict = active_treo_tasks[user_id_str].get(target_username)
            current_task = None
            try: current_task = asyncio.current_task(loop=asyncio.get_running_loop())
            except RuntimeError: pass

            # Chỉ xóa nếu task trong dict chính là task hiện tại VÀ nó đã xong
            if task_in_dict is current_task and task_in_dict and task_in_dict.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished task '{task_name}' from active tasks dict in finally block.")


# --- Lệnh /treo (VIP) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP)."""
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
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho thành viên <b>VIP</b>.\nDùng <code>/muatt</code> để xem thông tin nâng cấp."
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$"

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập username TikTok cần treo.\n"
                   "<b>Cú pháp đúng:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được để trống."
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"(Chỉ chứa chữ, số, '.', '_'; dài 2-24 ký tự; không bắt đầu/kết thúc bằng '.')")
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Check Giới Hạn và Trạng Thái Treo Hiện Tại ---
    if target_username:
        vip_limit = get_vip_limit(user_id)
        user_tasks = active_treo_tasks.get(user_id_str, {})
        current_treo_count = len(user_tasks)

        existing_task = user_tasks.get(target_username)
        if existing_task and not existing_task.done():
            logger.info(f"User {user_id} tried to /treo target @{target_username} which is already running.")
            await send_temporary_message(update, context, f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi.\nDùng <code>/dungtreo {target_username}</code> để dừng nếu muốn.", duration=20)
            await delete_user_message(update, context, original_message_id)
            return
        elif existing_task and existing_task.done():
             logger.warning(f"Found finished/cancelled task for {user_id_str}->{target_username} in dict. Cleaning up before new /treo.")
             await stop_treo_task(user_id_str, target_username, context, reason="Cleanup before new /treo")
             # Cập nhật lại số lượng sau khi dọn dẹp
             user_tasks = active_treo_tasks.get(user_id_str, {})
             current_treo_count = len(user_tasks)

        if current_treo_count >= vip_limit:
             logger.warning(f"User {user_id} tried to /treo target @{target_username} but reached limit ({current_treo_count}/{vip_limit}).")
             limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit} tài khoản).\n"
                         f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot hoặc nâng cấp VIP.")
             await send_temporary_message(update, context, limit_msg, duration=30)
             await delete_user_message(update, context, original_message_id)
             return

        # --- Bắt đầu Task Treo Mới ---
        try:
            app = context.application
            # Truyền chat_id hiện tại vào task loop
            task = app.create_task(
                run_treo_loop(user_id_str, target_username, context, chat_id), # Thêm chat_id
                name=f"treo_{user_id_str}_{target_username}_in_{chat_id}" # Cập nhật tên task
            )

            active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
            logger.info(f"Successfully created and stored treo task '{task.get_name()}' for user {user_id}")

            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n"
                           f"👤 Cho: {invoking_user_mention}\n"
                           f"🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {TREO_INTERVAL_SECONDS // 60} phút\n"
                           f"📊 Slot đã dùng: {current_treo_count + 1}/{vip_limit}\n"
                           f"ℹ️ <i>Trạng thái sẽ được cập nhật tại đây.</i>") # Thêm ghi chú
            await update.message.reply_html(success_msg)
            await delete_user_message(update, context, original_message_id)

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Vui lòng thử lại hoặc báo Admin.", duration=20)
             await delete_user_message(update, context, original_message_id)
    else:
        # Trường hợp này khó xảy ra do đã check err_txt
        logger.error(f"/treo command for user {user_id}: target_username became None unexpectedly.")
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi xử lý username. Vui lòng thử lại.", duration=15)
        await delete_user_message(update, context, original_message_id)


# --- Lệnh /dungtreo (VIP) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một user (chỉ VIP hoặc user có task đang chạy)."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    args = context.args
    target_username_clean = None
    err_txt = None

    if not args:
        user_tasks = active_treo_tasks.get(user_id_str, {})
        if not user_tasks:
             err_txt = ("⚠️ Bạn chưa nhập username cần dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n<i>(Bạn không có tài khoản nào đang treo.)</i>")
        else:
             running_targets = [f"<code>@{html.escape(t)}</code>" for t in user_tasks.keys()]
             err_txt = (f"⚠️ Bạn cần chỉ định username muốn dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                        f"<b>Đang treo:</b> {', '.join(running_targets)}")
    else:
        target_username_clean = args[0].strip().lstrip("@")
        if not target_username_clean: err_txt = "⚠️ Username không được để trống."

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    if target_username_clean:
        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        # stop_treo_task sẽ tự xóa status message
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        await delete_user_message(update, context, original_message_id)
        if stopped:
            vip_limit = get_vip_limit(user_id)
            current_treo_count = len(active_treo_tasks.get(user_id_str, {}))
            is_still_vip = is_user_vip(user_id)
            await update.message.reply_html(f"✅ Đã dừng treo follow tự động cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {current_treo_count}/{vip_limit if is_still_vip else '0'} - VIP {'Còn' if is_still_vip else 'Hết'})")
        else:
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy tác vụ treo nào đang chạy cho <code>@{html.escape(target_username_clean)}</code> để dừng.", duration=20)

# --- Job Thống Kê Follow Tăng ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    logger.info(f"[Stats Job] Starting statistics report job. Current time: {datetime.fromtimestamp(current_time).isoformat()}, Last report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

    target_chat_id_for_stats = None
    if ALLOWED_GROUP_ID: target_chat_id_for_stats = ALLOWED_GROUP_ID
    else:
        logger.info("[Stats Job] ALLOWED_GROUP_ID is not set. Stats report skipped.")
        return

    stats_snapshot = {uid: dict(targets) for uid, targets in treo_stats.items() if targets}
    treo_stats.clear()
    last_stats_report_time = current_time
    save_data() # Lưu lại trạng thái đã reset
    logger.info(f"[Stats Job] Cleared current stats and updated last report time. Processing snapshot...")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data found in snapshot. Skipping report.")
        return

    top_gainers = []
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        for target_username, gain in targets.items():
            if gain > 0:
                top_gainers.append((gain, user_id_str, target_username))
                total_gain_all += gain

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found in the snapshot. Skipping report.")
        return

    top_gainers.sort(key=lambda x: x[0], reverse=True)

    report_lines = [f"📊 <b>Thống Kê Tăng Follow (Trong 24 Giờ Qua)</b> 📊", f"<i>(Tổng cộng: {total_gain_all} follow)</i>", "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>"]
    num_top_to_show = 5
    displayed_count = 0
    user_mentions_cache = {}

    for gain, user_id_str, target_username in top_gainers[:num_top_to_show]:
        user_mention = user_mentions_cache.get(user_id_str)
        if not user_mention:
            try:
                user_info = await context.bot.get_chat(int(user_id_str))
                user_mention = user_info.mention_html() if user_info and hasattr(user_info, 'mention_html') else f"ID<code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str}: {e_get_chat}")
                user_mention = f"ID<code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention
        report_lines.append(f"  🏅 <b>+{gain} fl</b> @<code>{html.escape(target_username)}</code> (Treo: {user_mention})")
        displayed_count += 1

    if not displayed_count: report_lines.append("  <i>Không có dữ liệu tăng follow.</i>")
    report_lines.append(f"\n🕒 <i>Cập nhật mỗi 24 giờ.</i>")

    report_text = "\n".join(report_lines)
    try:
        await context.bot.send_message(chat_id=target_chat_id_for_stats, text=report_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
        logger.info(f"[Stats Job] Successfully sent statistics report to group {target_chat_id_for_stats}.")
    except (BadRequest, Forbidden, TelegramError) as e: logger.error(f"[Stats Job] Failed to send statistics report to group {target_chat_id_for_stats}: {e}")
    except Exception as e: logger.error(f"[Stats Job] Unexpected error sending statistics report: {e}", exc_info=True)
    logger.info("[Stats Job] Statistics report job finished.")


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
    # ... (Phần log kết quả giữ nguyên) ...

# --- YÊU CẦU: Hàm resume tasks ---
async def resume_persisted_tasks(app: Application):
    """Khôi phục các task /treo từ trạng thái đã lưu khi bot khởi động."""
    global pending_treo_resumptions, active_treo_tasks
    context = app # Application cũng có thể dùng như context cho các hàm bot cơ bản
    resumed_count = 0
    skipped_count = 0
    total_to_resume = sum(len(targets) for targets in pending_treo_resumptions.values())

    if not total_to_resume:
        logger.info("[Resume Tasks] No persisted treo tasks found to resume.")
        return

    logger.info(f"[Resume Tasks] Found {total_to_resume} potential treo tasks to resume. Starting process...")

    # Dùng list để tránh lỗi thay đổi dict khi duyệt
    users_to_process = list(pending_treo_resumptions.items())

    for user_id_str, targets in users_to_process:
        user_id = int(user_id_str)

        # Check 1: User còn VIP không?
        if not is_user_vip(user_id):
            logger.warning(f"[Resume Tasks] Skipping all {len(targets)} tasks for user {user_id_str}: No longer VIP.")
            skipped_count += len(targets)
            continue # Bỏ qua tất cả task của user này

        # Lấy giới hạn và số task đang chạy (có thể đã có task nào đó được resume)
        vip_limit = get_vip_limit(user_id)
        user_active_tasks = active_treo_tasks.get(user_id_str, {})
        current_active_count = len(user_active_tasks)

        targets_to_process = list(targets.items()) # Duyệt qua targets của user
        for target_username, chat_id in targets_to_process:
            # Check 2: User còn slot không?
            if current_active_count >= vip_limit:
                logger.warning(f"[Resume Tasks] Skipping task {user_id_str} -> @{target_username}: User reached limit ({current_active_count}/{vip_limit}).")
                skipped_count += 1
                continue # Bỏ qua task này, thử task khác của user nếu còn

            # Check 3: Task này đã chạy chưa (phòng trường hợp trùng lặp)
            if target_username in user_active_tasks and not user_active_tasks[target_username].done():
                 logger.warning(f"[Resume Tasks] Skipping task {user_id_str} -> @{target_username}: Task seems already active.")
                 skipped_count += 1
                 continue

            # Check 4: Chat ID có hợp lệ không? (Phải là số int)
            if not isinstance(chat_id, int):
                 logger.error(f"[Resume Tasks] Skipping task {user_id_str} -> @{target_username}: Invalid chat_id '{chat_id}' found in persisted state.")
                 skipped_count += 1
                 continue

            # --- Resume task ---
            try:
                logger.info(f"[Resume Tasks] Resuming task for user {user_id_str} -> @{target_username} in chat {chat_id}")
                task = app.create_task(
                    run_treo_loop(user_id_str, target_username, context, chat_id),
                    name=f"treo_{user_id_str}_{target_username}_in_{chat_id}" # Giữ tên task nhất quán
                )
                active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
                current_active_count += 1 # Tăng số lượng đang chạy cho user này
                resumed_count += 1

                # Thông báo resume (tùy chọn, có thể gửi vào group ALLOWED_GROUP_ID hoặc chat_id gốc)
                # Gửi vào chat_id gốc nơi lệnh /treo được gọi
                try:
                    user_mention = f"User ID <code>{user_id_str}</code>"
                    try: # Cố gắng lấy mention
                        user_info = await context.bot.get_chat(user_id)
                        if user_info and hasattr(user_info, 'mention_html'): user_mention = user_info.mention_html()
                    except Exception: pass
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🔄 {user_mention}, tác vụ treo cho <code>@{html.escape(target_username)}</code> đã được tự động khôi phục sau khi bot khởi động lại.",
                        parse_mode=ParseMode.HTML,
                        disable_notification=True
                    )
                except Exception as e_notify:
                     logger.warning(f"[Resume Tasks] Failed to send resumption notification to chat {chat_id} for {user_id_str}->{target_username}: {e_notify}")

            except Exception as e_resume:
                logger.error(f"[Resume Tasks] Failed to create/resume task for {user_id_str} -> @{target_username}: {e_resume}", exc_info=True)
                skipped_count += 1

    logger.info(f"[Resume Tasks] Resumption process finished. Resumed: {resumed_count}, Skipped: {skipped_count} out of {total_to_resume} potential tasks.")
    # Xóa danh sách cần resume sau khi xử lý xong
    pending_treo_resumptions.clear()


# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    start_time = time.time()
    print("--- Bot DinoTool Starting ---")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"Bot Token: {'Loaded' if BOT_TOKEN else 'Missing!'}")
    if ALLOWED_GROUP_ID: print(f"Primary Group ID (Bills/Stats/Resume): {ALLOWED_GROUP_ID}")
    else: print("ALLOWED_GROUP_ID: Not Set.")
    print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Link Shortener Key: {'Loaded' if LINK_SHORTENER_API_KEY else 'Missing!'}")
    print(f"Tim API Key: {'Loaded' if API_KEY else 'Missing!'}")
    print(f"Follow API URL: {FOLLOW_API_URL_BASE}")
    print(f"Info TT API URL: {INFO_TT_API_URL_TEMPLATE}") # Thêm log API info
    print(f"Data File: {DATA_FILE}")
    # ... (các log khác) ...
    print("-" * 30)

    print("Loading persistent data...")
    load_data()
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Cooldowns: Tim={len(user_tim_cooldown)}, Fl={len(user_fl_cooldown)}, GetKey={len(user_getkey_cooldown)}")
    print(f"Initial Treo Stats Users: {len(treo_stats)}, Last Stats Report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")
    print(f"Pending Treo Resumptions: {sum(len(t) for t in pending_treo_resumptions.values())} tasks for {len(pending_treo_resumptions)} users") # Log số task cần resume

    # Cấu hình Application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .pool_timeout(120) # Tăng timeout để xử lý nhiều task hơn nếu cần
        .connect_timeout(60)
        .read_timeout(90)
        .write_timeout(90)
        .build()
    )

    # --- Schedule Jobs ---
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
    logger.info(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")
    application.job_queue.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="report_treo_stats_job")
    logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours.")

    # --- Register Handlers ---
    # Lệnh chung
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("getkey", getkey_command))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command))
    application.add_handler(CommandHandler("tim", tim_command))
    application.add_handler(CommandHandler("fl", fl_command))
    application.add_handler(CommandHandler("muatt", muatt_command))
    application.add_handler(CommandHandler("treo", treo_command))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command))
    application.add_handler(CommandHandler("tt", tt_command)) # Thêm handler /tt

    # Lệnh Admin
    application.add_handler(CommandHandler("addtt", addtt_command))
    application.add_handler(CommandHandler("removett", removett_command)) # Thêm handler /removett

    # Callback Query Handler
    application.add_handler(CallbackQueryHandler(prompt_send_bill_callback, pattern="^prompt_send_bill$"))

    # Handler ảnh/bill (chỉ trong group ALLOWED)
    if ALLOWED_GROUP_ID:
        photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & filters.Chat(chat_id=ALLOWED_GROUP_ID) & (~filters.COMMAND) & filters.UpdateType.MESSAGE
        application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill))
        logger.info(f"Registered photo/bill handler for group {ALLOWED_GROUP_ID} only.")
    else:
         logger.warning("Photo/bill handler is disabled because ALLOWED_GROUP_ID is not set.")

    # --- YÊU CẦU: Khôi phục task treo sau khi bot sẵn sàng ---
    # Chạy hàm resume một lần sau khi application đã được build
    application.create_task(resume_persisted_tasks(application), name="resume_treo_tasks_on_startup")

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time
    print(f"(Initialization took {run_duration:.2f} seconds)")

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping bot gracefully...")
        logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an unhandled exception: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped due to unhandled exception: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown sequence...")
        logger.info("Initiating shutdown sequence...")

        # Dừng các task treo đang chạy
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("Collecting active treo tasks for shutdown...")
            for user_id_str, targets in list(active_treo_tasks.items()):
                for target_username, task in list(targets.items()):
                    if task and not task.done(): tasks_to_stop_on_shutdown.append(task)
        if tasks_to_stop_on_shutdown:
            print(f"Found {len(tasks_to_stop_on_shutdown)} active treo tasks. Attempting cancellation...")
            try: asyncio.run(shutdown_async_tasks(tasks_to_stop_on_shutdown))
            except Exception as e_shutdown: logger.error(f"Error during async task shutdown: {e_shutdown}", exc_info=True)
        else: print("No active treo tasks found running at shutdown.")

        print("Attempting final data save...")
        logger.info("Attempting final data save...")
        save_data() # Lưu trạng thái cuối cùng, bao gồm các task đang chạy để resume lần sau
        print("Final data save attempt complete.")

        print("Bot has stopped.")
        logger.info("Bot has stopped.")
        print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: Could not execute main function: {e}")
        logger.critical(f"FATAL ERROR preventing main execution: {e}", exc_info=True)
        # Log lỗi nghiêm trọng ra file riêng
        with open("fatal_error.log", "a", encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - FATAL ERROR: {e}\n")
            import traceback
            traceback.print_exc(file=f)

