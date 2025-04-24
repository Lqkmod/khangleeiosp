
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
ALLOWED_GROUP_ID = -1002191171631 # <--- GROUP ID CỦA BẠN (Bot chỉ hoạt động trong group này)
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

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (KHÔNG ĐỔI)
FOLLOW_API_URL_BASE = "https://apitangfltiktok.soundcast.me/telefl.php" # <-- API FOLLOW MỚI (BASE URL)

# --- Thông tin VIP ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg"
BANK_ACCOUNT = "KHANGDINO" # <--- THAY STK CỦA BẠN
BANK_NAME = "MB BANK" # <--- THAY TÊN NGÂN HÀNG
ACCOUNT_NAME = "NGO DINH VINH KHANG" # <--- THAY TÊN CHỦ TK
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
    # Chuyển key là số thành string để đảm bảo tương thích JSON
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    string_key_vip_users = {str(k): v for k, v in vip_users.items()}
    # Lưu trữ dữ liệu thống kê treo
    string_key_treo_stats = {str(uid): {uname: gain for uname, gain in udict.items()} for uid, udict in treo_stats.items()}

    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "vip_users": string_key_vip_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        },
        "treo_stats": string_key_treo_stats, # Thêm thống kê
        "last_stats_report_time": last_stats_report_time # Thêm thời gian báo cáo cuối
    }
    try:
        # Sử dụng ghi an toàn hơn (ghi vào file tạm rồi đổi tên)
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        os.replace(temp_file, DATA_FILE) # Đổi tên file tạm thành file chính
        logger.debug(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)
        # Cố gắng xóa file tạm nếu có lỗi
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
                # Đảm bảo key là string khi tải
                activated_users = {str(k): v for k, v in data.get("activated_users", {}).items()}
                vip_users = {str(k): v for k, v in data.get("vip_users", {}).items()}

                all_cooldowns = data.get("user_cooldowns", {})
                user_tim_cooldown = {str(k): v for k, v in all_cooldowns.get("tim", {}).items()}
                loaded_fl = all_cooldowns.get("fl", {})
                user_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in loaded_fl.items()}
                user_getkey_cooldown = {str(k): v for k, v in all_cooldowns.get("getkey", {}).items()}

                # Tải dữ liệu thống kê
                loaded_stats = data.get("treo_stats", {})
                treo_stats = {str(uid): {uname: gain for uname, gain in udict.items()} for uid, udict in loaded_stats.items()}
                last_stats_report_time = data.get("last_stats_report_time", 0)

                logger.info(f"Data loaded successfully from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}
            treo_stats = {}
            last_stats_report_time = 0
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        # Khởi tạo lại tất cả về trống nếu file bị lỗi
        valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}
        treo_stats = {}
        last_stats_report_time = 0

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    # Chỉ xóa trong group cho phép hoặc chat riêng với bot
    if original_chat_id != ALLOWED_GROUP_ID and update.effective_chat.type != 'private':
        logger.debug(f"Not deleting message {msg_id_to_delete} from unauthorized chat {original_chat_id}")
        return
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
    # Chỉ gửi trong group cho phép hoặc chat riêng
    if update.effective_chat.id != ALLOWED_GROUP_ID and update.effective_chat.type != 'private':
         logger.warning(f"Attempted to send temporary message to unauthorized chat {update.effective_chat.id}")
         return

    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if update.message else None
        if reply and reply_to_msg_id:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
        else:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True)

        if sent_message and context.job_queue:
            # Tạo job name duy nhất hơn
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
    # Đảm bảo phần chữ số và chữ hoa sau dấu gạch nối
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
            # Chờ task kết thúc sau khi cancel, với timeout nhỏ
            await asyncio.wait_for(task, timeout=1.0)
            logger.info(f"[Treo Task Stop] Task {user_id_str} -> @{target_username} finished after cancellation.")
        except asyncio.CancelledError:
            logger.info(f"[Treo Task Stop] Task {user_id_str} -> @{target_username} confirmed cancelled.")
            pass # Mong đợi
        except asyncio.TimeoutError:
             logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled task {user_id_str}->{target_username} to finish. Assuming stopped.")
        except Exception as e:
             logger.error(f"[Treo Task Stop] Error awaiting cancelled task for {user_id_str}->{target_username}: {e}")

        # Xóa khỏi danh sách sau khi đã cancel (hoặc cố gắng cancel)
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            del active_treo_tasks[user_id_str][target_username]
            if not active_treo_tasks[user_id_str]: # Nếu không còn task nào cho user này
                del active_treo_tasks[user_id_str]
            logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active tasks.")
            return True
        else:
             logger.warning(f"[Treo Task Stop] Task entry for {user_id_str} -> {target_username} already removed after cancellation attempt.")
             return True # Coi như thành công vì task không còn trong danh sách
    elif task and task.done():
         logger.info(f"[Treo Task Stop] Task for {user_id_str} -> @{target_username} was already done. Removing entry.")
         # Xóa khỏi danh sách nếu task đã hoàn thành nhưng entry vẫn còn
         if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
             del active_treo_tasks[user_id_str][target_username]
             if not active_treo_tasks[user_id_str]:
                 del active_treo_tasks[user_id_str]
             return True
         return False # Đã xong nhưng không có trong danh sách -> không tính là "dừng" được nữa
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
            # Hàm stop_treo_task đã bao gồm việc xóa khỏi dict
            if await stop_treo_task(user_id_str, target_username, context, reason):
                stopped_count += 1
        # Đảm bảo key user được xóa khỏi dict chính nếu nó vẫn còn vì lý do nào đó (ví dụ: lỗi khi xóa trong stop_treo_task)
        if user_id_str in active_treo_tasks and not active_treo_tasks[user_id_str]:
             del active_treo_tasks[user_id_str]
        logger.info(f"Finished stopping tasks for user {user_id_str}. Stopped: {stopped_count}/{len(targets_to_stop)}")
    else:
        logger.info(f"No active treo tasks found for user {user_id_str} to stop.")

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs)."""
    global valid_keys, activated_users, vip_users
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    data_changed = False

    logger.info("[Cleanup] Starting cleanup job...")

    # Check expired keys (chưa sử dụng)
    # Sử dụng list() để tạo bản sao, tránh lỗi thay đổi dict khi đang duyệt
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
    vip_users_to_stop_tasks = [] # Lưu user ID cần dừng task
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            expiry = float(vip_data.get("expiry", 0))
            if current_time > expiry:
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str) # Dừng task khi hết hạn
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry timestamp '{vip_data.get('expiry')}' for VIP user {user_id_str}, removing.")
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str) # Dừng task nếu dữ liệu lỗi

    # Perform deletions from data structures
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

    # Stop tasks for expired/invalid VIPs *after* updating the vip_users dict
    if vip_users_to_stop_tasks:
         logger.info(f"[Cleanup] Stopping tasks for {len(vip_users_to_stop_tasks)} expired/invalid VIP users: {vip_users_to_stop_tasks}")
         # Sử dụng application context để đảm bảo chạy đúng cách trong job
         app = context.application
         for user_id_str in vip_users_to_stop_tasks:
             # Tạo task riêng để dừng task của user, tránh block job cleanup quá lâu
             # Sử dụng context của job thay vì application context trực tiếp
             app.create_task(
                 stop_all_treo_tasks_for_user(user_id_str, context, reason="VIP Expired/Removed during Cleanup"),
                 # name=f"cleanup_stop_task_{user_id_str}" # Tên task (optional)
            )

    # Save if data changed
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
            if time.time() < expiry_time:
                return True
            # Không cần xóa ở đây, cleanup job sẽ làm
            else:
                logger.debug(f"VIP check for {user_id_str}: Expired (Expiry: {expiry_time}, Current: {time.time()})")
                # Có thể gọi cleanup ngay nếu muốn, nhưng để job làm thì tốt hơn
                # if user_id_str in vip_users:
                #     del vip_users[user_id_str] # Không nên làm ở đây, để cleanup job xử lý đồng bộ
        except (ValueError, TypeError):
             logger.warning(f"VIP check for {user_id_str}: Invalid expiry data '{vip_data.get('expiry')}'. Treating as not VIP.")
             # if user_id_str in vip_users:
             #     del vip_users[user_id_str] # Không nên làm ở đây
    return False

def get_vip_limit(user_id: int) -> int:
    """Lấy giới hạn treo user của VIP."""
    user_id_str = str(user_id)
    if is_user_vip(user_id): # Check lại VIP status trước khi lấy limit
        vip_data = vip_users.get(user_id_str, {})
        # Lấy limit, mặc định là 0 nếu không tìm thấy hoặc lỗi
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
                 logger.debug(f"Key activation check for {user_id_str}: Expired (Expiry: {expiry_time_str}, Current: {time.time()})")
                 # if user_id_str in activated_users:
                 #     del activated_users[user_id_str] # Để cleanup job xử lý
        except (ValueError, TypeError):
             logger.warning(f"Key activation check for {user_id_str}: Invalid expiry data '{expiry_time_str}'. Treating as not activated.")
             # if user_id_str in activated_users:
             #     del activated_users[user_id_str] # Để cleanup job xử lý
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl) không (VIP hoặc đã kích hoạt key)."""
    # Ưu tiên check VIP trước vì nó không hết hạn nhanh như key
    is_vip = is_user_vip(user_id)
    is_key = is_user_activated_by_key(user_id)
    logger.debug(f"Feature check for {user_id}: VIP={is_vip}, KeyActivated={is_key}")
    return is_vip or is_key

# --- Logic API Follow (Tách ra để dùng cho /fl và /treo) ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """
    Gọi API follow và trả về kết quả dưới dạng dict.
    Returns: {"success": bool, "message": str, "data": dict | None}
    'data' có thể chứa thông tin user, follower counts nếu API trả về.
    """
    api_params = {
        "user": target_username,
        "userid": user_id_str,
        "tokenbot": bot_token
    }
    # Log params nhưng giấu token
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")

    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None}
    try:
        # Tăng timeout vì API này có thể chậm
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                # Cố gắng đọc text response để debug, giới hạn độ dài
                response_text_for_debug = await resp.aread()
                response_text_for_debug = response_text_for_debug.decode('utf-8', errors='replace')[:1000]
            except Exception as e_read:
                 logger.warning(f"[API Call @{target_username}] Error reading response body: {e_read}")


            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                    result["data"] = data # Lưu data dù thành công hay thất bại

                    # API này trả về status: true/false
                    api_status = data.get("status") # True hoặc False
                    api_message = data.get("message", "Không có thông báo từ API.")

                    if api_status is True:
                        result["success"] = True
                        result["message"] = api_message or "Follow thành công."
                    else:
                        # status là false hoặc null/không có
                        result["success"] = False
                        result["message"] = api_message or f"Follow thất bại (API status={api_status})."

                except json.JSONDecodeError as e_json:
                    logger.error(f"[API Call @{target_username}] Response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    result["message"] = f"Lỗi: API không trả về JSON hợp lệ (Code: {resp.status_code})."
                except Exception as e_proc:
                    logger.error(f"[API Call @{target_username}] Error processing API JSON data: {e_proc}", exc_info=True)
                    result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
            # Xử lý các trường hợp lỗi khác
            elif resp.status_code == 200 and "application/json" not in content_type:
                 logger.error(f"[API Call @{target_username}] Response 200 OK but wrong Content-Type: {content_type}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type}, Code: {resp.status_code})."
            elif resp.status_code != 200:
                 logger.error(f"[API Call @{target_username}] HTTP Error Status: {resp.status_code}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi từ API follow (Code: {resp.status_code})."
            else: # Trường hợp không rơi vào các nhánh trên (ít xảy ra)
                 logger.error(f"[API Call @{target_username}] Unexpected response state. Status: {resp.status_code}, Content-Type: {content_type}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi không xác định từ API (Code: {resp.status_code})."

    except httpx.TimeoutException:
        logger.warning(f"[API Call @{target_username}] API timeout.")
        result["message"] = f"Lỗi: API timeout khi follow @{html.escape(target_username)}."
    except httpx.ConnectError as e_connect:
        logger.error(f"[API Call @{target_username}] Connection error: {e_connect}", exc_info=False) # Không cần full stack trace
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

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start."""
    if not update or not update.message: return
    user = update.effective_user
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id

    # Chỉ hoạt động trong group cho phép hoặc chat riêng
    if chat_type != 'private' and chat_id != ALLOWED_GROUP_ID:
        logger.info(f"User {user.id} tried /start in unauthorized group ({chat_id}). Ignored.")
        return

    act_h = ACTIVATION_DURATION_SECONDS // 3600
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
           f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n"
           f"<i>Bot này hoạt động tốt nhất trong nhóm hỗ trợ chính thức.</i>\n\n"
           f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
           f"   1️⃣ Dùng <code>/getkey</code> để nhận link (chỉ trong nhóm).\n"
           f"   2️⃣ Truy cập link, làm theo các bước để lấy Key.\n"
           f"       (Ví dụ: <code>Dinotool-ABC123XYZ</code>).\n"
           f"   3️⃣ Quay lại nhóm, dùng <code>/nhapkey &lt;key_cua_ban&gt;</code>.\n"
           f"   4️⃣ Sau khi kích hoạt, bạn có thể dùng <code>/tim</code> và <code>/fl</code> trong <b>{act_h} giờ</b>.\n\n"
           f"👑 <b>Nâng cấp VIP:</b>\n"
           f"   » Xem chi tiết và hướng dẫn với lệnh <code>/muatt</code> (chỉ trong nhóm).\n"
           f"   » Thành viên VIP có thể dùng <code>/treo</code>, <code>/dungtreo</code>, không cần lấy key và có nhiều ưu đãi khác.\n\n"
           f"ℹ️ <b>Danh sách lệnh:</b>\n"
           f"   » Gõ <code>/lenh</code> để xem tất cả các lệnh và trạng thái của bạn.\n\n"
           f"💬 Cần hỗ trợ? Liên hệ Admin trong nhóm.\n"
           f"<i>Bot được phát triển bởi <a href='https://t.me/dinotool'>DinoTool</a></i>")

    try:
        await update.message.reply_html(msg, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start message to {user.id} in chat {chat_id}: {e}")

async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type

    # Chỉ hoạt động trong group cho phép hoặc chat riêng
    if chat_type != 'private' and chat_id != ALLOWED_GROUP_ID:
        logger.info(f"User {user.id} tried /lenh in unauthorized group ({chat_id}). Ignored.")
        return

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
            except (ValueError, TypeError, OSError): pass # Bỏ qua nếu timestamp lỗi
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

    # Chỉ hiển thị lệnh Admin cho Admin
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;days&gt;</code> - Thêm ngày VIP (VD: /addtt 12345 30)")
        cmd_lines.append(f"  <code>/removett &lt;user_id&gt;</code> - Xóa VIP") # Có thể thêm lệnh xóa VIP
        cmd_lines.append(f"  <code>/stats</code> - Xem thống kê bot (chưa implement)") # Ví dụ

    cmd_lines.append("\n<b><u>ℹ️ Lệnh Chung:</u></b>")
    cmd_lines.append(f"  <code>/start</code> - Tin nhắn chào mừng")
    cmd_lines.append(f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này")

    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng.</i>")

    help_text = "\n".join(status_lines + cmd_lines)

    try:
        # Xóa lệnh gốc của user
        await delete_user_message(update, context)
        # Gửi bảng lệnh
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return # Không thể xảy ra nhưng để an toàn
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/tim command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt tài khoản bằng key</b> để sử dụng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » nhận link » lấy key » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
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
                await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp '{last_usage_str}' for /tim user {user_id}. Resetting.")
             if user_id_str in user_tim_cooldown:
                 del user_tim_cooldown[user_id_str]
                 save_data() # Lưu lại khi reset cooldown lỗi

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
        await delete_user_message(update, context, original_message_id)
        return

    # --- API Key Check ---
    if not API_KEY:
        logger.error(f"Missing API_KEY for /tim command triggered by user {user_id}")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key cho chức năng này. Vui lòng báo Admin.", duration=20)
        return

    # --- Call API ---
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    # Log URL nhưng giấu API key
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")

    processing_msg = None
    final_response_text = ""
    is_success = False

    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc ngay sau khi gửi tin chờ

        async with httpx.AsyncClient(verify=True, timeout=60.0) as client: # Tăng timeout
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
                    if data.get("success"): # API này dùng success: true/false
                        user_tim_cooldown[user_id_str] = time.time()
                        save_data()
                        is_success = True
                        # Trích xuất dữ liệu cẩn thận hơn
                        d = data.get("data", {})
                        a = html.escape(str(d.get("author", "?")))
                        ct = html.escape(str(d.get("create_time", "?")))
                        v = html.escape(str(d.get("video_url", video_url))) # Giữ link gốc nếu API ko trả về
                        db = html.escape(str(d.get('digg_before', '?')))
                        di = html.escape(str(d.get('digg_increased', '?')))
                        da = html.escape(str(d.get('digg_after', '?')))

                        final_response_text = (
                            f"🎉 <b>Tăng Tim Thành Công!</b> ❤️\n"
                            f"👤 Cho: {user.mention_html()}\n\n"
                            f"📊 <b>Thông tin Video:</b>\n"
                            f"🎬 <a href='{v}'>Link Video</a>\n"
                            f"✍️ Tác giả: <code>{a}</code>\n"
                            # f"🗓️ Tạo lúc: <code>{ct}</code>\n" # Có thể bỏ bớt nếu quá dài
                            f"👍 Trước: <code>{db}</code> ➜ 💖 Tăng: <code>+{di}</code> ➜ ✅ Sau: <code>{da}</code>"
                        )
                    else:
                        # API báo lỗi (success: false hoặc thiếu)
                        api_msg = data.get('message', 'Không rõ lý do từ API')
                        logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
                        final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {user.mention_html()}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"
                except json.JSONDecodeError as e_json:
                    logger.error(f"/tim API response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API</b>\n👤 Cho: {user.mention_html()}\nℹ️ API không trả về JSON hợp lệ."
            else:
                # Lỗi HTTP hoặc Content-Type sai
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
                 # Bỏ qua lỗi "Message is not modified"
                 if "Message is not modified" in str(e_edit): pass
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: Message not found (maybe deleted?)")
                 else: logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except Forbidden as e_edit:
                 logger.warning(f"Bot lacks permission to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except TelegramError as e_edit:
                 logger.error(f"Telegram error editing /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: # Bắt các lỗi khác có thể xảy ra
                 logger.error(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             # Nếu không có tin nhắn chờ (do lỗi trước đó?), gửi tin nhắn mới
             # Điều này ít khi xảy ra nếu logic đúng
             logger.warning(f"Processing message for /tim user {user_id} was None. Sending new message.")
             try:
                 await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send:
                  logger.error(f"Failed to send final /tim message for user {user_id} after processing msg was None: {e_send}")

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
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]
    api_message = api_result["message"]
    api_data = api_result["data"] # Có thể là None
    final_response_text = ""

    # --- Xây dựng khối thông tin người dùng ---
    user_info_block = ""
    if api_data:
        # Cố gắng lấy thông tin từ API, fallback về target_username nếu thiếu
        name = html.escape(str(api_data.get("name", "?")))
        tt_username_from_api = api_data.get("username")
        tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
        tt_user_id = html.escape(str(api_data.get("user_id", "?")))
        khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
        avatar = api_data.get("avatar", "") # URL ảnh
        create_time = html.escape(str(api_data.get("create_time", "?"))) # Ngày tạo tài khoản

        user_info_lines = []
        user_info_lines.append(f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)")
        if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
        if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
        if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
        if avatar and avatar.startswith("http"): user_info_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>")

        if user_info_lines: # Chỉ thêm nếu có ít nhất 1 dòng thông tin
             user_info_block = "\n".join(user_info_lines) + "\n" # Thêm dòng trống sau khối

    # --- Xây dựng khối thông tin follower ---
    follower_info_block = ""
    if api_data:
        f_before = html.escape(str(api_data.get("followers_before", "?")))
        f_add = html.escape(str(api_data.get("followers_add", "?")))
        f_after = html.escape(str(api_data.get("followers_after", "?")))

        # Chỉ hiển thị nếu có ít nhất một thông tin về follower
        if f_before != "?" or f_add != "?" or f_after != "?":
            follower_lines = ["📈 <b>Số lượng Follower:</b>"]
            if f_before != "?": follower_lines.append(f"   Trước: <code>{f_before}</code>")
            if f_add != "?" and f_add != "0": # Chỉ hiển thị tăng nếu khác 0
                 follower_lines.append(f"   Tăng:   <b><code>+{f_add}</code></b> ✨")
            elif f_add == "0":
                 follower_lines.append(f"   Tăng:   <code>+{f_add}</code>")
            if f_after != "?": follower_lines.append(f"   Sau:    <code>{f_after}</code>")
            follower_info_block = "\n".join(follower_lines)

    # --- Tạo nội dung phản hồi cuối cùng ---
    if success:
        current_time = time.time()
        # Cập nhật cooldown
        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = current_time
        save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")

        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n"
            f"✨ Cho: {invoking_user_mention}\n\n"
            f"{user_info_block}" # Đã có \n ở cuối nếu không rỗng
            f"{follower_info_block}"
        )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n"
            f"👤 Cho: {invoking_user_mention}\n"
            f"🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message)}</i>\n\n"
            f"{user_info_block}" # Vẫn hiển thị thông tin user nếu có
        )
        # Thêm gợi ý nếu API báo chờ
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
        if "Message is not modified" in str(e): pass # Bỏ qua nếu nội dung không đổi
        elif "message to edit not found" in str(e).lower(): logger.warning(f"[BG Task /fl] Message {processing_msg_id} not found for editing.")
        elif "Can't parse entities" in str(e) or "nested" in str(e).lower():
             logger.warning(f"[BG Task /fl] HTML parse error editing {processing_msg_id}. Falling back to plain text.")
             try:
                 # Chuyển HTML thành plain text đơn giản
                 plain_text = re.sub('<[^<]+?>', '', final_response_text)
                 plain_text = html.unescape(plain_text)
                 plain_text += "\n\n(Lỗi hiển thị định dạng)"
                 await context.bot.edit_message_text(chat_id, processing_msg_id, plain_text[:4096], disable_web_page_preview=True)
             except Exception as pt_edit_err: logger.error(f"[BG Task /fl] Failed plain text fallback edit for {processing_msg_id}: {pt_edit_err}")
        else: logger.error(f"[BG Task /fl] BadRequest editing msg {processing_msg_id}: {e}")
    except Forbidden as e:
         logger.error(f"[BG Task /fl] Bot lacks permission to edit msg {processing_msg_id}: {e}")
    except TelegramError as e:
         logger.error(f"[BG Task /fl] Telegram error editing msg {processing_msg_id}: {e}")
    except Exception as e:
         logger.error(f"[BG Task /fl] Unexpected error editing msg {processing_msg_id}: {e}", exc_info=True)

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

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/fl command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để sử dụng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9_.]{2,24}$" # Regex chuẩn cho username TikTok

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập username TikTok.\n"
                   "<b>Cú pháp đúng:</b> <code>/fl username</code> (không cần @)")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@") # Xóa @ nếu có ở đầu

        if not uname:
            err_txt = "⚠️ Username không được để trống."
        # Kiểm tra kỹ hơn: không chứa ký tự đặc biệt ngoài . và _, không bắt đầu/kết thúc bằng ., độ dài 2-24
        elif not re.match(username_regex, uname) or uname.startswith('.') or uname.endswith('.'):
            err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                       f"Username chỉ chứa chữ cái, số, dấu chấm (.), dấu gạch dưới (_), dài 2-24 ký tự và không bắt đầu/kết thúc bằng dấu chấm.")
        else:
            target_username = uname # Lưu username đã được làm sạch

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
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
                     await delete_user_message(update, context, original_message_id)
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
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc ngay sau khi gửi tin chờ

        # Lên lịch chạy task nền
        if processing_msg and target_username:
            logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
            # Sử dụng application context để tạo task đảm bảo quản lý tốt hơn
            context.application.create_task(
                process_fl_request_background(
                    context=context, chat_id=chat_id, user_id_str=user_id_str,
                    target_username=target_username, processing_msg_id=processing_msg.message_id,
                    invoking_user_mention=invoking_user_mention
                ),
                # update=update # Không cần truyền update vào create_task trừ khi task nền cần
                name=f"fl_bg_{user_id_str}_{target_username}" # Tên task (optional)
            )
        elif not target_username:
             # Trường hợp này không nên xảy ra nếu logic parse đúng
             logger.error(f"Target username became None before scheduling background task for /fl user {user_id}.")
             if processing_msg:
                 await context.bot.edit_message_text(chat_id, processing_msg.message_id, "❌ Lỗi: Username không hợp lệ.")
        elif not processing_msg:
             # Trường hợp này cũng không nên xảy ra
             logger.error(f"Could not send processing message for /fl @{target_username}, cannot schedule background task.")

    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Failed to send processing message or schedule task for /fl @{target_username}: {e}")
        # Cố gắng xóa lệnh gốc nếu chưa xóa
        await delete_user_message(update, context, original_message_id)
    except Exception as e:
         logger.error(f"Unexpected error in fl_command for user {user_id} target @{target_username}: {e}", exc_info=True)
         await delete_user_message(update, context, original_message_id)


# --- Lệnh /getkey ---
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
        logger.info(f"/getkey command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
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
              if user_id_str in user_getkey_cooldown:
                  del user_getkey_cooldown[user_id_str]
                  save_data()

    # --- Tạo Key và Link ---
    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    # Thêm tham số ngẫu nhiên để tránh cache phía blogspot/link shortener
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster

    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    # Log params nhưng giấu token
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY) > 6 else "***",
                           "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_saved_to_dict = False # Cờ để biết key đã được thêm vào valid_keys chưa

    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

        # --- Lưu Key vào bộ nhớ TRƯỚC KHI gọi API rút gọn ---
        # Để nếu API rút gọn thất bại, ta có thể xóa key này đi
        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id,
            "generation_time": generation_time,
            "expiry_time": expiry_time,
            "used_by": None,
            "activation_time": None # Thêm trường này để lưu thời gian kích hoạt
        }
        key_saved_to_dict = True
        # Chưa cần save_data() ngay, đợi API thành công mới lưu
        logger.info(f"Key {generated_key} temporarily stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        # --- Gọi API Rút Gọn Link ---
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
                    # API Yeumoney trả về JSON ngay cả khi lỗi
                    response_data = response.json()
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status") # "success" hoặc "error"
                    generated_short_url = response_data.get("shortenedUrl") # URL rút gọn

                    if status == "success" and generated_short_url:
                        # Thành công -> Cập nhật cooldown và LƯU DỮ LIỆU (bao gồm key mới)
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
                        # API báo lỗi (status="error" hoặc thiếu shortenedUrl)
                        api_message = response_data.get("message", "Lỗi không xác định từ API rút gọn link.")
                        logger.error(f"Shortener API returned error for user {user_id}. Status: {status}, Message: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Khi Tạo Link:</b>\n<code>{html.escape(str(api_message))}</code>\nVui lòng thử lại sau hoặc báo Admin."
                        # Xóa key đã tạm lưu vì không tạo được link
                        if key_saved_to_dict and generated_key in valid_keys:
                            del valid_keys[generated_key]
                            logger.info(f"Removed temporary key {generated_key} due to shortener API error.")
                            # Không cần save_data() vì key chưa bao giờ được lưu chính thức

                except json.JSONDecodeError:
                    logger.error(f"Shortener API Status 200 but JSON decode failed. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API:</b> Máy chủ rút gọn link trả về dữ liệu không hợp lệ. Vui lòng thử lại sau."
                    # Xóa key đã tạm lưu
                    if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; logger.info(f"Removed temporary key {generated_key} due to JSON decode error.")
            else:
                 # Lỗi HTTP từ API rút gọn
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Mã: {response.status_code}). Vui lòng thử lại sau hoặc báo Admin."
                 # Xóa key đã tạm lưu
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
                # Cho phép xem trước link rút gọn
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=False
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: Message not found.")
                 else: logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except Forbidden as e_edit:
                 logger.warning(f"Bot lacks permission to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except TelegramError as e_edit:
                 logger.error(f"Telegram error editing /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit:
                 logger.error(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit}", exc_info=True)
        else:
             # Nếu không có tin nhắn chờ (do lỗi trước đó?), gửi tin nhắn mới
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new message.")
             try:
                  await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
             except Exception as e_send:
                  logger.error(f"Failed to send final /getkey message for user {user_id} after processing msg was None: {e_send}")

# --- Lệnh /nhapkey ---
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
        logger.info(f"/nhapkey command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # --- Parse Input ---
    args = context.args
    submitted_key = None
    err_txt = ""
    key_prefix = "Dinotool-"
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]+$") # Regex kiểm tra định dạng

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập key.\n"
                   "<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1:
        err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        # Kiểm tra định dạng key chặt chẽ bằng regex
        if not key_format_regex.match(key_input):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\n"
                        f"Key phải bắt đầu bằng <code>{key_prefix}</code> và theo sau là các chữ cái IN HOA hoặc số.")
        else:
            submitted_key = key_input # Key hợp lệ về mặt định dạng

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
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' not found in valid_keys.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại. Vui lòng kiểm tra lại hoặc dùng <code>/getkey</code> để lấy key mới."
    elif key_data.get("used_by") is not None:
        used_by_id = key_data["used_by"]
        activation_time_ts = key_data.get("activation_time") # Lấy timestamp kích hoạt
        used_time_str = "không rõ thời gian"
        if activation_time_ts:
            try:
                used_time_str = f"lúc {datetime.fromtimestamp(float(activation_time_ts)).strftime('%H:%M:%S ngày %d/%m/%Y')}"
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Could not format activation timestamp '{activation_time_ts}' for used key {submitted_key}: {e}")
                pass # Giữ lại "không rõ thời gian"

        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by themself {used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi ({used_time_str})."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by another user ({used_by_id}) {used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng {used_time_str}."

    elif current_time > float(key_data.get("expiry_time", 0)): # Cần float() để so sánh
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = "không rõ thời gian"
        if expiry_time_ts:
            try:
                expiry_time_str = f"vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S ngày %d/%m/%Y')}"
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Could not format expiry timestamp '{expiry_time_ts}' for expired key {submitted_key}: {e}")
                pass

        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired {expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng {expiry_time_str}. Vui lòng dùng <code>/getkey</code> để lấy key mới."
        # Dọn dẹp key hết hạn ngay lập tức khỏi bộ nhớ và lưu lại
        if submitted_key in valid_keys:
             del valid_keys[submitted_key]
             save_data()
             logger.info(f"Removed expired key {submitted_key} from valid_keys upon activation attempt.")
    else:
        # Kích hoạt thành công!
        try:
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts # Lưu thời điểm hết hạn kích hoạt
            save_data() # Lưu trạng thái mới của key và activated_users

            expiry_dt = datetime.fromtimestamp(activation_expiry_ts)
            expiry_str = expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
            activation_success = True
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
             # Cân nhắc rollback trạng thái key nếu cần, nhưng thường lỗi ở đây là lúc lưu hoặc tính toán
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None # Thử rollback
                 valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users:
                 del activated_users[user_id_str] # Thử rollback

    # --- Gửi phản hồi cuối cùng ---
    await delete_user_message(update, context, original_message_id)
    try:
        await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
         logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")

# --- Lệnh /muatt ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    original_message_id = update.message.message_id

    # --- Check quyền truy cập ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/muatt command used outside allowed group ({chat_id}) by user {user.id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        # Có thể gửi tin nhắn tạm thời báo lỗi nếu muốn
        # await send_temporary_message(update, context, "Lệnh <code>/muatt</code> chỉ dùng được trong nhóm chính.", duration=10)
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
    text_lines.append(f"   2️⃣ Gửi ảnh đó <b>vào nhóm chat này</b>.")
    text_lines.append(f"   3️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin để xác nhận.")
    text_lines.append(f"   4️⃣ Admin sẽ kiểm tra và kích hoạt VIP cho bạn trong thời gian sớm nhất.")

    text_lines.append("\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️")

    text = "\n".join(text_lines)

    # --- Gửi tin nhắn kèm ảnh QR ---
    await delete_user_message(update, context, original_message_id) # Xóa lệnh /muatt

    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=QR_CODE_URL,
            caption=text,
            parse_mode=ParseMode.HTML
        )
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending /muatt photo+caption to chat {chat_id}: {e}")
        # Fallback: Gửi chỉ text nếu gửi ảnh lỗi
        logger.info(f"Falling back to sending text only for /muatt in chat {chat_id}")
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")
    except Exception as e_unexp:
        logger.error(f"Unexpected error sending /muatt command to chat {chat_id}: {e_unexp}", exc_info=True)


# --- Xử lý nhận ảnh bill ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh được gửi trong nhóm và chuyển tiếp cho admin."""
    if not update or not update.message: return
    # Chỉ xử lý trong group cho phép và không phải là caption của lệnh khác
    if update.effective_chat.id != ALLOWED_GROUP_ID or update.message.text and update.message.text.startswith('/'):
        # logger.debug(f"Ignoring message in handle_photo_bill: chat_id={update.effective_chat.id}, text='{update.message.text}'")
        return

    # Kiểm tra xem tin nhắn có phải là ảnh hoặc file ảnh không
    is_photo = bool(update.message.photo)
    is_image_document = bool(update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith('image/'))

    if not is_photo and not is_image_document:
        # logger.debug("Message is not photo or image document.")
        return

    user = update.effective_user
    chat = update.effective_chat
    message_id = update.message.message_id
    if not user or not chat: return # An toàn

    logger.info(f"Potential bill received: Photo/Doc in group {chat.id} from user {user.id} ({user.username or 'no_username'}). Forwarding to admin {ADMIN_USER_ID}.")

    # --- Tạo caption cho tin nhắn chuyển tiếp ---
    forward_caption_lines = []
    forward_caption_lines.append(f"📄 <b>Bill/Ảnh Nhận Được</b>")
    forward_caption_lines.append(f"👤 <b>Từ User:</b> {user.mention_html()} (<code>{user.id}</code>)")
    forward_caption_lines.append(f"👥 <b>Trong Group:</b> {html.escape(chat.title or str(chat.id))} (<code>{chat.id}</code>)")
    # Thêm link tới tin nhắn gốc trong group để admin dễ dàng truy cập
    try:
         # Lấy link tin nhắn một cách an toàn
         message_link = update.message.link
         if message_link:
             forward_caption_lines.append(f"🔗 <b>Link Tin Nhắn Gốc:</b> <a href='{message_link}'>Click vào đây</a>")
    except AttributeError:
         logger.debug(f"Could not get message link for message {message_id} in chat {chat.id}")
         forward_caption_lines.append(f"🔗 <b>Link Tin Nhắn Gốc:</b> (Không thể tạo)")


    # Thêm nội dung caption gốc (nếu có)
    original_caption = update.message.caption or update.message.text # Document có thể có text thay vì caption
    if original_caption:
         forward_caption_lines.append(f"\n💬 <b>Caption/Nội dung gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")

    forward_caption = "\n".join(forward_caption_lines)

    # --- Chuyển tiếp tin nhắn gốc và gửi caption ---
    try:
        # 1. Chuyển tiếp tin nhắn gốc (bao gồm ảnh/file)
        await context.bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=chat.id, message_id=message_id)
        # 2. Gửi tin nhắn chứa thông tin chi tiết
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=forward_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Successfully forwarded message {message_id} and sent info to admin {ADMIN_USER_ID}.")

        # --- Phản hồi nhẹ nhàng trong group (tùy chọn) ---
        # reply_text = f"✅ Đã gửi ảnh của {user.mention_html()} cho Admin xem xét."
        # await update.message.reply_html(reply_text, quote=True, disable_notification=True)
        # await send_temporary_message(update, context, reply_text, duration=60, reply=True)

    except Forbidden:
        logger.error(f"Bot cannot forward/send message to admin {ADMIN_USER_ID}. Check permissions/block status.")
        # Thông báo lỗi trong group để user biết
        try:
             error_admin_msg = f"⚠️ {user.mention_html()}, không thể gửi ảnh của bạn đến Admin lúc này (Bot bị chặn hoặc thiếu quyền). Vui lòng liên hệ Admin trực tiếp."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
        except Exception as e_reply:
             logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
    except TelegramError as e_fwd:
         logger.error(f"Telegram error forwarding bill message {message_id} to admin: {e_fwd}")
         try:
             error_admin_msg = f"⚠️ {user.mention_html()}, đã xảy ra lỗi khi gửi ảnh của bạn đến Admin. Vui lòng thử lại hoặc báo Admin."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
         except Exception as e_reply:
              logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")
    except Exception as e:
        logger.error(f"Unexpected error forwarding/sending bill to admin: {e}", exc_info=True)
        try:
             error_admin_msg = f"⚠️ {user.mention_html()}, lỗi hệ thống khi xử lý ảnh của bạn. Vui lòng báo Admin."
             await send_temporary_message(update, context, error_admin_msg, duration=60)
        except Exception as e_reply:
            logger.error(f"Failed to send error notification back to group {chat.id}: {e_reply}")


# --- Lệnh /addtt (Admin) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat: return
    original_message_id = update.message.message_id

    # --- Check Admin ---
    if admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id} ({admin_user.username}) in chat {chat.id}.")
        # Không cần xóa tin nhắn nếu không phải admin, chỉ cần bỏ qua
        # await delete_user_message(update, context, original_message_id)
        return

    # --- Check môi trường (nên dùng trong group chính) ---
    # Mặc dù lệnh chỉ chạy bởi admin, nhưng thông báo nên ở group chính
    # if chat.id != ALLOWED_GROUP_ID:
    #     await send_temporary_message(update, context, "⚠️ Lệnh /addtt nên được sử dụng trong group chính để thông báo cho người dùng.", duration=15, reply=True)
        # Không cần xóa lệnh của admin
        # await delete_user_message(update, context, original_message_id);
        # return # Vẫn cho phép chạy nếu admin cố tình dùng ở nơi khác

    # --- Parse Arguments ---
    args = context.args
    err_txt = None
    target_user_id = None
    days_to_add_input = None # Số ngày user nhập vào (15 hoặc 30)
    limit = None # Giới hạn user dựa trên gói
    duration_days = None # Số ngày thực tế (tên gói)

    valid_days = list(VIP_PRICES.keys()) # [15, 30]

    if len(args) != 2:
        err_txt = f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;số_ngày&gt;</code>\n<b>Ví dụ:</b> <code>/addtt 123456789 {valid_days[0]}</code> hoặc <code>/addtt 987654321 {valid_days[1]}</code>"
    else:
        # Validate User ID
        try:
            target_user_id = int(args[0])
            if target_user_id <= 0: raise ValueError("User ID must be positive")
        except ValueError:
            err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ. Phải là một số nguyên dương."

        # Validate Days và lấy thông tin gói
        if not err_txt: # Chỉ validate ngày nếu User ID hợp lệ
            try:
                days_to_add_input = int(args[1])
                if days_to_add_input not in VIP_PRICES:
                    err_txt = f"⚠️ Số ngày không hợp lệ. Chỉ chấp nhận: <b>{', '.join(map(str, valid_days))}</b> ngày."
                else:
                    # Lấy thông tin từ gói VIP tương ứng
                    vip_info = VIP_PRICES[days_to_add_input]
                    limit = vip_info["limit"]
                    duration_days = vip_info["duration_days"] # Số ngày thực tế của gói
            except ValueError:
                err_txt = f"⚠️ Số ngày '<code>{html.escape(args[1])}</code>' không phải là số hợp lệ."

    if err_txt:
        try: await update.message.reply_html(err_txt) # Phản hồi lỗi cho admin
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    # --- Cập nhật dữ liệu VIP ---
    target_user_id_str = str(target_user_id)
    current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time # Mặc định bắt đầu từ bây giờ

    operation_type = "Nâng cấp lên" # Mặc định là nâng cấp mới

    # Nếu user đã là VIP và chưa hết hạn, gia hạn thêm
    if current_vip_data:
         try:
             current_expiry = float(current_vip_data.get("expiry", 0))
             if current_expiry > current_time:
                 start_time = current_expiry # Bắt đầu tính từ ngày hết hạn cũ
                 operation_type = "Gia hạn thêm"
                 logger.info(f"User {target_user_id_str} already VIP. Extending from {datetime.fromtimestamp(start_time).isoformat()}.")
             else:
                  # VIP nhưng đã hết hạn, coi như nâng cấp mới
                  logger.info(f"User {target_user_id_str} was VIP but expired. Treating as new activation.")
         except (ValueError, TypeError):
              logger.warning(f"Invalid expiry data '{current_vip_data.get('expiry')}' for user {target_user_id_str}. Treating as new activation.")


    # Tính thời gian hết hạn mới
    new_expiry_ts = start_time + duration_days * 86400 # duration_days là số ngày của gói (15 hoặc 30)
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')

    # Cập nhật hoặc tạo mới dữ liệu VIP
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data() # Lưu lại dữ liệu
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days} days. New expiry: {new_expiry_str}, Limit: {limit}")

    # --- Gửi thông báo ---
    # 1. Thông báo cho Admin (trong chat hiện tại)
    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n"
                 f"✨ Gói: {duration_days} ngày\n"
                 f"⏳ Hạn sử dụng mới: <b>{new_expiry_str}</b>\n"
                 f"🚀 Giới hạn treo: <b>{limit} users</b>")
    try:
        await update.message.reply_html(admin_msg)
    except Exception as e:
        logger.error(f"Failed to send confirmation message to admin {admin_user.id} in chat {chat.id}: {e}")

    # 2. Thông báo cho người dùng (trong group chính ALLOWED_GROUP_ID)
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        user_mention = target_user_info.mention_html() if target_user_info else f"User ID <code>{target_user_id}</code>"
    except TelegramError as e_get_chat:
        logger.warning(f"Could not get chat info for target user {target_user_id}: {e_get_chat}. Using ID instead.")
        user_mention = f"User ID <code>{target_user_id}</code>"
    except Exception as e_get_chat:
        logger.error(f"Unexpected error getting chat info for {target_user_id}: {e_get_chat}", exc_info=True)
        user_mention = f"User ID <code>{target_user_id}</code>"


    group_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\n"
                 f"Bạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"✨ Gói VIP: <b>{duration_days} ngày</b>\n"
                 f"⏳ Hạn sử dụng đến: <b>{new_expiry_str}</b>\n"
                 f"🚀 Giới hạn treo: <b>{limit} tài khoản</b>\n\n"
                 f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n"
                 f"(Dùng <code>/lenh</code> để xem lại trạng thái)")
    try:
        # Luôn gửi thông báo vào group chính
        await context.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=group_msg, parse_mode=ParseMode.HTML)
        logger.info(f"Sent VIP notification to group {ALLOWED_GROUP_ID} for user {target_user_id}")
    except (BadRequest, Forbidden, TelegramError) as e_send_group:
        logger.error(f"Failed to send VIP notification to group {ALLOWED_GROUP_ID} for user {target_user_id}: {e_send_group}")
        # Có thể thông báo lại cho admin nếu gửi vào group thất bại
        try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} vào group {ALLOWED_GROUP_ID}. Lỗi: {e_send_group}")
        except Exception: pass
    except Exception as e_unexp_send:
         logger.error(f"Unexpected error sending VIP notification to group {ALLOWED_GROUP_ID} for user {target_user_id}: {e_unexp_send}", exc_info=True)

# --- Logic Treo ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE):
    """Vòng lặp chạy nền cho lệnh /treo."""
    user_id = int(user_id_str) # Chuyển lại thành int để check VIP
    task_name = f"treo_{user_id_str}_{target_username}"
    logger.info(f"[Treo Task Start] Task '{task_name}' started.")

    try:
        while True:
            # Check 1: Task còn trong danh sách active không? (Phòng trường hợp bị xóa bởi lệnh khác)
            current_task_in_dict = active_treo_tasks.get(user_id_str, {}).get(target_username)
            if current_task_in_dict is not asyncio.current_task():
                logger.warning(f"[Treo Task Stop] Task '{task_name}' seems replaced or removed from active_treo_tasks dict. Stopping.")
                break

            # Check 2: User còn VIP không? (Quan trọng)
            if not is_user_vip(user_id):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                # Tự động gọi hàm dừng để xóa task khỏi dict
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired")
                break

            # Thực hiện gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, context.bot.token)

            if api_result["success"]:
                gain = 0
                # Cập nhật thống kê nếu thành công và API trả về số lượt tăng
                if api_result["data"]:
                    try:
                        # followers_add có thể là số hoặc string
                        gain_str = str(api_result["data"].get("followers_add", "0"))
                        gain = int(gain_str)
                        if gain > 0:
                            # Dùng defaultdict để tự khởi tạo nếu chưa có
                            if user_id_str not in treo_stats:
                                treo_stats[user_id_str] = defaultdict(int)
                            elif not isinstance(treo_stats[user_id_str], defaultdict):
                                # Chuyển đổi dict thường thành defaultdict nếu cần
                                treo_stats[user_id_str] = defaultdict(int, treo_stats[user_id_str])

                            treo_stats[user_id_str][target_username] += gain
                            logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Current gain: {treo_stats[user_id_str][target_username]}")
                            # Không cần save_data() ở đây, để job thống kê hoặc các lệnh khác lưu
                        else:
                             logger.info(f"[Treo Task Success] Task '{task_name}' successful but gain was {gain}. API Msg: {api_result['message']}")
                    except (ValueError, TypeError) as e_gain:
                         logger.warning(f"[Treo Task Stats] Task '{task_name}' could not parse gain '{api_result['data'].get('followers_add')}' from API data: {e_gain}")
                    except Exception as e_stats:
                         logger.error(f"[Treo Task Stats] Task '{task_name}' unexpected error processing stats: {e_stats}", exc_info=True)
                else:
                    logger.info(f"[Treo Task Success] Task '{task_name}' successful but no data returned for stats. API Msg: {api_result['message']}")

            else: # API Follow thất bại
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed. API Msg: {api_result['message']}")
                # Quyết định có nên dừng task nếu lỗi lặp lại không? (Hiện tại chỉ log)
                # Ví dụ: nếu lỗi "Invalid username" thì nên dừng hẳn task đó.
                # if "invalid username" in api_result['message'].lower():
                #    logger.error(f"[Treo Task Stop] Stopping task '{task_name}' due to invalid username reported by API.")
                #    await stop_treo_task(user_id_str, target_username, context, reason="Invalid Username from API")
                #    break


            # Chờ đợi đến lần chạy tiếp theo
            sleep_duration = TREO_INTERVAL_SECONDS
            logger.debug(f"[Treo Task Sleep] Task '{task_name}' sleeping for {sleep_duration} seconds...")
            await asyncio.sleep(sleep_duration)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
        # Không cần làm gì thêm, finally sẽ xử lý cleanup nếu cần
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        # Cố gắng dừng task một cách sạch sẽ khi có lỗi không mong muốn
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Đảm bảo task được xóa khỏi dict khi kết thúc (dù bị cancel, lỗi hay hết VIP)
        # Hàm stop_treo_task đã xử lý việc xóa, nhưng kiểm tra lại cho chắc
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
             task_in_dict = active_treo_tasks[user_id_str].get(target_username)
             # Chỉ xóa nếu task trong dict đúng là task này (và nó đã xong)
             if task_in_dict is asyncio.current_task() and task_in_dict.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]:
                    del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished task '{task_name}' from active tasks dict.")


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

    # --- Check quyền truy cập Group ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/treo command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

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

    # --- Check Giới Hạn và Trạng Thái Treo Hiện Tại ---
    if target_username:
        vip_limit = get_vip_limit(user_id)
        # Đảm bảo active_treo_tasks[user_id_str] là một dict
        user_tasks = active_treo_tasks.get(user_id_str, {})
        current_treo_count = len(user_tasks)

        # Kiểm tra xem task cho target này đã tồn tại và đang chạy chưa
        existing_task = user_tasks.get(target_username)
        if existing_task and not existing_task.done():
            logger.info(f"User {user_id} tried to /treo target @{target_username} which is already running.")
            await send_temporary_message(update, context, f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi.\nDùng <code>/dungtreo {target_username}</code> để dừng nếu muốn.", duration=20)
            await delete_user_message(update, context, original_message_id)
            return
        elif existing_task and existing_task.done():
            # Task cũ đã xong/lỗi nhưng entry chưa bị xóa? Xóa đi để tạo mới.
             logger.warning(f"Found finished/cancelled task for {user_id_str}->{target_username} in dict. Removing old entry before creating new.")
             await stop_treo_task(user_id_str, target_username, context, reason="Cleanup before new /treo")
             # Cập nhật lại số lượng sau khi xóa task cũ
             user_tasks = active_treo_tasks.get(user_id_str, {})
             current_treo_count = len(user_tasks)


        # Kiểm tra giới hạn slot VIP
        if current_treo_count >= vip_limit:
             logger.warning(f"User {user_id} tried to /treo target @{target_username} but reached limit ({current_treo_count}/{vip_limit}).")
             limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit} tài khoản).\n"
                         f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot hoặc nâng cấp VIP (nếu có gói cao hơn).")
             await send_temporary_message(update, context, limit_msg, duration=30)
             await delete_user_message(update, context, original_message_id)
             return

        # --- Bắt đầu Task Treo Mới ---
        try:
            # Sử dụng application context để tạo task
            app = context.application
            task = app.create_task(
                run_treo_loop(user_id_str, target_username, context),
                name=f"treo_{user_id_str}_{target_username}"
            )

            # Thêm task vào danh sách quản lý
            if user_id_str not in active_treo_tasks:
                 active_treo_tasks[user_id_str] = {}
            active_treo_tasks[user_id_str][target_username] = task
            logger.info(f"Successfully created and stored treo task '{task.get_name()}' for user {user_id}")

            # Gửi thông báo thành công cho người dùng
            # +1 vào current_treo_count để hiển thị số slot *sau khi* đã thêm task mới
            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n"
                           f"👤 Cho: {invoking_user_mention}\n"
                           f"🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {TREO_INTERVAL_SECONDS // 60} phút\n"
                           f"📊 Slot đã dùng: {current_treo_count + 1}/{vip_limit}")
            await update.message.reply_html(success_msg)
            await delete_user_message(update, context, original_message_id) # Xóa lệnh /treo gốc

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Vui lòng thử lại hoặc báo Admin.", duration=20)
             await delete_user_message(update, context, original_message_id)

    else: # Trường hợp target_username là None (lỗi logic parse?)
        logger.error(f"/treo command for user {user_id}: target_username became None unexpectedly.")
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi xử lý username. Vui lòng thử lại.", duration=15)
        await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (VIP) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một user (chỉ VIP hoặc user có task đang chạy)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # --- Check quyền truy cập Group ---
    if chat_id != ALLOWED_GROUP_ID:
        logger.info(f"/dungtreo command used outside allowed group ({chat_id}) by user {user_id}. Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # --- Parse Arguments ---
    args = context.args
    target_username_input = None # Username người dùng nhập (có thể có @)
    target_username_clean = None # Username đã xóa @
    err_txt = None

    if not args:
        # Nếu không có username, kiểm tra xem user có task nào đang chạy không
        user_tasks = active_treo_tasks.get(user_id_str, {})
        if not user_tasks:
             err_txt = ("⚠️ Bạn chưa nhập username cần dừng treo.\n"
                        "<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                        "<i>(Hiện tại bạn không có tài khoản nào đang treo.)</i>")
        else:
             # Có task đang chạy nhưng không chỉ định username
             running_targets = [f"<code>@{html.escape(t)}</code>" for t in user_tasks.keys()]
             err_txt = (f"⚠️ Bạn cần chỉ định username muốn dừng treo.\n"
                        f"<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                        f"<b>Các tài khoản đang treo:</b> {', '.join(running_targets)}")
    else:
        target_username_input = args[0].strip()
        target_username_clean = target_username_input.lstrip("@")
        if not target_username_clean:
            err_txt = "⚠️ Username không được để trống."
        # Không cần check regex quá kỹ ở đây, vì ta chỉ cần tìm trong dict

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # --- Dừng Task ---
    if target_username_clean:
        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        # Hàm stop_treo_task sẽ xử lý việc tìm và hủy task, trả về True nếu thành công
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        # --- Gửi Phản Hồi ---
        await delete_user_message(update, context, original_message_id) # Xóa lệnh /dungtreo gốc
        if stopped:
            vip_limit = get_vip_limit(user_id) # Lấy lại limit (có thể đã hết hạn VIP nhưng task vẫn chạy?)
            current_treo_count = len(active_treo_tasks.get(user_id_str, {})) # Đếm lại số task còn lại
            # save_data() # Lưu lại trạng thái active_treo_tasks sau khi xóa (không bắt buộc ngay, cleanup hoặc lệnh khác sẽ lưu)
            await update.message.reply_html(f"✅ Đã dừng treo follow tự động cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {current_treo_count}/{vip_limit if is_user_vip(user_id) else 'N/A'})")
        else:
            # Hàm stop_treo_task trả về False nếu không tìm thấy task đang chạy
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy tác vụ treo nào đang chạy cho <code>@{html.escape(target_username_clean)}</code> để dừng.", duration=20)

# --- Job Thống Kê Follow Tăng ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow nhiều nhất."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    logger.info(f"[Stats Job] Starting statistics report job. Current time: {current_time}, Last report: {last_stats_report_time}")

    # Tạo bản sao của dữ liệu thống kê để xử lý, tránh race condition
    stats_snapshot = {uid: targets.copy() for uid, targets in treo_stats.items() if targets}
    # Dọn dẹp ngay lập tức dữ liệu gốc để bắt đầu đếm lại cho chu kỳ mới
    users_to_clear = list(treo_stats.keys())
    for user_id_str in users_to_clear:
         treo_stats[user_id_str] = defaultdict(int) # Reset về defaultdict rỗng
    last_stats_report_time = current_time # Cập nhật thời gian báo cáo cuối
    save_data() # Lưu lại trạng thái đã reset và thời gian báo cáo mới
    logger.info(f"[Stats Job] Cleared current stats and updated last report time to {current_time}. Processing snapshot...")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data found in snapshot. Skipping report.")
        return

    # --- Xử lý dữ liệu snapshot ---
    top_gainers = [] # List of tuples: (gain, user_id_str, target_username)
    total_gain_all = 0

    for user_id_str, targets in stats_snapshot.items():
        for target_username, gain in targets.items():
            if gain > 0:
                top_gainers.append((gain, user_id_str, target_username))
                total_gain_all += gain

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found in the snapshot. Skipping report.")
        return

    # Sắp xếp theo số lượt tăng giảm dần
    top_gainers.sort(key=lambda x: x[0], reverse=True)

    # --- Tạo nội dung báo cáo ---
    report_lines = []
    interval_hours = TREO_STATS_INTERVAL_SECONDS / 3600
    report_lines.append(f"📊 <b>Thống Kê Tăng Follow (Trong {interval_hours:.0f} Giờ Qua)</b> 📊")
    report_lines.append(f"<i>(Tổng cộng: {total_gain_all} follow được tăng bởi các tài khoản đang treo)</i>")

    report_lines.append("\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>")

    # Lấy thông tin user (mention) cho top gainers (ví dụ top 3)
    num_top_to_show = 3
    displayed_count = 0
    user_mentions_cache = {} # Cache để tránh gọi get_chat nhiều lần cho cùng user

    for gain, user_id_str, target_username in top_gainers[:num_top_to_show]:
        user_mention = user_mentions_cache.get(user_id_str)
        if not user_mention:
            try:
                user_info = await context.bot.get_chat(int(user_id_str))
                user_mention = user_info.mention_html() if user_info else f"User ID <code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention # Cache kết quả
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str}: {e_get_chat}")
                user_mention = f"User ID <code>{user_id_str}</code>"
                user_mentions_cache[user_id_str] = user_mention # Cache cả lỗi

        report_lines.append(f"  🏅 <b>+{gain} follow</b> cho <code>@{html.escape(target_username)}</code> (Treo bởi: {user_mention})")
        displayed_count += 1

    if not displayed_count:
         report_lines.append("  <i>Không có dữ liệu tăng follow đáng kể trong chu kỳ này.</i>")

    report_lines.append(f"\n🕒 <i>Thống kê được cập nhật mỗi {interval_hours:.0f} giờ.</i>")

    # --- Gửi báo cáo vào group ---
    report_text = "\n".join(report_lines)
    try:
        await context.bot.send_message(
            chat_id=ALLOWED_GROUP_ID,
            text=report_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        logger.info(f"[Stats Job] Successfully sent statistics report to group {ALLOWED_GROUP_ID}.")
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"[Stats Job] Failed to send statistics report to group {ALLOWED_GROUP_ID}: {e}")
    except Exception as e:
        logger.error(f"[Stats Job] Unexpected error sending statistics report: {e}", exc_info=True)

    logger.info("[Stats Job] Statistics report job finished.")


# --- Handler cho các lệnh không xác định trong group ---
async def unknown_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xóa các lệnh không xác định trong group cho phép."""
    if update.message and update.message.text and update.message.text.startswith('/') and update.effective_chat.id == ALLOWED_GROUP_ID:
        # Lấy danh sách các lệnh đã đăng ký
        known_commands = [h.command[0] for h in context.application.handlers.get(0, []) if isinstance(h, CommandHandler)]
        # Lấy lệnh từ tin nhắn
        command = update.message.text.split()[0].split('@')[0][1:] # Bỏ dấu /

        if command not in known_commands:
            logger.info(f"Unknown command '{update.message.text}' in allowed group {ALLOWED_GROUP_ID}. Deleting.")
            await delete_user_message(update, context)

# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task]):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        logger.info("No active treo tasks found to cancel during shutdown.")
        return

    logger.info(f"Attempting to gracefully cancel {len(tasks_to_cancel)} active treo tasks...")
    for task in tasks_to_cancel:
        if not task.done():
            task.cancel()

    # Sử dụng gather để chờ tất cả các task kết thúc (hoặc bị hủy)
    # return_exceptions=True để không dừng gather nếu 1 task báo lỗi
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    logger.info("Finished waiting for treo task cancellations during shutdown.")

    cancelled_count = 0
    errors_count = 0
    finished_normally_count = 0

    for i, result in enumerate(results):
        task_name = tasks_to_cancel[i].get_name() if hasattr(tasks_to_cancel[i], 'get_name') else f"Task_{i}"
        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
            logger.info(f"Task '{task_name}' confirmed cancelled during shutdown.")
        elif isinstance(result, Exception):
            errors_count += 1
            logger.error(f"Error occurred in task '{task_name}' during shutdown processing: {result}", exc_info=isinstance(result, BaseException))
        else:
            finished_normally_count += 1
            logger.debug(f"Task '{task_name}' finished with result during shutdown (not cancelled/error): {result}")

    logger.info(f"Shutdown task summary: {cancelled_count} cancelled, {errors_count} errors, {finished_normally_count} finished normally/other.")


# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    start_time = time.time()
    print("--- Bot DinoTool Starting ---")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"Bot Token: {'Loaded' if BOT_TOKEN else 'Missing!'}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID}")
    print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Link Shortener Key: {'Loaded' if LINK_SHORTENER_API_KEY else 'Missing!'}")
    print(f"Tim API Key: {'Loaded' if API_KEY else 'Missing!'}")
    print(f"Follow API URL: {FOLLOW_API_URL_BASE}")
    print(f"Data File: {DATA_FILE}")
    print(f"Key Expiry: {KEY_EXPIRY_SECONDS / 3600:.1f}h | Activation: {ACTIVATION_DURATION_SECONDS / 3600:.1f}h")
    print(f"Cooldowns: Tim/Fl={TIM_FL_COOLDOWN_SECONDS / 60:.1f}m | GetKey={GETKEY_COOLDOWN_SECONDS / 60:.1f}m")
    print(f"Treo Interval: {TREO_INTERVAL_SECONDS / 60:.1f}m | Stats Interval: {TREO_STATS_INTERVAL_SECONDS / 3600:.1f}h")
    print(f"VIP Prices: {VIP_PRICES}")
    print(f"Payment: {BANK_NAME} - {BANK_ACCOUNT} - {ACCOUNT_NAME}")
    print("-" * 30)
    # logger.warning("--- Ensure sensitive info like tokens/keys are handled securely (e.g., env variables) ---")
    print("-" * 30)

    print("Loading persistent data...")
    load_data()
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Cooldowns: Tim={len(user_tim_cooldown)}, Fl={len(user_fl_cooldown)}, GetKey={len(user_getkey_cooldown)}")
    print(f"Initial Treo Stats Users: {len(treo_stats)}, Last Stats Report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

    # Cấu hình Application với timeout dài hơn
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .pool_timeout(120) # Thời gian chờ lấy update từ Telegram
        .connect_timeout(60) # Thời gian chờ kết nối ban đầu
        .read_timeout(90) # Thời gian chờ đọc dữ liệu từ kết nối đã mở
        .write_timeout(90) # Thời gian chờ ghi dữ liệu
        .build()
    )

    # --- Schedule Jobs ---
    # Job dọn dẹp dữ liệu hết hạn (chạy sau 1 phút khởi động, lặp lại mỗi giờ)
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
    logger.info(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")

    # Job thống kê follow (chạy sau 5 phút khởi động, lặp lại mỗi 2 giờ)
    application.job_queue.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="report_treo_stats_job")
    logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours.")

    # --- Register Handlers ---
    # Bộ lọc: Chỉ trong group cho phép HOẶC chat riêng với bot
    group_or_private_filter = filters.Chat(chat_id=ALLOWED_GROUP_ID) | filters.ChatType.PRIVATE
    # Bộ lọc: Chỉ trong group cho phép
    group_only_filter = filters.Chat(chat_id=ALLOWED_GROUP_ID)

    # Lệnh hoạt động ở cả group và private
    application.add_handler(CommandHandler("start", start_command, filters=group_or_private_filter))
    application.add_handler(CommandHandler("lenh", lenh_command, filters=group_or_private_filter))

    # Lệnh chỉ hoạt động trong group cho phép
    application.add_handler(CommandHandler("getkey", getkey_command, filters=group_only_filter))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=group_only_filter))
    application.add_handler(CommandHandler("tim", tim_command, filters=group_only_filter))
    application.add_handler(CommandHandler("fl", fl_command, filters=group_only_filter))
    application.add_handler(CommandHandler("muatt", muatt_command, filters=group_only_filter))
    application.add_handler(CommandHandler("addtt", addtt_command, filters=group_only_filter)) # Admin command, nhưng để filter group
    application.add_handler(CommandHandler("treo", treo_command, filters=group_only_filter))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command, filters=group_only_filter))

    # Handler cho ảnh/bill (ưu tiên cao hơn unknown command)
    # Chỉ nhận ảnh và file ảnh, trong group, không phải command
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & group_only_filter & (~filters.COMMAND)
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=1) # Group 1

    # Handler cho lệnh không xác định (ưu tiên thấp hơn)
    # Chỉ xử lý command trong group cho phép
    application.add_handler(MessageHandler(filters.COMMAND & group_only_filter, unknown_in_group), group=10) # Group 10

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time
    print(f"(Initialization took {run_duration:.2f} seconds)")

    try:
        # Bắt đầu bot
        application.run_polling(
            allowed_updates=Update.ALL_TYPES, # Nhận tất cả các loại update
            drop_pending_updates=True # Bỏ qua các update cũ khi bot offline
            )
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping bot gracefully...")
        logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an unhandled exception: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped due to unhandled exception: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown sequence...")
        logger.info("Initiating shutdown sequence...")

        # --- Dừng các task treo đang chạy ---
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("Collecting active treo tasks for shutdown...")
            # Tạo list các task cần dừng từ dict
            for user_id_str, targets in active_treo_tasks.items():
                for target_username, task in targets.items():
                    if task and not task.done():
                        tasks_to_stop_on_shutdown.append(task)
                        logger.debug(f"Added task '{task.get_name()}' to shutdown list.")

        if tasks_to_stop_on_shutdown:
            print(f"Found {len(tasks_to_stop_on_shutdown)} active treo tasks. Attempting cancellation...")
            try:
                # Sử dụng asyncio.run để chạy hàm async shutdown nếu loop chưa chạy
                # Hoặc lấy loop hiện tại nếu đang chạy
                try:
                    loop = asyncio.get_running_loop()
                    # Nếu loop đang chạy, tạo task để chạy hàm shutdown
                    # Điều này không lý tưởng nhưng là cách để chạy async từ ngữ cảnh sync
                    # loop.create_task(shutdown_async_tasks(tasks_to_stop_on_shutdown))
                    # Thay vào đó, chạy trực tiếp nếu có thể, nhưng cần cẩn thận
                    # Thử chạy run_until_complete nếu loop không chạy
                    if not loop.is_running():
                        loop.run_until_complete(shutdown_async_tasks(tasks_to_stop_on_shutdown))
                    else:
                         logger.warning("Event loop is running during final shutdown. Cannot reliably wait for tasks. Cancelling directly.")
                         for task in tasks_to_stop_on_shutdown:
                             if not task.done(): task.cancel()

                except RuntimeError: # No running event loop
                     asyncio.run(shutdown_async_tasks(tasks_to_stop_on_shutdown))

            except Exception as e_shutdown:
                 logger.error(f"Error during async task shutdown: {e_shutdown}", exc_info=True)
                 print(f"Error during task shutdown: {e_shutdown}. Attempting direct cancellation...")
                 # Fallback: Hủy trực tiếp nếu chạy async lỗi
                 for task in tasks_to_stop_on_shutdown:
                      if not task.done(): task.cancel()
        else:
            print("No active treo tasks found running at shutdown.")
        # --- Kết thúc phần dừng task ---

        print("Attempting final data save...")
        logger.info("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")

        # Có thể thêm các hành động dọn dẹp khác ở đây

        print("Bot has stopped.")
        logger.info("Bot has stopped.")
        print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Lỗi nghiêm trọng ngăn cản việc khởi chạy hàm main
        print(f"\nFATAL ERROR: Could not execute main function: {e}")
        logger.critical(f"FATAL ERROR preventing main execution: {e}", exc_info=True)
        # Ghi vào file log riêng biệt nếu cần
        with open("fatal_error.log", "a", encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - FATAL ERROR: {e}\n")
            import traceback
            traceback.print_exc(file=f)

