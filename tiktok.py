# -*- coding: utf-8 -*-
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
ALLOWED_GROUP_ID = -1002191171631 # <--- GROUP ID CỦA BẠN
LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút (Dùng chung cho tim và fl thường)
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ
TREO_INTERVAL_SECONDS = 15 * 60 # 15 phút (Khoảng cách giữa các lần gọi API /treo) <--- ĐÃ SỬA THEO YÊU CẦU

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (KHÔNG ĐỔI)
FOLLOW_API_URL_BASE = "https://apitangfltiktok.soundcast.me/telefl.php" # <-- API FOLLOW MỚI (BASE URL)

# --- Thông tin VIP ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg"
BANK_ACCOUNT = "KHANGDINO"
PAYMENT_NOTE_PREFIX = "VIP DinoTool ID" # Nội dung chuyển khoản sẽ là: "VIP DinoTool ID <user_id>"

# --- Lưu trữ ---
DATA_FILE = "bot_persistent_data.json"

# --- Biến toàn cục ---
user_tim_cooldown = {}
user_fl_cooldown = {} # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {}
valid_keys = {} # {key: {"user_id_generator": ..., "expiry_time": ..., "used_by": ...}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": expiry_timestamp, "limit": user_limit}} - Người dùng VIP
active_treo_tasks = {} # {user_id_str: {target_username: asyncio.Task}} - Lưu các task /treo đang chạy

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
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

    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "vip_users": string_key_vip_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        }
    }
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        logger.debug(f"Data saved to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)

def load_data():
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown
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
                logger.info(f"Data loaded from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}, {}

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except (BadRequest, Forbidden) as e:
        if "Message to delete not found" in str(e) or "message can't be deleted" in str(e) or "MESSAGE_ID_INVALID" in str(e):
            logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted or no permission): {e}")
        else:
            logger.warning(f"Error deleting message {msg_id_to_delete}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete}: {e}", exc_info=True)

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
            if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
                logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else:
                 logger.warning(f"Job '{job_name}' error deleting message {message_id}: {e}")
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
        if reply and update.message:
            sent_message = await update.message.reply_html(text, disable_web_page_preview=True)
        else:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True)

        if sent_message and context.job_queue:
            context.job_queue.run_once(
                delete_message_job,
                duration,
                data={'chat_id': chat_id, 'message_id': sent_message.message_id},
                name=f"del_temp_{sent_message.message_id}"
            )
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending temporary message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in send_temporary_message: {e}", exc_info=True)

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên dạng Dinotool-xxxx."""
    return f"Dinotool-{''.join(random.choices(string.ascii_uppercase + string.digits, k=length))}"

async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng một task treo cụ thể. Trả về True nếu dừng thành công, False nếu không tìm thấy."""
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str][target_username]
        if task and not task.done():
            task.cancel()
            try:
                # Chờ task kết thúc sau khi cancel, với timeout nhỏ
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.CancelledError:
                pass # Mong đợi
            except asyncio.TimeoutError:
                 logger.warning(f"Timeout waiting for cancelled task {user_id_str}->{target_username} to finish.")
            except Exception as e:
                 logger.error(f"Error awaiting cancelled task for {user_id_str}->{target_username}: {e}")

        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]: # Nếu không còn task nào cho user này
            del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Stopped treo task for user {user_id_str} -> @{target_username}. Reason: {reason}")
        return True
    return False

async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng tất cả các task treo của một user."""
    if user_id_str in active_treo_tasks:
        targets_to_stop = list(active_treo_tasks[user_id_str].keys())
        logger.info(f"Stopping all {len(targets_to_stop)} treo tasks for user {user_id_str}. Reason: {reason}")
        stopped_count = 0
        for target_username in targets_to_stop:
            if await stop_treo_task(user_id_str, target_username, context, reason):
                stopped_count += 1
        # Đảm bảo key user được xóa khỏi dict chính nếu nó vẫn còn vì lý do nào đó
        if user_id_str in active_treo_tasks and not active_treo_tasks[user_id_str]:
             del active_treo_tasks[user_id_str]
        logger.info(f"Finished stopping tasks for user {user_id_str}. Stopped: {stopped_count}/{len(targets_to_stop)}")

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs)."""
    global valid_keys, activated_users, vip_users
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    data_changed = False

    # Check expired keys (chưa sử dụng)
    for key, data in list(valid_keys.items()):
        try:
            expiry = float(data.get("expiry_time", 0))
            if data.get("used_by") is None and current_time > expiry:
                keys_to_remove.append(key)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry_time for key {key}, removing.")
            keys_to_remove.append(key)

    # Check expired key activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp):
                users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid activation timestamp for user {user_id_str} (key system), removing.")
            users_to_deactivate_key.append(user_id_str)

    # Check expired VIP activations
    vip_users_to_stop_tasks = [] # Lưu user ID cần dừng task
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            if current_time > float(vip_data.get("expiry", 0)):
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str) # Dừng task khi hết hạn
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry timestamp for VIP user {user_id_str}, removing.")
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str) # Dừng task nếu dữ liệu lỗi

    # Perform deletions from data structures
    for key in keys_to_remove:
        if key in valid_keys:
             del valid_keys[key]; logger.info(f"[Cleanup] Removed expired unused key: {key}"); data_changed = True
    for user_id_str in users_to_deactivate_key:
        if user_id_str in activated_users:
             del activated_users[user_id_str]; logger.info(f"[Cleanup] Deactivated user (key system): {user_id_str}"); data_changed = True
    for user_id_str in users_to_deactivate_vip:
        if user_id_str in vip_users:
             del vip_users[user_id_str]; logger.info(f"[Cleanup] Deactivated VIP user: {user_id_str}"); data_changed = True

    # Stop tasks for expired/invalid VIPs *after* updating the vip_users dict
    if vip_users_to_stop_tasks:
         logger.info(f"[Cleanup] Stopping tasks for {len(vip_users_to_stop_tasks)} expired/invalid VIP users: {vip_users_to_stop_tasks}")
         # Sử dụng application context để đảm bảo chạy đúng cách trong job
         app = context.application
         for user_id_str in vip_users_to_stop_tasks:
             # Tạo task riêng để dừng task của user, tránh block job cleanup quá lâu
             app.create_task(stop_all_treo_tasks_for_user(user_id_str, context, reason="VIP Expired/Removed during Cleanup"))

    # Save if data changed
    if data_changed:
        logger.info("[Cleanup] Data changed, saving...")
        save_data()
    else:
        logger.debug("[Cleanup] No expired data to clean.")

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
        except (ValueError, TypeError):
             pass # Cleanup job sẽ xử lý
    return False

def get_vip_limit(user_id: int) -> int:
    """Lấy giới hạn treo user của VIP."""
    user_id_str = str(user_id)
    if is_user_vip(user_id): # Check lại VIP status trước khi lấy limit
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
        except (ValueError, TypeError):
             pass # Cleanup job sẽ xử lý
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl) không (VIP hoặc đã kích hoạt key)."""
    # Ưu tiên check VIP trước vì nó không hết hạn nhanh như key
    return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# --- Logic API Follow (Tách ra để dùng cho /fl và /treo) ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """
    Gọi API follow và trả về kết quả dưới dạng dict.
    Returns: {"success": bool, "message": str, "data": dict | None}
    """
    api_params = {
        "user": target_username,
        "userid": user_id_str,
        "tokenbot": bot_token
    }
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")

    result = {"success": False, "message": "Lỗi không xác định.", "data": None}
    try:
        async with httpx.AsyncClient(verify=True, timeout=45.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = resp.text
            except Exception: pass
            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}")

            if "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                    api_status = data.get("status")
                    api_message = data.get("message", "Không có thông báo từ API.")
                    result["data"] = data

                    if api_status is True:
                        result["success"] = True
                        result["message"] = api_message or "Follow thành công."
                    else:
                        result["success"] = False
                        result["message"] = api_message or f"Follow thất bại (API status={api_status})."
                except json.JSONDecodeError:
                    logger.error(f"[API Call @{target_username}] Response not valid JSON. Text: {response_text_for_debug[:500]}...")
                    result["message"] = "Lỗi: API không trả về JSON hợp lệ."
                except Exception as e:
                    logger.error(f"[API Call @{target_username}] Error processing API data: {e}", exc_info=True)
                    result["message"] = "Lỗi xử lý dữ liệu từ API."
            else:
                 logger.error(f"[API Call @{target_username}] Response type not JSON: {content_type}. Status: {resp.status_code}. Text: {response_text_for_debug[:500]}...")
                 result["message"] = f"Lỗi định dạng phản hồi API (Code: {resp.status_code})."
    except httpx.TimeoutException:
        logger.warning(f"[API Call @{target_username}] API timeout.")
        result["message"] = f"Lỗi: API timeout khi follow @{html.escape(target_username)}."
    except httpx.ConnectError as e:
        logger.error(f"[API Call @{target_username}] Connection error: {e}", exc_info=False)
        result["message"] = f"Lỗi kết nối đến API follow @{html.escape(target_username)}."
    except httpx.RequestError as e:
        logger.error(f"[API Call @{target_username}] Network error: {e}", exc_info=False)
        result["message"] = f"Lỗi mạng khi kết nối API follow @{html.escape(target_username)}."
    except Exception as e:
        logger.error(f"[API Call @{target_username}] Unexpected error: {e}", exc_info=True)
        result["message"] = f"Lỗi hệ thống Bot khi xử lý follow @{html.escape(target_username)}."
    return result

# --- Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start."""
    if not update or not update.message: return
    user = update.effective_user
    act_h = ACTIVATION_DURATION_SECONDS // 3600; key_exp_h = KEY_EXPIRY_SECONDS // 3600
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60; gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
           f"🤖 Bot hỗ trợ TikTok.\n<i>Chỉ dùng trong nhóm chỉ định.</i>\n\n"
           f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
           f"1️⃣ <code>/getkey</code> ➜ Nhận link.\n"
           f"2️⃣ Truy cập link ➜ Lấy Key (VD: <code>Dinotool-XXXX</code>).\n"
           f"3️⃣ <code>/nhapkey <key></code>.\n"
           f"4️⃣ Dùng <code>/tim</code>, <code>/fl</code> trong <b>{act_h} giờ</b>.\n\n"
           f"👑 <b>Nâng cấp VIP:</b>\n"
           f"   » Dùng <code>/muatt</code> để xem chi tiết.\n"
           f"   » VIP có thể dùng <code>/treo</code>, <code>/dungtreo</code> và không cần lấy key.\n\n"
           f"ℹ️ <b>Danh sách lệnh:</b> Dùng <code>/lenh</code>\n\n"
           f"<i>Bot by <a href='https://t.me/dinotool'>DinoTool</a></i>")

    if update.effective_chat.type == 'private' or update.effective_chat.id == ALLOWED_GROUP_ID:
        try:
            await update.message.reply_html(msg, disable_web_page_preview=True)
        except (BadRequest, Forbidden) as e:
            logger.warning(f"Failed to send /start message to {user.id} in chat {update.effective_chat.id}: {e}")
    else:
        logger.info(f"User {user.id} tried /start in unauthorized group ({update.effective_chat.id}). Message ignored.")

async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id

    if update.effective_chat.type != 'private' and chat_id != ALLOWED_GROUP_ID:
        logger.info(f"User {user.id} tried /lenh in unauthorized group ({chat_id}). Ignored.")
        return

    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60

    is_vip = is_user_vip(user.id)
    is_key_active = is_user_activated_by_key(user.id)
    vip_status = "✅ VIP" if is_vip else "❌ Chưa VIP"
    key_status = "✅ Đã kích hoạt (Key)" if is_key_active else "❌ Chưa kích hoạt (Key)"
    can_use_std_features = is_vip or is_key_active
    can_use_status = "✅ Có thể" if can_use_std_features else "❌ Không thể"

    help_text = f"📜 <b>Danh sách lệnh của Bot</b> ({user.mention_html()})\n\n"
    help_text += f"<b>Trạng thái của bạn:</b>\n"
    help_text += f"   {vip_status}"
    if is_vip:
        vip_data = vip_users.get(str(user.id), {})
        expiry_ts = vip_data.get("expiry")
        limit = vip_data.get("limit", "?")
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
            except: pass
        help_text += f" (Hết hạn: {expiry_str}, Limit: {limit} users)\n"
    else:
        help_text += "\n"

    help_text += f"   {key_status}"
    if is_key_active:
         expiry_ts = activated_users.get(str(user.id))
         expiry_str = "Không rõ"
         if expiry_ts:
             try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
             except: pass
         help_text += f" (Hết hạn: {expiry_str})\n"
    else:
        help_text += "\n"

    help_text += f"   » Dùng <code>/tim</code>, <code>/fl</code>: {can_use_status}\n\n"

    help_text += "<b><u>Lệnh Chung:</u></b>\n"
    help_text += f"<code>/start</code> - Tin nhắn chào mừng.\n"
    help_text += f"<code>/lenh</code> - Danh sách lệnh này.\n"
    help_text += f"<code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần).\n"
    help_text += f"<code>/nhapkey <key></code> - Nhập key (hiệu lực {key_exp_h}h, kích hoạt {act_h}h).\n"
    help_text += f"<code>/tim <link_video></code> - Tăng tim (Y/c VIP/Key, ⏳ {tf_cd_m}p/lần).\n"
    help_text += f"<code>/fl <username></code> - Tăng follow (Y/c VIP/Key, ⏳ {tf_cd_m}p/user).\n\n"

    help_text += "<b><u>Lệnh VIP:</u></b>\n"
    help_text += f"<code>/muatt</code> - Xem thông tin mua VIP.\n"
    help_text += f"<code>/treo <username></code> - Tự động follow (Mỗi {treo_interval_m}p, Y/c VIP).\n"
    help_text += f"<code>/dungtreo <username></code> - Dừng tự động follow.\n\n"

    help_text += "<b><u>Lệnh Admin:</u></b>\n"
    help_text += f"<code>/addtt <user_id> <days></code> - Cộng ngày VIP (VD: <code>/addtt 12345 30</code>).\n\n"

    help_text += f"<i>Lưu ý: Lệnh /treo sẽ dừng nếu bot khởi động lại.</i>"

    try:
        await update.message.reply_html(help_text, disable_web_page_preview=True)
    except (BadRequest, Forbidden) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user; user_id = user.id
    current_time = time.time(); original_message_id = update.message.message_id; user_id_str = str(user_id)

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là VIP hoặc kích hoạt tài khoản bằng key!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey <key></code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage = float(last_usage_str)
            elapsed = current_time - last_usage
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f}</b> giây nữa để dùng <code>/tim</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for tim user {user_id}. Resetting.")
             if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str]; save_data()

    args = context.args; video_url = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu link video.\nVD: <code>/tim https://tiktok.com/...</code>")
    elif "tiktok.com" not in args[0] or not args[0].startswith(("http://", "https://")): err_txt = "⚠️ Link không hợp lệ. Phải là link TikTok."
    else: video_url = args[0]

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15)
        await delete_user_message(update, context, original_message_id)
        return

    if not video_url or not API_KEY:
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình API Key /tim hoặc input.", duration=15); return

    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    logger.info(f"User {user_id} calling /tim API: {api_url.replace(API_KEY, '***')}")
    processing_msg = None; final_response_text = ""; is_success = False

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý ❤️...</i></b>")
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim'})
            if "application/json" in resp.headers.get("content-type","").lower():
                data = resp.json()
                if data.get("success"):
                    user_tim_cooldown[user_id_str] = time.time(); save_data()
                    d=data.get("data",{}); a=html.escape(str(d.get("author","?"))); ct=html.escape(str(d.get("create_time","?"))); v=html.escape(str(d.get("video_url", video_url))); db=html.escape(str(d.get('digg_before','?'))); di=html.escape(str(d.get('digg_increased','?'))); da=html.escape(str(d.get('digg_after','?')))
                    final_response_text = (f"🎉 <b>Tim OK!</b> ❤️ cho {user.mention_html()}\n\n📊 <b>Info:</b>\n🎬 <a href='{v}'>Link</a>\n👤 <code>{a}</code> | 🗓️ <code>{ct}</code>\n👍 <code>{db}</code>➜💖<code>+{di}</code>➜✅<code>{da}</code>")
                    is_success = True
                else: final_response_text = f"💔 <b>Lỗi Tim!</b> cho {user.mention_html()}\n<i>API:</i> <code>{html.escape(data.get('message','Không rõ'))}</code>"
            else: final_response_text = f"❌ Lỗi định dạng API cho {user.mention_html()} (Code: {resp.status_code}, Type: {resp.headers.get('content-type', '?')})."
    except httpx.TimeoutException: final_response_text = f"❌ Lỗi: API timeout /tim cho {user.mention_html()}."
    except httpx.RequestError as e: final_response_text = f"❌ Lỗi mạng/kết nối API /tim: {e}"
    except json.JSONDecodeError: final_response_text = f"❌ Lỗi: API trả về JSON không hợp lệ cho {user.mention_html()}."
    except Exception as e: logger.error(f"Unexpected error /tim: {e}", exc_info=True); final_response_text = f"❌ Lỗi hệ thống Bot /tim cho {user.mention_html()}."
    finally:
        await delete_user_message(update, context, original_message_id)
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 else: logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit}")
        else:
             await update.message.reply_html(final_response_text, disable_web_page_preview=True)


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
        user_info_block = (
            f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)\n"
            f"🆔 <b>ID:</b> <code>{tt_user_id}</code>\n"
            f"🌍 <b>Khu vực:</b> {khu_vuc}\n"
            f"📅 <b>Ngày tạo TK:</b> {create_time}\n"
        )
        if avatar and avatar.startswith("http"): user_info_block += f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>\n"

    follower_info_block = ""
    if api_data:
        f_before = html.escape(str(api_data.get("followers_before", "?")))
        f_add = html.escape(str(api_data.get("followers_add", "?")))
        f_after = html.escape(str(api_data.get("followers_after", "?")))
        if f_before != "?" or f_add != "?" or f_after != "?":
            follower_info_block = (
                f"📈 <b>Followers:</b>\n"
                f"   Trước: <code>{f_before}</code>\n"
                f"   Tăng:   <code>+{f_add}</code>\n"
                f"   Sau:    <code>{f_after}</code>"
            )

    if success:
        current_time = time.time()
        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = current_time
        save_data()
        logger.info(f"[BG Task] Updated /fl cooldown for user {user_id_str} on @{target_username}")
        final_response_text = (
            f"✅ <b>Follow Thành Công!</b> cho {invoking_user_mention}\n\n"
            f"{user_info_block}\n"
            f"{follower_info_block}"
        )
    else:
        final_response_text = (
            f"❌ <b>Lỗi Follow</b> cho {invoking_user_mention}!\n\n"
            f"💬 Lý do API: <code>{html.escape(api_message)}</code>\n\n"
            f"{user_info_block}"
        )
        if "đợi" in api_message.lower() and ("phút" in api_message.lower() or "giây" in api_message.lower()):
            final_response_text += f"\n\n<i>ℹ️ Vui lòng chờ theo yêu cầu của API.</i>"

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=processing_msg_id, text=final_response_text,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
        logger.info(f"[BG Task] Edited message {processing_msg_id} for /fl @{target_username}")
    except BadRequest as e:
        if "Message is not modified" in str(e): pass
        elif "message to edit not found" in str(e).lower(): logger.warning(f"[BG Task] Message {processing_msg_id} not found for editing.")
        elif "Can't parse entities" in str(e):
             logger.warning(f"[BG Task] HTML parse error editing {processing_msg_id}. Plain text fallback.")
             plain_text = re.sub('<[^<]+?>', '', final_response_text); plain_text = html.unescape(plain_text)
             plain_text += "\n\n(Lỗi hiển thị HTML)"
             try: await context.bot.edit_message_text(chat_id, processing_msg_id, plain_text[:4096], disable_web_page_preview=True)
             except Exception as pt_edit_err: logger.error(f"[BG Task] Failed plain text fallback edit: {pt_edit_err}")
        else: logger.error(f"[BG Task] BadRequest editing msg {processing_msg_id}: {e}")
    except Exception as e: logger.error(f"[BG Task] Unexpected error editing msg {processing_msg_id}: {e}", exc_info=True)


async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /fl - Check quyền, cooldown, gửi tin chờ và chạy task nền."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user
    if not user: return
    user_id = user.id; user_id_str = str(user_id); invoking_user_mention = user.mention_html()
    current_time = time.time(); original_message_id = update.message.message_id

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là VIP hoặc kích hoạt key!\n"
                   f"➡️ <code>/getkey</code> » <code>/nhapkey <key></code>\n"
                   f"👑 Hoặc <code>/muatt</code>.")
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username.\nVD: <code>/fl username</code>")
    else:
        uname = args[0].strip().lstrip("@")
        if not uname: err_txt = "⚠️ Username trống."
        elif not re.match(r"^[a-zA-Z0-9_.]{2,24}$", uname) or uname.endswith('.') or uname.startswith('.'):
            err_txt = f"⚠️ Username <code>{html.escape(uname)}</code> không hợp lệ."
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15)
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
                     cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f}s</b> nữa để <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                     await send_temporary_message(update, context, cd_msg, duration=15)
                     await delete_user_message(update, context, original_message_id)
                     return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp for fl user {user_id} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]; save_data()

    processing_msg = None
    try:
        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu follow <code>@{html.escape(target_username)}</code>. Vui lòng đợi..."
        )
    except Exception as e:
        logger.error(f"Failed to send processing message for /fl @{target_username}: {e}")
        await delete_user_message(update, context, original_message_id)
        return

    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

    if processing_msg and target_username:
        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        context.application.create_task(
            process_fl_request_background(
                context=context, chat_id=chat_id, user_id_str=user_id_str,
                target_username=target_username, processing_msg_id=processing_msg.message_id,
                invoking_user_mention=invoking_user_mention
            ), update=update
        )
    elif target_username:
         logger.error(f"Could not schedule background task for /fl @{target_username} - failed processing message.")

# --- Lệnh /getkey ---
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user; user_id = user.id
    current_time = time.time(); original_message_id = update.message.message_id; user_id_str = str(user_id)

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    last_usage_str = user_getkey_cooldown.get(user_id_str)
    if last_usage_str:
         try:
             last_usage = float(last_usage_str)
             elapsed = current_time - last_usage
             if elapsed < GETKEY_COOLDOWN_SECONDS:
                remaining = GETKEY_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{remaining:.0f}s</b> nữa để dùng <code>/getkey</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
         except (ValueError, TypeError):
              logger.warning(f"Invalid cooldown for getkey user {user_id}. Resetting.")
              if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]; save_data()

    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&_cb={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster

    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}", "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. New key: {generated_key}. Target: {final_target_url}")

    processing_msg = None; final_response_text = ""; key_saved_to_dict = False
    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key...</i></b> 🔑")

        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = { "user_id_generator": user_id, "generation_time": generation_time, "expiry_time": expiry_time, "used_by": None }
        key_saved_to_dict = True
        save_data()
        logger.info(f"Key {generated_key} saved for user {user_id}. Expires in {KEY_EXPIRY_SECONDS / 3600:.1f}h.")

        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)
            response_content_type = response.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = response.text
            except Exception: pass

            if response.status_code == 200:
                try:
                    try: response_data = json.loads(response.content.decode('utf-8', errors='replace'))
                    except json.JSONDecodeError as jde:
                         logger.error(f"API non-JSON /getkey 200 OK. Type: '{response_content_type}'. Err: {jde}. Text: {response_text_for_debug[:500]}")
                         raise jde
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time(); save_data()
                        logger.info(f"Success generating short link for user {user_id}: {generated_short_url}")
                        final_response_text = (
                            f"🚀 <b>Link lấy key ({user.mention_html()}):</b>\n\n"
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"❓ <b>Hướng dẫn:</b>\n"
                            f"   1️⃣ Click link.\n"
                            f"   2️⃣ Làm theo các bước nhận Key (VD: <code>Dinotool-XXXX</code>).\n"
                            f"   3️⃣ Dùng: <code>/nhapkey <key_cua_ban></code>\n\n"
                            f"⏳ <i>Key cần nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>.</i>"
                        )
                    else:
                        api_message = response_data.get("message", f"Lỗi hoặc thiếu 'shortenedUrl'")
                        logger.error(f"Shortener API error (JSON) user {user_id}. Msg: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Tạo Link:</b> <code>{html.escape(str(api_message))}</code>."
                        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
                except json.JSONDecodeError:
                    logger.error(f"API Status 200 but JSON decode fail. Type: '{response_content_type}'. Text: {response_text_for_debug[:500]}")
                    final_response_text = f"❌ <b>Lỗi API:</b> Phản hồi không phải JSON."
                    if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
            else:
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug[:500]}")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Code: {response.status_code})."
                 if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout /getkey user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> API tạo link không phản hồi."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except httpx.ConnectError as e:
        logger.error(f"Shortener API connection error /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối API tạo link."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except httpx.RequestError as e:
        logger.error(f"Shortener API network error /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except Exception as e:
        logger.error(f"Unexpected error /getkey user {user_id}: {e}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    finally:
        await delete_user_message(update, context, original_message_id)
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=False
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): pass
                 else: logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit: logger.error(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit}")
        else:
             await update.message.reply_html(final_response_text, disable_web_page_preview=False)

# --- Lệnh /nhapkey ---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user; user_id = user.id
    current_time = time.time(); original_message_id = update.message.message_id; user_id_str = str(user_id)

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    # Parse Input
    args = context.args; submitted_key = None; err_txt = ""
    if not args: err_txt = ("⚠️ Thiếu key.\nVD: <code>/nhapkey Dinotool-XXXX</code>")
    elif len(args) > 1: err_txt = "⚠️ Chỉ nhập 1 key."
    else:
        key = args[0].strip()
        # Kiểm tra định dạng key chặt chẽ hơn
        if not key.startswith("Dinotool-") or len(key) <= len("Dinotool-"):
             err_txt = f"⚠️ Key <code>{html.escape(key)}</code> sai định dạng."
        elif not key[len("Dinotool-"):].isalnum() or not all(c.isupper() or c.isdigit() for c in key[len("Dinotool-"):]):
             err_txt = f"⚠️ Key <code>{html.escape(key)}</code> sai định dạng (chỉ chữ HOA và số sau dấu '-')."
        else: submitted_key = key

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15)
        await delete_user_message(update, context, original_message_id)
        return

    # Validate Key
    logger.info(f"User {user_id} attempting key activation with: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key); final_response_text = ""; activation_success = False

    if not key_data:
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại."
    elif key_data.get("used_by") is not None:
        used_by_id = key_data["used_by"]
        act_time_ts = key_data.get("activation_time", 0)
        used_time_str = "(không rõ)"
        # SỬA LỖI SYNTAX Ở ĐÂY
        if act_time_ts:
            try:
                # Cố gắng chuyển đổi và định dạng timestamp
                used_time_str = datetime.fromtimestamp(float(act_time_ts)).strftime('%H:%M:%S %d/%m/%Y')
            except (ValueError, TypeError, OSError) as e:
                # Ghi log lỗi nếu timestamp không hợp lệ thay vì chỉ bỏ qua
                logger.warning(f"Could not format activation timestamp {act_time_ts} for key {submitted_key}: {e}")
                pass # Giữ lại "(không rõ)"

        if str(used_by_id) == user_id_str:
             final_response_text = f"⚠️ Bạn đã dùng key <code>{html.escape(submitted_key)}</code> này rồi (Lúc: {used_time_str})."
        else:
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng."

    elif current_time > key_data.get("expiry_time", 0):
        exp_time_ts = key_data.get("expiry_time", 0)
        exp_time_str = "(không rõ)"
        # SỬA LỖI SYNTAX Ở ĐÂY
        if exp_time_ts:
            try:
                 # Cố gắng chuyển đổi và định dạng timestamp
                exp_time_str = datetime.fromtimestamp(float(exp_time_ts)).strftime('%H:%M:%S %d/%m/%Y')
            except (ValueError, TypeError, OSError) as e:
                # Ghi log lỗi nếu timestamp không hợp lệ
                logger.warning(f"Could not format expiry timestamp {exp_time_ts} for key {submitted_key}: {e}")
                pass # Giữ lại "(không rõ)"

        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng (Hạn: {exp_time_str})."
        # Dọn dẹp key hết hạn ngay lập tức khỏi bộ nhớ
        if submitted_key in valid_keys:
             del valid_keys[submitted_key]; save_data()
             logger.info(f"Removed expired key {submitted_key} on attempt.")
    else: # Kích hoạt thành công!
        key_data["used_by"] = user_id
        key_data["activation_time"] = current_time
        activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
        activated_users[user_id_str] = activation_expiry_ts; save_data()
        expiry_str = datetime.fromtimestamp(activation_expiry_ts).strftime('%H:%M:%S %d/%m/%Y')
        activation_success = True
        final_response_text = (f"✅ <b>Kích hoạt Key Thành Công!</b>\n\n"
                               f"👤 User: {user.mention_html()}\n"
                               f"🔑 Key: <code>{html.escape(submitted_key)}</code>\n"
                               f"✨ Có thể dùng <code>/tim</code>, <code>/fl</code>.\n"
                               f"⏳ Đến: <b>{expiry_str}</b> ({ACTIVATION_DURATION_SECONDS // 3600} giờ)."
                             )

    # Gửi phản hồi cuối cùng
    await delete_user_message(update, context, original_message_id)
    await update.message.reply_html(final_response_text, disable_web_page_preview=True)
# --- Lệnh /muatt ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context); return

    user_id = user.id
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

    text = "👑 <b>Thông Tin Nâng Cấp VIP DinoTool</b> 👑\n\n"
    text += f"⭐️ <b>Gói 1:</b>\n"
    text += f"   - Giá: <b>{VIP_PRICES[15]['price']}</b>\n"
    text += f"   - Thời hạn: <b>{VIP_PRICES[15]['duration_days']} ngày</b>\n"
    text += f"   - Treo tối đa: <b>{VIP_PRICES[15]['limit']} users</b>\n\n"
    text += f"⭐️ <b>Gói 2:</b>\n"
    text += f"   - Giá: <b>{VIP_PRICES[30]['price']}</b>\n"
    text += f"   - Thời hạn: <b>{VIP_PRICES[30]['duration_days']} ngày</b>\n"
    text += f"   - Treo tối đa: <b>{VIP_PRICES[30]['limit']} users</b>\n\n"
    text += "🏦 <b>Thông tin thanh toán:</b>\n"
    text += f"   - Ngân hàng: <i>(Điền tên ngân hàng của bạn ở đây)</i>\n" # <<<--- THAY THẾ NGÂN HÀNG
    text += f"   - STK: <code>{BANK_ACCOUNT}</code> (Click để copy)\n"
    text += f"   - Tên TK: {BANK_ACCOUNT}\n\n"
    text += f"📝 <b>Nội dung chuyển khoản BẮT BUỘC:</b>\n"
    text += f"   <code>{payment_note}</code> (Click để copy)\n\n"
    text += f"📸 <b>Sau khi chuyển khoản thành công:</b>\n"
    text += f"   ➡️ Gửi ảnh chụp màn hình (bill) giao dịch <b>vào nhóm này</b>.\n"
    text += f"   ⏳ Admin sẽ kiểm tra và kích hoạt VIP cho bạn.\n\n"
    text += "<i>Cảm ơn bạn đã ủng hộ DinoTool!</i>"

    await delete_user_message(update, context) # Xóa lệnh /muatt

    try:
        await context.bot.send_photo(
            chat_id=chat_id, photo=QR_CODE_URL, caption=text, parse_mode=ParseMode.HTML
        )
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending /muatt photo or message: {e}")
        try: # Fallback to text only
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt: {e_text}")

# --- Xử lý nhận ảnh bill ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document được gửi trong nhóm và chuyển tiếp cho admin."""
    if not update or not update.message: return
    if update.effective_chat.id != ALLOWED_GROUP_ID or update.message.text and update.message.text.startswith('/'): return

    is_photo = bool(update.message.photo)
    is_image_document = bool(update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return

    user = update.effective_user; chat = update.effective_chat; message_id = update.message.message_id
    logger.info(f"Photo/Doc received in group {chat.id} from user {user.id}. Forwarding to admin {ADMIN_USER_ID}.")

    forward_caption = (
        f"📄 Bill/Ảnh nhận được từ:\n"
        f"👤 User: {user.mention_html()} (<code>{user.id}</code>)\n"
        f"👥 Group: {html.escape(chat.title or str(chat.id))} (<code>{chat.id}</code>)\n"
        f"🔗 Link tin nhắn: {update.message.link}"
    )

    try:
        await context.bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=chat.id, message_id=message_id)
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=forward_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        # Optional reply in group:
        # await update.message.reply_text("Đã gửi bill của bạn cho admin xem xét.", quote=True, disable_notification=True)
    except Forbidden:
        logger.error(f"Bot cannot forward/send message to admin {ADMIN_USER_ID}. Check permissions/block status.")
        try: await context.bot.send_message(chat_id=chat.id, text=f"⚠️ Không thể gửi bill của {user.mention_html()} đến admin. Vui lòng kiểm tra cài đặt hoặc liên hệ admin.")
        except: pass
    except Exception as e:
        logger.error(f"Error forwarding/sending bill to admin: {e}", exc_info=True)
        try: await context.bot.send_message(chat_id=chat.id, text=f"⚠️ Lỗi khi xử lý bill của {user.mention_html()}. Vui lòng thử lại hoặc báo admin.")
        except: pass

# --- Lệnh /addtt (Admin) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user; chat_id = update.effective_chat.id; original_message_id = update.message.message_id

    if admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id} in chat {chat_id}.")
        await delete_user_message(update, context, original_message_id); return

    if chat_id != ALLOWED_GROUP_ID:
        await send_temporary_message(update, context, "⚠️ Lệnh /addtt nên dùng trong group chính.", duration=15, reply=True)
        await delete_user_message(update, context, original_message_id); return

    args = context.args; err_txt = None; target_user_id = None; days_to_add = None; limit = None

    if len(args) != 2: err_txt = "⚠️ Sai cú pháp. Dùng: <code>/addtt <user_id> <days></code> (VD: /addtt 123456 30)"
    else:
        try: target_user_id = int(args[0])
        except ValueError: err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."
        try:
            days_to_add = int(args[1])
            if days_to_add <= 0: err_txt = "⚠️ Số ngày phải lớn hơn 0."
            elif days_to_add not in VIP_PRICES: err_txt = f"⚠️ Số ngày không hợp lệ ({', '.join(map(str, VIP_PRICES.keys()))} ngày)."
            else: limit = VIP_PRICES[days_to_add]["limit"]
        except ValueError: err_txt = f"⚠️ Số ngày '<code>{html.escape(args[1])}</code>' không hợp lệ."

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15, reply=True)
        await delete_user_message(update, context, original_message_id); return

    target_user_id_str = str(target_user_id); current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time
    if current_vip_data and float(current_vip_data.get("expiry", 0)) > current_time:
        start_time = float(current_vip_data["expiry"])
        logger.info(f"User {target_user_id_str} already VIP. Extending from {datetime.fromtimestamp(start_time)}.")

    new_expiry_ts = start_time + days_to_add * 86400
    new_expiry_str = datetime.fromtimestamp(new_expiry_ts).strftime('%H:%M:%S %d/%m/%Y')

    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data()
    logger.info(f"Admin {admin_user.id} added {days_to_add} days VIP for {target_user_id}. New expiry: {new_expiry_str}, Limit: {limit}")

    admin_msg = f"✅ Đã cộng <b>{days_to_add} ngày VIP</b> cho User ID <code>{target_user_id}</code>.\nHạn mới: <b>{new_expiry_str}</b>.\nTreo tối đa: <b>{limit} users</b>."
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation to admin {admin_user.id}: {e}")

    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        user_mention = target_user_info.mention_html() if target_user_info else f"User ID <code>{target_user_id}</code>"
    except Exception as e:
        logger.warning(f"Could not get chat info for {target_user_id}: {e}")
        user_mention = f"User ID <code>{target_user_id}</code>"

    group_msg = f"🎉 Chúc mừng {user_mention}! Bạn đã được nâng cấp/gia hạn <b>{days_to_add} ngày VIP</b>.\nHạn sử dụng đến: <b>{new_expiry_str}</b>.\nCó thể dùng <code>/treo</code> (tối đa {limit} users)."
    try:
        await context.bot.send_message(chat_id=chat_id, text=group_msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to send VIP notification to group {chat_id} for {target_user_id}: {e}")

# --- Logic Treo ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE):
    """Vòng lặp chạy nền cho lệnh /treo."""
    user_id = int(user_id_str)
    logger.info(f"[Treo Task Start] User {user_id_str} started treo for @{target_username}")
    while True:
        # Check 1: Task còn trong danh sách active không?
        if user_id_str not in active_treo_tasks or target_username not in active_treo_tasks[user_id_str]:
            logger.info(f"[Treo Task Stop] User {user_id_str} -> @{target_username} stopped (removed from active tasks).")
            break

        # Check 2: User còn VIP không? (Quan trọng)
        if not is_user_vip(user_id):
            logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping treo for @{target_username}.")
            # Không cần gọi stop_treo_task ở đây vì vòng lặp sẽ tự thoát
            # và cleanup job hoặc lệnh /dungtreo sẽ xóa task khỏi dict
            break

        # Thực hiện gọi API
        logger.info(f"[Treo Task Run] User {user_id_str} executing follow for @{target_username}")
        api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
        if api_result["success"]:
            logger.info(f"[Treo Task Success] User {user_id_str} -> @{target_username}. API Msg: {api_result['message']}")
        else:
            logger.warning(f"[Treo Task Fail] User {user_id_str} -> @{target_username}. API Msg: {api_result['message']}")
            # Cân nhắc dừng task nếu lỗi API nghiêm trọng hoặc lặp lại nhiều lần

        # Chờ đợi
        try:
            logger.debug(f"[Treo Task Sleep] User {user_id_str} -> @{target_username} sleeping for {TREO_INTERVAL_SECONDS}s")
            await asyncio.sleep(TREO_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info(f"[Treo Task Cancelled] Task for user {user_id_str} -> @{target_username} was cancelled.")
            break
        except Exception as e:
            logger.error(f"[Treo Task Error] Unexpected error during sleep {user_id_str}->{target_username}: {e}", exc_info=True)
            break # Dừng nếu có lỗi nghiêm trọng khi sleep

    # Cleanup khi vòng lặp kết thúc (dù vì lý do gì)
    logger.info(f"[Treo Task End] Loop finished for user {user_id_str} -> @{target_username}")
    # Xóa task khỏi dict nếu nó vẫn còn (ví dụ: user hết VIP tự thoát vòng lặp)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
         # Lấy task hiện tại trong dict để so sánh, tránh xóa nhầm task mới nếu user chạy lại /treo ngay lập tức
         current_task_in_dict = active_treo_tasks[user_id_str].get(target_username)
         # Chỉ xóa nếu task trong dict là chính task này (đã kết thúc)
         if current_task_in_dict is asyncio.current_task():
            del active_treo_tasks[user_id_str][target_username]
            if not active_treo_tasks[user_id_str]:
                del active_treo_tasks[user_id_str]
            logger.info(f"[Treo Task Cleanup] Removed self from active tasks dict: {user_id_str} -> {target_username}")


# --- Lệnh /treo (VIP) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user
    if not user: return
    user_id = user.id; user_id_str = str(user_id); original_message_id = update.message.message_id

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    if not is_user_vip(user_id):
        await send_temporary_message(update, context, f"⚠️ {user.mention_html()}, lệnh <code>/treo</code> chỉ dành cho VIP. Dùng <code>/muatt</code>.", duration=15)
        await delete_user_message(update, context, original_message_id); return

    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username.\nVD: <code>/treo username</code>")
    else:
        uname = args[0].strip().lstrip("@")
        if not uname: err_txt = "⚠️ Username trống."
        elif not re.match(r"^[a-zA-Z0-9_.]{2,24}$", uname) or uname.endswith('.') or uname.startswith('.'):
            err_txt = f"⚠️ Username <code>{html.escape(uname)}</code> không hợp lệ."
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15)
        await delete_user_message(update, context, original_message_id); return

    vip_limit = get_vip_limit(user_id)
    current_treo_count = len(active_treo_tasks.get(user_id_str, {}))

    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
         # Kiểm tra xem task cũ có thực sự đang chạy không
         old_task = active_treo_tasks[user_id_str][target_username]
         if old_task and not old_task.done():
             await send_temporary_message(update, context, f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi.", duration=15)
             await delete_user_message(update, context, original_message_id)
             return
         else: # Task cũ đã xong hoặc lỗi, cho phép tạo task mới
             logger.info(f"Found finished/cancelled task for {user_id_str}->{target_username}. Allowing new task.")


    if current_treo_count >= vip_limit:
         await send_temporary_message(update, context, f"⚠️ Đã đạt giới hạn treo <b>{current_treo_count}/{vip_limit} users</b>. Dùng <code>/dungtreo</code>.", duration=20)
         await delete_user_message(update, context, original_message_id); return

    if target_username:
        # Sử dụng application context để tạo task, đảm bảo nó được quản lý đúng cách
        app = context.application
        task = app.create_task(run_treo_loop(user_id_str, target_username, context), name=f"treo_{user_id_str}_{target_username}")

        if user_id_str not in active_treo_tasks: active_treo_tasks[user_id_str] = {}
        active_treo_tasks[user_id_str][target_username] = task

        # +1 vào current_treo_count để hiển thị số slot *sau khi* đã thêm task mới
        success_msg = f"✅ Đã bắt đầu treo follow tự động cho <code>@{html.escape(target_username)}</code>.\n(Slot đã dùng: {current_treo_count + 1}/{vip_limit})"
        await update.message.reply_html(success_msg)
        await delete_user_message(update, context, original_message_id)
    else:
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi bắt đầu treo.", duration=15)
        await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (VIP) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một user (chỉ VIP)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user
    if not user: return
    user_id = user.id; user_id_str = str(user_id); original_message_id = update.message.message_id

    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    # Check VIP chỉ để đưa ra thông báo phù hợp nếu họ cố dùng lệnh
    if not is_user_vip(user_id) and user_id_str not in active_treo_tasks: # Check nếu user ko còn VIP và cũng ko có task nào
        await send_temporary_message(update, context, f"⚠️ {user.mention_html()}, lệnh <code>/dungtreo</code> chỉ dành cho VIP.", duration=15)
        await delete_user_message(update, context, original_message_id); return

    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username.\nVD: <code>/dungtreo username</code>")
    else:
        uname = args[0].strip().lstrip("@")
        if not uname: err_txt = "⚠️ Username trống."
        else: target_username = uname # Không cần check regex kỹ

    if err_txt:
        await send_temporary_message(update, context, f"<b><i>{err_txt}</i></b>", duration=15)
        await delete_user_message(update, context, original_message_id); return

    if target_username:
        stopped = await stop_treo_task(user_id_str, target_username, context, reason="User command /dungtreo")

        if stopped:
            vip_limit = get_vip_limit(user_id) # Lấy lại limit phòng trường hợp user hết hạn VIP
            current_treo_count = len(active_treo_tasks.get(user_id_str, {}))
            await update.message.reply_html(f"✅ Đã dừng treo follow cho <code>@{html.escape(target_username)}</code>.\n(Slot còn lại: {vip_limit - current_treo_count}/{vip_limit})")
        else:
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy tác vụ treo nào đang chạy cho <code>@{html.escape(target_username)}</code>.", duration=15)

        await delete_user_message(update, context, original_message_id)

# --- Handler cho các lệnh không xác định trong group ---
async def unknown_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.text and update.message.text.startswith('/'):
        known_commands = [
            '/start', '/lenh', '/getkey', '/nhapkey', '/tim', '/fl',
            '/muatt', '/addtt', '/treo', '/dungtreo'
        ]
        command = update.message.text.split()[0].split('@')[0]
        if command not in known_commands:
            logger.info(f"Unknown command '{update.message.text}' in allowed group. Deleting.")
            await delete_user_message(update, context)

# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        print("No active treo tasks to cancel.")
        return

    print(f"Attempting to gracefully cancel {len(tasks_to_cancel)} treo tasks...")
    for task in tasks_to_cancel:
        task.cancel()

    # Sử dụng gather để chờ tất cả các task kết thúc (hoặc bị hủy)
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    print("Finished waiting for treo task cancellations.")

    cancelled_count = 0
    errors_count = 0
    finished_normally_count = 0 # Đếm task kết thúc mà không bị cancel (ít khả năng)

    for i, result in enumerate(results):
        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
        elif isinstance(result, Exception):
            errors_count += 1
            # Log lỗi cụ thể từ task nếu có
            logger.error(f"Error occurred in task {i} during shutdown: {result}", exc_info=isinstance(result, BaseException))
        else:
            # Task có thể đã kết thúc trước khi bị cancel hoặc trả về kết quả
            finished_normally_count +=1
            logger.debug(f"Task {i} finished with result during shutdown: {result}")

    logger.info(f"Shutdown task summary: {cancelled_count} cancelled, {errors_count} errors, {finished_normally_count} finished normally.")


# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    print("--- Bot Configuration ---")
    print(f"Bot Token: ...{BOT_TOKEN[-6:]}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID}")
    print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Link Shortener Key: ...{LINK_SHORTENER_API_KEY[-6:]}")
    print(f"Tim API Key: ...{API_KEY[-4:]}")
    print(f"Follow API URL: {FOLLOW_API_URL_BASE}")
    print(f"Data File: {DATA_FILE}")
    print(f"Key Expiry: {KEY_EXPIRY_SECONDS / 3600:.1f}h | Activation: {ACTIVATION_DURATION_SECONDS / 3600:.1f}h")
    print(f"Cooldowns: Tim/Fl={TIM_FL_COOLDOWN_SECONDS / 60:.1f}m | GetKey={GETKEY_COOLDOWN_SECONDS / 60:.1f}m")
    print(f"Treo Interval: {TREO_INTERVAL_SECONDS / 60:.1f}m")
    print(f"VIP Prices: {VIP_PRICES}")
    print("-" * 25)
    print("--- !!! WARNING: Hardcoded Tokens/Keys/IDs - Consider environment variables !!! ---")
    print("-" * 25)

    print("Loading saved data...")
    load_data()
    print(f"Loaded {len(valid_keys)} pending keys, {len(activated_users)} key-activated users, {len(vip_users)} VIP users.")
    print(f"Cooldowns: /tim={len(user_tim_cooldown)}, /fl={len(user_fl_cooldown)}, /getkey={len(user_getkey_cooldown)}")

    application = Application.builder().token(BOT_TOKEN).job_queue(JobQueue())\
        .pool_timeout(120).connect_timeout(60).read_timeout(90).build()

    # Schedule Jobs
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_job")
    print(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")

    # Register Handlers
    group_or_private = filters.Chat(chat_id=ALLOWED_GROUP_ID) | filters.ChatType.PRIVATE
    group_only = filters.Chat(chat_id=ALLOWED_GROUP_ID)

    application.add_handler(CommandHandler("start", start_command, filters=group_or_private))
    application.add_handler(CommandHandler("lenh", lenh_command, filters=group_or_private))
    application.add_handler(CommandHandler("getkey", getkey_command, filters=group_only))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=group_only))
    application.add_handler(CommandHandler("tim", tim_command, filters=group_only))
    application.add_handler(CommandHandler("fl", fl_command, filters=group_only))
    application.add_handler(CommandHandler("muatt", muatt_command, filters=group_only))
    application.add_handler(CommandHandler("addtt", addtt_command, filters=group_only))
    application.add_handler(CommandHandler("treo", treo_command, filters=group_only))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command, filters=group_only))

    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & group_only & ~filters.COMMAND
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=1)

    application.add_handler(MessageHandler(filters.COMMAND & group_only, unknown_in_group), group=2)

    print("Bot is starting polling...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        print("\nBot stopping due to KeyboardInterrupt...")
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an exception: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped: {e}", exc_info=True)
    finally:
        print("\nBot shutting down...")

        # --- Phần dừng task treo ---
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            print("Collecting active treo tasks for shutdown...")
            for user_id_str in list(active_treo_tasks.keys()):
                for target_username in list(active_treo_tasks.get(user_id_str, {}).keys()):
                    task = active_treo_tasks[user_id_str].get(target_username)
                    # Chỉ thêm task đang thực sự chạy
                    if task and not task.done():
                        tasks_to_stop_on_shutdown.append(task)

        if tasks_to_stop_on_shutdown:
            print(f"Found {len(tasks_to_stop_on_shutdown)} active treo tasks to cancel.")
            try:
                # Chạy hàm helper bất đồng bộ để hủy task
                # Sử dụng get_event_loop().run_until_complete nếu asyncio.run báo lỗi loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                     logger.warning("Event loop is already running during shutdown. Cannot run shutdown_async_tasks directly.")
                     # Cố gắng cancel trực tiếp, nhưng có thể không đợi được
                     for task in tasks_to_stop_on_shutdown: task.cancel()
                     print("Tasks cancelled directly, but may not have fully stopped.")
                else:
                     loop.run_until_complete(shutdown_async_tasks(tasks_to_stop_on_shutdown))
            except RuntimeError as e:
                 logger.error(f"RuntimeError during async shutdown: {e}. Trying direct cancellation.")
                 # Fallback: Cố gắng hủy trực tiếp nếu run_until_complete lỗi
                 for task in tasks_to_stop_on_shutdown: task.cancel()
            except Exception as e:
                 logger.error(f"Unexpected error during async shutdown: {e}", exc_info=True)
                 # Fallback: Cố gắng hủy trực tiếp
                 for task in tasks_to_stop_on_shutdown: task.cancel()
        else:
            print("No active treo tasks found running at shutdown.")
        # --- Kết thúc phần dừng task ---

        print("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")
        print("Bot has stopped.")
        logger.info("Bot has stopped.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error in main execution: {e}")
        logger.critical(f"Fatal error preventing main execution: {e}", exc_info=True)