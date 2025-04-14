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

from telegram import Update, Message # Import Message
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden
# import ssl # Bỏ comment nếu bạn thực sự cần tắt kiểm tra SSL, nhưng không khuyến khích

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "shareconcac" # <--- API KEY TIM/FL CỦA BẠN
ALLOWED_GROUP_ID = -1002191171631 # <--- GROUP ID CỦA BẠN

LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 12 * 3600   # 12 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 12 * 3600 # 12 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}"
FOLLOW_API_URL_TEMPLATE = "https://nvp310107.x10.mx/fltik.php?username={username}&key={api_key}"
GIF_API_URL = "https://media0.giphy.com/media/MVa8iDMGL70Jy/giphy.gif?cid=6c09b952qkfjck2dbqnzvbgw0q80kxf7rfg2bc4004v8cto2&ep=v1_internal_gif_by_id&rid=giphy.gif&ct=g" # GIF URL

# --- Lưu trữ ---
DATA_FILE = "bot_persistent_data.json"

# --- Biến toàn cục ---
user_tim_cooldown = {}
user_fl_cooldown = {}
user_getkey_cooldown = {}
valid_keys = {}
activated_users = {}

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# Giảm log thừa
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO) # Giữ lại log quan trọng của thư viện
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình ---
if not BOT_TOKEN: logger.critical("!!! BOT_TOKEN is missing !!!"); exit(1)
if not ALLOWED_GROUP_ID: logger.critical("!!! ALLOWED_GROUP_ID is missing !!!"); exit(1)
if not LINK_SHORTENER_API_KEY: logger.critical("!!! LINK_SHORTENER_API_KEY is missing !!!"); exit(1)
if not API_KEY: logger.warning("!!! API_KEY (for tim/fl) is missing. Commands might fail. !!!")

# --- Hàm lưu/tải dữ liệu ---
def save_data():
    # Đảm bảo tất cả các key dạng ID người dùng là string trước khi lưu JSON
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {str(uname): ts for uname, ts in udict.items()}
                              for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}

    data_to_save = {
        "valid_keys": valid_keys, # Key kích hoạt có thể giữ nguyên dạng string
        "activated_users": string_key_activated_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        }
    }
    try:
        # Sử dụng ensure_ascii=False để lưu trữ ký tự Unicode đúng cách
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        logger.debug(f"Data saved to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)

def load_data():
    global valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                valid_keys = data.get("valid_keys", {}) # Key kích hoạt là string

                # Chuyển key của activated_users thành string khi tải (thực ra đã lưu là string)
                activated_users = {str(k): v for k, v in data.get("activated_users", {}).items()}

                all_cooldowns = data.get("user_cooldowns", {})
                # Chuyển key của cooldown thành string khi tải
                user_tim_cooldown = {str(k): v for k, v in all_cooldowns.get("tim", {}).items()}
                # Chuyển key cấp 1 và 2 của fl_cooldown thành string khi tải
                user_fl_cooldown = {str(uid): {str(uname): ts for uname, ts in udict.items()}
                                   for uid, udict in all_cooldowns.get("fl", {}).items()}
                user_getkey_cooldown = {str(k): v for k, v in all_cooldowns.get("getkey", {}).items()}
                logger.info(f"Data loaded from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data.")
            valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse {DATA_FILE}: {e}. Backing up and using empty data.", exc_info=True)
        try:
            backup_filename = f"{DATA_FILE}.bak_{int(time.time())}"
            os.rename(DATA_FILE, backup_filename)
            logger.info(f"Backed up corrupted file to {backup_filename}")
        except OSError as backup_err:
            logger.error(f"Could not backup corrupted file {DATA_FILE}: {backup_err}")
        valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}
    except Exception as e:
        logger.error(f"Failed to load data from {DATA_FILE}: {e}. Using empty data.", exc_info=True)
        valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id:
        return
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except (BadRequest, Forbidden) as e:
        if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
            logger.info(f"Could not delete message {msg_id_to_delete} (already deleted or no permission): {e}")
        else: # Log các lỗi BadRequest khác là error
            logger.error(f"BadRequest deleting message {msg_id_to_delete}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa tin nhắn theo lịch trình."""
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except (BadRequest, Forbidden) as e:
            logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
        except Exception as e:
            logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else:
        logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def get_random_gif_url() -> str | None:
    """Lấy URL GIF ngẫu nhiên."""
    if not GIF_API_URL: return None
    gif_url = None
    try:
        # !!! verify=False LÀ KHÔNG AN TOÀN !!! Cân nhắc sử dụng certifi hoặc cấu hình SSL phù hợp
        async with httpx.AsyncClient(timeout=10.0, verify=False, follow_redirects=True) as client:
            response = await client.get(GIF_API_URL)
            response.raise_for_status() # Ném lỗi nếu status code không phải 2xx
            final_url = str(response.url)
            # Kiểm tra đơn giản dựa trên đuôi URL
            if any(final_url.lower().endswith(ext) for ext in ['.gif', '.webp', '.mp4', '.gifv']):
                gif_url = final_url
                logger.debug(f"Got GIF URL: {gif_url}")
            else:
                logger.warning(f"GIF API final URL doesn't look like a direct media link: {final_url}")
    except httpx.HTTPStatusError as e:
        logger.error(f"Error fetching GIF URL (HTTP Status): {e.response.status_code} - {e}", exc_info=False)
    except httpx.RequestError as e:
        logger.error(f"Error fetching GIF URL (Request Error): {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching GIF URL: {e}", exc_info=False) # Log ngắn gọn lỗi GIF
    return gif_url if gif_url and gif_url.startswith(('http://', 'https://')) else None

async def send_response_with_gif(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, processing_msg_id: int | None = None, original_user_msg_id: int | None = None, parse_mode: str = ParseMode.HTML, disable_web_page_preview: bool = True, reply_to_message: bool = False, include_gif: bool = True, delete_original_after: bool = True) -> Message | None:
    """Gửi phản hồi (GIF + Text), chỉnh sửa nếu có processing_msg_id, tùy chọn xóa tin nhắn gốc."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id if update.effective_user else "N/A"
    sent_gif_msg = None
    sent_text_msg = None

    # 1. Gửi GIF (nếu cần)
    if include_gif and GIF_API_URL:
        gif_url = await get_random_gif_url()
        if gif_url:
            try:
                sent_gif_msg = await context.bot.send_animation(chat_id=chat_id, animation=gif_url, connect_timeout=20, read_timeout=30)
                logger.debug(f"Sent GIF to user {user_id}")
            except Exception as e:
                logger.error(f"Error sending GIF ({gif_url}): {e}", exc_info=False)

    # 2. Chuẩn bị và Gửi Text
    final_text = text
    # Tự động bọc bằng <b><i> nếu không có thẻ HTML nào
    if not re.search(r'<[a-zA-Z/][^>]*>', text):
        final_text = f"<b><i>{text}</i></b>"
    if len(final_text) > 4096: # Giới hạn độ dài tin nhắn Telegram
        final_text = final_text[:4050].rstrip() + "...\n<i>(Nội dung bị cắt bớt)</i>"

    # Xác định ID tin nhắn để trả lời nếu reply_to_message là True
    reply_to_msg_id = None
    if reply_to_message:
         reply_to_msg_id = (update.message.message_id if update and update.message and not processing_msg_id and not sent_gif_msg else
                           (sent_gif_msg.message_id if sent_gif_msg else None))

    message_to_edit_id = processing_msg_id # Sử dụng ID đã truyền nếu có

    try:
        if message_to_edit_id:
            # Thử chỉnh sửa tin nhắn "processing"
            sent_text_msg = await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_to_edit_id, text=final_text,
                parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview
            )
            logger.info(f"Edited message {message_to_edit_id}")
        else:
            # Gửi tin nhắn mới
            sent_text_msg = await context.bot.send_message(
                chat_id=chat_id, text=final_text, parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview, reply_to_message_id=reply_to_msg_id
            )
            logger.info(f"Sent new text message to user {user_id}")
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Message {message_to_edit_id} not modified.")
            # Nếu không sửa được, ta vẫn có đối tượng tin nhắn gốc
            try:
                sent_text_msg = await context.bot.get_chat(chat_id).get_message(message_to_edit_id) # Lấy lại đối tượng
            except Exception: pass # Bỏ qua nếu không lấy được
        elif "message to edit not found" in str(e).lower() and message_to_edit_id:
            # Nếu chỉnh sửa thất bại vì tin nhắn bị xóa, gửi tin mới
            logger.warning(f"Message {message_to_edit_id} not found for editing, sending new message.")
            try:
                sent_text_msg = await context.bot.send_message(
                    chat_id=chat_id, text=final_text, parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview, reply_to_message_id=reply_to_msg_id
                )
                logger.info(f"Sent new text message as fallback for editing error.")
            except Exception as fallback_e:
                logger.error(f"Error sending fallback message: {fallback_e}", exc_info=True)
        elif "Can't parse entities" in str(e): # Xử lý lỗi phân tích HTML
             logger.warning("HTML parsing error, sending as plain text.")
             plain_text = re.sub('<[^<]+?>', '', text) # Xóa thẻ HTML
             plain_text = f"{plain_text}\n\n(Lỗi định dạng HTML)"
             try:
                 # Thử chỉnh sửa trước nếu có thể, nếu không thì gửi mới
                 target_msg_id = message_to_edit_id if message_to_edit_id else (sent_text_msg.message_id if sent_text_msg else None)
                 if target_msg_id:
                     await context.bot.edit_message_text(chat_id=chat_id, message_id=target_msg_id, text=plain_text[:4096], disable_web_page_preview=True)
                 else:
                     await context.bot.send_message(chat_id=chat_id, text=plain_text[:4096], disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
             except Exception as pt_fallback_e:
                 logger.error(f"Error sending plain text fallback: {pt_fallback_e}", exc_info=True)
        else:
            logger.error(f"BadRequest sending/editing text: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending/editing text: {e}", exc_info=True)

    # 3. Xóa Tin nhắn Gốc của Người dùng (nếu được yêu cầu và không phải là reply)
    if original_user_msg_id and delete_original_after and not reply_to_message:
        # Chỉ xóa nếu đã gửi được phản hồi (GIF hoặc text)
        if sent_gif_msg or sent_text_msg:
            await delete_user_message(update, context, original_user_msg_id)
        else:
            logger.warning(f"Not deleting original message {original_user_msg_id} because sending response failed.")

    # Trả về đối tượng Message text đã gửi/sửa (nếu có)
    return sent_text_msg

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên."""
    return f"Dinotool-{''.join(random.choices(string.ascii_letters + string.digits, k=length))}"

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn."""
    global valid_keys, activated_users
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate = []
    data_changed = False

    # Kiểm tra keys hết hạn (chưa dùng)
    for key, data in list(valid_keys.items()): # Lặp qua bản copy để tránh lỗi thay đổi dict khi lặp
        try:
            # Chỉ xóa key chưa được dùng (used_by is None) và đã hết hạn
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)):
                keys_to_remove.append(key)
        except (ValueError, TypeError) as e:
            logger.warning(f"[Cleanup] Invalid expiry_time for key {key}: {e}. Removing.")
            keys_to_remove.append(key) # Xóa key có dữ liệu lỗi

    # Kiểm tra user hết hạn kích hoạt
    for user_id_str, expiry_timestamp_str in list(activated_users.items()): # Lặp qua bản copy
        try:
            if current_time > float(expiry_timestamp_str):
                users_to_deactivate.append(user_id_str)
        except (ValueError, TypeError) as e:
            logger.warning(f"[Cleanup] Invalid activation timestamp for user {user_id_str}: {e}. Removing.")
            users_to_deactivate.append(user_id_str) # Xóa user có dữ liệu lỗi

    # Thực hiện xóa
    for key in keys_to_remove:
        if key in valid_keys:
            del valid_keys[key]
            logger.info(f"[Cleanup] Removed expired UNUSED key: {key}")
            data_changed = True
    for user_id_str in users_to_deactivate:
        if user_id_str in activated_users:
            del activated_users[user_id_str]
            logger.info(f"[Cleanup] Deactivated user: {user_id_str}")
            data_changed = True

    # Lưu nếu có thay đổi
    if data_changed:
        logger.info("[Cleanup] Data changed, saving...")
        save_data()
    else:
        logger.debug("[Cleanup] No expired data to clean.")

def is_user_activated(user_id: int) -> bool:
    """Kiểm tra trạng thái kích hoạt của người dùng."""
    user_id_str = str(user_id) # Luôn sử dụng string key
    expiry_time_str = activated_users.get(user_id_str)
    if expiry_time_str:
        try:
            expiry_time = float(expiry_time_str)
            if time.time() < expiry_time:
                return True # Còn hạn
            else: # Hết hạn -> Xóa khỏi danh sách kích hoạt và lưu
                if user_id_str in activated_users:
                    logger.info(f"User {user_id_str} activation expired. Removing.")
                    del activated_users[user_id_str]
                    save_data()
                return False # Hết hạn
        except (ValueError, TypeError): # Dữ liệu thời gian lỗi -> Xóa và lưu
             logger.warning(f"Invalid activation timestamp '{expiry_time_str}' for user {user_id_str}. Removing.")
             if user_id_str in activated_users:
                 del activated_users[user_id_str]
                 save_data()
             return False # Coi như không kích hoạt
    return False # Không tìm thấy trong danh sách kích hoạt

# --- Hàm tạo tin nhắn help ---
def get_help_message_text() -> str:
    """Tạo nội dung tin nhắn trợ giúp/lệnh."""
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    msg = (
        f"╭─── ⋅ ⋅ ─── 📜 <b>DANH SÁCH LỆNH</b> ─── ⋅ ⋅ ───╮\n\n"
        f"│ 🔑 <code>/getkey</code>\n"
        f"│    ➜ Lấy link để nhận Key kích hoạt.\n"
        f"│    <i>(⏳ {gk_cd_m} phút / lần)</i>\n\n"
        f"│ ⚡️ <code>/nhapkey <key></code>\n"
        f"│    ➜ Nhập Key bạn nhận được để kích hoạt.\n"
        f"│    <i>(Key dùng 1 lần, hiệu lực {key_exp_h} giờ nếu chưa nhập)</i>\n\n"
        f"│ ❤️ <code>/tim <link_video_tiktok></code>\n"
        f"│    ➜ Tăng ❤️ cho video TikTok.\n"
        f"│    <i>(Yêu cầu kích hoạt, ⏳ {tf_cd_m} phút / lần)</i>\n\n"
        f"│ 👥 <code>/fl <username_tiktok></code>\n"
        f"│    ➜ Tăng follow cho tài khoản TikTok.\n"
        f"│    <i>(Yêu cầu kích hoạt, ⏳ {tf_cd_m} phút / user)</i>\n\n"
        f"│ 📜 <code>/help</code> hoặc <code>/lenh</code>\n"
        f"│    ➜ Hiển thị danh sách lệnh này.\n\n"
        f"│ 👋 <code>/start</code>\n"
        f"│    ➜ Hiển thị tin nhắn chào mừng & hướng dẫn.\n\n"
        f"│ ✨ <b>Trạng thái kích hoạt:</b> Dùng lệnh trong <b>{act_h} giờ</b> sau khi nhập key thành công.\n\n"
        f"╰─── ⋅ ⋅ ─── 🤖 <a href='https://t.me/dinotool'>DinoTool Bot</a> ─── ⋅ ⋅ ───╯"
    )
    return msg

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /start."""
    if not update or not update.message: return
    user = update.effective_user
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    # Sử dụng hàm get_help_message_text để lấy phần lệnh
    help_text_part = get_help_message_text()

    msg = (
        f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Chào mừng đến với Bot hỗ trợ TikTok của DinoTool.\n"
        f"<i>Lưu ý: Bot chỉ hoạt động trong nhóm được chỉ định.</i>\n\n"
        f"✨ <b>Quy trình sử dụng:</b>\n"
        f"1️⃣ Dùng lệnh <code>/getkey</code> để nhận một link đặc biệt.\n"
        f"2️⃣ Truy cập link đó và làm theo hướng dẫn để lấy mã Key (Ví dụ: <code>Dinotool-xxxx</code>).\n"
        f"3️⃣ Quay lại đây và sử dụng lệnh <code>/nhapkey <key_cua_ban></code>.\n"
        f"4️⃣ Sau khi kích hoạt thành công, bạn có thể dùng lệnh <code>/tim</code> và <code>/fl</code> trong vòng <b>{act_h} giờ</b>.\n\n"
        f"{help_text_part}" # Thêm phần danh sách lệnh vào đây
    )

    # Cho phép /start trong nhóm hoặc chat riêng
    if update.effective_chat.type == 'private' or update.effective_chat.id == ALLOWED_GROUP_ID:
        await update.message.reply_html(msg, disable_web_page_preview=True)
        logger.info(f"User {user.id} used /start in chat {update.effective_chat.id}")
    else:
        logger.info(f"User {user.id} tried /start in unauthorized group ({update.effective_chat.id}). Ignored.")
        # Không cần xóa lệnh /start ở group khác

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /help hoặc /lenh."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id

    help_text = get_help_message_text()

    # Cho phép /help trong nhóm hoặc chat riêng
    if update.effective_chat.type == 'private' or chat_id == ALLOWED_GROUP_ID:
        await update.message.reply_html(help_text, disable_web_page_preview=True)
        logger.info(f"User {user.id} used /help in chat {chat_id}")
        # Xóa lệnh /help gốc trong group
        if chat_id == ALLOWED_GROUP_ID:
            await delete_user_message(update, context, original_message_id)
    else:
        logger.info(f"User {user.id} tried /help in unauthorized group ({chat_id}). Ignored.")
        # Không cần xóa lệnh /help ở group khác


async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /tim."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id) # Luôn dùng string key

    # 1. Kiểm tra Nhóm
    if chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/tim attempt by user {user_id} outside allowed group ({chat_id}). Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Kiểm tra Kích hoạt
    if not is_user_activated(user_id):
        act_msg = (f"⚠️ {user.mention_html()}, bạn chưa kích hoạt!\n"
                   f"➡️ Dùng <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.")
        # Gửi tin nhắn lỗi và xóa lệnh gốc
        sent_msg = await send_response_with_gif(update, context, act_msg,
                                                original_user_msg_id=original_message_id,
                                                include_gif=False, delete_original_after=True)
        if sent_msg and context.job_queue: # Lên lịch xóa tin nhắn lỗi sau 20 giây
            job_name = f"del_act_tim_{chat_id}_{sent_msg.message_id}"
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=job_name)
        return

    # 3. Kiểm tra Cooldown
    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage = float(last_usage_str)
            if (current_time - last_usage) < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - last_usage)
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng lại <code>/tim</code>."
                sent_cd_msg = None
                try: # Gửi tin nhắn cooldown (không cần xóa lệnh gốc ở đây vì sẽ xóa sau)
                    sent_cd_msg = await update.message.reply_html(f"<b><i>{cd_msg}</i></b>")
                except Exception as e: logger.error(f"Error sending /tim cooldown msg: {e}")
                # Xóa lệnh gốc
                await delete_user_message(update, context, original_message_id)
                if sent_cd_msg and context.job_queue: # Lên lịch xóa tin nhắn cooldown sau 15 giây
                    job_name = f"del_cd_tim_{chat_id}_{sent_cd_msg.message_id}"
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=job_name)
                return
        except (ValueError, TypeError):
            logger.warning(f"Invalid cooldown timestamp for tim user {user_id}. Resetting.")
            if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str]; save_data()

    # 4. Phân tích Input
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Thiếu link video.\n"
                   "➡️ Ví dụ: <code>/tim https://vt.tiktok.com/ZSru...</code>")
    elif not isinstance(args[0], str) or ("tiktok.com" not in args[0] or not args[0].startswith(("http://", "https://"))):
        err_txt = "⚠️ Link TikTok không hợp lệ. Vui lòng kiểm tra lại."
    else:
        video_url = args[0]

    if err_txt:
        sent_err_msg = None
        try: # Gửi tin nhắn lỗi input
            sent_err_msg = await update.message.reply_html(f"<b><i>{err_txt}</i></b>")
        except Exception as e: logger.error(f"Error sending /tim input error msg: {e}")
        # Xóa lệnh gốc
        await delete_user_message(update, context, original_message_id)
        if sent_err_msg and context.job_queue: # Lên lịch xóa tin nhắn lỗi sau 15 giây
            job_name = f"del_inp_tim_{chat_id}_{sent_err_msg.message_id}"
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=job_name)
        return

    # 5. Gọi API
    if not video_url or not API_KEY: # Kiểm tra lại phòng trường hợp lỗi logic
        logger.error(f"/tim: Invalid state - video_url or API_KEY missing for user {user_id}")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc
        await send_response_with_gif(update, context, text="❌ Lỗi cấu hình Bot hoặc dữ liệu nhập.",
                                     original_user_msg_id=None, include_gif=False, delete_original_after=False)
        return

    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    logger.info(f"User {user_id} calling /tim API for URL: {video_url[:50]}...")

    processing_msg_id = None
    final_response_text = ""
    is_success = False
    sent_processing_msg = None # Lưu lại đối tượng tin nhắn processing

    try:
        # Gửi tin nhắn "Đang xử lý..." và lấy ID
        try:
            sent_processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
            if sent_processing_msg: processing_msg_id = sent_processing_msg.message_id
        except Exception as e:
            logger.error(f"Failed to send 'Processing...' message for /tim: {e}")
            # Vẫn tiếp tục mà không có ID, sẽ gửi tin nhắn mới sau

        # !!! verify=False LÀ KHÔNG AN TOÀN !!!
        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'Telegram DinoTool Bot'})

            content_type = resp.headers.get("content-type", "").lower()
            logger.debug(f"/tim API response status: {resp.status_code}, content-type: {content_type}")

            if "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/tim API JSON response: {data}")
                    if data.get("success") is True: # Kiểm tra chính xác là True
                        user_tim_cooldown[user_id_str] = time.time(); save_data() # Cập nhật cooldown
                        is_success = True
                        d = data.get("data", {})

                        # Lấy thông tin an toàn với giá trị mặc định '?'
                        author = html.escape(str(d.get("author", "?")))
                        region = html.escape(str(d.get("region", "?")))
                        duration = d.get("duration", "?")
                        create_time = html.escape(str(d.get("create_time", "?")))
                        digg_before = d.get('digg_before', '?')
                        digg_increased = d.get('digg_increased', '?')
                        digg_after = d.get('digg_after', '?')
                        api_video_url = html.escape(str(d.get("video_url", video_url))) # Dùng URL từ API nếu có

                        # Format số cho dễ đọc (nếu là số)
                        try: digg_before_f = f"{int(digg_before):,}".replace(',', '.') if isinstance(digg_before, (int, float)) else digg_before
                        except ValueError: digg_before_f = digg_before
                        try: digg_increased_f = f"{int(digg_increased):,}".replace(',', '.') if isinstance(digg_increased, (int, float)) else digg_increased
                        except ValueError: digg_increased_f = digg_increased
                        try: digg_after_f = f"{int(digg_after):,}".replace(',', '.') if isinstance(digg_after, (int, float)) else digg_after
                        except ValueError: digg_after_f = digg_after
                        try: duration_f = f"{int(duration)} giây" if isinstance(duration, (int, float)) else duration
                        except ValueError: duration_f = duration

                        # --- GIAO DIỆN SIÊU HIỆN ĐẠI ---
                        final_response_text = (
                            f"╭─── ⋅ ⋅ ─── 🎉 <b>TIM THÀNH CÔNG</b> 🎉 ─── ⋅ ⋅ ───╮\n\n"
                            f"│ 🎬 <b>Video:</b> <a href='{api_video_url}'>Xem ngay</a>\n"
                            f"│ 👤 <b>Tác giả:</b> <code>{author}</code>\n"
                            f"│ 🌍 <b>Khu vực:</b> {region} | ⏱️ <b>Thời lượng:</b> {duration_f}\n"
                            f"│ 🗓️ <b>Ngày tạo:</b> <i>{create_time}</i>\n"
                            f"│───────── ✨ <b>Kết quả</b> ✨ ─────────\n"
                            f"│ 👍 <b>Trước:</b>   <code>{digg_before_f}</code> ❤️\n"
                            f"│ 💖 <b>Đã tăng:</b> <code>+{digg_increased_f}</code> ❤️\n"
                            f"│ ✅ <b>Hiện tại:</b> <code>{digg_after_f}</code> ❤️\n\n"
                            f"╰─── ⋅ ⋅ ─── 🤖 <a href='https://t.me/dinotool'>DinoTool</a> ⋅ ⋅ ───╯"
                        )

                    else: # success không phải True
                        api_message = data.get('message', 'Không có thông báo lỗi từ API.')
                        logger.warning(f"/tim API returned success=false. User: {user_id}. Message: {api_message}")
                        final_response_text = f"💔 <b>Lỗi Tăng Tim!</b>\n📄 <i>API báo:</i> <code>{html.escape(str(api_message))}</code>"

                except json.JSONDecodeError:
                    logger.error(f"/tim API response status {resp.status_code} but not valid JSON. User: {user_id}. Response: {resp.text[:500]}")
                    final_response_text = f"❌ Lỗi: API trả về dữ liệu không đúng định dạng JSON (mặc dù Content-Type là JSON)."
            elif resp.status_code == 200: # Status 200 nhưng content type không phải JSON
                 logger.error(f"/tim API response status 200 but unexpected Content-Type '{content_type}'. User: {user_id}. Response: {resp.text[:500]}")
                 final_response_text = f"❌ Lỗi: API trả về định dạng không mong muốn (Content-Type: {html.escape(content_type)})."
            else: # Lỗi HTTP khác
                logger.error(f"/tim API HTTP error. Status: {resp.status_code}. User: {user_id}. Response: {resp.text[:500]}")
                final_response_text = f"❌ Lỗi kết nối API tăng tim (Mã lỗi: {resp.status_code}). Thử lại sau."

    except httpx.TimeoutException:
        logger.warning(f"/tim API timeout for user {user_id}")
        final_response_text = "❌ Lỗi: Yêu cầu tăng tim tới API bị timeout. Thử lại sau."
    except httpx.ConnectError as e:
        logger.error(f"/tim API connection error for user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ Lỗi: Không thể kết nối đến máy chủ API tăng tim."
    except httpx.RequestError as e: # Các lỗi mạng khác
        logger.error(f"/tim API network error for user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ Lỗi mạng khi thực hiện yêu cầu tăng tim."
    except Exception as e:
        logger.error(f"Unexpected error during /tim processing for user {user_id}: {e}", exc_info=True)
        final_response_text = "❌ Lỗi hệ thống Bot không mong muốn khi xử lý /tim."
    finally:
        # Gửi phản hồi cuối cùng: chỉnh sửa tin nhắn "processing" nếu có, nếu không thì gửi mới
        # Luôn xóa tin nhắn lệnh gốc của người dùng
        await send_response_with_gif(update, context, text=final_response_text,
                                     processing_msg_id=processing_msg_id, # ID tin nhắn processing để thử edit
                                     original_user_msg_id=original_message_id, # ID lệnh gốc để xóa
                                     include_gif=is_success, # Chỉ gửi GIF nếu thành công
                                     reply_to_message=False, # Không reply
                                     delete_original_after=True) # Luôn xóa lệnh gốc

async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /fl."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id) # Luôn dùng string key

    # 1. Kiểm tra Nhóm
    if chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/fl attempt by user {user_id} outside allowed group ({chat_id}). Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Kiểm tra Kích hoạt
    if not is_user_activated(user_id):
        act_msg = (f"⚠️ {user.mention_html()}, bạn chưa kích hoạt!\n"
                   f"➡️ Dùng <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.")
        sent_msg = await send_response_with_gif(update, context, act_msg,
                                                original_user_msg_id=original_message_id,
                                                include_gif=False, delete_original_after=True)
        if sent_msg and context.job_queue:
            job_name = f"del_act_fl_{chat_id}_{sent_msg.message_id}"
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=job_name)
        return

    # 3. Phân tích Input
    args = context.args
    target_username = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Thiếu username TikTok.\n"
                   "➡️ Ví dụ: <code>/fl tiktokuser</code>")
    elif not isinstance(args[0], str):
         err_txt = "⚠️ Username không hợp lệ."
    else:
        uname = args[0].strip().lstrip("@") # Xóa khoảng trắng và dấu @ nếu có
        if not uname:
            err_txt = "⚠️ Username không được để trống."
        # Regex kiểm tra username TikTok (tương đối): 2-24 ký tự, chữ cái, số, dấu gạch dưới, dấu chấm. Không kết thúc bằng dấu chấm.
        elif not re.match(r"^[a-zA-Z0-9_.]{2,24}$", uname) or uname.endswith('.'):
            err_txt = f"⚠️ Username <code>{html.escape(uname)}</code> không hợp lệ."
        else:
            target_username = uname

    if err_txt:
        sent_err_msg = None
        try: # Gửi tin nhắn lỗi input
             sent_err_msg = await update.message.reply_html(f"<b><i>{err_txt}</i></b>")
        except Exception as e: logger.error(f"Error sending /fl input error msg: {e}")
        # Xóa lệnh gốc
        await delete_user_message(update, context, original_message_id)
        if sent_err_msg and context.job_queue: # Lên lịch xóa tin nhắn lỗi
            job_name = f"del_inp_fl_{chat_id}_{sent_err_msg.message_id}"
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=job_name)
        return

    # 4. Kiểm tra Cooldown (chỉ kiểm tra nếu username hợp lệ)
    if target_username:
        user_fl_cds = user_fl_cooldown.get(user_id_str, {}) # Lấy dict cooldown của user, trả về {} nếu chưa có
        last_usage_str = user_fl_cds.get(target_username) # Lấy cooldown cho username cụ thể

        if last_usage_str:
            try:
                last_usage = float(last_usage_str)
                if (current_time - last_usage) < TIM_FL_COOLDOWN_SECONDS:
                    rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - last_usage)
                    cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f} giây</b> nữa để <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                    sent_cd_msg = None
                    try: # Gửi tin nhắn cooldown
                        sent_cd_msg = await update.message.reply_html(f"<b><i>{cd_msg}</i></b>")
                    except Exception as e: logger.error(f"Error sending /fl cooldown msg: {e}")
                    # Xóa lệnh gốc
                    await delete_user_message(update, context, original_message_id)
                    if sent_cd_msg and context.job_queue: # Lên lịch xóa tin nhắn cooldown
                        job_name = f"del_cd_fl_{chat_id}_{sent_cd_msg.message_id}"
                        context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=job_name)
                    return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp for fl user {user_id}, target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]
                     if not user_fl_cooldown[user_id_str]: # Nếu dict con rỗng thì xóa luôn key user_id_str
                          del user_fl_cooldown[user_id_str]
                     save_data()

    # 5. Gọi API
    if not target_username or not API_KEY: # Kiểm tra lại
        logger.error(f"/fl: Invalid state - target_username or API_KEY missing for user {user_id}")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc
        await send_response_with_gif(update, context, text="❌ Lỗi cấu hình Bot hoặc dữ liệu nhập.",
                                     original_user_msg_id=None, include_gif=False, delete_original_after=False)
        return

    api_url = FOLLOW_API_URL_TEMPLATE.format(username=target_username, api_key=API_KEY)
    logger.info(f"User {user_id} calling /fl API for username: @{target_username}...")

    processing_msg_id = None
    final_response_text = ""
    is_success = False
    sent_processing_msg = None # Lưu lại đối tượng tin nhắn processing

    try:
        # Gửi tin nhắn "Đang xử lý..."
        try:
            sent_processing_msg = await update.message.reply_html(f"<b><i>⏳ Đang xử lý yêu cầu tăng follow cho @{html.escape(target_username)}...</i></b> 👥")
            if sent_processing_msg: processing_msg_id = sent_processing_msg.message_id
        except Exception as e:
            logger.error(f"Failed to send 'Processing...' message for /fl: {e}")

        # !!! verify=False LÀ KHÔNG AN TOÀN !!!
        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'Telegram DinoTool Bot'})

            content_type = resp.headers.get("content-type", "").lower()
            logger.debug(f"/fl API response status: {resp.status_code}, content-type: {content_type}")

            if "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/fl API JSON response: {data}")
                    if data.get("success") is True:
                        # Cập nhật cooldown cho user và target cụ thể
                        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = time.time()
                        save_data()
                        is_success = True
                        d = data.get("data", {})

                        # Lấy thông tin an toàn
                        api_username = html.escape(str(d.get("username", target_username))) # Dùng username từ API nếu có
                        nickname = html.escape(str(d.get("nickname", "?")))
                        user_id_tiktok = html.escape(str(d.get("user_id", "?")))
                        follower_before = d.get('follower_before', '?')
                        follower_increased = d.get('follower_increased', '?')
                        follower_after = d.get('follower_after', '?')

                        # Format số
                        try: fb_f = f"{int(follower_before):,}".replace(',', '.') if isinstance(follower_before, (int, float)) else follower_before
                        except ValueError: fb_f = follower_before
                        try: fi_f = f"{int(follower_increased):,}".replace(',', '.') if isinstance(follower_increased, (int, float)) else follower_increased
                        except ValueError: fi_f = follower_increased
                        try: fa_f = f"{int(follower_after):,}".replace(',', '.') if isinstance(follower_after, (int, float)) else follower_after
                        except ValueError: fa_f = follower_after

                        final_response_text = (
                            f"╭─── ⋅ ⋅ ─── 🎉 <b>FOLLOW THÀNH CÔNG</b> 🎉 ─── ⋅ ⋅ ───╮\n\n"
                            f"│ 👤 <b>Tài khoản:</b> <code>@{api_username}</code>\n"
                            f"│ 📛 <b>Tên hiển thị:</b> {nickname}\n"
                            f"│ 🆔 <b>TikTok ID:</b> <code>{user_id_tiktok}</code>\n"
                            f"│───────── ✨ <b>Kết quả</b> ✨ ─────────\n"
                            f"│ 👍 <b>Trước:</b>   <code>{fb_f}</code> followers\n"
                            f"│ 📈 <b>Đã tăng:</b> <code>+{fi_f}</code> followers\n"
                            f"│ ✅ <b>Hiện tại:</b> <code>{fa_f}</code> followers\n\n"
                            f"╰─── ⋅ ⋅ ─── 🤖 <a href='https://t.me/dinotool'>DinoTool</a> ⋅ ⋅ ───╯"
                        )

                    else: # success không phải True
                        api_message = data.get('message', 'Không có thông báo lỗi từ API.')
                        logger.warning(f"/fl API returned success=false for @{target_username}. User: {user_id}. Message: {api_message}")
                        final_response_text = f"💔 <b>Lỗi Tăng Follow</b> cho @{html.escape(target_username)}!\n📄 <i>API báo:</i> <code>{html.escape(str(api_message))}</code>"

                except json.JSONDecodeError:
                    logger.error(f"/fl API response status {resp.status_code} but not valid JSON for @{target_username}. User: {user_id}. Response: {resp.text[:500]}")
                    final_response_text = f"❌ Lỗi: API Follow trả về dữ liệu không đúng định dạng JSON."
            elif resp.status_code == 200:
                 logger.error(f"/fl API response status 200 but unexpected Content-Type '{content_type}' for @{target_username}. User: {user_id}. Response: {resp.text[:500]}")
                 final_response_text = f"❌ Lỗi: API Follow trả về định dạng không mong muốn (Content-Type: {html.escape(content_type)})."
            else:
                logger.error(f"/fl API HTTP error for @{target_username}. Status: {resp.status_code}. User: {user_id}. Response: {resp.text[:500]}")
                final_response_text = f"❌ Lỗi kết nối API tăng follow cho @{html.escape(target_username)} (Mã lỗi: {resp.status_code})."

    except httpx.TimeoutException:
        logger.warning(f"/fl API timeout for @{target_username}, user {user_id}")
        final_response_text = f"❌ Lỗi: Yêu cầu tăng follow cho @{html.escape(target_username)} bị timeout."
    except httpx.ConnectError as e:
        logger.error(f"/fl API connection error for @{target_username}, user {user_id}: {e}", exc_info=False)
        final_response_text = f"❌ Lỗi: Không thể kết nối đến máy chủ API tăng follow."
    except httpx.RequestError as e:
        logger.error(f"/fl API network error for @{target_username}, user {user_id}: {e}", exc_info=False)
        final_response_text = f"❌ Lỗi mạng khi thực hiện yêu cầu tăng follow cho @{html.escape(target_username)}."
    except Exception as e:
        logger.error(f"Unexpected error during /fl processing for @{target_username}, user {user_id}: {e}", exc_info=True)
        final_response_text = "❌ Lỗi hệ thống Bot không mong muốn khi xử lý /fl."
    finally:
        # Gửi phản hồi cuối cùng và xóa lệnh gốc
        await send_response_with_gif(update, context, text=final_response_text,
                                     processing_msg_id=processing_msg_id,
                                     original_user_msg_id=original_message_id,
                                     include_gif=is_success,
                                     reply_to_message=False,
                                     delete_original_after=True)


async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /getkey để tạo link lấy key sử dụng yeumoney.com."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id) # Luôn dùng string key

    # 1. Check Group
    if chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/getkey attempt by user {user_id} outside allowed group ({chat_id}). Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Check Cooldown
    last_usage_str = user_getkey_cooldown.get(user_id_str)
    if last_usage_str:
         try:
             last_usage = float(last_usage_str)
             if (current_time - last_usage) < GETKEY_COOLDOWN_SECONDS:
                remaining = GETKEY_COOLDOWN_SECONDS - (current_time - last_usage)
                cooldown_msg_content = f"⏳ {user.mention_html()}, bạn cần đợi <b>{remaining:.0f} giây</b> nữa để dùng <code>/getkey</code>."
                sent_cd_msg = None
                try: # Gửi tin nhắn cooldown
                    sent_cd_msg = await update.message.reply_html(f"<b><i>{cooldown_msg_content}</i></b>")
                except Exception as e: logger.error(f"Error sending /getkey cooldown msg: {e}")
                # Xóa lệnh gốc
                await delete_user_message(update, context, original_message_id)
                if sent_cd_msg and context.job_queue: # Lên lịch xóa tin nhắn cooldown
                    job_name = f"delete_cd_getkey_{chat_id}_{sent_cd_msg.message_id}"
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=job_name)
                return
         except (ValueError, TypeError):
              logger.warning(f"Invalid cooldown timestamp for getkey user {user_id}. Resetting.")
              if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]; save_data()

    # 3. Tạo Key & URL Đích
    generated_key = generate_random_key()
    while generated_key in valid_keys: # Đảm bảo key là duy nhất
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    # Thêm cache buster để tránh trình duyệt cache link đích cũ
    cache_buster = f"&_cb={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster

    # 4. Chuẩn bị tham số API rút gọn link
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    # Log params an toàn (che token)
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}", "format": "json", "url": final_target_url }

    logger.info(f"User {user_id} requesting key. New key: {generated_key}. Target URL (pre-shorten): {final_target_url}")

    processing_msg_id = None # ID tin nhắn "Đang xử lý..."
    final_response_text = ""
    key_saved_to_dict = False
    sent_processing_msg = None # Lưu đối tượng tin nhắn processing

    try:
        # Gửi tin nhắn "Đang xử lý..." và lấy ID
        try:
            sent_processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng đợi giây lát...</i></b> 🔑")
            if sent_processing_msg: processing_msg_id = sent_processing_msg.message_id
        except Exception as e:
            logger.error(f"Failed to send 'Processing...' message for /getkey: {e}")
            # Tiếp tục mà không có ID, sẽ gửi tin nhắn mới

        # 5. Lưu key tạm thời TRƯỚC KHI gọi API rút gọn
        # Điều này đảm bảo key tồn tại ngay cả khi API rút gọn lỗi, nhưng sẽ được dọn dẹp sau
        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id,
            "generation_time": generation_time,
            "expiry_time": expiry_time,
            "used_by": None # Chưa được sử dụng
        }
        key_saved_to_dict = True
        save_data() # Lưu ngay lập tức
        logger.info(f"Key {generated_key} temporarily saved for user {user_id}. Expires in {KEY_EXPIRY_SECONDS / 3600:.1f} hours.")

        # 6. Gọi API Rút gọn Link
        logger.debug(f"Calling link shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        # !!! verify=False LÀ KHÔNG AN TOÀN !!!
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)

            response_content_type = response.headers.get("content-type", "").lower()
            response_text = response.text # Đọc text để xử lý cả JSON và lỗi HTML/text

            # 7. Xử lý Phản hồi API (linh hoạt hơn với Content-Type)
            if response.status_code == 200:
                try:
                    # Thử parse JSON bất kể content-type là gì (vì API có thể trả về sai)
                    response_data = json.loads(response_text)
                    logger.info(f"Link shortener API response parsed as JSON (Content-Type: '{response_content_type}'). Data: {response_data}")

                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time(); save_data() # Cập nhật cooldown khi thành công
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}")
                        key_exp_h = KEY_EXPIRY_SECONDS // 3600
                        final_response_text = (
                            f"╭─── ⋅ ⋅ ─── 🚀 <b>LẤY KEY KÍCH HOẠT</b> 🚀 ─── ⋅ ⋅ ───╮\n\n"
                            f"│ 🔗 <b>Link của bạn:</b> <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n"
                            f"│    <i>(Click vào link trên để tiếp tục)</i>\n\n"
                            f"│ ❓ <b>Hướng dẫn:</b>\n"
                            f"│   1️⃣ Click vào link.\n"
                            f"│   2️⃣ Làm theo các bước yêu cầu trên trang web.\n"
                            f"│   3️⃣ Bạn sẽ nhận được mã Key dạng <code>Dinotool-xxxx</code>.\n"
                            f"│   4️⃣ Quay lại đây và dùng lệnh:\n"
                            f"│      <code>/nhapkey <key_cua_ban></code>\n\n"
                            f"│ ⏳ <i>Key cần được nhập trong vòng <b>{key_exp_h} giờ</b> kể từ bây giờ.</i>\n\n"
                            f"╰─── ⋅ ⋅ ─── 🤖 <a href='https://t.me/dinotool'>DinoTool</a> ⋅ ⋅ ───╯"
                        )
                        # Không xóa key khỏi valid_keys vì đã thành công
                        key_saved_to_dict = False # Đánh dấu là không cần xóa key nữa

                    else: # JSON hợp lệ nhưng status báo lỗi
                        api_message = response_data.get("message", status if status else f"Lỗi không rõ từ API")
                        logger.error(f"Link shortener API error (JSON status). User: {user_id}. Msg: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Tạo Link:</b> <code>{html.escape(str(api_message))}</code>."
                        # Key đã được lưu, cần xóa đi vì tạo link thất bại
                        if key_saved_to_dict and generated_key in valid_keys:
                            logger.warning(f"Removing key {generated_key} due to link shortener API error.")
                            del valid_keys[generated_key]; save_data()
                            key_saved_to_dict = False # Đã xóa, không cần xóa lại

                except json.JSONDecodeError: # Status 200, nhưng không phải JSON hợp lệ
                    logger.error(f"Link shortener API Status 200 but not valid JSON. User: {user_id}. Type: '{response_content_type}'. Text: {response_text[:500]}")
                    final_response_text = f"❌ <b>Lỗi API Rút Gọn Link:</b> Phản hồi không đúng định dạng JSON. Vui lòng thử lại sau."
                    if key_saved_to_dict and generated_key in valid_keys:
                         logger.warning(f"Removing key {generated_key} due to invalid JSON response from shortener.")
                         del valid_keys[generated_key]; save_data()
                         key_saved_to_dict = False
            else: # HTTP Status != 200
                 logger.error(f"Link shortener API HTTP error. User: {user_id}. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text[:500]}")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Rút Gọn Link</b> (Mã lỗi: {response.status_code}). Thử lại sau."
                 if key_saved_to_dict and generated_key in valid_keys:
                     logger.warning(f"Removing key {generated_key} due to shortener API HTTP error {response.status_code}.")
                     del valid_keys[generated_key]; save_data()
                     key_saved_to_dict = False

    # Xử lý lỗi mạng và lỗi chung
    except httpx.TimeoutException:
        logger.warning(f"Link shortener API timeout for /getkey user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> API rút gọn link không phản hồi kịp thời. Thử lại sau."
    except httpx.ConnectError as e:
        logger.error(f"Link shortener API connection error for /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối đến API rút gọn link."
    except httpx.RequestError as e: # Các lỗi httpx khác
        logger.error(f"Link shortener API network error for /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API rút gọn link."
    except Exception as e:
        logger.error(f"Unexpected error in /getkey for user {user_id}: {e}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> không mong muốn khi tạo key."
    finally:
        # Nếu key vẫn còn trong dict và chưa bị đánh dấu xóa (tức là có lỗi xảy ra trước khi xử lý xong)
        if key_saved_to_dict and generated_key in valid_keys:
             logger.warning(f"Removing key {generated_key} due to an error/exception during /getkey processing.")
             try: del valid_keys[generated_key]; save_data()
             except Exception as save_err: logger.error(f"Error saving data after removing key {generated_key} in finally block: {save_err}")

        # 8. Gửi Phản hồi Cuối cùng (Chỉnh sửa hoặc Gửi mới) và Xóa lệnh gốc
        await send_response_with_gif(update, context, final_response_text,
            processing_msg_id=processing_msg_id, # ID tin nhắn processing để thử edit
            original_user_msg_id=original_message_id, # ID lệnh gốc để xóa
            disable_web_page_preview=False, # Hiển thị preview cho link rút gọn
            include_gif=False, # Không cần GIF
            reply_to_message=False,
            delete_original_after=True # Luôn xóa lệnh /getkey gốc
        )


async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý lệnh /nhapkey."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id) # Luôn dùng string key

    # 1. Kiểm tra Nhóm
    if chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/nhapkey attempt by user {user_id} outside allowed group ({chat_id}). Deleting message.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Phân tích Input
    args = context.args
    submitted_key = None
    err_txt = ""
    if not args:
        err_txt = ("⚠️ Thiếu key cần nhập.\n"
                   "➡️ Ví dụ: <code>/nhapkey Dinotool-ABC123XYZ</code>")
    elif len(args) > 1:
        err_txt = "⚠️ Bạn chỉ nên nhập một key duy nhất sau lệnh <code>/nhapkey</code>."
    else:
        key = args[0].strip()
        if not key:
             err_txt = "⚠️ Key không được để trống."
        elif not key.startswith("Dinotool-") or len(key) < len("Dinotool-") + 4: # Ít nhất 4 ký tự sau prefix
             err_txt = f"⚠️ Key <code>{html.escape(key)}</code> có vẻ không đúng định dạng. Key phải bắt đầu bằng <code>Dinotool-</code>."
        # elif not key[len("Dinotool-"):].isalnum(): # Bỏ kiểm tra isalnum để linh hoạt hơn với key
        #     err_txt = f"⚠️ Phần sau 'Dinotool-' của key không hợp lệ."
        else:
            submitted_key = key # Key hợp lệ về mặt định dạng

    if err_txt:
        sent_err_msg = None
        try: # Gửi tin nhắn lỗi input
            sent_err_msg = await update.message.reply_html(f"<b><i>{err_txt}</i></b>")
        except Exception as e: logger.error(f"Error sending /nhapkey input error msg: {e}")
        # Xóa lệnh gốc
        await delete_user_message(update, context, original_message_id)
        if sent_err_msg and context.job_queue: # Lên lịch xóa tin nhắn lỗi
            job_name = f"del_err_nhapkey_{chat_id}_{sent_err_msg.message_id}"
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=job_name)
        return # Dừng xử lý

    # 3. Xác thực Key
    logger.info(f"User {user_id} attempting activation with key: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key) # Lấy dữ liệu key từ dict
    final_response_text = ""
    activation_success = False

    if not key_data: # Key không tồn tại trong dict
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại. Vui lòng kiểm tra lại hoặc lấy key mới bằng <code>/getkey</code>."
    elif key_data.get("used_by") is not None: # Key đã được sử dụng
        used_by_user_id = key_data["used_by"]
        if str(used_by_user_id) == user_id_str: # Chính người này đã dùng key này rồi
            # Kiểm tra xem họ còn đang active không
            if is_user_activated(user_id):
                 expiry_time = float(activated_users.get(user_id_str, 0))
                 expiry_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(expiry_time))
                 final_response_text = f"✨ Bạn đang được kích hoạt đến <b>{expiry_str}</b>. Không cần nhập lại key này."
            else:
                 # Lạ, đã dùng key nhưng không active? Có thể do cleanup hoặc lỗi logic. Kích hoạt lại.
                 logger.warning(f"User {user_id} used key {submitted_key} before but wasn't active. Reactivating.")
                 activation_expiry = current_time + ACTIVATION_DURATION_SECONDS
                 activated_users[user_id_str] = activation_expiry; save_data() # Lưu trạng thái kích hoạt mới
                 expiry_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(activation_expiry))
                 activation_success = True # Đánh dấu thành công để gửi GIF
                 act_h = ACTIVATION_DURATION_SECONDS // 3600
                 final_response_text = (f"✅ <b>Kích hoạt lại thành công!</b>\n\n"
                                        f"🔑 Key: <code>{html.escape(submitted_key)}</code>\n"
                                        f"✨ Bạn có thể dùng <code>/tim</code>, <code>/fl</code>.\n"
                                        f"⏳ Thời hạn sử dụng đến: <b>{expiry_str}</b> ({act_h} giờ).")

        else: # Key đã bị người khác sử dụng
             mention_generator = f" (được tạo bởi user {key_data.get('user_id_generator', '?')})" if key_data.get('user_id_generator') else ""
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được sử dụng bởi một người dùng khác{mention_generator}. Mỗi key chỉ dùng được một lần."
    elif "expiry_time" not in key_data or not isinstance(key_data["expiry_time"], (int, float)):
         logger.error(f"Invalid 'expiry_time' data for key {submitted_key}: {key_data.get('expiry_time')}")
         final_response_text = f"❌ Lỗi dữ liệu với key <code>{html.escape(submitted_key)}</code>. Vui lòng liên hệ quản trị viên."
         # Cân nhắc xóa key lỗi này
         if submitted_key in valid_keys: del valid_keys[submitted_key]; save_data()
    elif current_time > key_data["expiry_time"]: # Key chưa sử dụng nhưng đã hết hạn (quá hạn nhập)
        expiry_time = float(key_data["expiry_time"])
        expiry_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(expiry_time))
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn vào lúc {expiry_str}. Vui lòng lấy key mới bằng <code>/getkey</code>."
        # Xóa key hết hạn khỏi danh sách
        if submitted_key in valid_keys:
             logger.info(f"Removing expired key {submitted_key} during activation attempt.")
             del valid_keys[submitted_key]
             save_data()
    else: # Key hợp lệ, chưa dùng, chưa hết hạn => Kích hoạt!
        key_data["used_by"] = user_id # Đánh dấu key đã được sử dụng bởi user này
        activation_expiry = current_time + ACTIVATION_DURATION_SECONDS # Tính thời gian hết hạn kích hoạt
        activated_users[user_id_str] = activation_expiry # Lưu trạng thái kích hoạt
        save_data() # Lưu cả key đã dùng và user đã kích hoạt
        logger.info(f"User {user_id} successfully activated using key {submitted_key}. Active until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(activation_expiry))}")

        expiry_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(activation_expiry))
        activation_success = True # Đánh dấu thành công
        act_h = ACTIVATION_DURATION_SECONDS // 3600
        final_response_text = (
            f"╭─── ⋅ ⋅ ─── ✅ <b>KÍCH HOẠT THÀNH CÔNG</b> ✅ ─── ⋅ ⋅ ───╮\n\n"
            f"│ Chúc mừng {user.mention_html()}!\n\n"
            f"│ 🔑 Key đã sử dụng: <code>{html.escape(submitted_key)}</code>\n"
            f"│ ✨ Giờ bạn có thể sử dụng các lệnh:\n"
            f"│    <code>/tim <link></code>\n"
            f"│    <code>/fl <user></code>\n\n"
            f"│ ⏳ Thời hạn sử dụng đến: <b>{expiry_str}</b>\n"
            f"│    <i>(Còn lại khoảng {act_h} giờ)</i>\n\n"
            f"╰─── ⋅ ⋅ ─── 🤖 <a href='https://t.me/dinotool'>DinoTool</a> ⋅ ⋅ ───╯"
        )

    # 4. Gửi Phản hồi Cuối cùng và xóa lệnh gốc
    await send_response_with_gif(update, context, final_response_text,
                                 original_user_msg_id=original_message_id, # ID lệnh gốc để xóa
                                 include_gif=activation_success, # Gửi GIF nếu kích hoạt thành công
                                 reply_to_message=False,
                                 delete_original_after=True) # Luôn xóa lệnh /nhapkey gốc

# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    print("--- Bot Configuration ---")
    print(f"Bot Token: ...{BOT_TOKEN[-6:]}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID}")
    print(f"Link Shortener API Key (Token): {'Set' if LINK_SHORTENER_API_KEY else 'Not Set'}")
    print(f"Link Shortener API Base URL: {LINK_SHORTENER_API_BASE_URL}")
    print(f"Tim/Fl API Key: {'Set' if API_KEY else 'Not Set'}")
    print(f"Data File: {DATA_FILE}")
    print(f"Key Expiry (Unused): {KEY_EXPIRY_SECONDS / 3600:.1f} hours")
    print(f"Activation Duration: {ACTIVATION_DURATION_SECONDS / 3600:.1f} hours")
    print(f"Cooldown Tim/Fl: {TIM_FL_COOLDOWN_SECONDS / 60:.0f} minutes")
    print(f"Cooldown Getkey: {GETKEY_COOLDOWN_SECONDS / 60:.0f} minutes")
    print(f"Cleanup Interval: {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes")
    print("-" * 25)
    print("--- !!! WARNING: Hardcoded Tokens/Keys detected - Major SECURITY RISK !!! ---")
    print("--- !!! WARNING: SSL Verification may be Disabled (verify=False) - SECURITY RISK !!! ---")
    print("-" * 25)

    print("Loading saved data...")
    load_data()
    print(f"Loaded {len(valid_keys)} pending/used keys.")
    print(f"Loaded {len(activated_users)} activated users.")
    print(f"Loaded cooldowns: /tim={len(user_tim_cooldown)}, /fl={len(user_fl_cooldown)}, /getkey={len(user_getkey_cooldown)}")

    # Build Application
    # Tăng timeout để xử lý API chậm
    application = Application.builder().token(BOT_TOKEN).job_queue(JobQueue())\
        .pool_timeout(120).connect_timeout(30).read_timeout(70).write_timeout(70).build()

    # Schedule Jobs
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
    print(f"Scheduled data cleanup job running every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")

    # Register Handlers
    # Filter cho nhóm và chat riêng
    group_filter = filters.Chat(chat_id=ALLOWED_GROUP_ID)
    private_filter = filters.ChatType.PRIVATE
    allowed_chat_filter = group_filter | private_filter

    application.add_handler(CommandHandler("start", start_command, filters=allowed_chat_filter))
    # /help và /lenh dùng chung 1 hàm và filter
    application.add_handler(CommandHandler(["help", "lenh"], help_command, filters=allowed_chat_filter))

    # Các lệnh chỉ cho phép trong group
    application.add_handler(CommandHandler("getkey", getkey_command, filters=group_filter))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=group_filter))
    application.add_handler(CommandHandler("tim", tim_command, filters=group_filter))
    application.add_handler(CommandHandler("fl", fl_command, filters=group_filter))

    # Handler cho các lệnh không xác định trong group (để xóa)
    async def unknown_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message and update.message.text and update.message.text.startswith('/'):
            # Danh sách các lệnh đã biết (bao gồm cả alias)
            known_commands = ['/start', '/tim', '/fl', '/getkey', '/nhapkey', '/help', '/lenh']
            # Lấy phần command chính, xử lý cả dạng /cmd@botname
            cmd = update.message.text.split(' ')[0].split('@')[0]
            if cmd not in known_commands:
                logger.info(f"Unknown command '{update.message.text}' detected in the allowed group. Deleting.")
                await delete_user_message(update, context) # Xóa lệnh không xác định

    # Thêm handler này với priority thấp hơn (group=1) để nó chỉ chạy nếu các handler lệnh cụ thể không khớp
    application.add_handler(MessageHandler(filters.COMMAND & group_filter, unknown_in_group), group=1)

    # Start Bot
    print("Bot is starting polling...")
    try:
        # drop_pending_updates=True để bỏ qua các update cũ khi bot offline
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an exception: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped: {e}", exc_info=True)
    finally:
        # Cố gắng lưu dữ liệu lần cuối khi bot dừng
        print("\nBot has stopped or encountered a critical error.")
        logger.info("Bot shutdown process initiated.")
        print("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")

if __name__ == "__main__":
    main()
