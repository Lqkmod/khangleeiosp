#-*- coding: utf-8 -*-
import logging
import httpx
import json
import html
import os
import time
import random
import string
import re
import asyncio # Thêm asyncio

from telegram import Update, Message
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
# import ssl # Thường không cần trừ khi có vấn đề SSL cụ thể

# --- Cấu hình ---
# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (VẪN CẦN CHO LỆNH /tim)
ALLOWED_GROUP_ID = -1002191171631 # <--- GROUP ID CỦA BẠN

LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney


# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút (Dùng chung cho tim và fl)
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 6 * 3600   # 12 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 12 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (KHÔNG ĐỔI)
FOLLOW_API_URL_BASE = "https://apitangfltiktok.soundcast.me/telefl.php" # <-- API FOLLOW MỚI (BASE URL)

# GIF_API_URL = None # Tạm thời tắt GIF
GIF_API_URL = None # Tạm thời tắt GIF mặc định để tập trung vào text

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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình ---
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": logger.critical("!!! BOT_TOKEN is missing or not replaced !!!"); exit(1)
if not ALLOWED_GROUP_ID: logger.critical("!!! ALLOWED_GROUP_ID is missing !!!"); exit(1)
if not LINK_SHORTENER_API_KEY or LINK_SHORTENER_API_KEY == "YOUR_YEUMONEY_TOKEN": logger.critical("!!! LINK_SHORTENER_API_KEY is missing or not replaced !!!"); exit(1)
if not API_KEY or API_KEY == "YOUR_TIM_API_KEY": logger.warning("!!! API_KEY (for /tim) is missing or not replaced. /tim command might fail. !!!")

# --- Hàm lưu/tải dữ liệu ---
def save_data():
    # Convert keys to strings for JSON compatibility
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    # Handle nested structure for fl_cooldown
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}

    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown, # Save the correctly structured fl cooldown
            "getkey": string_key_getkey_cooldown
        }
    }
    try:
        # Use ensure_ascii=False to handle Unicode characters correctly
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
                valid_keys = data.get("valid_keys", {})
                # Load activated users ensuring keys are strings
                activated_users = {str(k): v for k, v in data.get("activated_users", {}).items()}
                all_cooldowns = data.get("user_cooldowns", {})
                # Load cooldowns ensuring keys are strings
                user_tim_cooldown = {str(k): v for k, v in all_cooldowns.get("tim", {}).items()}
                # Load fl_cooldown correctly handling nested structure and string keys
                loaded_fl = all_cooldowns.get("fl", {})
                user_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in loaded_fl.items()}
                user_getkey_cooldown = {str(k): v for k, v in all_cooldowns.get("getkey", {}).items()}
                logger.info(f"Data loaded from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data.")
            valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}
    except Exception as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data.", exc_info=True)
        valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except (BadRequest, Forbidden) as e:
        if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
             logger.info(f"Could not delete message {msg_id_to_delete} (already deleted or no permission): {e}")
        else:
             logger.error(f"BadRequest deleting message {msg_id_to_delete}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa tin nhắn theo lịch."""
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try: await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except (BadRequest, Forbidden) as e: logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
        except Exception as e: logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else: logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def get_random_gif_url() -> str | None:
    """Lấy URL GIF ngẫu nhiên."""
    if not GIF_API_URL: return None
    gif_url = None
    try:
        async with httpx.AsyncClient(timeout=10.0, verify=True, follow_redirects=True) as client:
            response = await client.get(GIF_API_URL)
            response.raise_for_status()
            final_url = str(response.url)
            if any(final_url.lower().endswith(ext) for ext in ['.gif', '.webp', '.mp4', '.gifv']):
                gif_url = final_url
                logger.debug(f"Got GIF URL: {gif_url}")
            else:
                 logger.warning(f"GIF API final URL doesn't look like a direct media link: {final_url}")
    except Exception as e:
        logger.error(f"Error fetching GIF URL: {e}", exc_info=False)
    return gif_url if gif_url and gif_url.startswith(('http://', 'https://')) else None

async def send_response_with_gif(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, processing_msg_id: int | None = None, original_user_msg_id: int | None = None, parse_mode: str = ParseMode.HTML, disable_web_page_preview: bool = True, reply_to_message: bool = False, include_gif: bool = True) -> Message | None:
    """Gửi phản hồi (GIF + Text), chỉnh sửa nếu có processing_msg_id, xóa tin nhắn gốc."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id if update.effective_user else "N/A"
    sent_gif_msg = None
    sent_text_msg = None

    # 1. Send GIF (nếu cần và có URL)
    if include_gif and GIF_API_URL:
        gif_url = await get_random_gif_url()
        if gif_url:
            try:
                sent_gif_msg = await context.bot.send_animation(chat_id=chat_id, animation=gif_url, connect_timeout=20, read_timeout=30)
                logger.debug(f"Sent GIF to user {user_id}")
            except Exception as e: logger.error(f"Error sending GIF ({gif_url}): {e}", exc_info=False)

    # 2. Prepare and Send Text
    final_text = text
    if len(final_text) > 4096: final_text = final_text[:4050].rstrip() + "...\n<i>(Nội dung bị cắt bớt)</i>"

    reply_to_msg_id = None
    if reply_to_message:
         reply_to_msg_id = (update.message.message_id if update and update.message and not processing_msg_id and not sent_gif_msg else
                           (sent_gif_msg.message_id if sent_gif_msg else None))

    message_to_edit_id = processing_msg_id # Use the passed ID if available

    try:
        if message_to_edit_id:
            sent_text_msg = await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_to_edit_id, text=final_text,
                parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview
            )
            logger.info(f"Edited message {message_to_edit_id}")
        else:
            sent_text_msg = await context.bot.send_message(
                chat_id=chat_id, text=final_text, parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview, reply_to_message_id=reply_to_msg_id
            )
            logger.info(f"Sent new text message to user {user_id}")
    except BadRequest as e:
        if "Message is not modified" in str(e): logger.info(f"Message {message_to_edit_id} not modified.")
        elif "message to edit not found" in str(e).lower() and message_to_edit_id:
            logger.warning(f"Message {message_to_edit_id} not found for editing, sending new message.")
            try:
                sent_text_msg = await context.bot.send_message(
                    chat_id=chat_id, text=final_text, parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview, reply_to_message_id=reply_to_msg_id
                )
                logger.info(f"Sent new text message as fallback for editing error.")
            except Exception as fallback_e: logger.error(f"Error sending fallback message: {fallback_e}", exc_info=True)
        elif "Can't parse entities" in str(e):
             logger.warning(f"HTML parsing error for message ID {message_to_edit_id}, sending as plain text.")
             plain_text = re.sub('<[^<]+?>', '', text) # Basic HTML tag stripping
             plain_text = html.unescape(plain_text) # Decode HTML entities like &
             plain_text = f"{plain_text}\n\n(Lỗi hiển thị định dạng HTML)"
             try:
                 target_msg_id = message_to_edit_id if message_to_edit_id else None
                 if target_msg_id:
                     await context.bot.edit_message_text(chat_id=chat_id, message_id=target_msg_id, text=plain_text[:4096], disable_web_page_preview=True)
                 else:
                     await context.bot.send_message(chat_id=chat_id, text=plain_text[:4096], disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
             except Exception as pt_fallback_e: logger.error(f"Error sending plain text fallback after parse error: {pt_fallback_e}", exc_info=True)
        else: logger.error(f"BadRequest sending/editing text (ID: {message_to_edit_id}): {e}")
    except Exception as e: logger.error(f"Unexpected error sending/editing text (ID: {message_to_edit_id}): {e}", exc_info=True)

    # 3. Delete Original User Message (nếu ID được cung cấp và không phải là reply)
    if original_user_msg_id and not reply_to_message:
        if sent_text_msg: # Check if text msg was sent/edited
            await delete_user_message(update, context, original_user_msg_id)
        else:
             logger.warning(f"Not deleting original message {original_user_msg_id} because sending/editing response failed.")

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

    # Check expired keys
    for key, data in list(valid_keys.items()):
        try:
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)):
                keys_to_remove.append(key)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid expiry_time for key {key}, removing.")
            keys_to_remove.append(key)

    # Check expired activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp):
                users_to_deactivate.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Invalid activation timestamp for user {user_id_str}, removing.")
            users_to_deactivate.append(user_id_str)

    # Perform deletions
    for key in keys_to_remove:
        if key in valid_keys: del valid_keys[key]; logger.info(f"[Cleanup] Removed expired unused key: {key}"); data_changed = True
    for user_id_str in users_to_deactivate:
        if user_id_str in activated_users: del activated_users[user_id_str]; logger.info(f"[Cleanup] Deactivated user: {user_id_str}"); data_changed = True

    # Save if data changed
    if data_changed: logger.info("[Cleanup] Data changed, saving..."); save_data()
    else: logger.debug("[Cleanup] No expired data to clean.")

def is_user_activated(user_id: int) -> bool:
    """Kiểm tra trạng thái kích hoạt."""
    user_id_str = str(user_id)
    expiry_time_str = activated_users.get(user_id_str)
    if expiry_time_str:
        try:
            if time.time() < float(expiry_time_str): return True
            else:
                if user_id_str in activated_users:
                    logger.info(f"User {user_id_str} activation expired. Removing.")
                    del activated_users[user_id_str]; save_data()
                return False
        except (ValueError, TypeError):
             logger.warning(f"Invalid activation timestamp for user {user_id_str}. Removing.")
             if user_id_str in activated_users: del activated_users[user_id_str]; save_data()
             return False
    return False

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start."""
    if not update or not update.message: return
    user = update.effective_user
    act_h = ACTIVATION_DURATION_SECONDS // 3600; key_exp_h = KEY_EXPIRY_SECONDS // 3600
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60; gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
           f"🤖 Bot hỗ trợ TikTok.\n<i>Chỉ dùng trong nhóm chỉ định.</i>\n\n"
           f"✨ <b>Quy trình:</b>\n"
           f"1️⃣ <code>/getkey</code> ➜ Nhận link.\n"
           f"2️⃣ Truy cập link ➜ Lấy Key (VD: <code>Dinotool-xxxx</code>).\n"
           f"3️⃣ <code>/nhapkey <key></code>.\n"
           f"4️⃣ Dùng <code>/tim</code>, <code>/fl</code> trong <b>{act_h} giờ</b>.\n\n"
           f"ℹ️ <b>Lệnh:</b>\n"
           f"🔑 <code>/getkey</code> (⏳ {gk_cd_m}p/lần).\n"
           f"⚡️ <code>/nhapkey <key></code> (Key dùng 1 lần, hiệu lực {key_exp_h}h).\n"
           f"❤️ <code>/tim <link></code> (Y/c kích hoạt, ⏳ {tf_cd_m}p/lần).\n"
           f"👥 <code>/fl <user></code> (Y/c kích hoạt, ⏳ {tf_cd_m}p/user).\n\n"
           f"<i>Bot by <a href='https://t.me/dinotool'>DinoTool</a></i>")
    if update.effective_chat.type == 'private' or update.effective_chat.id == ALLOWED_GROUP_ID:
        await update.message.reply_html(msg, disable_web_page_preview=True)
    else: logger.info(f"User {user.id} tried /start in unauthorized group ({update.effective_chat.id}).")

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim (Vẫn dùng API_KEY)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user; user_id = user.id
    current_time = time.time(); original_message_id = update.message.message_id; user_id_str = str(user_id)

    # 1. Check Group
    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    # 2. Check Activation
    if not is_user_activated(user_id):
        act_msg = (f"⚠️ {user.mention_html()}, bạn cần kích hoạt tài khoản trước!\n➡️ Dùng: <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.")
        sent_msg = await send_response_with_gif(update, context, act_msg, original_user_msg_id=original_message_id, include_gif=False) # Delete original cmd
        if sent_msg and hasattr(sent_msg, 'message_id') and context.job_queue: # Schedule deletion of error msg
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=f"del_act_tim_{sent_msg.message_id}")
        return

    # 3. Check Cooldown
    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage = float(last_usage_str)
            if (current_time - last_usage) < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - last_usage)
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f}</b> giây nữa để dùng <code>/tim</code>."
                sent_cd_msg = None
                try: sent_cd_msg = await update.message.reply_html(f"<b><i>{cd_msg}</i></b>")
                except Exception as e: logger.error(f"Error sending /tim cooldown msg: {e}")
                await delete_user_message(update, context, original_message_id) # Delete original cmd
                if sent_cd_msg and context.job_queue: # Schedule deletion of cooldown msg
                     context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=f"del_cd_tim_{sent_cd_msg.message_id}")
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for tim user {user_id}. Resetting.")
             if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str]; save_data()


    # 4. Parse Input
    args = context.args; video_url = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu link video.\nVD: <code>/tim link...</code>")
    elif "tiktok.com" not in args[0] or not args[0].startswith(("http://", "https://")): err_txt = "⚠️ Link không hợp lệ."
    else: video_url = args[0]

    if err_txt:
        sent_err_msg = None
        try: sent_err_msg = await update.message.reply_html(f"<b><i>{err_txt}</i></b>")
        except Exception as e: logger.error(f"Error sending /tim input error msg: {e}")
        await delete_user_message(update, context, original_message_id) # Delete original cmd
        if sent_err_msg and context.job_queue: # Schedule deletion of error msg
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=f"del_inp_tim_{sent_err_msg.message_id}")
        return

    # 5. API Call (Vẫn dùng VIDEO_API_URL_TEMPLATE và API_KEY)
    if not video_url or not API_KEY: # Kiểm tra API_KEY ở đây
        await delete_user_message(update, context, original_message_id)
        await send_response_with_gif(update, context, text="❌ Lỗi cấu hình API Key cho /tim hoặc input.", original_user_msg_id=None, include_gif=False); return

    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY) # Dùng API_KEY
    logger.info(f"User {user_id} calling /tim API: {api_url.replace(API_KEY, '***')}") # Che API Key khi log URL
    processing_msg_id = None; final_response_text = ""; is_success = False
    processing_msg_obj = None # Initialize
    try:
        # Send processing message FIRST
        try:
             processing_msg_obj = await update.message.reply_html("<b><i>⏳ Đang xử lý ❤️...</i></b>")
             if processing_msg_obj: processing_msg_id = processing_msg_obj.message_id
        except Exception as e:
             logger.error(f"Failed to send processing message for /tim: {e}")

        # Now make the API call
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot'})
            if "application/json" in resp.headers.get("content-type","").lower():
                data = resp.json()
                if data.get("success"): # API Tim dùng "success"
                    user_tim_cooldown[user_id_str] = time.time(); save_data()
                    d=data.get("data",{}); a=html.escape(str(d.get("author","?"))); ct=html.escape(str(d.get("create_time","?"))); v=html.escape(str(d.get("video_url", video_url))); db=html.escape(str(d.get('digg_before','?'))); di=html.escape(str(d.get('digg_increased','?'))); da=html.escape(str(d.get('digg_after','?')))
                    final_response_text = (f"🎉 <b>Tim OK!</b> ❤️\n\n📊 <b>Info:</b>\n🎬 <a href='{v}'>Link</a>\n👤 <code>{a}</code> | 🗓️ <code>{ct}</code>\n👍 <code>{db}</code>➜💖<code>+{di}</code>➜✅<code>{da}</code>")
                    is_success = True
                else: final_response_text = f"💔 <b>Lỗi Tim!</b>\n<i>API:</i> <code>{html.escape(data.get('message','?'))}</code>"
            else: final_response_text = f"❌ Lỗi định dạng phản hồi API (Code: {resp.status_code}, Type: {resp.headers.get('content-type', '?')})."
    except httpx.TimeoutException: final_response_text = "❌ Lỗi: API timeout."
    except httpx.RequestError as e: final_response_text = f"❌ Lỗi mạng/kết nối API: {e}"
    except json.JSONDecodeError: final_response_text = "❌ Lỗi: API trả về dữ liệu không phải JSON hợp lệ."
    except Exception as e: logger.error(f"Unexpected error /tim: {e}", exc_info=True); final_response_text = "❌ Lỗi hệ thống Bot."
    finally:
        await send_response_with_gif(update, context, text=final_response_text,
                                     processing_msg_id=processing_msg_id,
                                     original_user_msg_id=original_message_id,
                                     include_gif=(is_success and GIF_API_URL is not None),
                                     reply_to_message=False)

# --- /fl Command - Sử dụng API MỚI và xử lý JSON đã sửa ---

async def process_fl_request_background(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id_str: str, # Pass as string
    target_username: str,
    processing_msg_id: int,
    invoking_user_mention: str # Pass user mention for final message
):
    """Hàm chạy nền để xử lý API follow (API MỚI) và cập nhật kết quả."""

    # --- SỬ DỤNG API MỚI ---
    api_base_url = FOLLOW_API_URL_BASE
    api_params = {
        "user": target_username,
        "userid": user_id_str,
        "tokenbot": BOT_TOKEN
    }
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{BOT_TOKEN[-6:]}"
    logger.info(f"[BG Task] User {user_id_str} calling NEW /fl API for @{target_username} at {api_base_url} with params: {log_api_params}")
    # --- KẾT THÚC THAY ĐỔI API ---

    final_response_text = f"❌ Lỗi không xác định khi xử lý @{html.escape(target_username)}."
    success = False # Chỉ dùng để quyết định có cập nhật cooldown hay không

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            resp = await client.get(api_base_url, params=api_params, headers={'User-Agent': 'TG Bot FL'})

            content_type = resp.headers.get("content-type", "").lower()
            logger.debug(f"[BG Task /fl @{target_username}] API Status: {resp.status_code}, Content-Type: {content_type}")

            # Luôn cố gắng đọc text để debug nếu không phải JSON
            response_text_for_debug = ""
            try:
                response_text_for_debug = resp.text
            except Exception:
                pass # Ignore if reading text fails

            if "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"[BG Task /fl @{target_username}] API JSON Data: {data}")

                    # ---- SỬA LỖI TRÍCH XUẤT DỮ LIỆU ----
                    api_status = data.get("status") # Boolean or None
                    api_message = data.get("message", "Không có thông báo từ API.")

                    # Trích xuất thông tin user bất kể status
                    name = html.escape(str(data.get("name", "?")))
                    tt_username_from_api = data.get("username")
                    tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
                    tt_user_id = html.escape(str(data.get("user_id", "?")))
                    khu_vuc = html.escape(str(data.get("khu_vuc", "Không rõ")))
                    avatar = data.get("avatar", "")
                    create_time = html.escape(str(data.get("create_time", "?")))
                    f_before = html.escape(str(data.get("followers_before", "?")))
                    f_add = html.escape(str(data.get("followers_add", "?")))
                    f_after = html.escape(str(data.get("followers_after", "?")))
                    # ---- KẾT THÚC SỬA LỖI TRÍCH XUẤT ----

                    # ---- XÂY DỰNG PHẢN HỒI DỰA TRÊN STATUS ----
                    user_info_block = (
                        f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)\n"
                        f"🆔 <b>ID:</b> <code>{tt_user_id}</code>\n"
                        f"🌍 <b>Khu vực:</b> {khu_vuc}\n"
                        f"📅 <b>Ngày tạo TK:</b> {create_time}\n"
                    )
                    if avatar and avatar.startswith("http"):
                        user_info_block += f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>\n"

                    follower_info_block = (
                        f"📈 <b>Followers:</b>\n"
                        f"   Trước: <code>{f_before}</code>\n"
                        f"   Tăng:   <code>+{f_add}</code>\n"
                        f"   Sau:    <code>{f_after}</code>"
                    )

                    if api_status is True: # Thành công theo API
                        success = True
                        current_time = time.time()
                        user_fl_cooldown.setdefault(user_id_str, {})[target_username] = current_time
                        save_data()
                        logger.info(f"[BG Task] Updated cooldown for user {user_id_str} on @{target_username}")

                        final_response_text = (
                            f"✅ <b>Follow Thành Công!</b> cho {invoking_user_mention}\n\n"
                            f"{user_info_block}\n"
                            f"{follower_info_block}"
                        )
                    else: # api_status is False hoặc None (Lỗi theo API)
                        logger.warning(f"[BG Task /fl @{target_username}] API returned status={api_status}, message: {api_message}")
                        final_response_text = (
                            f"❌ <b>Lỗi Follow</b> cho {invoking_user_mention}!\n\n"
                            f"💬 Lý do API: <code>{html.escape(api_message)}</code>\n\n"
                            f"{user_info_block}" # Vẫn hiển thị thông tin user
                        )
                        if "đợi" in api_message.lower() and ("phút" in api_message.lower() or "giây" in api_message.lower()):
                            final_response_text += f"\n\n<i>ℹ️ Vui lòng chờ theo yêu cầu của API.</i>"

                except json.JSONDecodeError:
                    logger.error(f"[BG Task /fl @{target_username}] API response is not valid JSON. Text: {response_text_for_debug[:500]}...")
                    final_response_text = f"❌ Lỗi: API @{html.escape(target_username)} không trả về JSON hợp lệ."
                except Exception as e:
                    logger.error(f"[BG Task /fl @{target_username}] Error processing API data: {e}", exc_info=True)
                    final_response_text = f"❌ Lỗi xử lý dữ liệu từ API cho @{html.escape(target_username)}."

            else: # Content type is not JSON
                 logger.error(f"[BG Task /fl @{target_username}] API response type is not JSON: {content_type}. Status: {resp.status_code}. Text: {response_text_for_debug[:500]}...")
                 final_response_text = f"❌ Lỗi định dạng phản hồi từ API @{html.escape(target_username)} (Code: {resp.status_code})."

    except httpx.TimeoutException:
        logger.warning(f"[BG Task /fl @{target_username}] API timeout.")
        final_response_text = f"❌ Lỗi: API timeout khi follow @{html.escape(target_username)}."
    except httpx.RequestError as e:
        logger.error(f"[BG Task /fl @{target_username}] Network error: {e}", exc_info=False)
        final_response_text = f"❌ Lỗi mạng khi kết nối API follow @{html.escape(target_username)}."
    except Exception as e:
        logger.error(f"[BG Task /fl @{target_username}] Unexpected error: {e}", exc_info=True)
        final_response_text = f"❌ Lỗi hệ thống Bot khi xử lý follow @{html.escape(target_username)}."

    # --- Cập nhật tin nhắn gốc ---
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=processing_msg_id,
            text=final_response_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        logger.info(f"[BG Task] Edited message {processing_msg_id} for /fl @{target_username}")
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"[BG Task] Message {processing_msg_id} already contained the final text.")
        elif "message to edit not found" in str(e).lower():
            logger.warning(f"[BG Task] Message {processing_msg_id} not found for editing. Sending new message.")
            try:
                 await context.bot.send_message(chat_id, final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as send_err:
                 logger.error(f"[BG Task] Failed to send fallback message for /fl @{target_username}: {send_err}")
        elif "Can't parse entities" in str(e):
             logger.warning(f"[BG Task] HTML parsing error editing {processing_msg_id}. Sending plain text.")
             plain_text = re.sub('<[^<]+?>', '', final_response_text)
             plain_text = html.unescape(plain_text)
             plain_text += "\n\n(Lỗi hiển thị định dạng HTML)"
             try: await context.bot.edit_message_text(chat_id, processing_msg_id, plain_text[:4096], disable_web_page_preview=True)
             except Exception as pt_edit_err: logger.error(f"[BG Task] Failed to edit with plain text fallback: {pt_edit_err}")
        else:
            logger.error(f"[BG Task] BadRequest editing message {processing_msg_id}: {e}")
    except Exception as e:
        logger.error(f"[BG Task] Unexpected error editing message {processing_msg_id}: {e}", exc_info=True)


async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /fl - Chỉ kiểm tra, gửi tin nhắn chờ và chạy task nền (API MỚI)."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    invoking_user_mention = user.mention_html()
    current_time = time.time()
    original_message_id = update.message.message_id

    # 1. Check Group
    if chat_id != ALLOWED_GROUP_ID:
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Check Activation
    if not is_user_activated(user_id):
        act_msg = (f"⚠️ {invoking_user_mention}, bạn cần kích hoạt tài khoản trước!\n➡️ Dùng: <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.")
        sent_msg = None
        try:
            sent_msg = await update.message.reply_html(act_msg, disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error sending /fl activation required message: {e}")
        await delete_user_message(update, context, original_message_id) # Delete original cmd regardless
        if sent_msg and context.job_queue:
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=f"del_act_fl_{sent_msg.message_id}")
        return

    # 3. Parse Input
    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username.\nVD: <code>/fl user</code>")
    else:
        uname = args[0].strip().lstrip("@")
        if not uname: err_txt = "⚠️ Username trống."
        # Relaxed regex slightly to allow more valid usernames, but still basic validation
        elif not re.match(r"^[a-zA-Z0-9_.\-]{2,24}$", uname) or uname.endswith('.') or uname.endswith('-'):
            err_txt = f"⚠️ Username <code>{html.escape(uname)}</code> có vẻ không hợp lệ."
        else: target_username = uname

    if err_txt:
        sent_err_msg = None
        try:
            sent_err_msg = await update.message.reply_html(f"<b><i>{err_txt}</i></b>")
        except Exception as e:
            logger.error(f"Error sending /fl input error msg: {e}")
        await delete_user_message(update, context, original_message_id) # Delete original cmd regardless
        if sent_err_msg and context.job_queue:
             context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=f"del_inp_fl_{sent_err_msg.message_id}")
        return

    # 4. Check Cooldown (for this specific user and target)
    if target_username:
        user_cds = user_fl_cooldown.get(user_id_str, {})
        last_usage_str = user_cds.get(target_username)
        if last_usage_str:
            try:
                last_usage = float(last_usage_str)
                if (current_time - last_usage) < TIM_FL_COOLDOWN_SECONDS:
                     rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - last_usage)
                     cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f}</b> giây nữa để <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                     sent_cd_msg = None
                     try:
                         sent_cd_msg = await update.message.reply_html(f"<b><i>{cd_msg}</i></b>")
                     except Exception as e:
                         logger.error(f"Error sending /fl cooldown msg: {e}")
                     await delete_user_message(update, context, original_message_id) # Delete original cmd regardless
                     if sent_cd_msg and context.job_queue:
                         context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=f"del_cd_fl_{sent_cd_msg.message_id}")
                     return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp for fl user {user_id} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]
                     save_data()

    # --- All checks passed ---

    # 5. Send Immediate "Processing" Message
    processing_msg = None
    try:
        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu follow <code>@{html.escape(target_username)}</code>. Kết quả sẽ cập nhật tại đây..."
        )
    except Exception as e:
        logger.error(f"Failed to send initial processing message for /fl @{target_username}: {e}")
        await delete_user_message(update, context, original_message_id)
        return # Abort if processing message failed

    # 6. Delete the original user command NOW
    await delete_user_message(update, context, original_message_id)

    # 7. Schedule the Background Task
    if processing_msg and target_username:
        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        context.application.create_task(
            process_fl_request_background(
                context=context,
                chat_id=chat_id,
                user_id_str=user_id_str,
                target_username=target_username,
                processing_msg_id=processing_msg.message_id,
                invoking_user_mention=invoking_user_mention
            ),
            update=update
        )
    else:
         logger.error(f"Could not schedule background task for /fl - missing processing message or target username.")
         if processing_msg:
             try:
                 await context.bot.edit_message_text(chat_id, processing_msg.message_id, f"❌ Lỗi nội bộ khi chuẩn bị xử lý @{html.escape(target_username)}.")
             except: pass # Ignore errors editing here

# --- Lệnh /getkey ---
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

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
                cooldown_msg_content = f"⏳ {user.mention_html()}, bạn cần đợi <b>{remaining:.0f}</b> giây nữa để dùng <code>/getkey</code>."
                sent_cd_msg = None
                try: sent_cd_msg = await update.message.reply_html(f"<b><i>{cooldown_msg_content}</i></b>")
                except Exception as e: logger.error(f"Error sending /getkey cooldown msg: {e}")
                await delete_user_message(update, context, original_message_id) # Delete original cmd
                if sent_cd_msg and context.job_queue: # Schedule deletion of cooldown msg
                    job_name = f"delete_cd_getkey_{chat_id}_{sent_cd_msg.message_id}"
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd_msg.message_id}, name=job_name)
                return
         except (ValueError, TypeError):
              logger.warning(f"Invalid cooldown timestamp for getkey user {user_id}. Resetting.")
              if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]; save_data()

    # 3. Generate Key & Target URL
    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&_cb={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster

    # 4. Prepare API Params
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}", "format": "json", "url": final_target_url }

    logger.info(f"User {user_id} requesting key. New key: {generated_key}. Target URL (pre-shorten): {final_target_url}")

    processing_msg_id = None
    final_response_text = ""
    key_saved_to_dict = False
    processing_msg_obj = None # Initialize

    try:
        # Send "Processing..." message
        try:
            processing_msg_obj = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng đợi...</i></b> 🔑")
            if processing_msg_obj: processing_msg_id = processing_msg_obj.message_id
        except Exception as e:
            logger.error(f"Failed to send 'Processing...' message for /getkey: {e}")

        # Save key temporarily BEFORE calling the shortener API
        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = { "user_id_generator": user_id, "generation_time": generation_time, "expiry_time": expiry_time, "used_by": None }
        key_saved_to_dict = True
        save_data() # Save immediately
        logger.info(f"Key {generated_key} temporarily saved for user {user_id}. Expires in {KEY_EXPIRY_SECONDS / 3600:.1f} hours.")

        # 5. Call Shortener API
        logger.debug(f"Calling link shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)

            response_content_type = response.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                response_text_for_debug = response.text
            except Exception: pass

            # 6. Process Response
            if response.status_code == 200:
                try:
                    try:
                         response_data = json.loads(response.content.decode('utf-8', errors='replace')) # Try decoding explicitly
                    except json.JSONDecodeError as jde:
                         logger.error(f"API returned non-JSON for /getkey despite 200 OK. Content-Type: '{response_content_type}'. Error: {jde}. Text: {response_text_for_debug[:500]}")
                         raise jde # Re-raise

                    logger.info(f"Parsed shortener API response as JSON (Content-Type: '{response_content_type}'). Data: {response_data}")

                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time(); save_data()
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}")
                        final_response_text = (
                            f"🚀 <b>Link lấy key của bạn đây ({user.mention_html()}):</b>\n\n"
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"❓ <b>Hướng dẫn:</b>\n"
                            f"   1️⃣ Click link.\n"
                            f"   2️⃣ Làm theo các bước để nhận Key (VD: <code>Dinotool-xxxx</code>).\n"
                            f"   3️⃣ Dùng lệnh: <code>/nhapkey <key_cua_ban></code>\n\n"
                            f"⏳ <i>Key cần nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>.</i>"
                        )
                    else:
                        api_message = response_data.get("message", f"Lỗi không rõ hoặc thiếu 'status'/'shortenedUrl': {response_data}")
                        logger.error(f"Shortener API error via JSON for user {user_id}. Msg: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Tạo Link:</b> <code>{html.escape(str(api_message))}</code>."
                        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data() # Remove invalid key

                except json.JSONDecodeError:
                    # This case should be less likely now with explicit decode, but keep as fallback
                    logger.error(f"API Status 200 but failed JSON decode. Type: '{response_content_type}'. Text: {response_text_for_debug[:500]}")
                    final_response_text = f"❌ <b>Lỗi API:</b> Phản hồi không phải JSON hợp lệ."
                    if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
            else:
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug[:500]}")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Code: {response.status_code})."
                 if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()

    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout for /getkey user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> API tạo link không phản hồi."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except httpx.ConnectError as e:
        logger.error(f"Shortener API connection error for /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối API tạo link."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except httpx.RequestError as e:
        logger.error(f"Shortener API network error for /getkey user {user_id}: {e}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    except Exception as e:
        logger.error(f"Unexpected error in /getkey for user {user_id}: {e}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key."
        if key_saved_to_dict and generated_key in valid_keys: del valid_keys[generated_key]; save_data()
    finally:
        # 7. Send Final Response (Edit or Send New)
        await send_response_with_gif(update, context, final_response_text,
            processing_msg_id=processing_msg_id,
            original_user_msg_id=original_message_id,
            disable_web_page_preview=False, # Show link preview
            include_gif=False,
            reply_to_message=False
        )

# --- Lệnh /nhapkey ---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id; user = update.effective_user; user_id = user.id
    current_time = time.time(); original_message_id = update.message.message_id; user_id_str = str(user_id)

    # 1. Check Group
    if chat_id != ALLOWED_GROUP_ID: await delete_user_message(update, context, original_message_id); return

    # 2. Parse Input
    args = context.args; submitted_key = None; err_txt = ""
    if not args: err_txt = ("⚠️ Thiếu key.\nVD: <code>/nhapkey Dinotool-xxxx</code>")
    elif len(args) > 1: err_txt = "⚠️ Chỉ nhập 1 key."
    else:
        key = args[0].strip()
        if not key.startswith("Dinotool-") or len(key) < len("Dinotool-") + 4: err_txt = f"⚠️ Key <code>{html.escape(key)}</code> sai định dạng."
        elif not key[len("Dinotool-"):].isalnum(): err_txt = f"⚠️ Phần sau 'Dinotool-' của key chỉ chứa chữ/số."
        else: submitted_key = key

    if err_txt:
        sent_err_msg = None
        try:
            sent_err_msg = await send_response_with_gif(update, context, err_txt, original_user_msg_id=None, include_gif=False)
        except Exception as e: logger.error(f"Error sending /nhapkey input error msg: {e}")
        await delete_user_message(update, context, original_message_id)
        if sent_err_msg and hasattr(sent_err_msg, 'message_id') and context.job_queue:
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err_msg.message_id}, name=f"del_err_nhapkey_{sent_err_msg.message_id}")
        return

    # 3. Validate Key
    logger.info(f"User {user_id} attempting activation with key: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key); final_response_text = ""; activation_success = False

    if not key_data: final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ/tồn tại."
    elif key_data.get("used_by") is not None:
        used_by = key_data["used_by"]
        try:
            # Try to format the activation time if available
            act_time = float(key_data.get("activation_time", 0))
            used_time_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(act_time)) if act_time > 0 else "(không rõ)"
        except (ValueError, TypeError):
            used_time_str = "(lỗi thời gian)"
        if str(used_by) == user_id_str: final_response_text = f"⚠️ Bạn đã dùng key <code>{html.escape(submitted_key)}</code> này rồi (Lúc: {used_time_str})."
        else: final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng."
    elif current_time > key_data.get("expiry_time", 0):
        try:
            exp_time_val = float(key_data.get("expiry_time", 0))
            exp_time = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(exp_time_val)) if exp_time_val > 0 else "(không rõ)"
        except (ValueError, TypeError):
             exp_time = "(lỗi thời gian)"
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng (Hạn: {exp_time})."
        if submitted_key in valid_keys: del valid_keys[submitted_key]; save_data() # Clean up expired key
    else: # Activate!
        key_data["used_by"] = user_id
        key_data["activation_time"] = current_time
        activation_expiry = current_time + ACTIVATION_DURATION_SECONDS
        activated_users[user_id_str] = activation_expiry; save_data()
        expiry_str = time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(activation_expiry))
        activation_success = True
        final_response_text = (f"✅ <b>Kích hoạt Thành Công!</b>\n\n"
                               f"👤 User: {user.mention_html()}\n"
                               f"🔑 Key: <code>{html.escape(submitted_key)}</code>\n"
                               f"✨ Có thể dùng <code>/tim</code>, <code>/fl</code>.\n"
                               f"⏳ Đến: <b>{expiry_str}</b> ({ACTIVATION_DURATION_SECONDS // 3600} giờ)."
                             )

    # 4. Send Final Response (deleting original command)
    await send_response_with_gif(update, context, final_response_text,
                                 original_user_msg_id=original_message_id,
                                 include_gif=(activation_success and GIF_API_URL is not None),
                                 reply_to_message=False)

# --- Main Function ---
def main() -> None:
    """Khởi động và chạy bot."""
    print("--- Bot Configuration ---")
    print(f"Bot Token: ...{BOT_TOKEN[-6:]}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID}")
    print(f"Link Shortener API Key (Token): ...{LINK_SHORTENER_API_KEY[-6:]}")
    print(f"Tim API Key (for /tim): ...{API_KEY[-4:]}")
    print(f"Follow API Base URL (for /fl): {FOLLOW_API_URL_BASE}")
    print(f"Data File: {DATA_FILE}")
    print(f"Key Expiry: {KEY_EXPIRY_SECONDS / 3600:.1f} hours")
    print(f"Activation Duration: {ACTIVATION_DURATION_SECONDS / 3600:.1f} hours")
    print(f"Tim/Fl Cooldown: {TIM_FL_COOLDOWN_SECONDS / 60:.1f} minutes")
    print("-" * 25)
    print("--- !!! WARNING: Hardcoded Tokens/Keys detected - Consider using environment variables !!! ---")
    print("--- !!! INFO: Using httpx with verify=True (default) for HTTPS calls. !!! ---")
    print("-" * 25)

    print("Loading saved data...")
    load_data()
    print(f"Loaded {len(valid_keys)} pending keys.")
    print(f"Loaded {len(activated_users)} activated users.")
    print(f"Loaded cooldowns: /tim={len(user_tim_cooldown)}, /fl={len(user_fl_cooldown)}, /getkey={len(user_getkey_cooldown)}")

    # Build Application
    # Increased timeouts slightly
    application = Application.builder().token(BOT_TOKEN).job_queue(JobQueue())\
        .pool_timeout(90).connect_timeout(45).read_timeout(60).build()

    # Schedule Jobs
    application.job_queue.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
    print(f"Scheduled data cleanup job running every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")

    # Register Handlers
    group_filter = filters.Chat(chat_id=ALLOWED_GROUP_ID)
    application.add_handler(CommandHandler("start", start_command, filters=filters.ChatType.PRIVATE | group_filter))
    application.add_handler(CommandHandler("getkey", getkey_command, filters=group_filter))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=group_filter))
    application.add_handler(CommandHandler("tim", tim_command, filters=group_filter)) # Uses old API Key
    application.add_handler(CommandHandler("fl", fl_command, filters=group_filter)) # Uses new API Base + Task

    # Handler for unknown commands in the allowed group
    async def unknown_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message and update.message.text and update.message.text.startswith('/'):
            known_commands = ['/start', '/tim', '/fl', '/getkey', '/nhapkey']
            cmd = update.message.text.split(' ')[0].split('@')[0]
            if cmd not in known_commands:
                logger.info(f"Unknown command '{update.message.text}' in group. Deleting.")
                await delete_user_message(update, context) # Delete the unknown command

    application.add_handler(MessageHandler(filters.COMMAND & group_filter, unknown_in_group), group=1) # group=1 to run after known command handlers

    # Start Bot
    print("Bot is starting polling...")
    try:
        # drop_pending_updates=True can help avoid issues after restarts
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an exception: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped: {e}", exc_info=True)
    finally:
        print("\nBot has stopped.")
        logger.info("Bot has stopped.")
        print("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")

if __name__ == "__main__":
    main()