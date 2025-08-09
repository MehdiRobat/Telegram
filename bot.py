# ======================= BoxUp_bot — Final bot.py =======================
# ویژگی‌ها:
# • عضویت اجباری (چند کانال) + دکمه "عضو شدم" (ورودی .env نرمال‌سازی می‌شود)
# • دیپ‌لینک start برای دانلود + شمارش دانلود (token)
# • آمار واقعی زیر پست کانالی: ⬇️ دانلود | ↗️ اشتراک | 👁 بازدید (رفرش)
# • ارسال فوری/زمان‌بندی یک پست کانالی (کاور + کپشن + دکمه دانلود)
# • فلو آپلود چندمرحله‌ای (عنوان→ژانر→سال→کاور→فایل‌ها)
# • پنل ادمین کامل (لیست/جست‌وجو/ویرایش/حذف/جابجایی/افزودن فایل)
# • خروجی CSV
# • حذف خودکار پیام‌های ارسالی به کاربر بعد از DELETE_DELAY ثانیه
# -----------------------------------------------------------------------
# بالای فایل کنار importها اضافه کن

import os, re, json, io, csv, asyncio, logging, unicodedata, string
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------------- 📜 Logging ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------- ⚙️ Env ----------------------
load_dotenv()

def _get_env_str(key: str, required=True, default=None):
    v = os.getenv(key, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"❌ مقدار {key} در فایل .env تنظیم نشده است.")
    return v

def _get_env_int(key: str, required=True, default=None):
    v = os.getenv(key, None if required else (str(default) if default is not None else None))
    if v is None:
        if required:
            raise RuntimeError(f"❌ مقدار {key} در فایل .env تنظیم نشده است.")
        return int(default) if default is not None else None
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"❌ مقدار {key} باید عدد باشد. مقدار فعلی: {v}")

def _norm_channel(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r'^(?:https?://)?t\.me/+', '', s, flags=re.IGNORECASE)
    s = s.split("?")[0].strip()
    s = s.strip("/").lstrip("@")
    s = re.sub(r"[^A-Za-z0-9_]", "", s)
    return s

print("🚀 در حال بارگذاری تنظیمات...")

API_ID        = _get_env_int("API_ID")
API_HASH      = _get_env_str("API_HASH")
BOT_TOKEN     = _get_env_str("BOT_TOKEN")
BOT_USERNAME  = _get_env_str("BOT_USERNAME")
MONGO_URI     = _get_env_str("MONGO_URI")
MONGO_DB_NAME = _get_env_str("MONGO_DB", required=False, default="BoxOfficeDB")

WELCOME_IMAGE = _get_env_str("WELCOME_IMAGE")
CONFIRM_IMAGE = _get_env_str("CONFIRM_IMAGE")
DELETE_DELAY  = _get_env_int("DELETE_DELAY", required=False, default=30)

_raw_required = _get_env_str("REQUIRED_CHANNELS")
REQUIRED_CHANNELS = [c for c in (_norm_channel(x) for x in _raw_required.split(",")) if c]

TARGET_CHANNELS   = {str(k): int(v) for k, v in json.loads(_get_env_str("TARGET_CHANNELS_JSON")).items()}

ADMIN_IDS = [int(x.strip()) for x in _get_env_str("ADMIN_IDS").split(",") if x.strip().isdigit()]
if not ADMIN_IDS:
    raise RuntimeError("❌ ADMIN_IDS خالی است.")
ADMIN_ID = ADMIN_IDS[0]

REACTIONS = [x.strip() for x in os.getenv("REACTIONS", "👍,❤️,💔,👎").split(",") if x.strip()]

print("✅ تنظیمات از محیط بارگذاری شد.")

# ---------------------- 🗄️ DB ----------------------
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    films_col        = db["films"]
    scheduled_posts  = db["scheduled_posts"]
    settings_col     = db["settings"]
    user_sources     = db["user_sources"]
    post_stats       = db["post_stats"]  # {film_id, channel_id, message_id, downloads, shares, views}
    print(f"✅ اتصال به MongoDB برقرار شد. DB = {MONGO_DB_NAME}")
except Exception as e:
    raise RuntimeError(f"❌ خطا در اتصال به MongoDB: {e}")

# ---------------------- 🤖 Bot ----------------------
bot = Client(
    "BoxUploader",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=ParseMode.HTML
)

# ---------------------- 🧠 State ----------------------
upload_data: dict[int, dict] = {}
schedule_data: dict[int, dict] = {}
admin_edit_state: dict[int, dict] = {}

# ---------------------- 🧰 Helpers ----------------------
def caption_to_buttons(caption: str):
    pattern = r'([^\n()]{1,}?)\s*\((https?://[^\s)]+)\)'
    matches = re.findall(pattern, caption)
    if not matches:
        return caption, None
    cleaned = caption
    buttons = []
    for label, url in matches:
        lbl = label.strip()
        if lbl:
            buttons.append(InlineKeyboardButton(lbl, url=url))
        cleaned = cleaned.replace(f"{label}({url})", "")
        cleaned = cleaned.replace(f"{label} ({url})", "")
    cleaned = re.sub(r'[ \t]+\n', '\n', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    if not cleaned:
        cleaned = caption
    kb = InlineKeyboardMarkup([[b] for b in buttons]) if buttons else None
    return cleaned, kb

def slugify(title: str) -> str:
    t = unicodedata.normalize("NFKD", title)
    allowed = string.ascii_letters + string.digits + " _-"
    t = "".join(ch for ch in t if ch in allowed)
    t = t.strip().replace(" ", "_")
    return (t.lower() or "title")[:64]

async def delete_after_delay(client: Client, chat_id: int, message_id: int):
    try:
        await asyncio.sleep(DELETE_DELAY)
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        logging.warning("delete_after_delay: %s", e)

async def user_is_member(client: Client, uid: int) -> bool:
    for channel in REQUIRED_CHANNELS:
        try:
            m = await client.get_chat_member(f"@{channel}", uid)
            if m.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return False
        except Exception:
            return False
    return True

def join_buttons_markup():
    rows = []
    for ch in REQUIRED_CHANNELS:
        if not ch or not re.match(r"^[A-Za-z0-9_]{5,}$", ch):
            continue
        rows.append([InlineKeyboardButton(f"📣 عضویت در @{ch}", url=f"https://t.me/{ch}")])
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="check_membership")])
    return InlineKeyboardMarkup(rows)

async def _safe_send_welcome(client: Client, chat_id: int):
    text = ("🎬 به ربات UpBox خوش آمدید!\n\n"
            "ابتدا لطفاً در کانال‌های زیر عضو شوید، سپس روی «✅ عضو شدم» بزنید:")
    try:
        await client.send_photo(chat_id, photo=WELCOME_IMAGE, caption=text, reply_markup=join_buttons_markup())
        return
    except Exception as e1:
        logging.warning("welcome with keyboard failed: %s", e1)
    try:
        await client.send_photo(chat_id, photo=WELCOME_IMAGE, caption=text)
        return
    except Exception as e2:
        logging.warning("welcome with photo failed: %s", e2)
    try:
        await client.send_message(chat_id, text)
    except Exception as e3:
        logging.error("WELCOME failed: %s", e3)

def _encode_channel_id(cid: int) -> str:
    return f"{'n' if cid < 0 else 'p'}{abs(cid)}"

def _decode_channel_id(s: str) -> int:
    sign = -1 if s and s[0] == 'n' else 1
    return sign * int(s[1:]) if s else 0

def build_stats_token(film_id: str, channel_id: int, message_id: int) -> str:
    return f"{film_id}__{_encode_channel_id(channel_id)}__m{message_id}__dl"

def parse_stats_token(token: str):
    try:
        parts = token.split("__")
        if len(parts) != 4 or parts[-1] != "dl":
            return None
        film_id = parts[0]
        cid = _decode_channel_id(parts[1])
        mid = int(parts[2][1:])
        return film_id, cid, mid
    except Exception:
        return None

def build_stats_keyboard(film_id: str, channel_id: int, message_id: int, downloads: int, shares: int, views: int):
    row1 = [InlineKeyboardButton("📥 دانلود", url=f"https://t.me/{BOT_USERNAME}?start={build_stats_token(film_id, channel_id, message_id)}")]
    row2 = [
        InlineKeyboardButton(f"⬇️ دانلود: {downloads}", callback_data="stat:noop"),
        InlineKeyboardButton(f"↗️ اشتراک: {shares}", callback_data="stat:share"),
        InlineKeyboardButton(f"👁 بازدید: {views}", callback_data="stat:noop"),
    ]
    row3 = [InlineKeyboardButton("🔄 بروزرسانی آمار", callback_data="stat:refresh")]
    return InlineKeyboardMarkup([row1, row2, row3])

async def ensure_reactions(client: Client, channel_id: int):
    try:
        if REACTIONS:
            await client.set_chat_available_reactions(chat_id=channel_id, available_reactions=REACTIONS)
    except Exception as e:
        logging.info("ensure_reactions skipped: %s", e)

def compose_channel_caption(film: dict) -> str:
    title = film.get("title", film.get("film_id", ""))
    genre = film.get("genre", "")
    year  = film.get("year", "")
    lines = [f"🎬 <b>{title}</b>"]
    if genre: lines.append(f"🎭 ژانر: {genre}")
    if year:  lines.append(f"📆 سال: {year}")
    lines.append("👇 برای دریافت، روی دکمه دانلود بزنید.")
    return "\n".join(lines)

# ---------------------- 🧪 Health ----------------------
@bot.on_message(filters.command("ping") & filters.private)
async def ping_handler(client, message):
    logging.info("PING from %s", message.from_user.id)
    await message.reply("pong ✅")

# ---------------------- 🚪 START + Membership + Downloads ----------------------
async def _send_film_files_to_user(client: Client, chat_id: int, film_doc: dict):
    files = film_doc.get("files", [])
    if not files:
        await client.send_message(chat_id, "❌ هیچ فایلی برای این فیلم ثبت نشده است.")
        return
    title = film_doc.get("title", film_doc.get("film_id", ""))
    for f in files:
        cap = f"🎬 {title}{' (' + f.get('quality','') + ')' if f.get('quality') else ''}\n\n{f.get('caption','')}"
        cleaned, kb = caption_to_buttons(cap)
        try:
            msg = await client.send_video(chat_id=chat_id, video=f["file_id"], caption=cleaned, reply_markup=kb)
            asyncio.create_task(delete_after_delay(client, msg.chat.id, msg.id))
        except Exception as e:
            await client.send_message(chat_id, f"❌ خطا در ارسال یک فایل: {e}")
    warn = await client.send_message(chat_id, "⚠️ فایل‌ها تا ۳۰ ثانیه دیگر حذف می‌شوند، لطفاً سریعاً ذخیره کنید.")
    asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))

@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    logging.info("/start from %s text=%r", message.from_user.id, message.text)
    user_id = message.from_user.id
    parts = (message.text or "").split(maxsplit=1)
    start_param = parts[1].strip() if len(parts) == 2 else None

    film_id_for_delivery: Optional[str] = None
    if start_param:
        parsed = parse_stats_token(start_param)
        if parsed:
            fid, cid, mid = parsed
            post_stats.update_one(
                {"film_id": fid, "channel_id": cid, "message_id": mid},
                {"$inc": {"downloads": 1}, "$setOnInsert": {"shares": 0, "views": 0, "created_at": datetime.now()}},
                upsert=True
            )
            film_id_for_delivery = fid
        else:
            film_id_for_delivery = start_param  # maybe direct film_id

    if film_id_for_delivery and await user_is_member(client, user_id):
        film = films_col.find_one({"film_id": film_id_for_delivery})
        if not film:
            await message.reply("❌ لینک فایل معتبر نیست یا فیلم پیدا نشد.")
            return
        await _send_film_files_to_user(client, message.chat.id, film)
        return

    if film_id_for_delivery:
        user_sources.update_one({"user_id": user_id}, {"$set": {"from_film_id": film_id_for_delivery}}, upsert=True)

    await _safe_send_welcome(client, message.chat.id)

@bot.on_callback_query(filters.regex(r"^check_membership$"))
async def check_membership_cb(client: Client, cq: CallbackQuery):
    user_id = cq.from_user.id
    # verify
    for ch in REQUIRED_CHANNELS:
        try:
            m = await client.get_chat_member(f"@{ch}", user_id)
            if m.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                await cq.answer("⛔️ هنوز در همه کانال‌ها عضو نشده‌ای!", show_alert=True)
                return
        except Exception:
            await cq.answer("⛔️ هنوز در همه کانال‌ها عضو نشده‌ای!", show_alert=True)
            return

    await cq.answer("✅ عضویتت تأیید شد!", show_alert=True)
    try:
        await client.send_photo(cq.message.chat.id, CONFIRM_IMAGE, caption="✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")
    except Exception:
        await client.send_message(cq.message.chat.id, "✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")

    src = user_sources.find_one({"user_id": user_id})
    fid = src.get("from_film_id") if src else None
    if fid:
        film = films_col.find_one({"film_id": fid})
        if not film:
            await client.send_message(cq.message.chat.id, "❌ لینک فیلم معتبر نیست یا اطلاعاتی یافت نشد.")
            user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
            return
        await _send_film_files_to_user(client, cq.message.chat.id, film)
        user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
    else:
        await client.send_message(cq.message.chat.id, "ℹ️ الان عضو شدی. برای دریافت محتوا، روی لینک داخل پست‌های کانال کلیک کن.")

# ---------------------- ⬆️ Upload flow ----------------------
@bot.on_message(filters.command("upload") & filters.private & filters.user(ADMIN_IDS))
async def upload_command(client: Client, message: Message):
    uid = message.from_user.id
    upload_data[uid] = {"step": "awaiting_title", "files": []}
    await message.reply("🎬 لطفاً <b>عنوان</b> را وارد کن (مثال: آواتار ۲).")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_text_router(client: Client, message: Message):
    uid = message.from_user.id

    # schedule flow
    if uid in schedule_data:
        data = schedule_data[uid]
        if data.get("step") == "date":
            data["date"] = message.text.strip()
            data["step"] = "time"
            return await message.reply("🕒 ساعت انتشار را وارد کن (HH:MM):")
        if data.get("step") == "time":
            data["time"] = message.text.strip()
            prefix = f"film_sched_save::{data['date']}::{data['time']}"
            rows = [[InlineKeyboardButton(title, callback_data=f"{prefix}::{data['film_id']}::{chat_id}")]
                    for title, chat_id in TARGET_CHANNELS.items()]
            rows.append([InlineKeyboardButton("❌ لغو", callback_data="sched_cancel")])
            data["step"] = "channel_await"
            return await message.reply("🎯 کانال مقصد را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))
        return

    # admin panel edit/search
    if uid in admin_edit_state:
        st = admin_edit_state[uid]
        mode = st.get("mode")
        fid = st.get("film_id")

        if mode == "search":
            q = message.text.strip()
            regs = {"$regex": q, "$options": "i"}
            films = list(films_col.find({"$or": [{"title": regs}, {"genre": regs}, {"year": regs}, {"film_id": regs}]}).sort("timestamp", -1))
            admin_edit_state.pop(uid, None)
            if not films:
                return await message.reply("❌ چیزی یافت نشد. /admin")
            rows = [[InlineKeyboardButton(f"{f.get('title', f['film_id'])} ({f.get('year','-')})", callback_data=f"film_open::{f['film_id']}")] for f in films[:50]]
            rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
            return await message.reply("🔎 نتایج جست‌وجو:", reply_markup=InlineKeyboardMarkup(rows))

        if not fid:
            admin_edit_state.pop(uid, None)
            return await message.reply("⚠️ کانتکست ویرایش از دست رفت. دوباره امتحان کن.")

        if mode == "edit_title":
            films_col.update_one({"film_id": fid}, {"$set": {"title": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ عنوان ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

        if mode == "edit_genre":
            films_col.update_one({"film_id": fid}, {"$set": {"genre": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ ژانر ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

        if mode == "edit_year":
            new_year = message.text.strip()
            if new_year and not new_year.isdigit():
                return await message.reply("⚠️ سال باید عدد باشد.")
            films_col.update_one({"film_id": fid}, {"$set": {"year": new_year}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ سال ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

        idx = st.get("file_index", 0)
        if mode == "file_edit_caption":
            films_col.update_one({"film_id": fid}, {"$set": {f"files.{idx}.caption": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کپشن فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
        if mode == "file_edit_quality":
            films_col.update_one({"film_id": fid}, {"$set": {f"files.{idx}.quality": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کیفیت فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

        if mode == "file_add_caption":
            st["tmp_caption"] = message.text.strip()
            st["mode"] = "file_add_quality"
            return await message.reply("📽 کیفیت فایل جدید را وارد کن (مثلاً 720p):")
        if mode == "file_add_quality":
            new_q = message.text.strip()
            if not st.get("tmp_file_id"):
                admin_edit_state.pop(uid, None)
                return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
            films_col.update_one({"film_id": fid}, {"$push": {"files": {
                "film_id": fid,
                "file_id": st["tmp_file_id"],
                "caption": st.get("tmp_caption", ""),
                "quality": new_q
            }}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جدید اضافه شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
        return

    # upload flow
    if uid in upload_data:
        data = upload_data[uid]
        step = data.get("step")

        if step == "awaiting_title":
            title = message.text.strip()
            if not title:
                return await message.reply("⚠️ عنوان خالیه! دوباره بفرست.")
            data["title"] = title
            base = slugify(title)
            candidate = base
            i = 2
            while films_col.find_one({"film_id": candidate}):
                candidate = f"{base}_{i}"; i += 1
            data["film_id"] = candidate
            data["step"] = "awaiting_genre"
            return await message.reply("🎭 <b>ژانر</b> را وارد کن (مثال: اکشن، درام):")

        if step == "awaiting_genre":
            data["genre"] = message.text.strip()
            data["step"] = "awaiting_year"
            return await message.reply("📅 <b>سال تولید</b> را وارد کن (مثال: 2023):")

        if step == "awaiting_year":
            year = message.text.strip()
            if year and not year.isdigit():
                return await message.reply("⚠️ سال باید عدد باشد.")
            data["year"] = year
            data["step"] = "awaiting_cover"
            return await message.reply("🖼 <b>کاور</b> را بفرست (عکس).")

        if step == "awaiting_caption":
            caption = message.text.strip()
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل را بفرست.")
            data["current_file"] = {"caption": caption}
            data["step"] = "awaiting_quality"
            return await message.reply("📽 کیفیت این فایل را وارد کن (مثال: 720p):")

        if step == "awaiting_quality":
            quality = message.text.strip()
            if not quality:
                return await message.reply("⚠️ کیفیت خالیه! دوباره بفرست.")
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل را بفرست.")
            data["files"].append({
                "film_id": data["film_id"],
                "file_id": data["pending_file_id"],
                "caption": data["current_file"]["caption"],
                "quality": quality
            })
            data.pop("pending_file_id", None)
            data.pop("current_file", None)
            data["step"] = "confirm_more_files"
            buttons = InlineKeyboardMarkup([[InlineKeyboardButton("✅ بله", callback_data="more_yes"),
                                             InlineKeyboardButton("❌ خیر", callback_data="more_no")]])
            return await message.reply(
                f"✅ فایل اضافه شد.\n🎬 عنوان: {data.get('title')}\n📽 کیفیت: {quality}\n\nآیا فایل دیگری داری؟",
                reply_markup=buttons
            )
        return

# media router (admin)
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_router(client: Client, message: Message):
    uid = message.from_user.id

    if uid in admin_edit_state:
        st = admin_edit_state[uid]
        mode = st.get("mode")
        fid  = st.get("film_id")

        if mode == "replace_cover":
            if not message.photo:
                return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
            films_col.update_one({"film_id": fid}, {"$set": {"cover_id": message.photo.file_id}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کاور جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

        if mode == "file_replace":
            if message.video:
                fid_new = message.video.file_id
            elif message.document:
                fid_new = message.document.file_id
            elif message.audio:
                fid_new = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            idx = st.get("file_index", 0)
            films_col.update_one({"film_id": fid}, {"$set": {f"files.{idx}.file_id": fid_new}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

        if mode == "file_add_pickfile":
            if message.video:
                fid_new = message.video.file_id
            elif message.document:
                fid_new = message.document.file_id
            elif message.audio:
                fid_new = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            st["tmp_file_id"] = fid_new
            st["mode"] = "file_add_caption"
            return await message.reply("📝 کپشن فایل جدید را وارد کن:")

    if uid in upload_data:
        data = upload_data[uid]
        step = data.get("step")

        if step == "awaiting_cover":
            if not message.photo:
                return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
            data["cover_id"] = message.photo.file_id
            data["step"] = "awaiting_first_file"
            return await message.reply("📤 کاور ثبت شد. حالا فایلِ اول را بفرست (ویدیو/سند/صوت).")

        if step in ("awaiting_first_file", "awaiting_next_file"):
            if message.video:
                file_id = message.video.file_id
            elif message.document:
                file_id = message.document.file_id
            elif message.audio:
                file_id = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قبول است. دوباره بفرست.")
            data["pending_file_id"] = file_id
            data["step"] = "awaiting_caption"
            return await message.reply("📝 کپشن این فایل را وارد کن:")
        return

# پایان/ادامه آپلود
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^more_"))
async def upload_more_files_cb(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = upload_data.get(uid)
    if not data:
        return await cq.answer("⚠️ اطلاعات آپلود پیدا نشد.", show_alert=True)

    if cq.data == "more_yes":
        await cq.answer()
        data["step"] = "awaiting_next_file"
        data.pop("pending_file_id", None)
        data.pop("current_file", None)
        return await cq.message.reply("📤 لطفاً فایل بعدی را بفرست.")

    if cq.data == "more_no":
        await cq.answer()
        film_id = data["film_id"]
        film_doc = {
            "film_id": film_id, "user_id": uid,
            "title": data.get("title"), "genre": data.get("genre", ""), "year": data.get("year", ""),
            "cover_id": data.get("cover_id"), "timestamp": datetime.now(), "files": data["files"]
        }
        films_col.update_one({"film_id": film_id}, {"$set": film_doc}, upsert=True)
        deep_link = f"https://t.me/{BOT_USERNAME}?start={film_id}"
        await cq.message.reply(
            f"✅ فیلم ذخیره شد.\n\n🎬 عنوان: {film_doc['title']}\n📂 تعداد فایل: {len(film_doc['files'])}\n🔗 لینک دانلود: {deep_link}"
        )
        await cq.message.reply(
            "🕓 انتخاب کن:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ زمان‌بندی", callback_data=f"sched_yes::{film_id}")],
                [InlineKeyboardButton("📣 ارسال فوری", callback_data=f"sched_no::{film_id}")]
            ])
        )
        upload_data.pop(uid, None)

# ---------------------- زمان‌بندی/انتشار فوری ----------------------
@bot.on_callback_query(filters.regex(r"^sched_yes::(.+)$") & filters.user(ADMIN_IDS))
async def ask_schedule_date(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    schedule_data[cq.from_user.id] = {"film_id": film_id, "step": "date"}
    await cq.message.reply("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^sched_cancel$") & filters.user(ADMIN_IDS))
async def sched_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("⛔️ زمان‌بندی لغو شد.")

@bot.on_callback_query(filters.regex(r"^sched_no::(.+)$") & filters.user(ADMIN_IDS))
async def ask_publish_immediate(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{film_id}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="pub_cancel")])
    await cq.message.reply("📣 می‌خوای همین الان ارسال کنیم؟ کانال رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^pub_cancel$") & filters.user(ADMIN_IDS))
async def pub_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer("لغو شد.")
    await cq.message.edit_text("🚫 ارسال فوری لغو شد.")

@bot.on_callback_query(filters.regex(r"^film_sched_save::(\d{4}-\d{2}-\d{2})::(\d{2}:\d{2})::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_sched_save_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    date_str, time_str, film_id, channel_id = cq.matches[0].groups()
    channel_id = int(channel_id)
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    except ValueError:
        return await cq.answer("❌ تاریخ/ساعت نامعتبر.", show_alert=True)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.answer("⚠️ فیلم پیدا نشد.", show_alert=True)
    scheduled_posts.insert_one({
        "film_id": film_id, "title": film.get("title",""),
        "channel_id": channel_id, "scheduled_time": dt
    })
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("✅ زمان‌بندی ذخیره شد.")

async def send_channel_post_with_stats(client: Client, film: dict, channel_id: int):
    caption = compose_channel_caption(film)
    await ensure_reactions(client, channel_id)

    if film.get("cover_id"):
        sent = await client.send_photo(channel_id, photo=film["cover_id"], caption=caption)
    else:
        sent = await client.send_message(channel_id, text=caption)

    post_stats.update_one(
        {"film_id": film["film_id"], "channel_id": channel_id, "message_id": sent.id},
        {"$setOnInsert": {"downloads": 0, "shares": 0, "views": int(getattr(sent, "views", 0) or 0), "created_at": datetime.now()}},
        upsert=True
    )
    s = post_stats.find_one({"film_id": film["film_id"], "channel_id": channel_id, "message_id": sent.id}) or {}
    kb = build_stats_keyboard(film["film_id"], channel_id, sent.id,
                              downloads=int(s.get("downloads",0)),
                              shares=int(s.get("shares",0)),
                              views=int(s.get("views",0)))
    try:
        await client.edit_message_reply_markup(chat_id=channel_id, message_id=sent.id, reply_markup=kb)
    except Exception as e:
        logging.info("edit_message_reply_markup: %s", e)

@bot.on_callback_query(filters.regex(r"^film_pub_go::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_pub_go_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id, channel_id = cq.data.split("::")[1:]
    channel_id = int(channel_id)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.")
    await send_channel_post_with_stats(client, film, channel_id)
    await cq.message.edit_text("✅ پست ارسال شد (کاور + دکمه دانلود + آمار).")

# ---------------------- آمار: دکمه‌ها ----------------------
@bot.on_callback_query(filters.regex(r"^stat:share$"))
async def stat_share_cb(client: Client, cq: CallbackQuery):
    ch_id = cq.message.chat.id
    msg_id = cq.message.id
    stat = post_stats.find_one({"channel_id": ch_id, "message_id": msg_id})
    if not stat:
        return await cq.answer("⏳ آمار در حال آماده‌سازی است.", show_alert=True)
    post_stats.update_one({"_id": stat["_id"]}, {"$inc": {"shares": 1}})
    s = post_stats.find_one({"_id": stat["_id"]})
    kb = build_stats_keyboard(s["film_id"], ch_id, msg_id,
                              downloads=int(s.get("downloads",0)),
                              shares=int(s.get("shares",0)),
                              views=int(s.get("views",0)))
    try:
        await client.edit_message_reply_markup(ch_id, msg_id, kb)
    except Exception as e:
        logging.info("share/edit kb: %s", e)
    try:
        chat = await client.get_chat(ch_id)
        if chat.username:
            await cq.answer(f"https://t.me/{chat.username}/{msg_id}", show_alert=True)
        else:
            await cq.answer("کانال خصوصی است؛ لینک مستقیم ندارد.", show_alert=True)
    except Exception:
        await cq.answer("ثبت شد ✅", show_alert=True)

@bot.on_callback_query(filters.regex(r"^stat:refresh$"))
async def stat_refresh_cb(client: Client, cq: CallbackQuery):
    ch_id = cq.message.chat.id
    msg_id = cq.message.id
    stat = post_stats.find_one({"channel_id": ch_id, "message_id": msg_id})
    if not stat:
        return await cq.answer("⏳ آمار هنوز ثبت نشده.", show_alert=True)
    try:
        msg = await client.get_messages(ch_id, msg_id)
        current_views = int(getattr(msg, "views", 0) or 0)
    except Exception:
        current_views = int(stat.get("views", 0))
    post_stats.update_one({"_id": stat["_id"]}, {"$set": {"views": current_views}})
    s = post_stats.find_one({"_id": stat["_id"]})
    kb = build_stats_keyboard(s["film_id"], ch_id, msg_id,
                              downloads=int(s.get("downloads",0)),
                              shares=int(s.get("shares",0)),
                              views=int(s.get("views",0)))
    try:
        await client.edit_message_reply_markup(ch_id, msg_id, kb)
    except Exception as e:
        logging.info("refresh/edit kb: %s", e)
    await cq.answer("آمار به‌روزرسانی شد ✅", show_alert=False)

@bot.on_callback_query(filters.regex(r"^stat:noop$"))
async def stat_noop_cb(client: Client, cq: CallbackQuery):
    await cq.answer()

# ---------------------- پنل ادمین (لیست/جست‌وجو/ویرایش و...) ----------------------
def kb_admin_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 لیست/جست‌وجوی فیلم‌ها", callback_data="admin_films_1")],
        [InlineKeyboardButton("⏰ مدیریت زمان‌بندی‌ها", callback_data="admin_sched_list_1")],
        [InlineKeyboardButton("📥 خروجی CSV", callback_data="admin_export_csv")],
    ])

def _fmt_film_info(film: dict) -> str:
    return (f"🎬 <b>{film.get('title','-')}</b>\n"
            f"🎭 ژانر: {film.get('genre','-')}\n"
            f"📆 سال: {film.get('year','-')}\n"
            f"🖼 کاور: {'✅' if film.get('cover_id') else '❌'}\n"
            f"📂 تعداد فایل: {len(film.get('files', []))}\n"
            f"🆔 {film.get('film_id','-')}")

def _paginate(items, page, page_size=10):
    total = len(items); start = (page-1)*page_size; end = start+page_size
    return items[start:end], total

@bot.on_message(filters.command("admin") & filters.user(ADMIN_IDS))
async def admin_entry(client: Client, message: Message):
    await message.reply("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_home$") & filters.user(ADMIN_IDS))
async def admin_home_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    await cq.message.edit_text("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_films_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_films_list(client: Client, cq: CallbackQuery):
    await cq.answer()
    page = int(cq.matches[0].group(1))
    films = list(films_col.find().sort("timestamp", -1))
    page_items, total = _paginate(films, page, 10)
    if not page_items and page > 1:
        return await cq.message.edit_text("⛔️ صفحه خالی است.", reply_markup=kb_admin_main())
    rows = []
    for f in page_items:
        title = f.get("title") or f.get("film_id"); year = f.get("year", "")
        rows.append([InlineKeyboardButton(f"{title} {f'({year})' if year else ''}", callback_data=f"film_open::{f['film_id']}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_films_{page-1}"))
    if page*10 < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_films_{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("🔎 جست‌وجو", callback_data="admin_search")])
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("🎬 لیست فیلم‌ها:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^admin_search$") & filters.user(ADMIN_IDS))
async def admin_search_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    admin_edit_state[cq.from_user.id] = {"mode": "search"}
    await cq.message.edit_text("🔎 عبارت جست‌وجو را بفرست (عنوان/ژانر/سال/film_id)...")

@bot.on_callback_query(filters.regex(r"^film_open::(.+)$") & filters.user(ADMIN_IDS))
async def film_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    info = _fmt_film_info(film)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ ویرایش عنوان", callback_data=f"film_edit_title::{fid}")],
        [InlineKeyboardButton("🎭 ویرایش ژانر",  callback_data=f"film_edit_genre::{fid}")],
        [InlineKeyboardButton("📆 ویرایش سال",   callback_data=f"film_edit_year::{fid}")],
        [InlineKeyboardButton("🖼 جایگزینی کاور", callback_data=f"film_replace_cover::{fid}")],
        [InlineKeyboardButton("📂 مدیریت فایل‌ها", callback_data=f"film_files::{fid}")],
        [InlineKeyboardButton("📣 ارسال فوری", callback_data=f"film_pub_pick::{fid}")],
        [InlineKeyboardButton("⏰ زمان‌بندی ارسال", callback_data=f"film_sched_start::{fid}")],
        [InlineKeyboardButton("🗑 حذف فیلم", callback_data=f"film_delete_confirm::{fid}")],
        [InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")]
    ])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^film_edit_title::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_title_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_title", "film_id": fid}
    await cq.message.edit_text("🖊 عنوان جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_genre::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_genre_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_genre", "film_id": fid}
    await cq.message.edit_text("🎭 ژانر جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_year::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_year_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_year", "film_id": fid}
    await cq.message.edit_text("📆 سال جدید را بفرست (مثلاً 2024):")

@bot.on_callback_query(filters.regex(r"^film_replace_cover::(.+)$") & filters.user(ADMIN_IDS))
async def film_replace_cover_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "replace_cover", "film_id": fid}
    await cq.message.edit_text("🖼 عکس کاور جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_files::(.+)$") & filters.user(ADMIN_IDS))
async def film_files_list(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    rows = [[InlineKeyboardButton(f"#{i+1} • کیفیت: {f.get('quality','-')}", callback_data=f"film_file_open::{fid}::{i}")] for i, f in enumerate(files)]
    rows.append([InlineKeyboardButton("➕ افزودن فایل جدید", callback_data=f"film_file_add::{fid}")])
    rows.append([InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")])
    await cq.message.edit_text("📂 فایل‌ها:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^film_file_open::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def film_file_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    f = files[idx]
    cap = f.get("caption", ""); q = f.get("quality", "")
    info = f"📄 <b>فایل #{idx+1}</b>\n🎞 کیفیت: {q}\n📝 کپشن:\n{cap[:800] + ('…' if len(cap) > 800 else '')}"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ ویرایش کپشن", callback_data=f"file_edit_caption::{fid}::{idx}")],
        [InlineKeyboardButton("🎞 ویرایش کیفیت", callback_data=f"file_edit_quality::{fid}::{idx}")],
        [InlineKeyboardButton("🔁 جایگزینی فایل", callback_data=f"file_replace::{fid}::{idx}")],
        [InlineKeyboardButton("🔼 بالا", callback_data=f"file_move_up::{fid}::{idx}"),
         InlineKeyboardButton("🔽 پایین", callback_data=f"file_move_down::{fid}::{idx}")],
        [InlineKeyboardButton("🗑 حذف فایل", callback_data=f"file_delete_confirm::{fid}::{idx}")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]
    ])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_edit_caption::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_caption_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_caption", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📝 کپشن جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^file_edit_quality::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_quality_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_quality", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("🎞 کیفیت جدید را بفرست (مثلاً 1080p):")

@bot.on_callback_query(filters.regex(r"^file_replace::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_replace_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_replace", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست تا جایگزین شود:")

@bot.on_callback_query(filters.regex(r"^file_move_up::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_up_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return
    files = film.get("files", [])
    if idx <= 0 or idx >= len(files):
        return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx-1], files[idx] = files[idx], files[idx-1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_move_down::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_down_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return
    files = film.get("files", [])
    if idx < 0 or idx >= len(files)-1:
        return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx+1], files[idx] = files[idx], files[idx+1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_delete_confirm::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_confirm_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_file_open::{fid}::{idx}")],
        [InlineKeyboardButton("🗑 حذف", callback_data=f"file_delete::{fid}::{idx}")]
    ])
    await cq.message.edit_text("❗️ مطمئنی این فایل حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_delete::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_do_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    files.pop(idx)
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ فایل حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^film_file_add::(.+)$") & filters.user(ADMIN_IDS))
async def film_file_add_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "file_add_pickfile", "film_id": fid}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_delete_confirm::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_confirm_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_open::{fid}")],
        [InlineKeyboardButton("🗑 حذف قطعی", callback_data=f"film_delete::{fid}")]
    ])
    await cq.message.edit_text("❗️ مطمئنی کل فیلم و فایل‌ها حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^film_delete::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_do_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    films_col.delete_one({"film_id": fid})
    await cq.message.edit_text("✅ فیلم حذف شد.", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^film_pub_pick::(.+)$") & filters.user(ADMIN_IDS))
async def film_pub_pick_channel(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{fid}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    await cq.message.edit_text(
        "📣 کانال مقصد برای انتشار فوری را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(rows + [[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]])
    )

@bot.on_callback_query(filters.regex(r"^film_sched_start::(.+)$") & filters.user(ADMIN_IDS))
async def film_sched_start_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    schedule_data[cq.from_user.id] = {"film_id": fid, "step": "date"}
    await cq.message.edit_text("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^admin_sched_list_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_sched_list_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    page = int(cq.matches[0].group(1))
    posts = list(scheduled_posts.find().sort("scheduled_time", 1))
    page_items, total = _paginate(posts, page, 10)
    if not page_items and page > 1:
        return await cq.message.edit_text("⛔️ صفحه خالی است.", reply_markup=kb_admin_main())
    rows = []
    for p in page_items:
        dt = p["scheduled_time"].strftime("%Y-%m-%d %H:%M")
        rows.append([InlineKeyboardButton(f"{p.get('title','(بدون عنوان)')} • {dt}", callback_data=f"sched_open::{str(p['_id'])}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_sched_list_{page-1}"))
    if page*10 < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_sched_list_{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("⏰ زمان‌بندی‌های ثبت‌شده:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^sched_open::(.+)$") & filters.user(ADMIN_IDS))
async def sched_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    sid = cq.matches[0].group(1)
    try:
        post = scheduled_posts.find_one({"_id": ObjectId(sid)})
    except Exception:
        post = None
    if not post:
        return await cq.message.edit_text("❌ برنامه زمان‌بندی یافت نشد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))
    dt = post["scheduled_time"].strftime("%Y-%m-%d %H:%M")
    info = (f"🆔 {sid}\n🎬 {post.get('title','(بدون عنوان)')}\n📅 {dt}\n📡 کانال: {post.get('channel_id')}\n🎞 فیلم: {post.get('film_id')}")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 حذف از صف", callback_data=f"sched_delete::{sid}")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]
    ])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^sched_delete::(.+)$") & filters.user(ADMIN_IDS))
async def sched_delete_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    sid = cq.matches[0].group(1)
    try:
        scheduled_posts.delete_one({"_id": ObjectId(sid)})
        await cq.message.edit_text("✅ حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))
    except Exception as e:
        await cq.message.edit_text(f"❌ خطا در حذف: {e}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))

@bot.on_callback_query(filters.regex(r"^admin_export_csv$") & filters.user(ADMIN_IDS))
async def admin_export_csv_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    films = list(films_col.find().sort("timestamp", -1))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["film_id", "title", "genre", "year", "files_count", "timestamp"])
    for f in films:
        w.writerow([
            f.get("film_id", ""),
            (f.get("title", "") or "").replace("\n", " "),
            (f.get("genre", "") or "").replace("\n", " "),
            f.get("year", ""),
            len(f.get("files", [])),
            f.get("timestamp", "")
        ])
    buf.seek(0)
    bio = io.BytesIO(buf.getvalue().encode("utf-8"))
    bio.name = "films_export.csv"
    await client.send_document(cq.message.chat.id, document=bio, caption="📥 خروجی CSV فیلم‌ها")

# ---------------------- Scheduler (send queued posts) ----------------------
async def send_scheduled_posts():
    try:
        now = datetime.now()
        posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now}}))
    except Exception as e:
        logging.error("DB unavailable: %s", e)
        return
    for post in posts:
        try:
            film = films_col.find_one({"film_id": post["film_id"]})
            if not film:
                scheduled_posts.delete_one({"_id": post["_id"]})
                continue
            await send_channel_post_with_stats(bot, film, post["channel_id"])
            scheduled_posts.delete_one({"_id": post["_id"]})
        except Exception as e:
            logging.error("❌ scheduled send error: %s", e)
            scheduled_posts.delete_one({"_id": post["_id"]})

# ---------------------- 🚀 Run ----------------------
async def _main():
    await bot.start()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_scheduled_posts, "interval", minutes=1)
    scheduler.start()
    print("🤖 ربات با موفقیت راه‌اندازی شد و منتظر دستورات است...")
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(_main())
