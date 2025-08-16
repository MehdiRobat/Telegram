# ======================= BoxUp_bot — Ultimate bot.py =======================
# نسخه‌ی کامل با یوزربات + انتشار خودکار از کانال‌های منبع + مدیریت کامل
# تمام بخش‌ها کامنت فارسی دارد تا بدانید هر خط چه می‌کند.

import os, re, json, asyncio, io, csv, unicodedata, string, pathlib, traceback
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv                            # خواندن متغیرهای .env
from zoneinfo import ZoneInfo                             # تبدیل دقیق تایم‌زون
from pyrogram import Client, filters, idle               # هسته Pyrogram (Bot/UserBot)
from pyrogram.enums import ChatMemberStatus              # برای چک عضویت اجباری
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient                           # اتصال به MongoDB
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler # زمان‌بندی کارها

# ---------------------- ⚙️ بارگذاری تنظیمات از .env ----------------------
print("🚀 Loading .env ...")
load_dotenv()  # همه متغیرها را در محیط برنامه ست می‌کند

# توابع کمکی امن برای خواندن .env
def _get_env_str(key: str, required=True, default=None):
    v = os.getenv(key, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"❌ {key} not set in .env")
    return v

def _get_env_int(key: str, required=True, default=None):
    v = os.getenv(key, None if required else (str(default) if default is not None else None))
    if v is None:
        if required:
            raise RuntimeError(f"❌ {key} not set in .env")
        return int(default) if default is not None else None
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"❌ {key} must be int. Got: {v}")

# منطقه‌ی زمانی پیش‌فرض (مثلاً Europe/Berlin)
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")

# مسیرهای ذخیره‌سازی (جلسات Pyrogram، خروجی‌ها، …)
DATA_DIR    = os.getenv("DATA_DIR", "./data")
SESSION_DIR = os.getenv("SESSION_DIR", os.path.join(DATA_DIR, "pyro_sessions"))
EXPORTS_DIR = os.getenv("EXPORTS_DIR", os.path.join(DATA_DIR, "exports"))
for p in (DATA_DIR, SESSION_DIR, EXPORTS_DIR):
    os.makedirs(p, exist_ok=True)  # اگر نبود بساز

# اطلاعات اتصال و نمایش
API_ID        = _get_env_int("API_ID")
API_HASH      = _get_env_str("API_HASH")
BOT_TOKEN     = _get_env_str("BOT_TOKEN")
BOT_USERNAME  = _get_env_str("BOT_USERNAME")
MONGO_URI     = _get_env_str("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB", "BoxOfficeDB")
WELCOME_IMAGE = _get_env_str("WELCOME_IMAGE")
CONFIRM_IMAGE = _get_env_str("CONFIRM_IMAGE")
DELETE_DELAY  = _get_env_int("DELETE_DELAY", required=False, default=30)

# ادمین‌ها و کانال‌های اجباری برای عضویت
ADMIN_IDS = [int(x.strip()) for x in _get_env_str("ADMIN_IDS").split(",") if x.strip().isdigit()]
ADMIN_ID = ADMIN_IDS[0]
REQUIRED_CHANNELS = [x.strip().lstrip("@") for x in _get_env_str("REQUIRED_CHANNELS").split(",") if x.strip()]

# مقصدها (کلیدها دلخواه: films/series/animation/… → آیدی کانال)
TARGET_CHANNELS = {str(k): int(v) for k, v in json.loads(_get_env_str("TARGET_CHANNELS_JSON")).items()}

# ---------------------- ⚡️ تنظیمات یوزربات (UserBot) ----------------------
USER_SESSION_STRING = _get_env_str("USER_SESSION_STRING") # سشن اکانت شخصی
SOURCE_CHANNELS = [x.strip().lstrip("@") for x in os.getenv("SOURCE_CHANNELS", "").split(",") if x.strip()]  # کانال‌های منبع
SOURCE_MAP = json.loads(os.getenv("SOURCE_MAP_JSON", "{}"))  # مپ مستقیم @src → chat_id مقصد
AUTO_PUBLISH = os.getenv("AUTO_PUBLISH_FROM_SOURCES", "false").lower() == "true"  # ارسال خودکار یا Pending؟

# ---------------------- 🗄️ اتصال به MongoDB ----------------------
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB_NAME]
films_col        = db["films"]          # اطلاعات فیلم/سریال (title/genre/year/cover/files/…)
scheduled_posts  = db["scheduled_posts"]# صف ارسال زمان‌بندی‌شده
user_sources     = db["user_sources"]   # نگهداری منبع کاربر برای DeepLink
stats_col        = db["stats"]          # آمار (downloads/shares/reactions/…)
post_refs        = db["post_refs"]      # نگاشت film_id ↔ (channel_id,message_id)
pending_posts    = db["pending_posts"]  # موارد Pending برای دسته‌بندی دستی
reactions_col    = db["reactions"]      # واکنش کاربر به فیلم (یک واکنش در هر فیلم)

# ---------------------- 🤖 ساخت کلاینت Bot و UserBot ----------------------
bot = Client(
    "BoxUploader",                        # نام سشن Bot
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    workdir=SESSION_DIR
)
user = Client(
    "UserForwarder",                      # نام سشن User
    api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION_STRING,
    workdir=SESSION_DIR
)

# ---------------------- 🧰 ابزارها و توابع کمکی ----------------------
def slugify(title: str) -> str:
    """ساخت شناسه‌ی تمیز برای film_id از روی عنوان"""
    t = unicodedata.normalize("NFKD", title)
    allowed = string.ascii_letters + string.digits + " _-"
    t = "".join(ch for ch in t if ch in allowed)
    t = t.strip().replace(" ", "_")
    return (t.lower() or "title")[:64]

def caption_to_buttons(caption: str):
    """تبدیل الگوی 'متن (URL)' داخل کپشن به دکمه‌های زیر پیام + پاکسازی کپشن"""
    pattern = r'([^\n()]{1,}?)\s*\((https?://[^\s)]+)\)'
    matches = re.findall(pattern, caption or "")
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

def compose_channel_caption(film: dict) -> str:
    """ساخت کپشن برای پست کانالی (کاور + توضیح کوتاه + راهنمای دانلود)"""
    title = film.get("title", film.get("film_id", ""))
    genre = film.get("genre", "")
    year  = film.get("year", "")
    lines = [f"🎬 <b>{title}</b>"]
    if genre: lines.append(f"🎭 ژانر: {genre}")
    if year:  lines.append(f"📆 سال: {year}")
    lines.append("👇 برای دریافت، روی دکمه دانلود بزنید.")
    return "\n".join(lines)

def _stats_keyboard(film_id: str, channel_id: int, message_id: int, views=0):
    """کیبورد آمار (👁/📥/🔁) + دکمه دانلود (DeepLink)؛ بدون Reactions"""
    st = stats_col.find_one({"film_id": film_id}) or {}
    dl = int(st.get("downloads", 0))
    sh = int(st.get("shares", 0))
    v  = int(views or 0)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 دانلود", url=f"https://t.me/{BOT_USERNAME}?start={film_id}")],
        [
            InlineKeyboardButton(f"👁 {v}",  callback_data=f"sr::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"📥 {dl}", callback_data=f"sr::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"🔁 {sh}", callback_data=f"ss::{channel_id}::{message_id}")
        ]
    ])

def _reaction_keyboard(film_id: str, channel_id: int, message_id: int, views=0):
    """کیبورد Reactions (❤️ 👍 👎 😢) + آمار"""
    st = stats_col.find_one({"film_id": film_id}) or {}
    rec = st.get("reactions", {})
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"❤️ {rec.get('love',0)}", callback_data=f"react::love::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"👍 {rec.get('like',0)}", callback_data=f"react::like::{channel_id}::{message_id}")
        ],
        [
            InlineKeyboardButton(f"👎 {rec.get('dislike',0)}", callback_data=f"react::dislike::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"😢 {rec.get('sad',0)}", callback_data=f"react::sad::{channel_id}::{message_id}")
        ],
        [
            InlineKeyboardButton(f"👁 {views}", callback_data=f"sr::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"📥 {st.get('downloads',0)}", callback_data=f"sr::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"🔁 {st.get('shares',0)}", callback_data=f"ss::{channel_id}::{message_id}")
        ]
    ])

async def delete_after_delay(client: Client, chat_id: int, message_id: int):
    """حذف پیام‌های موقت بعد از N ثانیه"""
    try:
        await asyncio.sleep(DELETE_DELAY)
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        print("⚠️ delete_after_delay:", e)

async def user_is_member(client: Client, uid: int) -> bool:
    """بررسی عضویت کاربر در تمام کانال‌های اجباری"""
    for channel in REQUIRED_CHANNELS:
        try:
            m = await client.get_chat_member(f"@{channel}", uid)
            if m.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return False
        except Exception:
            return False
    return True

def join_buttons_markup():
    """ساخت کیبورد عضویت اجباری + دکمه «عضو شدم»"""
    rows = []
    for ch in REQUIRED_CHANNELS:
        title = ch.lstrip("@")
        rows.append([InlineKeyboardButton(f"📣 عضویت در @{title}", url=f"https://t.me/{title}")])
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="check_membership")])
    return InlineKeyboardMarkup(rows)

def detect_category_by_text(caption: str) -> str | None:
    """تشخیص مقصد از روی کلیدواژه‌ها: سریال/انیمیشن/فیلم"""
    txt = (caption or "").lower()
    if any(k in txt for k in ["قسمت", "episode", "فصل"]):
        return TARGET_CHANNELS.get("series")
    if "انیمیشن" in txt or "animation" in txt or "cartoon" in txt:
        return TARGET_CHANNELS.get("animation")
    if "فیلم" in txt or "movie" in txt or "سینمایی" in txt:
        return TARGET_CHANNELS.get("films")
    return None

def format_source_footer(caption: str, source_username: str) -> str:
    """افزودن امضای منبع + کانال خودت به کپشنِ ورودی"""
    footer = "\n\n━━━━━━━━━━━\n"
    footer += f"📢 منبع: @{source_username}\n"
    footer += f"🔗 @BoxOfficeMoviiie"
    return (caption or "") + footer

def log_source_entry(entry: dict):
    """نوشتن لاگِ ورود پست از کانال منبع داخل فایل JSON"""
    exports = pathlib.Path(EXPORTS_DIR) / "sources.json"
    old = []
    if exports.exists():
        old = json.loads(exports.read_text(encoding="utf-8"))
    old.append(entry)
    exports.write_text(json.dumps(old, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------------------- 🧠 Stateهای فرایندهای چندمرحله‌ای ----------------------
upload_data: dict[int, dict] = {}        # وضعیت آپلود دستی ادمین
schedule_data: dict[int, dict] = {}      # وضعیت زمان‌بندی ادمین
admin_edit_state: dict[int, dict] = {}   # وضعیت ویرایش در پنل

# ---------------------- 🚪 /start + عضویت اجباری + DeepLink ----------------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    """ورود کاربر؛ اگر start=film_id باشد و عضو باشد → فایل‌ها ارسال می‌شود"""
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    film_id = parts[1].strip() if len(parts) == 2 else None

    if film_id and await user_is_member(client, user_id):
        try:
            stats_col.update_one({"film_id": film_id}, {"$inc": {"downloads": 1}}, upsert=True)  # شمارش دانلود
        except Exception:
            pass
        film = films_col.find_one({"film_id": film_id})
        if not film:
            return await message.reply("❌ لینک فایل معتبر نیست یا فیلم پیدا نشد.")
        # ارسال همه فایل‌های فیلم به کاربر
        for f in film.get("files", []):
            cap = f"🎬 {film.get('title', film_id)}"
            if f.get("quality"): cap += f" ({f['quality']})"
            cap += "\n\n" + (f.get("caption","") or "")
            cleaned, kb = caption_to_buttons(cap)
            try:
                if kb:
                    m = await client.send_video(message.chat.id, f["file_id"], caption=cleaned, reply_markup=kb)
                else:
                    m = await client.send_video(message.chat.id, f["file_id"], caption=cleaned)
                asyncio.create_task(delete_after_delay(client, m.chat.id, m.id))
            except Exception as e:
                await client.send_message(message.chat.id, f"⚠️ خطا در ارسال یک فایل: {e}")
        warn = await client.send_message(message.chat.id, "⚠️ فایل‌ها تا ۳۰ ثانیه دیگر حذف می‌شوند، ذخیره کنید.")
        asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))
        return

    # اگر start داشت ولی عضو نبود → منبع را نگه می‌داریم تا بعد از عضویت فایل‌ها بدهیم
    if film_id:
        user_sources.update_one({"user_id": user_id}, {"$set": {"from_film_id": film_id}}, upsert=True)

    # ارسال پیام خوش‌آمد + کیبورد عضویت
    try:
        await message.reply_photo(
            WELCOME_IMAGE,
            caption="🎬 به ربات UpBox خوش آمدی!\n\nابتدا در کانال‌های زیر عضو شو و روی «✅ عضو شدم» بزن:",
            reply_markup=join_buttons_markup()
        )
    except Exception:
        await message.reply(
            "🎬 به ربات UpBox خوش آمدی!\n\nابتدا در کانال‌های زیر عضو شو و روی «✅ عضو شدم» بزن:",
            reply_markup=join_buttons_markup()
        )

@bot.on_callback_query(filters.regex(r"^check_membership$"))
async def check_membership_cb(client: Client, cq: CallbackQuery):
    """دکمه «عضو شدم»؛ اگر همه کانال‌ها عضو بود → فایل‌های DeepLink را بده"""
    user_id = cq.from_user.id
    missing = []
    for ch in REQUIRED_CHANNELS:
        try:
            m = await client.get_chat_member(f"@{ch}", user_id)
            if m.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    if missing:
        return await cq.answer("⛔️ هنوز در همه کانال‌ها عضو نیستی!", show_alert=True)

    await cq.answer("✅ عضویت تایید شد!", show_alert=True)
    try:
        await client.send_photo(cq.message.chat.id, CONFIRM_IMAGE, caption="✅ عضویت تایید شد. در حال بررسی…")
    except Exception:
        await client.send_message(cq.message.chat.id, "✅ عضویت تایید شد. در حال بررسی…")

    # اگر از start آمده بود، فایل‌ها را بده
    src = user_sources.find_one({"user_id": user_id})
    film_id = src.get("from_film_id") if src else None
    if film_id:
        try:
            stats_col.update_one({"film_id": film_id}, {"$inc": {"downloads": 1}}, upsert=True)
        except Exception:
            pass
        film = films_col.find_one({"film_id": film_id})
        if not film:
            await client.send_message(cq.message.chat.id, "❌ لینک فیلم معتبر نیست یا اطلاعاتی یافت نشد.")
            user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
            return
        for f in film.get("files", []):
            cap = f"🎬 {film.get('title', film_id)}"
            if f.get("quality"): cap += f" ({f['quality']})"
            cap += "\n\n" + (f.get("caption","") or "")
            cleaned, kb = caption_to_buttons(cap)
            try:
                if kb:
                    m = await client.send_video(cq.message.chat.id, f["file_id"], caption=cleaned, reply_markup=kb)
                else:
                    m = await client.send_video(cq.message.chat.id, f["file_id"], caption=cleaned)
                asyncio.create_task(delete_after_delay(client, m.chat.id, m.id))
            except Exception:
                pass
        user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
    else:
        await client.send_message(cq.message.chat.id, "ℹ️ الان عضو شدی. برای دریافت محتوا، روی لینک داخل پست‌های کانال کلیک کن.")

# ---------------------- ⬆️ آپلود چندمرحله‌ای برای ادمین ----------------------
@bot.on_message(filters.command("upload") & filters.private & filters.user(ADMIN_IDS))
async def upload_command(client: Client, message: Message):
    """شروع آپلود دستی؛ مرحله به مرحله اطلاعات فیلم را می‌گیرد"""
    uid = message.from_user.id
    upload_data[uid] = {"step": "awaiting_title", "files": []}
    await message.reply("🎬 لطفاً عنوان را بفرست (مثال: آواتار ۲).")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_text_router(client: Client, message: Message):
    """مسیر‌دهی پیام‌های متنی ادمین (آپلود/زمان‌بندی/ویرایش/جست‌وجو)"""
    uid = message.from_user.id

    # --- حالت زمان‌بندی: دریافت تاریخ/ساعت و انتخاب کانال ---
    if uid in schedule_data:
        st = schedule_data[uid]
        if st.get("step") == "date":
            st["date"] = message.text.strip()
            st["step"] = "time"
            return await message.reply("🕒 ساعت را وارد کن (HH:MM):")
        if st.get("step") == "time":
            st["time"] = message.text.strip()
            st["step"] = "channel_await"
            rows = [[InlineKeyboardButton(title, callback_data=f"sched_pick::{chat_id}")]
                    for title, chat_id in TARGET_CHANNELS.items()]
            rows.append([InlineKeyboardButton("❌ لغو", callback_data="sched_cancel")])
            return await message.reply("🎯 کانال مقصد را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))
        return

    # --- حالت‌های پنل ادمین (جست‌وجو/ویرایش) ---
    if uid in admin_edit_state:
        st = admin_edit_state[uid]
        mode = st.get("mode")
        film_id = st.get("film_id")

        # جست‌وجو
        if mode == "search":
            q = message.text.strip()
            regs = {"$regex": q, "$options": "i"}
            films = list(films_col.find({"$or": [
                {"title": regs}, {"genre": regs}, {"year": regs}, {"film_id": regs}
            ]}).sort("timestamp", -1))
            admin_edit_state.pop(uid, None)
            if not films:
                return await message.reply("❌ چیزی پیدا نشد. /admin")
            rows = [[InlineKeyboardButton(f"{f.get('title', f['film_id'])} ({f.get('year','-')})", callback_data=f"film_open::{f['film_id']}")] for f in films[:50]]
            rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
            return await message.reply("🔎 نتایج:", reply_markup=InlineKeyboardMarkup(rows))

        # اگر فیلم مشخص نیست، کانتکست از بین رفته
        if not film_id:
            admin_edit_state.pop(uid, None)
            return await message.reply("⚠️ کانتکست از دست رفت. دوباره تلاش کن.")

        # ویرایش عنوان/ژانر/سال
        if mode == "edit_title":
            films_col.update_one({"film_id": film_id}, {"$set": {"title": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ عنوان ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{film_id}")]]))
        if mode == "edit_genre":
            films_col.update_one({"film_id": film_id}, {"$set": {"genre": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ ژانر ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{film_id}")]]))
        if mode == "edit_year":
            new_year = message.text.strip()
            if new_year and not new_year.isdigit():
                return await message.reply("⚠️ سال باید عدد باشد.")
            films_col.update_one({"film_id": film_id}, {"$set": {"year": new_year}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ سال ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{film_id}")]]))

        # ویرایش فایل‌های فیلم
        idx = st.get("file_index", 0)
        if mode == "file_edit_caption":
            films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.caption": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کپشن فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))
        if mode == "file_edit_quality":
            films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.quality": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کیفیت فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

        # افزودن فایل جدید (مرحله کپشن → کیفیت)
        if mode == "file_add_caption":
            st["tmp_caption"] = message.text.strip()
            st["mode"] = "file_add_quality"
            return await message.reply("📽 کیفیت فایل جدید را بفرست (مثل 720p):")
        if mode == "file_add_quality":
            new_q = message.text.strip()
            if not st.get("tmp_file_id"):
                admin_edit_state.pop(uid, None)
                return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
            films_col.update_one({"film_id": film_id}, {"$push": {"files": {
                "film_id": film_id, "file_id": st["tmp_file_id"],
                "caption": st.get("tmp_caption", ""), "quality": new_q
            }}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جدید اضافه شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))
        return

    # --- فلو آپلود دستی ---
    if uid in upload_data:
        data = upload_data[uid]
        step = data.get("step")

        if step == "awaiting_title":
            title = message.text.strip()
            if not title:
                return await message.reply("⚠️ عنوان خالیه! دوباره بفرست.")
            data["title"] = title
            base = slugify(title)
            candidate, i = base, 2
            while films_col.find_one({"film_id": candidate}):
                candidate = f"{base}_{i}"; i += 1
            data["film_id"] = candidate
            data["step"] = "awaiting_genre"
            return await message.reply("🎭 ژانر را بفرست:")

        if step == "awaiting_genre":
            data["genre"] = message.text.strip()
            data["step"] = "awaiting_year"
            return await message.reply("📆 سال تولید را بفرست (مثال: 2023):")

        if step == "awaiting_year":
            year = message.text.strip()
            if year and not year.isdigit():
                return await message.reply("⚠️ سال باید عدد باشد.")
            data["year"] = year
            if data.get("cover_id"):
                data["step"] = "awaiting_first_file"
                return await message.reply("🗂 حالا فایل اول را بفرست (ویدیو/سند/صوت).")
            else:
                data["step"] = "awaiting_cover"
                return await message.reply("🖼 کاور را بفرست (یک‌بار).")

        if step == "awaiting_caption":
            caption = message.text.strip()
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
            data["current_file"] = {"caption": caption}
            data["step"] = "awaiting_quality"
            return await message.reply("📽 کیفیت فایل را بفرست (مثل 720p):")

        if step == "awaiting_quality":
            quality = message.text.strip()
            if not quality:
                return await message.reply("⚠️ کیفیت خالیه! دوباره بفرست.")
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
            data["files"].append({
                "film_id": data["film_id"], "file_id": data["pending_file_id"],
                "caption": data["current_file"]["caption"], "quality": quality
            })
            data.pop("pending_file_id", None); data.pop("current_file", None)
            data["step"] = "confirm_more_files"
            buttons = InlineKeyboardMarkup([[InlineKeyboardButton("✅ بله", callback_data="more_yes"),
                                             InlineKeyboardButton("❌ خیر", callback_data="more_no")]])
            return await message.reply("✅ فایل اضافه شد. فایل دیگری داری؟", reply_markup=buttons)
        return

# ---------------------- 🖼 روتر رسانه‌ای ادمین (کاور/فایل‌ها) ----------------------
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_router(client: Client, message: Message):
    """دریافت رسانه در فلو آپلود یا جایگزینی در پنل ادمین"""
    uid = message.from_user.id

    # حالت پنل ادمین: جایگزینی کاور/فایل/افزودن فایل
    if uid in admin_edit_state:
        st = admin_edit_state[uid]
        mode = st.get("mode"); fid = st.get("film_id")

        if mode == "replace_cover":
            if not message.photo:
                return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
            films_col.update_one({"film_id": fid}, {"$set": {"cover_id": message.photo.file_id}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کاور جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

        if mode == "file_replace":
            if message.video: fid_new = message.video.file_id
            elif message.document: fid_new = message.document.file_id
            elif message.audio: fid_new = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            idx = st.get("file_index", 0)
            films_col.update_one({"film_id": fid}, {"$set": {f"files.{idx}.file_id": fid_new}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

        if mode == "file_add_pickfile":
            if message.video: fid_new = message.video.file_id
            elif message.document: fid_new = message.document.file_id
            elif message.audio: fid_new = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            st["tmp_file_id"] = fid_new
            st["mode"] = "file_add_caption"
            return await message.reply("📝 کپشن فایل جدید را بفرست:")
        # ادامه در admin_text_router
    # فلو آپلود
    if uid in upload_data:
        data = upload_data[uid]; step = data.get("step")
        if step == "awaiting_cover":
            if not message.photo:
                return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
            data["cover_id"] = message.photo.file_id
            data["step"] = "awaiting_first_file"
            return await message.reply("📤 کاور ثبت شد. حالا فایل اول را بفرست.")
        if step in ("awaiting_first_file", "awaiting_next_file"):
            if message.video: file_id = message.video.file_id
            elif message.document: file_id = message.document.file_id
            elif message.audio: file_id = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            data["pending_file_id"] = file_id
            data["step"] = "awaiting_caption"
            return await message.reply("📝 کپشن این فایل را بفرست:")
        return

# ---------------------- پایان/ادامه آپلود (دکمه‌های more_yes/no) ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^more_"))
async def upload_more_files_cb(client: Client, cq: CallbackQuery):
    """ادامه دادن افزودن فایل (بله/خیر) و در نهایت ذخیره‌ی فیلم"""
    uid = cq.from_user.id; data = upload_data.get(uid)
    if not data:
        return await cq.answer("⚠️ اطلاعات آپلود پیدا نشد.", show_alert=True)

    if cq.data == "more_yes":
        await cq.answer(); data["step"] = "awaiting_next_file"
        data.pop("pending_file_id", None); data.pop("current_file", None)
        return await cq.message.reply("📤 فایل بعدی را بفرست.")

    if cq.data == "more_no":
        await cq.answer()
        film_id = data["film_id"]
        film_doc = {
            "film_id": film_id, "user_id": uid, "title": data.get("title"),
            "genre": data.get("genre",""), "year": data.get("year",""),
            "cover_id": data.get("cover_id"),
            "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            "files": data["files"]
        }
        films_col.update_one({"film_id": film_id}, {"$set": film_doc}, upsert=True)
        deep_link = f"https://t.me/{BOT_USERNAME}?start={film_id}"
        await cq.message.reply(f"✅ ذخیره شد.\n🎬 {film_doc['title']}\n📂 فایل‌ها: {len(film_doc['files'])}\n🔗 {deep_link}")
        await cq.message.reply(
            "🕓 انتخاب کن:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ زمان‌بندی", callback_data=f"sched_yes::{film_id}")],
                [InlineKeyboardButton("📣 ارسال فوری", callback_data=f"sched_no::{film_id}")]
            ])
        )
        upload_data.pop(uid, None)

# ---------------------- زمان‌بندی و انتشار فوری ----------------------
@bot.on_callback_query(filters.regex(r"^sched_yes::(.+)$") & filters.user(ADMIN_IDS))
async def ask_schedule_date(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    schedule_data[cq.from_user.id] = {"film_id": film_id, "step": "date"}
    await cq.message.reply("📅 تاریخ (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^sched_no::(.+)$") & filters.user(ADMIN_IDS))
async def ask_publish_immediate(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{film_id}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="pub_cancel")])
    await cq.message.reply("📣 ارسال فوری؟ کانال را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^pub_cancel$") & filters.user(ADMIN_IDS))
async def pub_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); await cq.message.edit_text("🚫 ارسال فوری لغو شد.")

@bot.on_callback_query(filters.regex(r"^sched_cancel$") & filters.user(ADMIN_IDS))
async def sched_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("⛔️ زمان‌بندی لغو شد.")

@bot.on_callback_query(filters.regex(r"^sched_pick::(-?\d+)$") & filters.user(ADMIN_IDS))
async def sched_pick_cb(client: Client, cq: CallbackQuery):
    """ثبت زمان‌بندی: ذخیره UTC naive برای کریستالی بودن مقایسه‌ها"""
    await cq.answer()
    uid = cq.from_user.id; st = schedule_data.get(uid)
    if not st or st.get("step") not in ("channel_await", "pick_channel"):
        return await cq.message.edit_text("⛔️ اطلاعات زمان‌بندی منقضی شده.")

    chat_id  = int(cq.matches[0].group(1))
    date_str = st.get("date"); time_str = st.get("time"); film_id = st.get("film_id")

    try:
        local_dt     = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        aware_local  = local_dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        dt_utc_naive = aware_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    except ValueError:
        return await cq.answer("❌ تاریخ/ساعت نامعتبر.", show_alert=True)

    film = films_col.find_one({"film_id": film_id})
    if not film:
        schedule_data.pop(uid, None)
        return await cq.answer("⚠️ فیلم پیدا نشد.", show_alert=True)

    scheduled_posts.insert_one({
        "film_id": film_id, "title": film.get("title",""),
        "channel_id": chat_id, "scheduled_time": dt_utc_naive
    })
    schedule_data.pop(uid, None)
    await cq.message.edit_text("✅ زمان‌بندی ذخیره شد.")

@bot.on_callback_query(filters.regex(r"^film_pub_go::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_pub_go_cb(client: Client, cq: CallbackQuery):
    """انتشار فوری به کانال انتخابی + ثبت post_refs + کیبورد آمار/ری‌اکشن"""
    await cq.answer()
    film_id, channel_id = cq.data.split("::")[1:]
    channel_id = int(channel_id)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.")
    caption = compose_channel_caption(film)
    try:
        if film.get("cover_id"):
            sent = await client.send_photo(channel_id, film["cover_id"], caption=caption,
                                           reply_markup=_reaction_keyboard(film_id, channel_id, 0))
        else:
            sent = await client.send_message(channel_id, caption, reply_markup=_reaction_keyboard(film_id, channel_id, 0))
    except Exception as e:
        return await cq.message.edit_text(f"❌ خطا در ارسال: {e}")
    # ثبت مرجع پیام
    post_refs.update_one({"film_id": film_id, "channel_id": channel_id}, {"$set": {"message_id": sent.id}}, upsert=True)
    # آپدیت اولیه آمار ویو
    try:
        fresh = await client.get_messages(channel_id, sent.id)
        await client.edit_message_reply_markup(
            chat_id=channel_id, message_id=sent.id,
            reply_markup=_reaction_keyboard(film_id, channel_id, sent.id, views=fresh.views or 0)
        )
    except Exception:
        pass
    await cq.message.edit_text("✅ ارسال شد.")

# ---------------------- 📌 پنل ادمین + مدیریت Pending ----------------------
def kb_admin_main():
    """منوی اصلی پنل ادمین"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 لیست/جست‌وجو", callback_data="admin_films_1")],
        [InlineKeyboardButton("⏰ زمان‌بندی‌ها", callback_data="admin_sched_list_1")],
        [InlineKeyboardButton("📌 Pending Posts", callback_data="admin_pending_1")],
        [InlineKeyboardButton("📥 خروجی CSV", callback_data="admin_export_csv")],
    ])

def _fmt_film_info(film: dict) -> str:
    """رشته‌ی اطلاعات یک فیلم برای نمایش"""
    return (f"🎬 <b>{film.get('title','-')}</b>\n"
            f"🎭 ژانر: {film.get('genre','-')}\n"
            f"📆 سال: {film.get('year','-')}\n"
            f"🖼 کاور: {'✅' if film.get('cover_id') else '❌'}\n"
            f"📂 فایل‌ها: {len(film.get('files', []))}\n"
            f"🆔 {film.get('film_id','-')}")

def _paginate(items, page, page_size=10):
    """برگ‌بندی ساده لیست‌ها"""
    total = len(items); start = (page - 1) * page_size; end = start + page_size
    return items[start:end], total

@bot.on_message(filters.command("admin") & filters.user(ADMIN_IDS))
async def admin_entry(client: Client, message: Message):
    """ورود به منوی ادمین"""
    await message.reply("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_home$") & filters.user(ADMIN_IDS))
async def admin_home_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); await cq.message.edit_text("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_films_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_films_list(client: Client, cq: CallbackQuery):
    """لیست فیلم‌ها با برگ‌بندی"""
    await cq.answer()
    page = int(cq.matches[0].group(1))
    films = list(films_col.find().sort("timestamp", -1))
    page_items, total = _paginate(films, page, 10)
    if not page_items and page > 1:
        return await cq.message.edit_text("⛔️ صفحه خالی است.", reply_markup=kb_admin_main())
    rows = []
    for f in page_items:
        title = f.get("title") or f.get("film_id")
        year = f.get("year", "")
        rows.append([InlineKeyboardButton(f"{title} {f'({year})' if year else ''}", callback_data=f"film_open::{f['film_id']}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_films_{page-1}"))
    if page * 10 < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_films_{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("🔎 جست‌وجو", callback_data="admin_search")])
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("🎬 لیست فیلم‌ها:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^admin_search$") & filters.user(ADMIN_IDS))
async def admin_search_cb(client: Client, cq: CallbackQuery):
    """شروع جست‌وجو در پنل ادمین"""
    await cq.answer()
    admin_edit_state[cq.from_user.id] = {"mode": "search"}
    await cq.message.edit_text("🔎 عبارت جست‌وجو را بفرست (عنوان/ژانر/سال/film_id)...")

@bot.on_callback_query(filters.regex(r"^film_open::(.+)$") & filters.user(ADMIN_IDS))
async def film_open_cb(client: Client, cq: CallbackQuery):
    """نمایش جزییات یک فیلم + منوی عملیات"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    info = _fmt_film_info(film)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ ویرایش عنوان", callback_data=f"film_edit_title::{fid}")],
        [InlineKeyboardButton("🎭 ویرایش ژانر", callback_data=f"film_edit_genre::{fid}")],
        [InlineKeyboardButton("📆 ویرایش سال", callback_data=f"film_edit_year::{fid}")],
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
    await cq.answer(); fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_title", "film_id": fid}
    await cq.message.edit_text("🖊 عنوان جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_genre::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_genre_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_genre", "film_id": fid}
    await cq.message.edit_text("🎭 ژانر جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_year::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_year_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_year", "film_id": fid}
    await cq.message.edit_text("📆 سال جدید را بفرست (مثلاً 2024):")

@bot.on_callback_query(filters.regex(r"^film_replace_cover::(.+)$") & filters.user(ADMIN_IDS))
async def film_replace_cover_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "replace_cover", "film_id": fid}
    await cq.message.edit_text("🖼 عکس کاور جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_files::(.+)$") & filters.user(ADMIN_IDS))
async def film_files_list(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
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
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid}); files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    f = files[idx]
    cap = f.get("caption", ""); q = f.get("quality", "")
    info = f"📄 <b>فایل #{idx+1}</b>\n🎞 کیفیت: {q}\n📝 کپشن:\n{cap[:800] + ('…' if len(cap) > 800 else '')}"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ ویرایش کپشن", callback_data=f"file_edit_caption::{fid}::{idx}")],
        [InlineKeyboardButton("🎞 ویرایش کیفیت", callback_data=f"file_edit_quality::{fid}::{idx}")],
        [InlineKeyboardButton("🔁 جایگزینی فایل", callback_data=f"file_replace::{fid}::{idx}")],
        [InlineKeyboardButton("🗑 حذف فایل", callback_data=f"file_delete_confirm::{fid}::{idx}")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]
    ])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_edit_caption::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_caption_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_caption", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📝 کپشن جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^file_edit_quality::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_quality_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_quality", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("🎞 کیفیت جدید را بفرست (مثلاً 1080p):")

@bot.on_callback_query(filters.regex(r"^file_replace::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_replace_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_replace", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست:")

@bot.on_callback_query(filters.regex(r"^file_delete_confirm::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_confirm_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_file_open::{fid}::{idx}")],
        [InlineKeyboardButton("🗑 حذف", callback_data=f"file_delete::{fid}::{idx}")]
    ])
    await cq.message.edit_text("❗️ مطمئنی حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_delete::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_do_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid}); files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    files.pop(idx); films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ فایل حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^film_file_add::(.+)$") & filters.user(ADMIN_IDS))
async def film_file_add_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "file_add_pickfile", "film_id": fid}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_delete_confirm::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_confirm_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_open::{fid}")],
        [InlineKeyboardButton("🗑 حذف قطعی", callback_data=f"film_delete::{fid}")]
    ])
    await cq.message.edit_text("❗️ حذف کل فیلم و فایل‌ها؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^film_delete::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_do_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    films_col.delete_one({"film_id": fid})
    await cq.message.edit_text("✅ فیلم حذف شد.", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^film_pub_pick::(.+)$") & filters.user(ADMIN_IDS))
async def film_pub_pick_channel(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{fid}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    await cq.message.edit_text("📣 مقصد انتشار فوری را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows + [[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^film_sched_start::(.+)$") & filters.user(ADMIN_IDS))
async def film_sched_start_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
    schedule_data[cq.from_user.id] = {"film_id": fid, "step": "date"}
    await cq.message.edit_text("📅 تاریخ (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^admin_pending_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_pending_list(client: Client, cq: CallbackQuery):
    """نمایش لیست Pending"""
    await cq.answer()
    page = int(cq.matches[0].group(1))
    posts = list(pending_posts.find().sort("timestamp", -1))
    page_items, total = _paginate(posts, page, 10)
    rows = []
    for p in page_items:
        rows.append([InlineKeyboardButton(f"{p.get('title')} • {p.get('source')}", callback_data=f"pending_open::{p['_id']}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_pending_{page-1}"))
    if page * 10 < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_pending_{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("📌 Pending Posts:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^pending_open::(.+)$") & filters.user(ADMIN_IDS))
async def pending_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); pid = cq.matches[0].group(1)
    post = pending_posts.find_one({"_id": ObjectId(pid)})
    if not post:
        return await cq.message.edit_text("❌ Pending پیدا نشد.", reply_markup=kb_admin_main())
    info = f"🎬 {post['title']}\n📡 منبع: {post['source']}\n🆔 {post['film_id']}"
    rows = []
    for title, chat_id in TARGET_CHANNELS.items():
        rows.append([InlineKeyboardButton(f"ارسال به {title}", callback_data=f"pending_send::{pid}::{chat_id}")])
    rows.append([InlineKeyboardButton("❌ حذف", callback_data=f"pending_delete::{pid}")])
    rows.append([InlineKeyboardButton("↩️ بازگشت", callback_data="admin_pending_1")])
    await cq.message.edit_text(info, reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^pending_send::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def pending_send_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); pid = cq.matches[0].group(1); chat_id = int(cq.matches[0].group(2))
    post = pending_posts.find_one({"_id": ObjectId(pid)})
    if not post: return await cq.answer("❌ پیدا نشد", show_alert=True)
    film = films_col.find_one({"film_id": post["film_id"]})
    if not film: return await cq.answer("❌ فیلم پیدا نشد", show_alert=True)
    caption = compose_channel_caption(film)
    sent = await client.send_message(chat_id, caption, reply_markup=_reaction_keyboard(film["film_id"], chat_id, 0))
    post_refs.update_one({"film_id": film["film_id"], "channel_id": chat_id}, {"$set": {"message_id": sent.id}}, upsert=True)
    pending_posts.delete_one({"_id": ObjectId(pid)})
    await cq.message.edit_text("✅ ارسال شد و از Pending حذف شد.", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^pending_delete::(.+)$") & filters.user(ADMIN_IDS))
async def pending_delete_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); pid = cq.matches[0].group(1)
    pending_posts.delete_one({"_id": ObjectId(pid)})
    await cq.message.edit_text("🗑 حذف شد.", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_export_csv$") & filters.user(ADMIN_IDS))
async def admin_export_csv_cb(client: Client, cq: CallbackQuery):
    """خروجی CSV از لیست فیلم‌ها"""
    await cq.answer()
    films = list(films_col.find().sort("timestamp", -1))
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["film_id", "title", "genre", "year", "files_count", "timestamp"])
    for f in films:
        w.writerow([
            f.get("film_id",""), (f.get("title","") or "").replace("\n"," "),
            (f.get("genre","") or "").replace("\n"," "),
            f.get("year",""), len(f.get("files",[])), f.get("timestamp","")
        ])
    buf.seek(0); bio = io.BytesIO(buf.getvalue().encode("utf-8")); bio.name = "films_export.csv"
    await client.send_document(cq.message.chat.id, document=bio, caption="📥 خروجی CSV")

# ---------------------- ⏱ زمان‌بند خودکار ارسال‌های زمان‌بندی‌شده ----------------------
async def send_scheduled_posts():
    """هر دقیقه: پست‌هایی که زمانشان رسیده را ارسال می‌کند"""
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now}}))
    except Exception as e:
        print("DB unavailable:", e); return

    for post in posts:
        film = films_col.find_one({"film_id": post["film_id"]})
        if not film:
            scheduled_posts.delete_one({"_id": post["_id"]}); continue
        caption = compose_channel_caption(film)
        try:
            if film.get("cover_id"):
                sent = await bot.send_photo(post["channel_id"], film["cover_id"], caption=caption,
                                            reply_markup=_reaction_keyboard(film["film_id"], post["channel_id"], 0))
            else:
                sent = await bot.send_message(post["channel_id"], caption,
                                              reply_markup=_reaction_keyboard(film["film_id"], post["channel_id"], 0))
            post_refs.update_one({"film_id": film["film_id"], "channel_id": post["channel_id"]}, {"$set": {"message_id": sent.id}}, upsert=True)
            try:
                fresh = await bot.get_messages(post["channel_id"], sent.id)
                await bot.edit_message_reply_markup(
                    chat_id=post["channel_id"], message_id=sent.id,
                    reply_markup=_reaction_keyboard(film["film_id"], post["channel_id"], sent.id, views=fresh.views or 0)
                )
            except Exception:
                pass
        except Exception as e:
            print("❌ scheduled send error:", e)
        scheduled_posts.delete_one({"_id": post["_id"]})

# ---------------------- 📊 Reactions و آمار زیر پست ----------------------
@bot.on_callback_query(filters.regex(r"^react::(.+)::(-?\d+)::(\d+)$"))
async def react_cb(client: Client, cq: CallbackQuery):
    """ثبت واکنش کاربر (یک واکنش برای هر فیلم) و رفرش کیبورد"""
    reaction = cq.matches[0].group(1); channel_id = int(cq.matches[0].group(2)); message_id = int(cq.matches[0].group(3))
    film_doc = post_refs.find_one({"channel_id": channel_id, "message_id": message_id})
    film_id = film_doc.get("film_id") if film_doc else None
    if not film_id: return await cq.answer("❌ خطا در شناسایی فیلم", show_alert=True)

    old = reactions_col.find_one({"film_id": film_id, "user_id": cq.from_user.id})
    if old and old["reaction"] == reaction:
        return await cq.answer("⛔️ قبلاً همین واکنش را دادی.", show_alert=True)
    elif old:
        stats_col.update_one({"film_id": film_id}, {"$inc": {f"reactions.{old['reaction']}": -1}})
        reactions_col.update_one({"_id": old["_id"]}, {"$set": {"reaction": reaction}})
    else:
        reactions_col.insert_one({"film_id": film_id, "user_id": cq.from_user.id, "reaction": reaction})
    stats_col.update_one({"film_id": film_id}, {"$inc": {f"reactions.{reaction}": 1}}, upsert=True)

    try:
        msg = await client.get_messages(channel_id, message_id); views = int(msg.views or 0)
        await client.edit_message_reply_markup(chat_id=channel_id, message_id=message_id,
                                               reply_markup=_reaction_keyboard(film_id, channel_id, message_id, views=views))
    except Exception:
        pass
    await cq.answer("✅ ثبت شد.")

@bot.on_callback_query(filters.regex(r"^sr::(-?\d+)::(\d+)$"))
async def stat_refresh_cb(client: Client, cq: CallbackQuery):
    """رفرش دستی آمار (👁/📥/🔁)"""
    await cq.answer()
    channel_id = int(cq.matches[0].group(1)); message_id = int(cq.matches[0].group(2))
    film_doc = post_refs.find_one({"channel_id": channel_id, "message_id": message_id})
    film_id = film_doc.get("film_id") if film_doc else None
    views = 0
    try:
        msg = await client.get_messages(channel_id, message_id); views = int(msg.views or 0)
    except Exception:
        pass
    if film_id:
        try:
            await client.edit_message_reply_markup(chat_id=channel_id, message_id=message_id,
                                                   reply_markup=_reaction_keyboard(film_id, channel_id, message_id, views=views))
        except Exception:
            pass

@bot.on_callback_query(filters.regex(r"^ss::(-?\d+)::(\d+)$"))
async def stat_share_cb(client: Client, cq: CallbackQuery):
    """افزایش شمارنده‌ی Share و رفرش سریع"""
    await cq.answer("🔁 شمارش اشتراک افزوده شد.", show_alert=False)
    channel_id = int(cq.matches[0].group(1)); message_id = int(cq.matches[0].group(2))
    film_doc = post_refs.find_one({"channel_id": channel_id, "message_id": message_id})
    film_id = film_doc.get("film_id") if film_doc else None
    if not film_id: return
    stats_col.update_one({"film_id": film_id}, {"$inc": {"shares": 1}}, upsert=True)
    try:
        msg = await client.get_messages(channel_id, message_id); views = int(msg.views or 0)
        await client.edit_message_reply_markup(chat_id=channel_id, message_id=message_id,
                                               reply_markup=_reaction_keyboard(film_id, channel_id, message_id, views=views))
    except Exception:
        pass
    try:
        await client.send_message(cq.from_user.id, f"✨ این لینک را برای دوستانت بفرست:\nhttps://t.me/{BOT_USERNAME}?start={film_id}")
    except Exception:
        pass

# ---------------------- 🔄 Auto Refresh Stats (هر ۵ دقیقه) ----------------------
async def refresh_all_stats():
    """هر ۵ دقیقه: تمام پست‌های ثبت‌شده در post_refs را رفرش می‌کند"""
    try:
        refs = list(post_refs.find())
        for ref in refs:
            film_id = ref["film_id"]; channel_id = ref["channel_id"]; message_id = ref.get("message_id")
            if not message_id: continue
            try:
                msg = await bot.get_messages(channel_id, message_id); views = int(msg.views or 0)
                await bot.edit_message_reply_markup(chat_id=channel_id, message_id=message_id,
                                                    reply_markup=_reaction_keyboard(film_id, channel_id, message_id, views=views))
            except Exception as e:
                print("⚠️ refresh error:", e)
    except Exception as e:
        print("❌ DB error in refresh_all_stats:", e)

# ---------------------- 🧭 UserBot: شنود کانال‌های منبع و انتشار خودکار ----------------------
@user.on_message(filters.chat(SOURCE_CHANNELS))
async def catch_source_posts(client: Client, message: Message):
    """هر پست جدید از کانال‌های منبع: خواندن، ویرایش کپشن، تشخیص مقصد، ذخیره در DB، ارسال خودکار یا Pending"""
    try:
        source_username = message.chat.username or ""
        raw_caption = message.caption or message.text or ""
        title = (raw_caption.split("\n")[0] if raw_caption else "بدون عنوان")[:80]
        film_id = slugify(title)

        # آماده‌سازی سند فیلم برای DB
        base_doc = {
            "film_id": film_id,
            "title": title,
            "genre": "",
            "year": "",
            "cover_id": message.photo.file_id if message.photo else None,
            "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            "files": []
        }
        # اگر مدیا دارد (ویدیو/سند/صوت) ثبت کن تا بعداً از DeepLink قابل دریافت باشد
        if message.video:
            base_doc["files"].append({"film_id": film_id, "file_id": message.video.file_id, "caption": raw_caption, "quality": ""})
        elif message.document:
            base_doc["files"].append({"film_id": film_id, "file_id": message.document.file_id, "caption": raw_caption, "quality": ""})
        elif message.audio:
            base_doc["files"].append({"film_id": film_id, "file_id": message.audio.file_id, "caption": raw_caption, "quality": ""})

        films_col.update_one({"film_id": film_id}, {"$set": base_doc}, upsert=True)

        # کپشن جدید با امضاء
        new_caption = format_source_footer(raw_caption, source_username)

        # تشخیص مقصد: اول SOURCE_MAP → بعد کلیدواژه
        dest = None
        if f"@{source_username}" in SOURCE_MAP:
            dest = SOURCE_MAP[f"@{source_username}"]
        if not dest:
            dest = detect_category_by_text(raw_caption)

        status = "pending"
        if dest and AUTO_PUBLISH:
            # ارسال پست به کانال مقصد از طرف Bot (با کیبورد آمار/ری‌اکشن)
            preview_caption = compose_channel_caption(base_doc)
            if base_doc.get("cover_id"):
                sent = await bot.send_photo(dest, base_doc["cover_id"], caption=preview_caption,
                                            reply_markup=_reaction_keyboard(film_id, dest, 0))
            else:
                sent = await bot.send_message(dest, preview_caption,
                                              reply_markup=_reaction_keyboard(film_id, dest, 0))
            # ثبت مرجع پیام برای آمار
            post_refs.update_one({"film_id": film_id, "channel_id": dest}, {"$set": {"message_id": sent.id}}, upsert=True)
            status = f"published → {dest}"
        else:
            # اگر مقصد نامشخص بود یا AUTO_PUBLISH خاموش بود → Pending برای تایید دستی
            pending_posts.insert_one({
                "film_id": film_id, "title": title, "source": source_username,
                "timestamp": datetime.now(timezone.utc).replace(tzinfo=None)
            })
            # اطلاع به ادمین
            try:
                for admin_id in ADMIN_IDS:
                    await bot.send_message(admin_id, f"📌 Pending جدید:\n🎬 {title}\n👤 منبع: @{source_username}\n🆔 {film_id}")
            except Exception:
                pass

        # لاگ فایل
        log_source_entry({
            "film_id": film_id, "title": title, "source": source_username,
            "status": status, "time": datetime.now().isoformat()
        })
        print(f"📥 Source post saved: {film_id} ({status})")

    except Exception as e:
        print("❌ error in catch_source_posts:", e)
        traceback.print_exc()

# ---------------------- 🗓 گزارش روزانه ساعت 22:00 ----------------------
async def daily_report():
    """هر شب بر اساس TIMEZONE گزارش روزانه برای ادمین‌ها می‌فرستد"""
    try:
        now_local = datetime.now(ZoneInfo(TIMEZONE))
        start_local = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=ZoneInfo(TIMEZONE))
        end_local   = start_local + timedelta(days=1) - timedelta(seconds=1)

        # تبدیل بازه به UTC naive برای تطابق با داده‌های ذخیره‌شده
        start_utc_naive = start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        end_utc_naive   = end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

        # تعداد آیتم‌های امروز
        films_today = films_col.count_documents({"timestamp": {"$gte": start_utc_naive, "$lte": end_utc_naive}})

        # آمار کلی (از stats_col + post_refs برای ویو)
        total_downloads = 0; total_shares = 0
        total_reacts = {"love":0, "like":0, "dislike":0, "sad":0}
        top_post = None; top_views = 0

        for st in stats_col.find():
            film_id = st["film_id"]
            total_downloads += int(st.get("downloads",0))
            total_shares += int(st.get("shares",0))
            recs = st.get("reactions", {})
            for k in total_reacts:
                total_reacts[k] += int(recs.get(k,0))
            # آخرین ویو را از پیام کانالی بخوان
            ref = post_refs.find_one({"film_id": film_id})
            if ref:
                try:
                    msg = await bot.get_messages(ref["channel_id"], ref["message_id"])
                    v = int(msg.views or 0)
                    if v > top_views:
                        top_views = v; top_post = film_id
                except Exception:
                    pass

        text = (
            f"📊 گزارش روزانه — {now_local.strftime('%Y-%m-%d')}\n\n"
            f"🎬 موارد جدید: {films_today}\n"
            f"📥 دانلودها: {total_downloads}\n"
            f"🔁 اشتراک‌گذاری‌ها: {total_shares}\n"
            f"❤️ {total_reacts['love']} | 👍 {total_reacts['like']} | 👎 {total_reacts['dislike']} | 😢 {total_reacts['sad']}\n"
        )
        if top_post:
            text += f"\n👑 پربازدیدترین: {top_post} ({top_views} 👁)\n"

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text)
            except Exception as e:
                print("⚠️ report send error:", e)

        print("✅ Daily report sent.")
    except Exception as e:
        print("❌ daily_report error:", e)

# ---------------------- 💾 بکاپ هفتگی CSV و ارسال به ادمین ----------------------
async def weekly_backup():
    """هر هفته: خروجی CSV از films و ارسال برای ادمین‌ها"""
    try:
        films = list(films_col.find().sort("timestamp", -1))
        buf = io.StringIO(); w = csv.writer(buf)
        w.writerow(["film_id","title","genre","year","files_count","timestamp"])
        for f in films:
            w.writerow([
                f.get("film_id",""),
                (f.get("title","") or "").replace("\n"," "),
                (f.get("genre","") or "").replace("\n"," "),
                f.get("year",""),
                len(f.get("files",[])),
                f.get("timestamp","")
            ])
        buf.seek(0); bio = io.BytesIO(buf.getvalue().encode("utf-8")); bio.name = f"backup_{datetime.utcnow().date()}.csv"
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_document(admin_id, bio, caption="🗄 بکاپ هفتگی دیتابیس")
            except Exception as e:
                print("⚠️ backup send err:", e)
        print("✅ Weekly backup sent.")
    except Exception as e:
        print("❌ weekly_backup error:", e)

# ---------------------- 🚀 اجرای نهایی (Bot + UserBot + Scheduler) ----------------------
scheduler = AsyncIOScheduler()

async def main():
    """حذف وبهوک، استارت هر دو کلاینت، ثبت جاب‌ها، و ورود به idle"""
    # اطمینان از خاموش بودن وبهوک (برای polling)
    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true"
        with urllib.request.urlopen(url, timeout=10) as r:
            print(f"🧹 Webhook delete HTTP status: {r.status}")
    except Exception as e:
        print("⚠️ deleteWebhook error:", e)

    # استارت Bot و UserBot
    await bot.start()
    await user.start()

    me = await bot.get_me(); print(f"🤖 Bot @{me.username} started")
    me2 = await user.get_me(); print(f"👤 Userbot {me2.id} started")

    # جاب‌ها:
    scheduler.add_job(send_scheduled_posts, "interval", minutes=1)     # چک صف زمان‌بندی
    scheduler.add_job(refresh_all_stats, "interval", minutes=5)        # رفرش آمار زیر پست
    scheduler.add_job(daily_report, "cron", hour=22, minute=0)         # گزارش روزانه ساعت 22:00 (TIMEZONE)
    scheduler.add_job(weekly_backup, "cron", day_of_week="sun", hour=3, minute=0)  # بکاپ هفتگی یکشنبه 03:00

    scheduler.start(); print("📅 Scheduler started!")
    await idle()  # برنامه را زنده نگه‌دار

if __name__ == "__main__":
    # اجرای main داخل event-loop Pyrogram
    bot.run(main)
