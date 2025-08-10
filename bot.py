# ======================= BoxUp_bot — Final bot.py =======================
# تمام تنظیمات از .env خوانده می‌شود

# ---------------------- 📦 ایمپورت‌ها ----------------------
import os, re, json, io, csv, unicodedata, string, asyncio
from datetime import datetime
from dotenv import load_dotenv

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from pymongo import MongoClient
from bson import ObjectId

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------------- ⚙️ بارگذاری env و پوشه سشن ----------------------
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

ADMIN_IDS = [int(x.strip()) for x in _get_env_str("ADMIN_IDS").split(",") if x.strip().isdigit()]
if not ADMIN_IDS:
    raise RuntimeError("❌ ADMIN_IDS خالی است.")
ADMIN_ID = ADMIN_IDS[0]

REQUIRED_CHANNELS = [x.strip().lstrip("@") for x in _get_env_str("REQUIRED_CHANNELS").split(",") if x.strip()]
TARGET_CHANNELS = {str(k): int(v) for k, v in json.loads(_get_env_str("TARGET_CHANNELS_JSON")).items()}

# مسیر سشن (فقط یک‌بار)
SESSION_DIR = os.getenv("SESSION_DIR", "./.sessions")
os.makedirs(SESSION_DIR, exist_ok=True)

print("✅ تنظیمات از محیط بارگذاری شد.")

# ---------------------- 🗄️ اتصال دیتابیس ----------------------
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    films_col        = db["films"]
    scheduled_posts  = db["scheduled_posts"]
    settings_col     = db["settings"]
    user_sources     = db["user_sources"]   # نگهداری film_id برای کاربر غیرعضو
    stats_col        = db["stats"]          # آمار (downloads, shares) بر اساس film_id
    post_refs        = db["post_refs"]      # نگاشت پست کانال → film_id
    print(f"✅ اتصال به MongoDB برقرار شد. DB = {MONGO_DB_NAME}")
except Exception as e:
    raise RuntimeError(f"❌ خطا در اتصال به MongoDB: {e}")

# ---------------------- 🤖 ساخت کلاینت Pyrogram (فقط یک‌بار) ----------------------
bot = Client(
    "BoxUploader",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=ParseMode.HTML,
    workdir=SESSION_DIR  # ✅ سشن پایدار (برای Render و لوکال)
)
# ---------------------- 🧠 وضعیت‌ها (State) ----------------------
# نگهداری وضعیت مکالمه برای ادمین‌ها
upload_data: dict[int, dict] = {}        # فلو آپلود (عنوان → ژانر → سال → کاور → فایل‌ها)
schedule_data: dict[int, dict] = {}      # فلو زمان‌بندی
admin_edit_state: dict[int, dict] = {}   # فلو ویرایش/پنل ادمین

# ---------------------- 🧰 توابع کمکی عمومی ----------------------
def caption_to_buttons(caption: str):
    """
    الگوهای «متن (لینک)» داخل کپشن را به دکمه تبدیل می‌کند.
    خروجی: کپشن تمیز شده + InlineKeyboardMarkup
    """
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
        # حذف الگوی پیدا شده از متن
        cleaned = cleaned.replace(f"{label}({url})", "")
        cleaned = cleaned.replace(f"{label} ({url})", "")

    # تمیزکاری فاصله‌ها و خطوط اضافه
    cleaned = re.sub(r'[ \t]+\n', '\n', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    if not cleaned:
        cleaned = caption

    kb = InlineKeyboardMarkup([[b] for b in buttons]) if buttons else None
    return cleaned, kb

def build_deeplink_kb(film_id: str) -> InlineKeyboardMarkup:
    """کیبورد یک‌دکمه‌ای برای دیپ‌لینک start=film_id در ربات."""
    url = f"https://t.me/{BOT_USERNAME}?start={film_id}"
    return InlineKeyboardMarkup([[InlineKeyboardButton("📥 دانلود", url=url)]])

def compose_channel_caption(film: dict) -> str:
    """کپشن استاندارد پست کانالی (بدون لینک داخل متن)."""
    title = film.get("title", film.get("film_id", ""))
    genre = film.get("genre", "")
    year  = film.get("year", "")
    lines = [f"🎬 <b>{title}</b>"]
    if genre: lines.append(f"🎭 ژانر: {genre}")
    if year:  lines.append(f"📆 سال: {year}")
    lines.append("👇 برای دریافت، روی دکمه دانلود بزنید.")
    return "\n".join(lines)

def _reset_upload_state(uid: int):
    """پاک‌سازی وضعیت فلو آپلود برای یک ادمین."""
    upload_data.pop(uid, None)

def slugify(title: str) -> str:
    """
    ساخت film_id امن از روی عنوان:
      - فقط A-Z/a-z/0-9 و فاصله/خط‌تیره/خط‌زیر نگه داشته می‌شوند
      - فاصله‌ها به '_' تبدیل می‌شوند
      - حروف کوچک می‌شوند، طول حداکثر 64
    """
    t = unicodedata.normalize("NFKD", title)
    allowed = string.ascii_letters + string.digits + " _-"
    t = "".join(ch for ch in t if ch in allowed)
    t = t.strip().replace(" ", "_")
    return (t.lower() or "title")[:64]

async def delete_after_delay(client: Client, chat_id: int, message_id: int):
    """حذف پیام بعد از DELETE_DELAY ثانیه (برای کاربر عادی)."""
    try:
        await asyncio.sleep(DELETE_DELAY)
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        print(f"⚠️ خطا در حذف پیام: {e}")

async def user_is_member(client: Client, uid: int) -> bool:
    """بررسی عضویت کاربر در همه کانال‌های REQUIRED_CHANNELS."""
    for channel in REQUIRED_CHANNELS:
        try:
            m = await client.get_chat_member(f"@{channel}", uid)
            if m.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return False
        except Exception:
            return False
    return True

def join_buttons_markup():
    """کیبورد دکمه‌های عضویت + دکمه تایید عضویت."""
    rows = []
    for ch in REQUIRED_CHANNELS:
        title = ch.lstrip("@")
        rows.append([InlineKeyboardButton(f"📣 عضویت در @{title}", url=f"https://t.me/{title}")])
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="check_membership")])
    return InlineKeyboardMarkup(rows)

# ---------------------- ⚙️ کیبورد آمار پست کانال ----------------------
def _stats_keyboard(film_id: str, channel_id: int, message_id: int, views=None):
    """
    کیبورد آمار زیر پست کانال:
      - دکمه دانلود (URL به دیپ‌لینک)
      - سه دکمه آمار: 👁 ویو (تلگرام) / 📥 دانلود (DB) / 🔁 اشتراک (DB)
    """
    st = stats_col.find_one({"film_id": film_id}) or {}
    dl = int(st.get("downloads", 0))
    sh = int(st.get("shares", 0))
    v  = int(views or 0)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 دانلود", url=f"https://t.me/{BOT_USERNAME}?start={film_id}")],
        [
            InlineKeyboardButton(f"👁 {v}",  callback_data=f"stat_refresh::{film_id}::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"📥 {dl}", callback_data=f"stat_refresh::{film_id}::{channel_id}::{message_id}"),
            InlineKeyboardButton(f"🔁 {sh}", callback_data=f"stat_share::{film_id}::{channel_id}::{message_id}")
        ]
    ])

async def _delayed_stat_refresh(client, film_id: str, channel_id: int, message_id: int, delay_sec: int = 10):
    """چند ثانیه بعد از ارسال پست، ویوها را خوانده و کیبورد آمار را رفرش می‌کند."""
    try:
        await asyncio.sleep(delay_sec)
        try:
            msg = await client.get_messages(channel_id, message_id)
            views = int(msg.views or 0)
        except Exception:
            views = 0
        await client.edit_message_reply_markup(
            chat_id=channel_id,
            message_id=message_id,
            reply_markup=_stats_keyboard(film_id, channel_id, message_id, views=views)
        )
    except Exception:
        pass
# ---------------------- 🚚 ارسال فایل‌ها به کاربر عادی ----------------------
async def _send_film_files_to_user(client: Client, chat_id: int, film_doc: dict):
    """
    تمام فایل‌های ثبت‌شده‌ی یک فیلم را برای کاربر می‌فرستد
    و بعد از DELETE_DELAY ثانیه حذف می‌کند.
    """
    files = film_doc.get("files", [])
    if not files:
        await client.send_message(chat_id, "❌ هیچ فایلی برای این فیلم ثبت نشده است.")
        return

    title = film_doc.get("title", film_doc.get("film_id", ""))
    for f in files:
        cap = f"🎬 {title}{' (' + f.get('quality','') + ')' if f.get('quality') else ''}\n\n{f.get('caption','')}"
        cleaned, kb = caption_to_buttons(cap)
        try:
            msg = await client.send_video(
                chat_id=chat_id,
                video=f["file_id"],
                caption=cleaned,
                reply_markup=kb
            ) if kb else await client.send_video(
                chat_id=chat_id,
                video=f["file_id"],
                caption=cleaned
            )
            asyncio.create_task(delete_after_delay(client, msg.chat.id, msg.id))
        except Exception as e:
            await client.send_message(chat_id, f"❌ خطا در ارسال یک فایل: {e}")

    warn = await client.send_message(chat_id, "⚠️ فایل‌ها تا ۳۰ ثانیه دیگر حذف می‌شوند، لطفاً سریعاً ذخیره کنید.")
    asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))

# ---------------------- 🚪 START + Membership ----------------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    """
    رفتار /start:
      • اگر start=film_id داشت و عضو بود → مستقیم فایل‌ها را می‌فرستد
      • اگر عضو نبود → film_id را ذخیره می‌کند تا بعد از تایید عضویت ارسال شود
      • همیشه: پیام خوش‌آمد + دکمه‌های عضویت
    """
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    film_id = parts[1].strip() if len(parts) == 2 else None

    if film_id and await user_is_member(client, user_id):
        # +۱ دانلود (ورود از دیپ‌لینک)
        try:
            stats_col.update_one({"film_id": film_id}, {"$inc": {"downloads": 1}}, upsert=True)
        except Exception:
            pass

        film = films_col.find_one({"film_id": film_id})
        if not film:
            await message.reply("❌ لینک فایل معتبر نیست یا فیلم پیدا نشد.")
            return
        await _send_film_files_to_user(client, message.chat.id, film)
        return

    # ذخیره‌ی film_id برای بعد از تایید عضویت
    if film_id:
        user_sources.update_one({"user_id": user_id}, {"$set": {"from_film_id": film_id}}, upsert=True)

    # پیام خوش‌آمد + دکمه‌های عضویت
    try:
        await message.reply_photo(
            photo=WELCOME_IMAGE,
            caption="🎬 به ربات UpBox خوش آمدید!\n\nابتدا لطفاً در کانال‌های زیر عضو شوید، سپس روی «✅ عضو شدم» بزنید:",
            reply_markup=join_buttons_markup()
        )
    except Exception:
        await message.reply(
            "🎬 به ربات UpBox خوش آمدید!\n\nابتدا لطفاً در کانال‌های زیر عضو شوید، سپس روی «✅ عضو شدم» بزنید:",
            reply_markup=join_buttons_markup()
        )

@bot.on_callback_query(filters.regex(r"^check_membership$"))
async def check_membership_cb(client: Client, cq: CallbackQuery):
    """
    دکمه‌ی «✅ عضو شدم»:
      • بررسی دوباره‌ی عضویت
      • اگر film_id ذخیره شده بود → ارسال فایل‌ها و پاک‌سازی
    """
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
        return await cq.answer("⛔️ هنوز در همه کانال‌ها عضو نشده‌ای!", show_alert=True)

    await cq.answer("✅ عضویتت تأیید شد!", show_alert=True)
    try:
        await client.send_photo(cq.message.chat.id, CONFIRM_IMAGE, caption="✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")
    except Exception:
        await client.send_message(cq.message.chat.id, "✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")

    src = user_sources.find_one({"user_id": user_id})
    film_id = src.get("from_film_id") if src else None

    if film_id:
        # +۱ دانلود (ورود از دیپ‌لینک پس از تایید)
        try:
            stats_col.update_one({"film_id": film_id}, {"$inc": {"downloads": 1}}, upsert=True)
        except Exception:
            pass

        film = films_col.find_one({"film_id": film_id})
        if not film:
            await client.send_message(cq.message.chat.id, "❌ لینک فیلم معتبر نیست یا اطلاعاتی یافت نشد.")
            user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
            return

        await _send_film_files_to_user(client, cq.message.chat.id, film)
        user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
    else:
        await client.send_message(cq.message.chat.id, "ℹ️ الان عضو شدی. برای دریافت محتوا، روی لینک داخل پست‌های کانال کلیک کن.")
# ---------------------- ⬆️ شروع فلو آپلود ادمین ----------------------
@bot.on_message(filters.command("upload") & filters.private & filters.user(ADMIN_IDS))
async def upload_command(client: Client, message: Message):
    """
    شروع فرآیند آپلود برای ادمین:
      - دیگه film_id از کاربر نمی‌گیریم.
      - از عنوان شروع می‌کنیم، بعد ژانر، سال، کاور، فایل‌ها...
    """
    uid = message.from_user.id
    upload_data[uid] = {"step": "awaiting_title", "files": []}
    await message.reply("🎬 لطفاً <b>عنوان</b> را وارد کن (مثال: آواتار ۲).")

# ---------------------- 📨 روتر واحد پیام‌های متنی ادمین ----------------------
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_text_router(client: Client, message: Message):
    """
    تمام متن‌های ادمین به اینجا می‌آید و با توجه به state تصمیم گرفته می‌شود:
      1) اگر در فلو زمان‌بندی باشد → در بخش زمان‌بندی رسیدگی می‌شود (بخش 5/6)
      2) اگر در پنل ادمین باشد → در همان بخش رسیدگی می‌شود (بخش 5/6)
      3) اگر در فلو آپلود باشد → مراحل عنوان/ژانر/سال/کپشن/کیفیت
    """
    uid = message.from_user.id

    # اگر در فلو آپلود است:
    if uid in upload_data:
        data = upload_data[uid]
        step = data.get("step")

        if step == "awaiting_title":
            title = message.text.strip()
            if not title:
                return await message.reply("⚠️ عنوان خالیه! دوباره بفرست.")
            data["title"] = title

            # ساخت film_id یکتا از روی عنوان
            base = slugify(title)
            candidate = base
            i = 2
            while films_col.find_one({"film_id": candidate}):
                candidate = f"{base}_{i}"
                i += 1
            data["film_id"] = candidate

            data["step"] = "awaiting_genre"
            return await message.reply("🎭 <b>ژانر</b> را وارد کن (مثال: اکشن، درام):")

        if step == "awaiting_genre":
            data["genre"] = message.text.strip()
            data["step"] = "awaiting_year"
            return await message.reply("📅 <b>سال تولید</b> را وارد کن (مثال: <code>2023</code>):")

        if step == "awaiting_year":
            year = message.text.strip()
            if year and not year.isdigit():
                return await message.reply("⚠️ سال باید عدد باشد.")
            data["year"] = year

            if data.get("cover_id"):
                data["step"] = "awaiting_first_file"
                return await message.reply("🗂 حالا <b>فایلِ اول</b> را بفرست (ویدیو/سند/صوت).")
            else:
                data["step"] = "awaiting_cover"
                return await message.reply("🖼 <b>کاور</b> را بفرست (فقط یک‌بار).")

        if step == "awaiting_caption":
            caption = message.text.strip()
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل را بفرست.")
            data["current_file"] = {"caption": caption}
            data["step"] = "awaiting_quality"
            return await message.reply("📽 <b>کیفیت</b> این فایل را وارد کن (مثال: <code>720p</code>):")

        if step == "awaiting_quality":
            quality = message.text.strip()
            if not quality:
                return await message.reply("⚠️ کیفیت خالیه! دوباره بفرست.")
            if "pending_file_id" not in data:
                data["step"] = "awaiting_first_file" if len(data["files"]) == 0 else "awaiting_next_file"
                return await message.reply("⚠️ ابتدا فایل را بفرست.")

            # ثبت فایل در لیست
            data["files"].append({
                "film_id": data["film_id"],
                "file_id": data["pending_file_id"],
                "caption": data["current_file"]["caption"],
                "quality": quality
            })

            # پاک‌سازی وضعیت موقت و پرسیدن «فایل دیگری داری؟»
            data.pop("pending_file_id", None)
            data.pop("current_file", None)
            data["step"] = "confirm_more_files"

            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ بله", callback_data="more_yes"),
                 InlineKeyboardButton("❌ خیر", callback_data="more_no")]
            ])
            return await message.reply(
                f"✅ فایل اضافه شد.\n🎬 عنوان: {data.get('title')}\n📽 کیفیت: {quality}\n\nآیا <b>فایل دیگری</b> برای این عنوان داری؟",
                reply_markup=buttons
            )

        # اگر مرحله‌ای نبود، کاری نکن (ممکن است در پنل/زمان‌بندی باشد)
        return

# ---------------------- 🖼 روتر واحد رسانه‌ای ادمین ----------------------
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_router(client: Client, message: Message):
    """
    هر پیام رسانه‌ای ادمین (عکس/ویدیو/سند/صوت) به این تابع می‌آید.
    - اگر در پنل ادمین باشد، در بخش 5/6 هندل می‌شود.
    - اگر در فلو آپلود باشد، برای کاور یا فایل جدید استفاده می‌شود.
    """
    uid = message.from_user.id

    # فلو آپلود
    if uid in upload_data:
        data = upload_data[uid]
        step = data.get("step")

        if step == "awaiting_cover":
            if not message.photo:
                return await message.reply("⚠️ لطفاً <b>عکس کاور</b> بفرست.")
            data["cover_id"] = message.photo.file_id
            data["step"] = "awaiting_first_file"
            return await message.reply("📤 کاور ثبت شد. حالا <b>فایلِ اول</b> را بفرست (ویدیو/سند/صوت).")

        if step in ("awaiting_first_file", "awaiting_next_file"):
            # پذیرش ویدیو/سند/صوت به‌عنوان فایل
            if message.video:
                file_id = message.video.file_id
            elif message.document:
                file_id = message.document.file_id
            elif message.audio:
                file_id = message.audio.file_id
            else:
                return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")

            data["pending_file_id"] = file_id
            data["step"] = "awaiting_caption"
            return await message.reply("📝 <b>کپشن</b> این فایل را وارد کن:")

        # در سایر مراحل رسانه‌ای کاری نکن
        return

# ---------------------- ادامه/پایان آپلود (دکمه‌ها) ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^more_"))
async def upload_more_files_cb(client: Client, cq: CallbackQuery):
    """
    بعد از ثبت یک فایل، از ادمین می‌پرسیم فایل دیگری دارد یا نه.
    """
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
        # سند نهایی فیلم برای ذخیره در DB
        film_doc = {
            "film_id": film_id,
            "user_id": uid,
            "title": data.get("title"),
            "genre": data.get("genre", ""),
            "year": data.get("year", ""),
            "cover_id": data.get("cover_id"),
            "timestamp": datetime.now(),
            "files": data["files"]
        }
        films_col.update_one({"film_id": film_id}, {"$set": film_doc}, upsert=True)

        deep_link = f"https://t.me/{BOT_USERNAME}?start={film_id}"
        await cq.message.reply(
            f"✅ فیلم ذخیره شد.\n\n🎬 عنوان: {film_doc['title']}\n📂 تعداد فایل: {len(film_doc['files'])}\n🔗 لینک دانلود: {deep_link}"
        )

        # انتخاب ارسال فوری یا زمان‌بندی (ادامه در بخش 5/6)
        await cq.message.reply(
            "🕓 انتخاب کن:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ زمان‌بندی", callback_data=f"sched_yes::{film_id}")],
                [InlineKeyboardButton("📣 ارسال فوری", callback_data=f"sched_no::{film_id}")]
            ])
        )

        _reset_upload_state(uid)
# ---------------------- زمان‌بندی: تاریخ/ساعت/انتخاب کانال ----------------------
@bot.on_callback_query(filters.regex(r"^sched_yes::(.+)$") & filters.user(ADMIN_IDS))
async def ask_schedule_date(client: Client, cq: CallbackQuery):
    """شروع فلو زمان‌بندی: تاریخ → ساعت → کانال مقصد"""
    await cq.answer()
    film_id = cq.data.split("::")[1]
    schedule_data[cq.from_user.id] = {"film_id": film_id, "step": "date"}
    await cq.message.reply("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def schedule_text_router(client: Client, message: Message):
    """روتر اختصاصی متنی برای فلو زمان‌بندی (وقتی schedule_data فعال است)."""
    uid = message.from_user.id
    if uid not in schedule_data:
        return  # اگر در فلو زمان‌بندی نیست، این روتر ساکت می‌ماند (روتر اصلی قبلا تعریف شده)

    data = schedule_data[uid]
    if data.get("step") == "date":
        data["date"] = message.text.strip()
        data["step"] = "time"
        return await message.reply("🕒 ساعت انتشار را وارد کن (HH:MM):")

    if data.get("step") == "time":
        data["time"] = message.text.strip()
        # انتخاب کانال مقصد
        prefix = f"film_sched_save::{data['date']}::{data['time']}"
        rows = [[InlineKeyboardButton(title, callback_data=f"{prefix}::{data['film_id']}::{chat_id}")]
                for title, chat_id in TARGET_CHANNELS.items()]
        rows.append([InlineKeyboardButton("❌ لغو", callback_data="sched_cancel")])
        data["step"] = "channel_await"
        return await message.reply("🎯 کانال مقصد را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^sched_cancel$") & filters.user(ADMIN_IDS))
async def sched_cancel_cb(client: Client, cq: CallbackQuery):
    """لغو فلو زمان‌بندی"""
    await cq.answer()
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("⛔️ زمان‌بندی لغو شد.")

@bot.on_callback_query(filters.regex(r"^film_sched_save::(\d{4}-\d{2}-\d{2})::(\d{2}:\d{2})::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_sched_save_cb(client: Client, cq: CallbackQuery):
    """ذخیره‌ی آیتم زمان‌بندی در DB"""
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
        "film_id": film_id,
        "title": film.get("title", ""),
        "channel_id": channel_id,
        "scheduled_time": dt
    })
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("✅ زمان‌بندی ذخیره شد.")

# ---------------------- انتشار فوری (فقط 1 پست: کاور+کپشن+دکمه+آمار) ----------------------
@bot.on_callback_query(filters.regex(r"^sched_no::(.+)$") & filters.user(ADMIN_IDS))
async def ask_publish_immediate(client: Client, cq: CallbackQuery):
    """انتخاب کانال مقصد برای انتشار فوری"""
    await cq.answer()
    film_id = cq.data.split("::")[1]
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{film_id}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="pub_cancel")])
    await cq.message.reply("📣 می‌خوای همین الان ارسال کنیم؟ کانال رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^film_pub_go::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_pub_go_cb(client: Client, cq: CallbackQuery):
    """ارسال فوری به کانال: کاور + کپشن استاندارد + دکمه دانلود + آمار"""
    await cq.answer()
    film_id, channel_id = cq.data.split("::")[1:]
    channel_id = int(channel_id)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.")

    caption = compose_channel_caption(film)
    try:
        if film.get("cover_id"):
            sent = await client.send_photo(
                channel_id,
                photo=film["cover_id"],
                caption=caption,
                reply_markup=_stats_keyboard(film_id, channel_id, 0)
            )
        else:
            sent = await client.send_message(
                channel_id,
                text=caption,
                reply_markup=_stats_keyboard(film_id, channel_id, 0)
            )
    except Exception as e:
        return await cq.message.edit_text(f"❌ خطا در ارسال: {e}")

    # ذخیره مرجع پیام برای رفرش آمار
    try:
        post_refs.update_one(
            {"film_id": film_id, "channel_id": channel_id},
            {"$set": {"message_id": sent.id}},
            upsert=True
        )
    except Exception:
        pass

    # رفرش اولیه‌ی ویو
    try:
        fresh = await client.get_messages(channel_id, sent.id)
        await client.edit_message_reply_markup(
            chat_id=channel_id,
            message_id=sent.id,
            reply_markup=_stats_keyboard(film_id, channel_id, sent.id, views=fresh.views or 0)
        )
    except Exception:
        pass

    # رفرش نرم ۱۰ ثانیه بعد
    asyncio.create_task(_delayed_stat_refresh(client, film_id, channel_id, sent.id, 10))

    await cq.message.edit_text("✅ پست ارسال شد (کاور + دکمه دانلود + آمار).")

# ---------------------- پنل ادمین: ورودی و صفحه‌ها ----------------------
def kb_admin_main():
    """کیبورد منوی اصلی پنل ادمین"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 لیست/جست‌وجوی فیلم‌ها", callback_data="admin_films_1")],
        [InlineKeyboardButton("⏰ مدیریت زمان‌بندی‌ها", callback_data="admin_sched_list_1")],
        [InlineKeyboardButton("📥 خروجی CSV", callback_data="admin_export_csv")],
    ])

def _fmt_film_info(film: dict) -> str:
    """نمایش خلاصه اطلاعات فیلم"""
    return (f"🎬 <b>{film.get('title','-')}</b>\n"
            f"🎭 ژانر: {film.get('genre','-')}\n"
            f"📆 سال: {film.get('year','-')}\n"
            f"🖼 کاور: {'✅' if film.get('cover_id') else '❌'}\n"
            f"📂 تعداد فایل: {len(film.get('files', []))}\n"
            f"🆔 {film.get('film_id','-')}")

def _paginate(items, page, page_size=10):
    """صفحه‌بندی ساده"""
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], total

@bot.on_message(filters.command("admin") & filters.user(ADMIN_IDS))
async def admin_entry(client: Client, message: Message):
    """ورود به پنل ادمین"""
    await message.reply("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_home$") & filters.user(ADMIN_IDS))
async def admin_home_cb(client: Client, cq: CallbackQuery):
    """بازگشت به منوی اصلی"""
    await cq.answer()
    await cq.message.edit_text("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_films_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_films_list(client: Client, cq: CallbackQuery):
    """لیست فیلم‌ها با صفحه‌بندی"""
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
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_films_{page-1}"))
    if page * 10 < total:
        nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_films_{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔎 جست‌وجو", callback_data="admin_search")])
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("🎬 لیست فیلم‌ها:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^admin_search$") & filters.user(ADMIN_IDS))
async def admin_search_cb(client: Client, cq: CallbackQuery):
    """ورود به حالت جست‌وجو"""
    await cq.answer()
    admin_edit_state[cq.from_user.id] = {"mode": "search"}
    await cq.message.edit_text("🔎 عبارت جست‌وجو را بفرست (عنوان/ژانر/سال/film_id)...")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_edit_router(client: Client, message: Message):
    """روتر متنی پنل ادمین (جست‌وجو/ویرایش)"""
    uid = message.from_user.id
    if uid not in admin_edit_state:
        return

    st = admin_edit_state[uid]
    mode = st.get("mode")
    film_id = st.get("film_id")

    if mode == "search":
        q = message.text.strip()
        regs = {"$regex": q, "$options": "i"}
        films = list(films_col.find({"$or": [
            {"title": regs}, {"genre": regs}, {"year": regs}, {"film_id": regs}
        ]}).sort("timestamp", -1))
        admin_edit_state.pop(uid, None)
        if not films:
            return await message.reply("❌ چیزی یافت نشد. /admin")

        rows = [[InlineKeyboardButton(f"{f.get('title', f['film_id'])} ({f.get('year','-')})", callback_data=f"film_open::{f['film_id']}")] for f in films[:50]]
        rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
        return await message.reply("🔎 نتایج جست‌وجو:", reply_markup=InlineKeyboardMarkup(rows))

    if not film_id:
        admin_edit_state.pop(uid, None)
        return await message.reply("⚠️ کانتکست ویرایش از دست رفت. دوباره امتحان کن.")

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

    # ویرایش فایل‌ها
    idx = st.get("file_index", 0)
    if mode == "file_edit_caption":
        films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.caption": message.text.strip()}})
        admin_edit_state.pop(uid, None)
        return await message.reply("✅ کپشن فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))
    if mode == "file_edit_quality":
        films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.quality": message.text.strip()}})
        admin_edit_state.pop(uid, None)
        return await message.reply("✅ کیفیت فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

@bot.on_callback_query(filters.regex(r"^film_open::(.+)$") & filters.user(ADMIN_IDS))
async def film_open_cb(client: Client, cq: CallbackQuery):
    """نمایش جزئیات فیلم و عملیات"""
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
    """ویرایش عنوان"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_title", "film_id": fid}
    await cq.message.edit_text("🖊 عنوان جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_genre::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_genre_cb(client: Client, cq: CallbackQuery):
    """ویرایش ژانر"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_genre", "film_id": fid}
    await cq.message.edit_text("🎭 ژانر جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_edit_year::(.+)$") & filters.user(ADMIN_IDS))
async def film_edit_year_cb(client: Client, cq: CallbackQuery):
    """ویرایش سال"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "edit_year", "film_id": fid}
    await cq.message.edit_text("📆 سال جدید را بفرست (مثلاً 2024):")

@bot.on_callback_query(filters.regex(r"^film_replace_cover::(.+)$") & filters.user(ADMIN_IDS))
async def film_replace_cover_cb(client: Client, cq: CallbackQuery):
    """جایگزینی کاور"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "replace_cover", "film_id": fid}
    await cq.message.edit_text("🖼 عکس کاور جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^film_files::(.+)$") & filters.user(ADMIN_IDS))
async def film_files_list(client: Client, cq: CallbackQuery):
    """لیست فایل‌های یک فیلم"""
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
    """جزئیات یک فایل و عملیات"""
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
    cap = f.get("caption", "")
    q = f.get("quality", "")
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
    """ورود به حالت ویرایش کپشن فایل"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_caption", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📝 کپشن جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^file_edit_quality::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_quality_cb(client: Client, cq: CallbackQuery):
    """ورود به حالت ویرایش کیفیت فایل"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_quality", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("🎞 کیفیت جدید را بفرست (مثلاً 1080p):")

@bot.on_callback_query(filters.regex(r"^file_replace::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_replace_cb(client: Client, cq: CallbackQuery):
    """جایگزینی فایل رسانه‌ای"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_replace", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست تا جایگزین شود:")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_edit_router(client: Client, message: Message):
    """روتر رسانه‌ای برای حالت‌های ویرایش در پنل ادمین"""
    uid = message.from_user.id
    if uid not in admin_edit_state:
        return

    st = admin_edit_state[uid]
    mode = st.get("mode")
    film_id = st.get("film_id")

    if mode == "replace_cover":
        if not message.photo:
            return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
        films_col.update_one({"film_id": film_id}, {"$set": {"cover_id": message.photo.file_id}})
        admin_edit_state.pop(uid, None)
        return await message.reply("✅ کاور جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{film_id}")]]))

    if mode == "file_replace":
        if message.video:
            fid = message.video.file_id
        elif message.document:
            fid = message.document.file_id
        elif message.audio:
            fid = message.audio.file_id
        else:
            return await message.reply("⚠️ فقط ویدیو/سند/صوت برای جایگزینی قابل قبول است.")
        idx = st.get("file_index", 0)
        films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.file_id": fid}})
        admin_edit_state.pop(uid, None)
        return await message.reply("✅ فایل جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

    if mode == "file_add_pickfile":
        if message.video:
            fid = message.video.file_id
        elif message.document:
            fid = message.document.file_id
        elif message.audio:
            fid = message.audio.file_id
        else:
            return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
        st["tmp_file_id"] = fid
        st["mode"] = "file_add_caption"
        return await message.reply("📝 کپشن فایل جدید را وارد کن:")

@bot.on_callback_query(filters.regex(r"^film_file_add::(.+)$") & filters.user(ADMIN_IDS))
async def film_file_add_cb(client: Client, cq: CallbackQuery):
    """آغاز افزودن فایل جدید از پنل (ابتدا فایل، سپس کپشن، سپس کیفیت)"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    admin_edit_state[cq.from_user.id] = {"mode": "file_add_pickfile", "film_id": fid}
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست:")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_file_add_text_router(client: Client, message: Message):
    """ادامه افزودن فایل جدید: کپشن و کیفیت"""
    uid = message.from_user.id
    if uid not in admin_edit_state:
        return

    st = admin_edit_state[uid]
    if st.get("mode") == "file_add_caption":
        st["tmp_caption"] = message.text.strip()
        st["mode"] = "file_add_quality"
        return await message.reply("📽 کیفیت فایل جدید را وارد کن (مثلاً 720p):")

    if st.get("mode") == "file_add_quality":
        new_q = message.text.strip()
        if not st.get("tmp_file_id"):
            admin_edit_state.pop(uid, None)
            return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
        films_col.update_one({"film_id": st["film_id"]}, {"$push": {"files": {
            "film_id": st["film_id"],
            "file_id": st["tmp_file_id"],
            "caption": st.get("tmp_caption", ""),
            "quality": new_q
        }}})
        admin_edit_state.pop(uid, None)
        return await message.reply("✅ فایل جدید اضافه شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{st['film_id']}")]]))

@bot.on_callback_query(filters.regex(r"^file_move_up::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_up_cb(client: Client, cq: CallbackQuery):
    """جابجایی فایل به بالا"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film:
        return
    files = film.get("files", [])
    if idx <= 0 or idx >= len(files):
        return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx-1], files[idx] = files[idx], files[idx-1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_move_down::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_down_cb(client: Client, cq: CallbackQuery):
    """جابجایی فایل به پایین"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film:
        return
    files = film.get("files", [])
    if idx < 0 or idx >= len(files)-1:
        return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx+1], files[idx] = files[idx], files[idx+1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_delete_confirm::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_confirm_cb(client: Client, cq: CallbackQuery):
    """تایید حذف فایل"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_file_open::{fid}::{idx}")],
        [InlineKeyboardButton("🗑 حذف", callback_data=f"file_delete::{fid}::{idx}")]
    ])
    await cq.message.edit_text("❗️ مطمئنی این فایل حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_delete::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_do_cb(client: Client, cq: CallbackQuery):
    """حذف فایل از آرایه‌ی files"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    files.pop(idx)
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ فایل حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^film_delete_confirm::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_confirm_cb(client: Client, cq: CallbackQuery):
    """تایید حذف کل فیلم"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو", callback_data=f"film_open::{fid}")],
        [InlineKeyboardButton("🗑 حذف قطعی", callback_data=f"film_delete::{fid}")]
    ])
    await cq.message.edit_text("❗️ مطمئنی کل فیلم و فایل‌ها حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^film_delete::(.+)$") & filters.user(ADMIN_IDS))
async def film_delete_do_cb(client: Client, cq: CallbackQuery):
    """حذف سند فیلم از DB"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    films_col.delete_one({"film_id": fid})
    await cq.message.edit_text("✅ فیلم حذف شد.", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^film_pub_pick::(.+)$") & filters.user(ADMIN_IDS))
async def film_pub_pick_channel(client: Client, cq: CallbackQuery):
    """انتخاب کانال مقصد برای انتشار فوری از صفحه‌ی فیلم"""
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
    """شروع فلو زمان‌بندی از صفحه‌ی فیلم"""
    await cq.answer()
    fid = cq.matches[0].group(1)
    schedule_data[cq.from_user.id] = {"film_id": fid, "step": "date"}
    await cq.message.edit_text("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^admin_sched_list_(\d+)$") & filters.user(ADMIN_IDS))
async def admin_sched_list_cb(client: Client, cq: CallbackQuery):
    """لیست زمان‌بندی‌ها با صفحه‌بندی"""
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
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_sched_list_{page-1}"))
    if page * 10 < total:
        nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_sched_list_{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("⏰ زمان‌بندی‌های ثبت‌شده:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^sched_open::(.+)$") & filters.user(ADMIN_IDS))
async def sched_open_cb(client: Client, cq: CallbackQuery):
    """نمایش جزئیات یک آیتم زمان‌بندی"""
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
    """حذف آیتم زمان‌بندی"""
    await cq.answer()
    sid = cq.matches[0].group(1)
    try:
        scheduled_posts.delete_one({"_id": ObjectId(sid)})
        await cq.message.edit_text("✅ حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))
    except Exception as e:
        await cq.message.edit_text(f"❌ خطا در حذف: {e}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))

# ---------------------- خروجی CSV ----------------------
@bot.on_callback_query(filters.regex(r"^admin_export_csv$") & filters.user(ADMIN_IDS))
async def admin_export_csv_cb(client: Client, cq: CallbackQuery):
    """خروجی CSV لیست فیلم‌ها"""
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
# ---------------------- ⏰ هندل انتخاب زمان‌بندی یا ارسال فوری ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^sched_"))
async def handle_schedule_choice(client: Client, cq: CallbackQuery):
    choice, film_id = cq.data.split("::", 1)
    admin_id = cq.from_user.id

    if choice == "sched_yes":
        schedule_data[admin_id] = {"film_id": film_id, "step": "awaiting_date"}
        await cq.message.reply("📅 تاریخ ارسال را وارد کن (YYYY-MM-DD):")
    elif choice == "sched_no":
        film = films_col.find_one({"film_id": film_id})
        if not film:
            return await cq.answer("⚠️ فیلم پیدا نشد.", show_alert=True)
        await publish_film_to_channel(client, film, immediate=True)
        await cq.answer("✅ ارسال فوری انجام شد.", show_alert=True)

# ---------------------- 📅 دریافت تاریخ و ساعت زمان‌بندی ----------------------
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def schedule_text_handler(client: Client, message: Message):
    uid = message.from_user.id
    if uid not in schedule_data:
        return  # نه در حال زمان‌بندی هستیم، نه کاری کنیم

    step = schedule_data[uid]["step"]

    if step == "awaiting_date":
        date_text = message.text.strip()
        try:
            datetime.strptime(date_text, "%Y-%m-%d")
        except ValueError:
            return await message.reply("⚠️ تاریخ نامعتبر. فرمت درست: YYYY-MM-DD")
        schedule_data[uid]["date"] = date_text
        schedule_data[uid]["step"] = "awaiting_time"
        return await message.reply("🕒 ساعت ارسال را وارد کن (HH:MM):")

    if step == "awaiting_time":
        time_text = message.text.strip()
        try:
            datetime.strptime(time_text, "%H:%M")
        except ValueError:
            return await message.reply("⚠️ ساعت نامعتبر. فرمت درست: HH:MM")
        schedule_data[uid]["time"] = time_text
        schedule_data[uid]["step"] = "awaiting_channel"
        btns = [[InlineKeyboardButton(f"📢 {ch}", callback_data=f"choose_channel::{ch}")]
                for ch in REQUIRED_CHANNELS]
        return await message.reply("📡 کانال مقصد را انتخاب کن:", reply_markup=InlineKeyboardMarkup(btns))

# ---------------------- 🎯 انتخاب کانال زمان‌بندی ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^choose_channel::"))
async def choose_channel_for_schedule(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    if uid not in schedule_data:
        return await cq.answer("⚠️ اطلاعات زمان‌بندی پیدا نشد.", show_alert=True)

    ch = cq.data.split("::", 1)[1]
    data = schedule_data[uid]
    dt_str = f"{data['date']} {data['time']}"
    send_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")

    scheduled_posts_col.insert_one({
        "film_id": data["film_id"],
        "channel": ch,
        "send_time": send_dt
    })

    await cq.message.reply(f"✅ زمان‌بندی انجام شد:\n📅 {dt_str}\n📡 کانال: {ch}")
    schedule_data.pop(uid, None)

# ---------------------- 📣 تابع انتشار پست به کانال ----------------------
async def publish_film_to_channel(client: Client, film_doc, immediate=False, channel=None):
    ch = channel or REQUIRED_CHANNELS[0]  # پیش‌فرض اولین کانال
    caption = f"🎬 <b>{film_doc['title']}</b>\n🎭 {film_doc['genre']}\n📅 {film_doc['year']}"
    if film_doc.get("cover_id"):
        await client.send_photo(ch, film_doc["cover_id"], caption=caption)

    for f in film_doc["files"]:
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 دانلود", url=f"https://t.me/{BOT_USERNAME}?start={film_doc['film_id']}")]
        ])
        await client.send_video(ch, f["file_id"], caption=f["caption"], reply_markup=btns)

# ---------------------- 🗄 پنل ادمین ----------------------
@bot.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_IDS))
async def admin_panel(client: Client, message: Message):
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 لیست فیلم‌ها", callback_data="admin_list_films")],
        [InlineKeyboardButton("📊 خروجی CSV آمار", callback_data="export_csv")],
        [InlineKeyboardButton("📢 ارسال پیام همگانی", callback_data="broadcast_all")]
    ])
    await message.reply("🛠 پنل مدیریت:", reply_markup=btns)

# ---------------------- 🎬 لیست فیلم‌ها ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex("^admin_list_films$"))
async def admin_list_films(client: Client, cq: CallbackQuery):
    films = films_col.find().sort("timestamp", -1)
    if films.count() == 0:
        return await cq.answer("📭 هیچ فیلمی موجود نیست.", show_alert=True)

    text = "🎬 لیست فیلم‌ها:\n\n"
    for f in films:
        text += f"▫️ {f['title']} — /edit_{f['film_id']}\n"
    await cq.message.reply(text)

# ---------------------- 📊 خروجی CSV ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex("^export_csv$"))
async def export_csv(client: Client, cq: CallbackQuery):
    import csv, io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Film ID", "Title", "Views", "Downloads", "Shares"])
    for f in films_col.find():
        writer.writerow([
            f["film_id"],
            f["title"],
            f.get("views", 0),
            f.get("downloads", 0),
            f.get("shares", 0)
        ])
    output.seek(0)
    await cq.message.reply_document(("stats.csv", output.read().encode()), caption="📊 خروجی CSV")
    output.close()

# ---------------------- 📢 ارسال پیام همگانی ----------------------
@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex("^broadcast_all$"))
async def broadcast_all(client: Client, cq: CallbackQuery):
    bc_data[cq.from_user.id] = {"step": "awaiting_message"}
    await cq.message.reply("📝 متن پیام همگانی را ارسال کن:")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def broadcast_message(client: Client, message: Message):
    uid = message.from_user.id
    if uid in bc_data and bc_data[uid]["step"] == "awaiting_message":
        text = message.text
        users = users_col.find()
        sent = 0
        for u in users:
            try:
                await client.send_message(u["user_id"], text)
                sent += 1
            except:
                pass
        bc_data.pop(uid, None)
        await message.reply(f"✅ پیام برای {sent} کاربر ارسال شد.")
# ---------------------- ⏱ زمان‌بند خودکار: ارسال پست‌های زمان‌بندی‌شده ----------------------
async def send_scheduled_posts():
    """
    هر 1 دقیقه:
      • scheduled_posts با زمان <= حالا را پیدا می‌کند
      • دقیقاً یک پست کانالی می‌فرستد: کاور + کپشن + دکمه دانلود + آمار
      • آیتم زمان‌بندی را حذف می‌کند
    """
    try:
        now = datetime.now()
        posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now}}))
    except Exception as e:
        print("DB unavailable:", e)
        return

    for post in posts:
        film = films_col.find_one({"film_id": post["film_id"]})
        if not film:
            scheduled_posts.delete_one({"_id": post["_id"]})
            continue

        caption = compose_channel_caption(film)
        try:
            if film.get("cover_id"):
                sent = await bot.send_photo(
                    post["channel_id"],
                    photo=film["cover_id"],
                    caption=caption,
                    reply_markup=_stats_keyboard(film["film_id"], post["channel_id"], 0)
                )
            else:
                sent = await bot.send_message(
                    post["channel_id"],
                    text=caption,
                    reply_markup=_stats_keyboard(film["film_id"], post["channel_id"], 0)
                )

            # مرجع پیام برای رفرش‌های بعدی
            try:
                post_refs.update_one(
                    {"film_id": film["film_id"], "channel_id": post["channel_id"]},
                    {"$set": {"message_id": sent.id}},
                    upsert=True
                )
            except Exception:
                pass

            # آپدیت اولیه views در کیبورد
            try:
                fresh = await bot.get_messages(post["channel_id"], sent.id)
                await bot.edit_message_reply_markup(
                    chat_id=post["channel_id"],
                    message_id=sent.id,
                    reply_markup=_stats_keyboard(film["film_id"], post["channel_id"], sent.id, views=fresh.views or 0)
                )
            except Exception:
                pass

            # رفرش نرم چند ثانیه بعد
            asyncio.create_task(_delayed_stat_refresh(bot, film["film_id"], post["channel_id"], sent.id, 10))

        except Exception as e:
            print("❌ scheduled send error:", e)

        scheduled_posts.delete_one({"_id": post["_id"]})

# ---------------------- ♻️ جاب رفرش دوره‌ای آمار زیر پست ----------------------
async def refresh_stats_job():
    """هر 2 دقیقه همه پست‌های ثبت‌شده را با آخرین views رفرش می‌کند."""
    try:
        refs = list(post_refs.find({}))
    except Exception as e:
        print("DB unavailable (post_refs):", e)
        return

    for ref in refs:
        film_id    = ref.get("film_id")
        channel_id = ref.get("channel_id")
        message_id = ref.get("message_id")
        if not (film_id and channel_id and message_id):
            continue

        views = 0
        try:
            msg = await bot.get_messages(channel_id, message_id)
            views = int(msg.views or 0)
        except Exception:
            pass

        try:
            await bot.edit_message_reply_markup(
                chat_id=channel_id,
                message_id=message_id,
                reply_markup=_stats_keyboard(film_id, channel_id, message_id, views=views)
            )
        except Exception:
            pass

# ---------------------- 📊 کال‌بک‌های آمار زیر پست ----------------------
@bot.on_callback_query(filters.regex(r"^stat_refresh::(.+)::(-?\d+)::(\d+)$"))
async def stat_refresh_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id    = cq.matches[0].group(1)
    channel_id = int(cq.matches[0].group(2))
    message_id = int(cq.matches[0].group(3))

    views = 0
    try:
        msg = await client.get_messages(channel_id, message_id)
        views = int(msg.views or 0)
    except Exception:
        pass

    try:
        await client.edit_message_reply_markup(
            chat_id=channel_id,
            message_id=message_id,
            reply_markup=_stats_keyboard(film_id, channel_id, message_id, views=views)
        )
    except Exception:
        pass

@bot.on_callback_query(filters.regex(r"^stat_share::(.+)::(-?\d+)::(\d+)$"))
async def stat_share_cb(client: Client, cq: CallbackQuery):
    film_id    = cq.matches[0].group(1)
    channel_id = int(cq.matches[0].group(2))
    message_id = int(cq.matches[0].group(3))

    # افزایش شمارش اشتراک
    try:
        stats_col.update_one({"film_id": film_id}, {"$inc": {"shares": 1}}, upsert=True)
    except Exception:
        pass

    # رفرش سریع کیبورد
    try:
        msg = await client.get_messages(channel_id, message_id)
        views = int(msg.views or 0)
        await client.edit_message_reply_markup(
            chat_id=channel_id,
            message_id=message_id,
            reply_markup=_stats_keyboard(film_id, channel_id, message_id, views=views)
        )
    except Exception:
        pass

    await cq.answer("🔁 شمارش اشتراک افزوده شد.", show_alert=False)
    try:
        await client.send_message(cq.from_user.id, f"✨ این لینک را برای دوستانت بفرست:\nhttps://t.me/{BOT_USERNAME}?start={film_id}")
    except Exception:
        pass

scheduler = AsyncIOScheduler()

async def main():
    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true"
        with urllib.request.urlopen(url, timeout=10) as r:
            print(f"🧹 Webhook delete HTTP status: {r.status}")
    except Exception as e:
        print("⚠️ deleteWebhook (HTTP) error:", e)

    scheduler.add_job(send_scheduled_posts, "interval", minutes=1)
    scheduler.add_job(refresh_stats_job,    "interval", minutes=2)
    scheduler.start()
    print("📅 Scheduler started successfully!")
    print("🤖 Bot started. Waiting for updates…")

    await idle()


    
# ---------------------- ▶️ اجرای نهایی ----------------------
if __name__ == "__main__":
    bot.run(main())  # ✅ حتما با پرانتز
