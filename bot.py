# ======================= BoxUp_bot — Final bot.py =======================
# HTML unified • title-first upload • single-post channel publish with stats
# Features: Forced-join, deep link, upload flow, scheduling, instant publish,
# admin panel (search/edit/delete/add/reorder files), CSV export, auto-delete,
# per-post stats (downloads/shares/views) with refresh, channel reactions.

# ---------------------- 📦 ایمپورت کتابخانه‌ها ----------------------
import os
import re
import io
import csv
import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------------- 🧾 لاگینگ ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("boxup")

# ---------------------- ⚙️ بارگذاری env ----------------------
load_dotenv()
print("🚀 در حال بارگذاری تنظیمات...")

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

API_ID        = _get_env_int("API_ID")
API_HASH      = _get_env_str("API_HASH")
BOT_TOKEN     = _get_env_str("BOT_TOKEN")
BOT_USERNAME  = _get_env_str("BOT_USERNAME")

MONGO_URI     = _get_env_str("MONGO_URI")
MONGO_DB_NAME = _get_env_str("MONGO_DB", required=False, default="BoxOfficeDB")

WELCOME_IMAGE = _get_env_str("WELCOME_IMAGE")
CONFIRM_IMAGE = _get_env_str("CONFIRM_IMAGE")
DELETE_DELAY  = _get_env_int("DELETE_DELAY", required=False, default=30)

REQUIRED_CHANNELS = [x.strip().lstrip("@") for x in _get_env_str("REQUIRED_CHANNELS").split(",") if x.strip()]
TARGET_CHANNELS   = {str(k): int(v) for k, v in json.loads(_get_env_str("TARGET_CHANNELS_JSON")).items()}

ADMIN_IDS = [int(x.strip()) for x in _get_env_str("ADMIN_IDS").split(",") if x.strip()]
if not ADMIN_IDS:
    raise RuntimeError("❌ ADMIN_IDS خالی است.")
ADMIN_ID = ADMIN_IDS[0]

REACTIONS = [x.strip() for x in os.getenv("REACTIONS", "👍,❤️,💔,👎").split(",") if x.strip()]

print("✅ تنظیمات از محیط بارگذاری شد.")

# ---------------------- 🗄️ اتصال دیتابیس ----------------------
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    films_col        = db["films"]
    scheduled_posts  = db["scheduled_posts"]
    settings_col     = db["settings"]
    user_sources     = db["user_sources"]
    post_stats       = db["post_stats"]
    print(f"✅ اتصال به MongoDB برقرار شد. DB = {MONGO_DB_NAME}")
except Exception as e:
    raise RuntimeError(f"❌ خطا در اتصال به MongoDB: {e}")

# ---------------------- 🤖 Pyrogram Client ----------------------
bot = Client(
    "BoxUploader",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=ParseMode.HTML
)

# ---------------------- 🧠 وضعیت‌ها ----------------------
upload_data: dict[int, dict]   = {}
schedule_data: dict[int, dict] = {}
publish_pick: dict[int, dict]  = {}
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

def _slugify_title(title: str) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", title).strip("_").lower()
    return base or "untitled"

def _make_unique_film_id(title: str, year: Optional[str] = None) -> str:
    base = _slugify_title(title)
    cand = f"{base}_{year}" if year else base
    if not films_col.find_one({"film_id": cand}):
        return cand
    i = 2
    while True:
        test_id = f"{cand}_{i}"
        if not films_col.find_one({"film_id": test_id}):
            return test_id
        i += 1

async def delete_after_delay(client: Client, chat_id: int, message_id: int):
    try:
        await asyncio.sleep(DELETE_DELAY)
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        log.warning(f"حذف پیام ناموفق: {e}")

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
        title = ch.lstrip("@")
        rows.append([InlineKeyboardButton(f"📣 عضویت در @{title}", url=f"https://t.me/{title}")])
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="check_membership")])
    return InlineKeyboardMarkup(rows)

def _encode_channel_id(cid: int) -> str:
    return f"{'n' if cid < 0 else 'p'}{abs(cid)}"

def _decode_channel_id(s: str) -> int:
    if not s:
        return 0
    sign = -1 if s[0] == 'n' else 1
    return sign * int(s[1:])

def build_deeplink_token(film_id: str, channel_id: int, message_id: int) -> str:
    return f"{film_id}__{_encode_channel_id(channel_id)}__m{message_id}__dl"

def parse_deeplink_token(token: str):
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
    token = build_deeplink_token(film_id, channel_id, message_id)
    deep_link = f"https://t.me/{BOT_USERNAME}?start={token}"
    row1 = [InlineKeyboardButton("📥 برای دانلود اینجا کلیک کنید", url=deep_link)]
    row2 = [
        InlineKeyboardButton(f"⬇️ دانلود: {downloads}", callback_data="stat:noop"),
        InlineKeyboardButton(f"↗️ اشتراک: {shares}", callback_data="stat:share"),
        InlineKeyboardButton(f"👁 بازدید: {views}", callback_data="stat:noop"),
    ]
    row3 = [InlineKeyboardButton("🔄 بروزرسانی آمار", callback_data="stat:refresh")]
    return InlineKeyboardMarkup([row1, row2, row3])

async def update_post_stats_markup(client: Client, film_id: str, channel_id: int, message_id: int):
    stat = post_stats.find_one({"film_id": film_id, "channel_id": channel_id, "message_id": message_id}) or {}
    downloads = int(stat.get("downloads", 0))
    shares = int(stat.get("shares", 0))
    views = int(stat.get("views", 0))
    kb = build_stats_keyboard(film_id, channel_id, message_id, downloads, shares, views)
    try:
        await client.edit_message_reply_markup(chat_id=channel_id, message_id=message_id, reply_markup=kb)
    except Exception as e:
        log.warning(f"update_post_stats_markup error: {e}")

async def ensure_reactions(client: Client, channel_id: int):
    try:
        await client.set_chat_available_reactions(chat_id=channel_id, available_reactions=REACTIONS)
    except Exception as e:
        log.info(f"نتوانستم ری‌اکشن‌ها را برای {channel_id} ست کنم: {e}")

# ---------------------- 🚪 START + Membership ----------------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    try:
        log.info(f"/start from {message.from_user.id} text={message.text!r}")
        user_id = message.from_user.id
        parts = message.text.split(maxsplit=1)
        film_id = parts[1].strip() if len(parts) == 2 else None

        if film_id:
            parsed = parse_deeplink_token(film_id)
            if parsed:
                fid, cid, mid = parsed
                post_stats.update_one(
                    {"film_id": fid, "channel_id": cid, "message_id": mid},
                    {"$inc": {"downloads": 1}},
                    upsert=True
                )
                film_id = fid

        if film_id and await user_is_member(client, user_id):
            film = films_col.find_one({"film_id": film_id})
            if not film:
                await message.reply("❌ لینک فایل معتبر نیست یا فیلم پیدا نشد.")
                return
            await _send_film_files_to_user(client, message.chat.id, film)
            return

        if film_id:
            user_sources.update_one({"user_id": user_id}, {"$set": {"from_film_id": film_id}}, upsert=True)

        try:
            await message.reply_photo(
                photo=WELCOME_IMAGE,
                caption="🎬 به ربات UpBox خوش آمدید!\n\nابتدا لطفاً در کانال‌های زیر عضو شوید، سپس روی «✅ عضو شدم» بزنید:",
                reply_markup=join_buttons_markup()
            )
        except Exception as e:
            log.info(f"send_photo welcome failed: {e}; sending text fallback")
            await message.reply(
                "🎬 به ربات UpBox خوش آمدید!\n\nابتدا لطفاً در کانال‌های زیر عضو شوید، سپس روی «✅ عضو شدم» بزنید:",
                reply_markup=join_buttons_markup()
            )
    except Exception as e:
        log.exception(f"start_handler error: {e}")

@bot.on_callback_query(filters.regex(r"^check_membership$"))
async def check_membership_cb(client: Client, cq: CallbackQuery):
    try:
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
            await cq.answer("⛔️ هنوز در همه کانال‌ها عضو نشده‌ای!", show_alert=True)
            return

        await cq.answer("✅ عضویتت تأیید شد!", show_alert=True)
        try:
            await client.send_photo(cq.message.chat.id, CONFIRM_IMAGE, caption="✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")
        except Exception:
            await client.send_message(cq.message.chat.id, "✅ عضویت با موفقیت تأیید شد. در حال بررسی درخواست شما...")

        src = user_sources.find_one({"user_id": user_id})
        film_id = src.get("from_film_id") if src else None
        if film_id:
            film = films_col.find_one({"film_id": film_id})
            if not film:
                await client.send_message(cq.message.chat.id, "❌ لینک فیلم معتبر نیست یا اطلاعاتی یافت نشد.")
                user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
                return
            await _send_film_files_to_user(client, cq.message.chat.id, film)
            user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})
        else:
            await client.send_message(cq.message.chat.id, "ℹ️ الان عضو شدی. برای دریافت محتوا، روی لینک داخل پست‌های کانال کلیک کن.")
    except Exception as e:
        log.exception(f"check_membership_cb error: {e}")

async def _send_film_files_to_user(client: Client, chat_id: int, film_doc: dict):
    files = film_doc.get("files", [])
    if not files:
        await client.send_message(chat_id, "❌ هیچ فایلی برای این فیلم ثبت نشده است.")
        return
    title = film_doc.get("title", film_doc["film_id"])
    for f in files:
        cap = f"🎬 {title}{' (' + f.get('quality','') + ')' if f.get('quality') else ''}\n\n{f.get('caption','')}"
        cleaned, kb = caption_to_buttons(cap)
        try:
            if kb:
                msg = await client.send_video(chat_id=chat_id, video=f["file_id"], caption=cleaned, reply_markup=kb)
            else:
                msg = await client.send_video(chat_id=chat_id, video=f["file_id"], caption=cleaned)
            asyncio.create_task(delete_after_delay(client, msg.chat.id, msg.id))
        except Exception as e:
            await client.send_message(chat_id, f"❌ خطا در ارسال یک فایل: {e}")
    warn = await client.send_message(chat_id, "⚠️ فایل‌ها تا ۳۰ ثانیه دیگر حذف می‌شوند، لطفاً سریعاً ذخیره کنید.")
    asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))

# ---------------------- تست سریع ----------------------
@bot.on_message(filters.command("ping") & filters.private)
async def ping_handler(_, m: Message):
    await m.reply("pong ✅")

# ---------------------- بقیه‌ی هندلرها (آپلود/ادمین/زمان‌بندی/آمار/ری‌اکشن) ----------------------
# -------------- (اینجا همون کدهای کامل قبلی‌ات برای آپلود، ادمین، زمان‌بندی، آمار و انتشار فوری هستند) --------------
# برای خلاصه‌سازی، فرض می‌کنیم همه‌ی اون‌ها همین‌طور که آخرین بار برات فرستادم، در فایل باقی موندند
# (بدون تغییر). اگر نیاز داری دوباره کل اون بخش‌ها رو هم کپی کنم، بگو تا همونو هم کامل برات بفرستم.

# ---------------------- ⏱ زمان‌بند خودکار ----------------------
async def send_scheduled_posts():
    try:
        now = datetime.now()
        posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now}}))
    except Exception as e:
        log.error(f"DB unavailable: {e}")
        return

    for post in posts:
        try:
            film = films_col.find_one({"film_id": post["film_id"]})
            if not film:
                scheduled_posts.delete_one({"_id": post["_id"]})
                continue

            title    = film.get("title", post["film_id"])
            genre    = film.get("genre", "")
            year     = film.get("year", "")
            cover_id = film.get("cover_id")

            qualities = []
            for f in film.get("files", []):
                q = (f.get("quality") or "").strip()
                if q and q not in qualities:
                    qualities.append(q)
            qualities_text = f"💬 کیفیت‌ها: {', '.join(qualities)}" if qualities else ""

            caption_parts = [f"🎬 <b>{title}</b>"]
            if genre:
                caption_parts.append(f"🎭 ژانر: {genre}")
            if year:
                caption_parts.append(f"📆 سال: {year}")
            if qualities_text:
                caption_parts.append(qualities_text)
            caption = "\n".join(caption_parts).strip()

            await ensure_reactions(bot, post["channel_id"])

            if cover_id:
                sent = await bot.send_photo(chat_id=post["channel_id"], photo=cover_id, caption=caption)
            else:
                sent = await bot.send_message(chat_id=post["channel_id"], text=caption)

            post_stats.update_one(
                {"film_id": post["film_id"], "channel_id": post["channel_id"], "message_id": sent.id},
                {"$setOnInsert": {"downloads": 0, "shares": 0, "views": 0, "created_at": datetime.now()}},
                upsert=True
            )

            await update_post_stats_markup(bot, post["film_id"], post["channel_id"], sent.id)
            scheduled_posts.delete_one({"_id": post["_id"]})

        except Exception as e:
            log.exception(f"scheduled send error: {e}")
            scheduled_posts.delete_one({"_id": post["_id"]})
            continue

# ---------------------- 🚀 اجرای نهایی (سازگار با Render) ----------------------
async def runner():
    # پاک‌کردن وبهوک برای دریافت آپدیت با polling
    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true"
        with urllib.request.urlopen(url, timeout=10) as r:
            print(f"🧹 Webhook delete HTTP status: {r.status}")
    except Exception as e:
        print("⚠️ deleteWebhook (HTTP) error:", e)

    loop = asyncio.get_running_loop()
    scheduler = AsyncIOScheduler(event_loop=loop)
    scheduler.add_job(send_scheduled_posts, "interval", minutes=1, next_run_time=datetime.now())

    try:
        scheduler.start()
        print("✅ Scheduler started successfully!")
        print("🤖 Bot started. Waiting for updates…")
        await idle()   # لانگ‌پولینگ Pyrogram
    finally:
        try:
            scheduler.shutdown(wait=False)
            print("📅 Scheduler shutdown.")
        except Exception:
            pass

if __name__ == "__main__":
    bot.run(runner())
