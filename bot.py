# ======================= BoxUp_bot — Final bot.py (TZ-safe) =======================
# 🇮🇷 توضیحات کامل فارسی — نسخه پایدار، پیشرفته و آماده‌ی دیپلوی در Render (Background Worker)
# امکانات: عضویت اجباری، دیپ‌لینک (F<film_id>), آپلود چندمرحله‌ای ادمین، پنل ادمین کامل،
# زمان‌بندی ارسال، آمار و ری‌اکشن، دکمه «💬 نظر بده» (Discussion)، CSV Export، آپدیت بازدیدها،
# و مدیریت FloodWait — همه‌ی زمان‌ها timezone-aware هستند.

import os
import re
import io
import csv
import asyncio
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
load_dotenv()

import logging
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from pyrogram.errors import FloodWait

from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

# ---------------------- ⚙️ تنظیمات از محیط ----------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "BoxUp_bot")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "BoxOfficeDB")

WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.imgur.com/uZqKsRs.png")
CONFIRM_IMAGE = os.getenv("CONFIRM_IMAGE", "https://i.imgur.com/fAGPuXo.png")
DELETE_DELAY = int(os.getenv("DELETE_DELAY", "30"))

# REQUIRED_CHANNELS نمونه: BoxOffice_Animation,BoxOfficeMoviiie,BoxOffice_Irani,BoxOfficeGoftegu
REQUIRED_CHANNELS = [c.strip() for c in os.getenv("REQUIRED_CHANNELS", "").split(",") if c.strip()]

# ADMIN_IDS نمونه: 7872708405,6867380442
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# TARGET_CHANNELS نمونه: ایرانی:-1002422139602,فیلم:-1002601782167,انیمیشن:-1002573288143
_target_pairs = [p for p in os.getenv("TARGET_CHANNELS", "").split(",") if p.strip()]
TARGET_CHANNELS = {}
for p in _target_pairs:
    try:
        title, cid = p.split(":", 1)
        TARGET_CHANNELS[title.strip()] = int(cid.strip())
    except Exception:
        pass

# منطقه زمانی (آلمان)
TZ_DE = pytz.timezone("Europe/Berlin")

# ---------------------- 🖥 لاگ‌ها ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("boxup")

# ---------------------- 📡 MongoDB ----------------------
mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DB]
films_col = db["films"]
ch_posts_col = db["channel_posts"]
scheduled_posts = db["scheduled_posts"]
reactions_log = db["reactions_log"]

# ---------------------- 🤖 Bot Client ----------------------
bot = Client(
    "BoxUp_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

# ---------------------- 🧠 State ----------------------
upload_data = {}        # uid -> {step,title,genre,year,cover_id,film_id,files[],pending_file_id,current_file}
schedule_data = {}      # uid -> {step, film_id, date, time}
admin_edit_state = {}   # uid -> {mode, film_id, file_index, ...}

# ---------------------- 🛠 Helpers ----------------------
def slugify(text: str) -> str:
    text = re.sub(r"[^\w\s\-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text.strip())
    text = re.sub(r"_+", "_", text)
    return text.lower()[:64] or "untitled"

def compose_channel_caption(film: dict) -> str:
    title = film.get("title", "")
    genre = film.get("genre", "")
    year = film.get("year", "")
    fid = film.get("film_id", "")
    deep_link = f"https://t.me/{BOT_USERNAME}?start=F{fid}"
    lines = [f"🎬 <b>{title}</b>"]
    if genre: lines.append(f"🎭 ژانر: {genre}")
    if year:  lines.append(f"📆 سال: {year}")
    lines += ["", f"🧩 لینک ربات و فایل‌ها: {deep_link}"]
    return "\n".join(lines)

async def user_is_subscribed(client: Client, user_id: int) -> bool:
    for channel in REQUIRED_CHANNELS:
        try:
            member = await client.get_chat_member(channel, user_id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
                return False
        except Exception:
            return False
    return True

def get_subscribe_buttons() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"عضویت در @{ch}", url=f"https://t.me/{ch}")] for ch in REQUIRED_CHANNELS]
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="check_subscription")])
    return InlineKeyboardMarkup(rows)

def build_post_link_for_comments(channel_id: int, message_id: int) -> str:
    # برای کانال‌های -100...
    abs_id = str(channel_id).replace("-100", "") if str(channel_id).startswith("-100") else str(abs(channel_id))
    return f"https://t.me/c/{abs_id}/{message_id}?comment=1"

def build_channel_keyboard(channel_id: int, message_id: int, film_id: str, stats_doc: dict) -> InlineKeyboardMarkup:
    views = int(stats_doc.get("views", 0))
    downloads = int(stats_doc.get("downloads", 0))
    shares = int(stats_doc.get("shares", 0))
    reactions = stats_doc.get("reactions", {}) or {}
    like = int(reactions.get("like", 0))
    heart = int(reactions.get("heart", 0))
    broken = int(reactions.get("broken", 0))
    dislike = int(reactions.get("dislike", 0))
    comments_url = build_post_link_for_comments(channel_id, message_id)
    rows = [
        [InlineKeyboardButton(f"👁 {views}", callback_data="noop"),
         InlineKeyboardButton(f"⬇️ {downloads}", callback_data="noop"),
         InlineKeyboardButton(f"🔁 {shares}", callback_data=f"share::{channel_id}::{message_id}")],
        [InlineKeyboardButton(f"👍 {like}",   callback_data=f"react::like::{channel_id}::{message_id}"),
         InlineKeyboardButton(f"❤️ {heart}",  callback_data=f"react::heart::{channel_id}::{message_id}"),
         InlineKeyboardButton(f"💔 {broken}", callback_data=f"react::broken::{channel_id}::{message_id}"),
         InlineKeyboardButton(f"👎 {dislike}",callback_data=f"react::dislike::{channel_id}::{message_id}")],
        [InlineKeyboardButton("💬 نظر بده", url=comments_url)]
    ]
    return InlineKeyboardMarkup(rows)

async def check_discussion_linked(client: Client, channel_id: int) -> bool:
    try:
        chat = await client.get_chat(channel_id)
        return bool(getattr(chat, "linked_chat", None))
    except Exception:
        return False

def _paginate(items, page, page_size=10):
    total = len(items); start = (page-1)*page_size; end = start+page_size
    return items[start:end], total

# ---------------------- 🚦 /start + عضویت + دیپ‌لینک ----------------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    uid = message.from_user.id
    args = message.text.split()
    payload = args[1] if len(args) == 2 else None

    if payload and payload.startswith("F"):
        film_id = payload[1:]
        if "film_requests" not in client.__dict__:
            client.film_requests = {}
        client.film_requests[uid] = film_id

    kb = get_subscribe_buttons()
    try:
        await message.reply_photo(
            WELCOME_IMAGE,
            caption="🎬 به ربات UpBox خوش آمدی!\nبرای دسترسی، ابتدا در کانال‌های زیر عضو شو و بعد روی <b>✅ عضو شدم</b> بزن.",
            reply_markup=kb
        )
    except Exception:
        await message.reply("🎬 به ربات UpBox خوش آمدی!\nلطفاً در کانال‌های زیر عضو شو و بعد روی «✅ عضو شدم» بزن.", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^check_subscription$"))
async def check_subscription_cb(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    ok = await user_is_subscribed(client, uid)
    if not ok:
        return await cq.answer("❗️ هنوز عضو همه کانال‌ها نشدی.", show_alert=True)

    await cq.message.delete()
    try:
        await client.send_photo(uid, CONFIRM_IMAGE, caption="✅ عضویتت تأیید شد! حالا می‌تونی از امکانات ربات استفاده کنی.")
    except Exception:
        await client.send_message(uid, "✅ عضویتت تأیید شد! حالا می‌تونی از امکانات ربات استفاده کنی.")

    film_id = getattr(client, "film_requests", {}).pop(uid, None)
    if film_id:
        film = films_col.find_one({"film_id": film_id})
        if not film:
            return await client.send_message(uid, "⚠️ متأسفانه فیلم درخواست‌شده پیدا نشد.")
        files = film.get("files", [])
        if not files:
            return await client.send_message(uid, "⚠️ هنوز برای این فیلم فایلی ثبت نشده.")
        warn_msg = await client.send_message(uid, "⚠️ فایل‌ها تا ۳۰ ثانیه دیگر حذف می‌شوند. لطفاً ذخیره‌شان کن.")
        sent_msgs = [warn_msg]
        for f in files:
            fid = f.get("file_id")
            cap = f.get("caption", "")
            try:
                m = await client.send_video(uid, fid, caption=cap)
            except Exception:
                try:
                    m = await client.send_document(uid, fid, caption=cap)
                except Exception:
                    m = await client.send_message(uid, cap or "فایل")
            sent_msgs.append(m)
        await asyncio.sleep(DELETE_DELAY)
        for m in sent_msgs:
            try:
                await m.delete()
            except Exception:
                pass

# ---------------------- ⬆️ فلو آپلود ادمین ----------------------
@bot.on_message(filters.command("upload") & filters.private & filters.user(ADMIN_IDS))
async def upload_command(client: Client, message: Message):
    uid = message.from_user.id
    upload_data[uid] = {"step": "awaiting_title", "files": []}
    await message.reply("🎬 لطفاً <b>عنوان</b> را وارد کن (مثال: آواتار ۲).")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_text_router(client: Client, message: Message):
    uid = message.from_user.id

    # 1) زمان‌بندی
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

    # 2) پنل ادمین — جستجو/ویرایش
    if uid in admin_edit_state:
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

        # فایل‌ها
        idx = st.get("file_index", 0)
        if mode == "file_edit_caption":
            films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.caption": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کپشن فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

        if mode == "file_edit_quality":
            films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.quality": message.text.strip()}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کیفیت فایل ذخیره شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

        if mode == "file_add_caption":
            st["tmp_caption"] = message.text.strip()
            st["mode"] = "file_add_quality"
            return await message.reply("📽 کیفیت فایل جدید را وارد کن (مثلاً 720p):")

        if mode == "file_add_quality":
            new_q = message.text.strip()
            if not st.get("tmp_file_id"):
                admin_edit_state.pop(uid, None)
                return await message.reply("⚠️ ابتدا فایل رسانه را بفرست.")
            films_col.update_one({"film_id": film_id}, {"$push": {"files": {
                "film_id": film_id,
                "file_id": st["tmp_file_id"],
                "caption": st.get("tmp_caption", ""),
                "quality": new_q
            }}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جدید اضافه شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))
        return

    # 3) فلو آپلود
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
            data["files"].append({
                "film_id": data["film_id"],
                "file_id": data["pending_file_id"],
                "caption": data["current_file"]["caption"],
                "quality": quality
            })
            data.pop("pending_file_id", None); data.pop("current_file", None)
            data["step"] = "confirm_more_files"
            buttons = InlineKeyboardMarkup([[InlineKeyboardButton("✅ بله", callback_data="more_yes"),
                                             InlineKeyboardButton("❌ خیر", callback_data="more_no")]])
            return await message.reply(
                f"✅ فایل اضافه شد.\n🎬 عنوان: {data.get('title')}\n📽 کیفیت: {quality}\n\nآیا <b>فایل دیگری</b> برای این عنوان داری؟",
                reply_markup=buttons
            )
        return

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_router(client: Client, message: Message):
    uid = message.from_user.id

    # پنل ادمین: تعویض کاور/فایل/افزودن فایل
    if uid in admin_edit_state:
        st = admin_edit_state[uid]; mode = st.get("mode"); film_id = st.get("film_id")

        if mode == "replace_cover":
            if not message.photo: return await message.reply("⚠️ لطفاً عکس کاور بفرست.")
            films_col.update_one({"film_id": film_id}, {"$set": {"cover_id": message.photo.file_id}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ کاور جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{film_id}")]]))

        if mode == "file_replace":
            if message.video: fid = message.video.file_id
            elif message.document: fid = message.document.file_id
            elif message.audio: fid = message.audio.file_id
            else: return await message.reply("⚠️ فقط ویدیو/سند/صوت برای جایگزینی قابل قبول است.")
            idx = st.get("file_index", 0)
            films_col.update_one({"film_id": film_id}, {"$set": {f"files.{idx}.file_id": fid}})
            admin_edit_state.pop(uid, None)
            return await message.reply("✅ فایل جایگزین شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{film_id}")]]))

        if mode == "file_add_pickfile":
            if message.video: fid = message.video.file_id
            elif message.document: fid = message.document.file_id
            elif message.audio: fid = message.audio.file_id
            else: return await message.reply("⚠️ فقط ویدیو/سند/صوت قابل قبول است.")
            st["tmp_file_id"] = fid; st["mode"] = "file_add_caption"
            return await message.reply("📝 کپشن فایل جدید را وارد کن:")

    # فلو آپلود
    if uid in upload_data:
        data = upload_data[uid]; step = data.get("step")

        if step == "awaiting_cover":
            if not message.photo: return await message.reply("⚠️ لطفاً <b>عکس کاور</b> بفرست.")
            data["cover_id"] = message.photo.file_id; data["step"] = "awaiting_first_file"
            return await message.reply("📤 کاور ثبت شد. حالا <b>فایلِ اول</b> را بفرست (ویدیو/سند/صوت).")

        if step in ("awaiting_first_file", "awaiting_next_file"):
            if message.video: file_id = message.video.file_id
            elif message.document: file_id = message.document.file_id
            elif message.audio: file_id = message.audio.file_id
            else: return await message.reply("⚠️ فقط ویدیو/سند/صوت قبول است. دوباره بفرست.")
            data["pending_file_id"] = file_id; data["step"] = "awaiting_caption"
            return await message.reply("📝 <b>کپشن</b> این فایل را وارد کن:")

        return

@bot.on_callback_query(filters.user(ADMIN_IDS) & filters.regex(r"^more_"))
async def upload_more_files_cb(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id; data = upload_data.get(uid)
    if not data: return await cq.answer("⚠️ اطلاعات آپلود پیدا نشد.", show_alert=True)

    if cq.data == "more_yes":
        await cq.answer()
        data["step"] = "awaiting_next_file"
        data.pop("pending_file_id", None); data.pop("current_file", None)
        return await cq.message.reply("📤 لطفاً فایل بعدی را بفرست.")

    if cq.data == "more_no":
        await cq.answer()
        film_id = data["film_id"]
        film_doc = {
            "film_id": film_id, "user_id": uid,
            "title": data.get("title"),
            "genre": data.get("genre",""), "year": data.get("year",""),
            "cover_id": data.get("cover_id"),
            "timestamp": datetime.now(timezone.utc),
            "files": data["files"]
        }
        films_col.update_one({"film_id": film_id}, {"$set": film_doc}, upsert=True)
        deep_link = f"https://t.me/{BOT_USERNAME}?start=F{film_id}"
        await cq.message.reply(
            f"✅ فیلم ذخیره شد.\n\n🎬 عنوان: {film_doc['title']}\n📂 تعداد فایل: {len(film_doc['files'])}\n🔗 لینک ربات: {deep_link}"
        )
        await cq.message.reply(
            "🕓 انتخاب کن:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ زمان‌بندی", callback_data=f"sched_yes::{film_id}")],
                [InlineKeyboardButton("📣 ارسال فوری", callback_data=f"sched_no::{film_id}")]
            ])
        )
        upload_data.pop(uid, None)

# ---------------------- زمان‌بندی ----------------------
@bot.on_callback_query(filters.regex(r"^sched_yes::(.+)$") & filters.user(ADMIN_IDS))
async def ask_schedule_date(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    schedule_data[cq.from_user.id] = {"film_id": film_id, "step": "date"}
    await cq.message.reply("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^sched_no::(.+)$") & filters.user(ADMIN_IDS))
async def ask_publish_immediate(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{film_id}::{chat_id}")]
            for title, chat_id in TARGET_CHANNELS.items()]
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="pub_cancel")])
    await cq.message.reply("📣 می‌خوای همین الان ارسال کنیم؟ کانال رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^sched_cancel$") & filters.user(ADMIN_IDS))
async def sched_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("⛔️ زمان‌بندی لغو شد.")

@bot.on_callback_query(filters.regex(r"^film_sched_save::(\d{4}-\d{2}-\d{2})::(\d{2}:\d{2})::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_sched_save_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    date_str, time_str, film_id, channel_id = cq.matches[0].groups()
    channel_id = int(channel_id)
    try:
        local_dt = TZ_DE.localize(datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M"))
        dt_utc_naive = local_dt.astimezone(pytz.utc).replace(tzinfo=None)  # در DB به‌صورت naive-UTC می‌ریزیم
    except Exception:
        return await cq.answer("❌ تاریخ/ساعت نامعتبر.", show_alert=True)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.answer("⚠️ فیلم پیدا نشد.", show_alert=True)
    scheduled_posts.insert_one({
        "film_id": film_id,
        "title": film.get("title",""),
        "channel_id": channel_id,
        "scheduled_time": dt_utc_naive
    })
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("✅ زمان‌بندی ذخیره شد.")

# ---------------------- انتشار فوری (با چک Discussion + کیبورد) ----------------------
@bot.on_callback_query(filters.regex(r"^film_pub_go::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_pub_go_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id, channel_id = cq.data.split("::")[1:]
    channel_id = int(channel_id)
    film = films_col.find_one({"film_id": film_id})
    if not film:
        return await cq.message.edit_text("❌ فیلم یافت نشد.")

    has_discussion = await check_discussion_linked(client, channel_id)
    if not has_discussion and ADMIN_IDS:
        try:
            await client.send_message(
                cq.from_user.id,
                f"⚠️ در کانال {channel_id} Discussion لینک نشده. دکمه «💬 نظر بده» ممکن است پنل کامنت را نشان ندهد."
            )
        except Exception:
            pass

    stats_doc = {
        "channel_id": channel_id,
        "message_id": None,
        "film_id": film_id,
        "downloads": 0,
        "shares": 0,
        "views": 0,
        "reactions": {"like": 0, "heart": 0, "broken": 0, "dislike": 0},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc)
    }

    caption = compose_channel_caption(film)
    try:
        if film.get("cover_id"):
            sent = await client.send_photo(channel_id, photo=film["cover_id"], caption=caption)
        else:
            sent = await client.send_message(channel_id, text=caption)
    except Exception as e:
        return await cq.message.edit_text(f"❌ خطا در ارسال: {e}")

    stats_doc["message_id"] = sent.id
    try:
        fresh = await client.get_messages(channel_id, sent.id)
        stats_doc["views"] = int(getattr(fresh, "views", 0) or 0)
    except Exception:
        stats_doc["views"] = 0

    ch_posts_col.update_one(
        {"channel_id": channel_id, "message_id": sent.id},
        {"$set": stats_doc},
        upsert=True
    )
    kb = build_channel_keyboard(channel_id, sent.id, film_id, stats_doc)
    try:
        await client.edit_message_reply_markup(channel_id, sent.id, reply_markup=kb)
    except Exception as e:
        log.warning(f"keyboard set error: {e}")

    await cq.message.edit_text("✅ پست ارسال شد (کاور + دکمه‌ها).")

# ---------------------- پنل ادمین ----------------------
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

@bot.on_message(filters.command("admin") & filters.user(ADMIN_IDS))
async def admin_entry(client: Client, message: Message):
    await message.reply("🛠 پنل ادمین:", reply_markup=kb_admin_main())

@bot.on_callback_query(filters.regex(r"^admin_home$") & filters.user(ADMIN_IDS))
async def admin_home_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); await cq.message.edit_text("🛠 پنل ادمین:", reply_markup=kb_admin_main())

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
        title = f.get("title") or f.get("film_id")
        year = f.get("year", "")
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
    if not film: return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
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
    await cq.answer()
    fid = cq.matches[0].group(1)
    film = films_col.find_one({"film_id": fid})
    if not film: return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    rows = [[InlineKeyboardButton(f"#{i+1} • کیفیت: {f.get('quality','-')}", callback_data=f"film_file_open::{fid}::{i}")] for i, f in enumerate(files)]
    rows.append([InlineKeyboardButton("➕ افزودن فایل جدید", callback_data=f"film_file_add::{fid}")])
    rows.append([InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")])
    await cq.message.edit_text("📂 فایل‌ها:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^film_file_open::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def film_file_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
    if idx < 0 or idx >= len(files):
        return await cq.message.edit_text("❌ اندیس فایل نامعتبر.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))
    f = files[idx]; cap = f.get("caption",""); q = f.get("quality","")
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
    await cq.message.edit_text("📤 فایل جدید (ویدیو/سند/صوت) را بفرست تا جایگزین شود:")

@bot.on_callback_query(filters.regex(r"^file_move_up::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_up_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return
    files = film.get("files", [])
    if idx <= 0 or idx >= len(files): return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx-1], files[idx] = files[idx], files[idx-1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_move_down::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_move_down_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return
    files = film.get("files", [])
    if idx < 0 or idx >= len(files)-1: return await cq.answer("⛔️ امکان جابجایی نیست.", show_alert=True)
    files[idx+1], files[idx] = files[idx], files[idx+1]
    films_col.update_one({"film_id": fid}, {"$set": {"files": files}})
    await cq.message.edit_text("✅ جابجا شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^file_delete_confirm::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_confirm_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو", callback_data=f"film_file_open::{fid}::{idx}")],
                               [InlineKeyboardButton("🗑 حذف", callback_data=f"file_delete::{fid}::{idx}")]])
    await cq.message.edit_text("❗️ مطمئنی این فایل حذف شود؟", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_delete::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_delete_do_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    film = films_col.find_one({"film_id": fid})
    if not film: return await cq.message.edit_text("❌ فیلم یافت نشد.", reply_markup=kb_admin_main())
    files = film.get("files", [])
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
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو", callback_data=f"film_open::{fid}")],
                               [InlineKeyboardButton("🗑 حذف قطعی", callback_data=f"film_delete::{fid}")]])
    await cq.message.edit_text("❗️ مطمئنی کل فیلم و فایل‌ها حذف شود؟", reply_markup=kb)

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
    await cq.message.edit_text("📣 کانال مقصد برای انتشار فوری را انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows + [[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]]))

@bot.on_callback_query(filters.regex(r"^film_sched_start::(.+)$") & filters.user(ADMIN_IDS))
async def film_sched_start_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); fid = cq.matches[0].group(1)
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
        local_show = pytz.utc.localize(p["scheduled_time"]).astimezone(TZ_DE)
        dt = local_show.strftime("%Y-%m-%d %H:%M")
        rows.append([InlineKeyboardButton(f"{p.get('title','(بدون عنوان)')} • {dt}", callback_data=f"sched_open::{str(p['_id'])}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"admin_sched_list_{page-1}"))
    if page*10 < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"admin_sched_list_{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 منو اصلی", callback_data="admin_home")])
    await cq.message.edit_text("⏰ زمان‌بندی‌های ثبت‌شده:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^sched_open::(.+)$") & filters.user(ADMIN_IDS))
async def sched_open_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); sid = cq.matches[0].group(1)
    try: post = scheduled_posts.find_one({"_id": ObjectId(sid)})
    except Exception: post = None
    if not post:
        return await cq.message.edit_text("❌ برنامه زمان‌بندی یافت نشد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))
    local_show = pytz.utc.localize(post["scheduled_time"]).astimezone(TZ_DE)
    dt = local_show.strftime("%Y-%m-%d %H:%M")
    info = (f"🆔 {sid}\n🎬 {post.get('title','(بدون عنوان)')}\n📅 {dt}\n📡 کانال: {post.get('channel_id')}\n🎞 فیلم: {post.get('film_id')}")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 حذف از صف", callback_data=f"sched_delete::{sid}")],
                               [InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^sched_delete::(.+)$") & filters.user(ADMIN_IDS))
async def sched_delete_cb(client: Client, cq: CallbackQuery):
    await cq.answer(); sid = cq.matches[0].group(1)
    try:
        scheduled_posts.delete_one({"_id": ObjectId(sid)})
        await cq.message.edit_text("✅ حذف شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))
    except Exception as e:
        await cq.message.edit_text(f"❌ خطا در حذف: {e}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="admin_sched_list_1")]]))

@bot.on_callback_query(filters.regex(r"^admin_export_csv$") & filters.user(ADMIN_IDS))
async def admin_export_csv_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    films = list(films_col.find().sort("timestamp", -1))
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["film_id","title","genre","year","files_count","timestamp"])
    for f in films:
        w.writerow([f.get("film_id",""),
                    (f.get("title","") or "").replace("\n"," "),
                    (f.get("genre","") or "").replace("\n"," "),
                    f.get("year",""),
                    len(f.get("files", [])),
                    f.get("timestamp","")])
    buf.seek(0)
    bio = io.BytesIO(buf.getvalue().encode("utf-8")); bio.name = "films_export.csv"
    await client.send_document(cq.message.chat.id, document=bio, caption="📥 خروجی CSV فیلم‌ها")

# ---------------------- ⏱ جاب‌های خودکار ----------------------
async def send_scheduled_posts():
    """
    هر یک دقیقه: ارسال پست‌های رسیده به موعدشان (UTC naive در DB) + تنظیم کیبورد
    """
    now = datetime.now(timezone.utc)  # ✅ جایگزین utcnow()
    posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now.replace(tzinfo=None)}}))  # DB: naive UTC
    for post in posts:
        film = films_col.find_one({"film_id": post["film_id"]})
        if not film:
            scheduled_posts.delete_one({"_id": post["_id"]})
            continue

        # هشدار Discussion
        has_discussion = await check_discussion_linked(bot, post["channel_id"])
        if not has_discussion and ADMIN_IDS:
            try:
                await bot.send_message(ADMIN_IDS[0], f"⚠️ در کانال {post['channel_id']} Discussion لینک نشده است.")
            except Exception:
                pass

        caption = compose_channel_caption(film)
        try:
            if film.get("cover_id"):
                sent = await bot.send_photo(post["channel_id"], photo=film["cover_id"], caption=caption)
            else:
                sent = await bot.send_message(post["channel_id"], text=caption)
        except Exception as e:
            log.error(f"scheduled send error: {e}")
            scheduled_posts.delete_one({"_id": post["_id"]})
            continue

        try:
            fresh = await bot.get_messages(post["channel_id"], sent.id)
            views = int(getattr(fresh, "views", 0) or 0)
        except Exception:
            views = 0

        stats_doc = {
            "channel_id": post["channel_id"],
            "message_id": sent.id,
            "film_id": post["film_id"],
            "downloads": 0,
            "shares": 0,
            "views": views,
            "reactions": {"like": 0, "heart": 0, "broken": 0, "dislike": 0},
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        ch_posts_col.update_one(
            {"channel_id": post["channel_id"], "message_id": sent.id},
            {"$set": stats_doc},
            upsert=True
        )
        kb = build_channel_keyboard(post["channel_id"], sent.id, post["film_id"], stats_doc)
        try:
            await bot.edit_message_reply_markup(post["channel_id"], sent.id, reply_markup=kb)
        except Exception as e:
            log.warning(f"keyboard set error (sched): {e}")

        scheduled_posts.delete_one({"_id": post["_id"]})

async def refresh_channel_post_views():
    """
    هر 3 دقیقه: بازخوانی بازدید پست‌های 48 ساعت اخیر و به‌روزرسانی کیبورد
    """
    since = datetime.now(timezone.utc) - timedelta(hours=48)  # ✅
    recent = list(ch_posts_col.find({"created_at": {"$gte": since}}).sort("created_at", DESCENDING))

    from collections import defaultdict
    buckets = defaultdict(list)
    for p in recent:
        buckets[p["channel_id"]].append(p)

    for cid, posts in buckets.items():
        mids = [p["message_id"] for p in posts]
        for i in range(0, len(mids), 50):
            chunk = mids[i:i+50]
            try:
                msgs = await bot.get_messages(cid, chunk)
                if not isinstance(msgs, list): msgs = [msgs]
                views_map = {m.id: int(getattr(m, "views", 0) or 0) for m in msgs if m}
            except Exception as e:
                log.warning(f"get_messages batch error: {e}")
                continue

            for m in msgs:
                if not m: continue
                doc = ch_posts_col.find_one({"channel_id": cid, "message_id": m.id})
                if not doc: continue
                new_views = views_map.get(m.id, doc.get("views", 0))
                if new_views != doc.get("views", 0):
                    ch_posts_col.update_one(
                        {"channel_id": cid, "message_id": m.id},
                        {"$set": {"views": new_views, "updated_at": datetime.now(timezone.utc)}}
                    )
                    doc["views"] = new_views
                    kb = build_channel_keyboard(cid, m.id, doc.get("film_id",""), doc)
                    try:
                        await bot.edit_message_reply_markup(cid, m.id, reply_markup=kb)
                    except Exception as e:
                        log.warning(f"edit keyboard (views) error: {e}")

# ---------------------- 📊 کال‌بک‌های ری‌اکشن/اشتراک ----------------------
@bot.on_callback_query(filters.regex(r"^react::(like|heart|broken|dislike)::(-?\d+)::(\d+)$"))
async def react_callback(client: Client, cq: CallbackQuery):
    typ = cq.matches[0].group(1)
    cid = int(cq.matches[0].group(2))
    mid = int(cq.matches[0].group(3))
    uid = cq.from_user.id

    try:
        reactions_log.insert_one({
            "channel_id": cid, "message_id": mid, "user_id": uid,
            "type": typ, "at": datetime.now(timezone.utc)  # ✅
        })
        ch_posts_col.update_one(
            {"channel_id": cid, "message_id": mid},
            {"$inc": {f"reactions.{typ}": 1}, "$set": {"updated_at": datetime.now(timezone.utc)}},  # ✅
            upsert=True
        )
        doc = ch_posts_col.find_one({"channel_id": cid, "message_id": mid})
        kb = build_channel_keyboard(cid, mid, doc.get("film_id", ""), doc)
        try:
            await client.edit_message_reply_markup(cid, mid, reply_markup=kb)
        except Exception as e:
            log.warning(f"react keyboard update error: {e}")
        await cq.answer("✔️ ثبت شد", show_alert=False)
    except Exception:
        await cq.answer("👌 قبلاً واکنش دادی.", show_alert=False)

@bot.on_callback_query(filters.regex(r"^share::(-?\d+)::(\d+)$"))
async def share_callback(client: Client, cq: CallbackQuery):
    cid = int(cq.matches[0].group(1))
    mid = int(cq.matches[0].group(2))

    ch_posts_col.update_one(
        {"channel_id": cid, "message_id": mid},
        {"$inc": {"shares": 1}, "$set": {"updated_at": datetime.now(timezone.utc)}},  # ✅
        upsert=True
    )
    doc = ch_posts_col.find_one({"channel_id": cid, "message_id": mid})
    kb = build_channel_keyboard(cid, mid, doc.get("film_id",""), doc)
    try:
        await client.edit_message_reply_markup(cid, mid, reply_markup=kb)
    except Exception as e:
        log.warning(f"share keyboard update error: {e}")

    abs_id = str(cid).replace("-100", "") if str(cid).startswith("-100") else str(abs(cid))
    link = f"https://t.me/c/{abs_id}/{mid}"
    await cq.answer("لینک پست کپی کن و به اشتراک بگذار ✅", show_alert=False)

# ---------------------- /stats ----------------------
@bot.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats_cmd(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply("استفاده: /stats <film_id>")
    fid = parts[1].strip()
    agg = list(ch_posts_col.aggregate([
        {"$match": {"film_id": fid}},
        {"$group": {"_id": "$film_id",
                    "posts": {"$sum": 1},
                    "views": {"$sum": "$views"},
                    "downloads": {"$sum": "$downloads"},
                    "shares": {"$sum": "$shares"},
                    "like": {"$sum": "$reactions.like"},
                    "heart": {"$sum": "$reactions.heart"},
                    "broken": {"$sum": "$reactions.broken"},
                    "dislike": {"$sum": "$reactions.dislike"}}}
    ]))
    if not agg:
        return await message.reply("برای این film_id هنوز پستی ارسال نشده.")
    r = agg[0]
    txt = (f"📊 آمار فیلم <b>{fid}</b>\n"
           f"پست‌ها: {r.get('posts',0)}\n"
           f"👁️ views: {r.get('views',0)}\n"
           f"⬇️ downloads: {r.get('downloads',0)}\n"
           f"🔁 shares: {r.get('shares',0)}\n"
           f"👍 {r.get('like',0)} | ❤️ {r.get('heart',0)} | 💔 {r.get('broken',0)} | 👎 {r.get('dislike',0)}")
    await message.reply(txt)

# ---------------------- ⏱ Scheduler Boot ----------------------
async def scheduler_boot():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_scheduled_posts, "interval", minutes=1)
    scheduler.add_job(refresh_channel_post_views, "interval", minutes=3)
    scheduler.start()
    return scheduler

# ---------------------- NOOP for display-only buttons ----------------------
@bot.on_callback_query(filters.regex(r"^noop$"))
async def noop_cb(client: Client, cq: CallbackQuery):
    await cq.answer()

# ---------------------- اجرای پایدار ----------------------
async def main():
    print("🤖 ربات با موفقیت راه‌اندازی شد و منتظر دستورات است...")
    while True:
        try:
            async with bot:
                scheduler = await scheduler_boot()
                await idle()
                scheduler.shutdown(wait=False)
            break
        except FloodWait as e:
            wait = int(getattr(e, "value", 60)) + 5
            print(f"⏳ FloodWait: باید {wait} ثانیه صبر کنیم...")
            await asyncio.sleep(wait)
        except Exception as e:
            print("❌ Unexpected error on startup:", repr(e))
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
