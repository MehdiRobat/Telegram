# ======================= BoxUp_bot — Final bot.py =======================
# تمام تنظیمات از .env خوانده می‌شود؛ bot = Client(...) پیش از همهٔ هندلرها ساخته می‌شود.
# قابلیت‌ها:
#   • عضویت اجباری چندکاناله + دکمه «عضو شدم»
#   • دیپ‌لینک start=DL_... و ارسال مستقیم فایل‌ها بعد از تأیید عضویت (بدون پیام اضافه)
#   • آپلود چندمرحله‌ای فقط برای ادمین (عنوان/ژانر/سال/کاور/چندفایل با کیفیت و کپشن)
#   • تبدیل «متن (لینک)» به دکمه‌های زیر پست
#   • آمار 👁/📥/🔁 و ری‌اکشن 👍❤️💔👎 (زیر پیام‌های بات و پست کانالی)
#   • حذف خودکار پیام‌های کاربر بعد از DELETE_DELAY ثانیه
#   • زمان‌بندی ارسال به کانال با APScheduler (کاور+کیبورد آمار/ری‌اکشن+لینک ربات)
#   • پنل ادمین ساده (لیست/حذف/لینک) + خروجی CSV آمار
#   • حالت سکوت (Silent) با متغیرهای محیطی

# ---------------------- 📦 ایمپورت‌ها ----------------------
import os
import re
import io
import csv
import asyncio
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------------- ⚙️ بارگذاری env ----------------------
load_dotenv()

API_ID       = int(os.getenv("API_ID", "0"))
API_HASH     = os.getenv("API_HASH", "")
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "BoxUp_bot")

MONGO_URI     = os.getenv("MONGO_URI", "")
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.imgur.com/uZqKsRs.png")
CONFIRM_IMAGE = os.getenv("CONFIRM_IMAGE", "https://i.imgur.com/fAGPuXo.png")
DELETE_DELAY  = int(os.getenv("DELETE_DELAY", "30"))

REQUIRED_CHANNELS = [x.strip() for x in os.getenv("REQUIRED_CHANNELS", "").split(",") if x.strip()]
ADMIN_IDS = list(map(int, [x.strip() for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]))

# حالت سکوت: بین دو ساعت مشخص، نوتیف خاموش
SILENT_START = os.getenv("SILENT_START", "22:00")  # "22:00"
SILENT_END   = os.getenv("SILENT_END",   "10:00")  # "10:00"

if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URI]) or not ADMIN_IDS or not REQUIRED_CHANNELS:
    raise RuntimeError("❌ مقداردهی .env ناقص است. API_ID, API_HASH, BOT_TOKEN, MONGO_URI, ADMIN_IDS, REQUIRED_CHANNELS را پر کن.")

# ---------------------- 🗄 MongoDB ----------------------
mongo = MongoClient(MONGO_URI)
db = mongo["boxup_db"]
users_col = db["users"]
films_col = db["films"]
scheduled_posts = db["scheduled_posts"]

films_col.create_index([("film_id", ASCENDING)], unique=True)
films_col.create_index([("created_at", ASCENDING)])
scheduled_posts.create_index([("scheduled_time", ASCENDING)])

# ---------------------- 🤖 ساخت ربات (قبل از هندلرها) ----------------------
bot = Client(
    name=BOT_USERNAME,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# ---------------------- 🧠 وضعیت‌های موقت ----------------------
upload_state: Dict[int, Dict[str, Any]] = {}     # برای آپلود چندمرحله‌ای ادمین
pending_film_for_user: Dict[int, str] = {}       # نگهداری film_id دیپ‌لینک برای ارسال بعد از تایید عضویت
schedule_draft_by_admin: Dict[int, Any] = {}     # پیش‌نویس زمان‌بندی برای هر ادمین

# ---------------------- 🧰 توابع کمکی ----------------------
def is_silent_now() -> bool:
    """آیا الان داخل بازه سکوت هستیم؟"""
    try:
        now = datetime.now().time()
        s_h, s_m = map(int, SILENT_START.split(":"))
        e_h, e_m = map(int, SILENT_END.split(":"))
        start_t, end_t = dtime(s_h, s_m), dtime(e_h, e_m)
        if start_t <= end_t:
            return start_t <= now <= end_t
        else:
            # بازه نصف‌شب‌گذر
            return now >= start_t or now <= end_t
    except:
        return False

def buttons_join_channels() -> InlineKeyboardMarkup:
    rows = []
    for ch in REQUIRED_CHANNELS:
        rows.append([InlineKeyboardButton(f"👥 عضویت در @{ch}", url=f"https://t.me/{ch}")])
    rows.append([InlineKeyboardButton("✅ عضو شدم", callback_data="joined_check")])
    # دکمه ورود به پنل ادمین (صرفاً نمایش؛ محدودیت در کال‌بک enforce می‌شود)
    rows.append([InlineKeyboardButton("🛠 پنل ادمین", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)

def parse_label_links_to_buttons(caption: str) -> (str, List[List[InlineKeyboardButton]]):
    """
    تبدیل الگوی: «متن (https://link)» به دکمه.
    متن کپشن تمیزسازی می‌شود و دکمه‌ها برمی‌گردند.
    """
    if not caption:
        return "", []
    pattern = r'([^\n\r(]+)\s*\((https?://[^\s)]+)\)'
    buttons = []
    clean = caption
    for m in re.finditer(pattern, caption):
        label = m.group(1).strip()
        url = m.group(2).strip()
        if label and url:
            buttons.append([InlineKeyboardButton(label, url=url)])
            clean = clean.replace(m.group(0), "").strip()
    return clean.strip(), buttons

async def is_member(user_id: int) -> bool:
    """ چک عضویت کاربر در همه کانال‌های اجباری. """
    try:
        for ch in REQUIRED_CHANNELS:
            cm = await bot.get_chat_member(chat_id=f"@{ch}", user_id=user_id)
            if cm.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return False
        return True
    except Exception:
        return False

def stat_keyboard(film_id: str, include_reactions: bool = True, extra: Optional[List[List[InlineKeyboardButton]]] = None):
    """ دکمه‌های آمار و ری‌اکشن + دکمه‌های لینک (extra) """
    doc = films_col.find_one({"film_id": film_id}) or {}
    s = doc.get("stats", {})
    s.setdefault("views", 0)
    s.setdefault("downloads", 0)
    s.setdefault("shares", 0)
    s.setdefault("r_like", 0)
    s.setdefault("r_heart", 0)
    s.setdefault("r_broken", 0)
    s.setdefault("r_dislike", 0)

    rows = [[
        InlineKeyboardButton(f"👁 {s['views']}", callback_data=f"st:view:{film_id}"),
        InlineKeyboardButton(f"📥 {s['downloads']}", callback_data=f"st:dwl:{film_id}"),
        InlineKeyboardButton(f"🔁 {s['shares']}", callback_data=f"st:shr:{film_id}")
    ]]
    if include_reactions:
        rows.append([
            InlineKeyboardButton(f"👍 {s['r_like']}", callback_data=f"rx:like:{film_id}"),
            InlineKeyboardButton(f"❤️ {s['r_heart']}", callback_data=f"rx:heart:{film_id}"),
            InlineKeyboardButton(f"💔 {s['r_broken']}", callback_data=f"rx:broken:{film_id}"),
            InlineKeyboardButton(f"👎 {s['r_dislike']}", callback_data=f"rx:dislike:{film_id}"),
        ])
    if extra:
        rows.extend(extra)
    return InlineKeyboardMarkup(rows)

async def auto_delete_later(client: Client, chat_id: int, message_ids: List[int], delay: int = DELETE_DELAY):
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id, message_ids)
    except Exception:
        pass

async def send_film_to_user(client: Client, user_id: int, film_id: str):
    """ ارسال همهٔ فایل‌های یک عنوان به کاربر؛ بعد از DELETE_DELAY حذف می‌شوند. """
    film = films_col.find_one({"film_id": film_id})
    if not film:
        await client.send_message(user_id, "❌ فیلم پیدا نشد.")
        return

    sent_ids: List[int] = []

    m1 = await client.send_photo(
        chat_id=user_id,
        photo=film.get("cover_id") or WELCOME_IMAGE,
        caption=f"🎬 <b>{film.get('title','')}</b>\n🎭 {film.get('genre','')}  •  📅 {film.get('year','')}",
        reply_markup=buttons_join_channels(),
        disable_notification=is_silent_now()
    )
    sent_ids.append(m1.id)

    files = film.get("files", [])
    if not files:
        m = await client.send_message(user_id, "⚠️ برای این عنوان هنوز فایلی ثبت نشده است.", disable_notification=is_silent_now())
        sent_ids.append(m.id)
    else:
        for f in files:
            cap_clean, link_buttons = parse_label_links_to_buttons(f.get("caption", ""))
            kb = stat_keyboard(film_id, extra=link_buttons)
            v = await client.send_video(
                chat_id=user_id,
                video=f["file_id"],
                caption=f"🎬 <b>{film.get('title','')}</b>\n💎 کیفیت: {f.get('quality','')}\n\n{cap_clean}",
                reply_markup=kb,
                disable_notification=is_silent_now()
            )
            sent_ids.append(v.id)

    warn = await client.send_message(
        user_id, f"⏳ همه‌ی پیام‌ها بعد از {DELETE_DELAY} ثانیه حذف می‌شوند. لطفاً ذخیره کنید.",
        disable_notification=is_silent_now()
    )
    sent_ids.append(warn.id)
    asyncio.create_task(auto_delete_later(client, user_id, sent_ids, DELETE_DELAY))

# ---------------------- 🚪 /start + دیپ‌لینک ----------------------
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    user_id = message.from_user.id
    users_col.update_one({"user_id": user_id}, {"$set": {"user_id": user_id, "last_seen": datetime.utcnow()}}, upsert=True)

    payload = message.command[1].strip() if len(message.command) > 1 else None
    if payload and payload.startswith("DL_"):
        pending_film_for_user[user_id] = payload

    if not await is_member(user_id):
        await message.reply_photo(
            photo=WELCOME_IMAGE,
            caption=("👋 خوش آمدی!\n"
                     "برای استفاده از ربات ابتدا در کانال‌های زیر عضو شو و سپس «✅ عضو شدم» را بزن.\n"
                     "اگر از لینک اختصاصی آمدی، پس از تایید عضویت، فایل‌ها خودکار نمایش داده می‌شوند."),
            reply_markup=buttons_join_channels(),
            disable_notification=is_silent_now()
        )
        return

    if payload and payload.startswith("DL_"):
        await send_film_to_user(client, user_id, payload)
        return

    await message.reply_photo(
        photo=CONFIRM_IMAGE,
        caption="✅ عضویتت تأیید شد! از منوی زیر استفاده کن.",
        reply_markup=buttons_join_channels(),
        disable_notification=is_silent_now()
    )

# ---------------------- ✅ کال‌بک «عضو شدم» (بدون پیام اضافه) ----------------------
@bot.on_callback_query(filters.regex("^joined_check$"))
async def cb_joined_check(client: Client, cq: CallbackQuery):
    user_id = cq.from_user.id
    if not await is_member(user_id):
        await cq.answer("❌ هنوز عضو همه کانال‌ها نشدی.", show_alert=True)
        return

    await cq.answer("✅ عضویت تایید شد.")

    # اگر از دیپ‌لینک آمده: پیام فعلی حذف، فایل‌ها ارسال
    film_id = pending_film_for_user.pop(user_id, None)
    if film_id:
        try:
            await cq.message.delete()
        except:
            pass
        await send_film_to_user(client, user_id, film_id)
        return

    # در غیر اینصورت پیام کوتاه یا سکوت
    try:
        await cq.message.edit_caption("✅ عضویت تأیید شد.", reply_markup=buttons_join_channels())
    except:
        await client.send_message(user_id, "✅ عضویت تأیید شد.", disable_notification=is_silent_now())

# ---------------------- 🔐 محدودکننده ادمین ----------------------
def admin_only():
    return filters.user(ADMIN_IDS) & filters.private

# ---------------------- 📤 شروع آپلود ----------------------
@bot.on_message(filters.command("upload") & admin_only())
async def cmd_upload(client: Client, message: Message):
    user_id = message.from_user.id
    upload_state[user_id] = {"step": "title", "files": []}
    await message.reply("🎬 عنوان فیلم را وارد کن:")

# ---------------------- 🔁 جریان آپلود چندمرحله‌ای ----------------------
@bot.on_message(filters.private & admin_only() & ~filters.command(["start","admin","upload"]))
async def upload_flow(client: Client, message: Message):
    user_id = message.from_user.id

    # اگر در حالت زمان‌بندی هستی، اجازه نده این هندلر تداخلی بزند
    if schedule_draft_by_admin.get(user_id):
        return

    state = upload_state.get(user_id)
    if not state:
        return

    step = state.get("step")

    if step == "title":
        state["title"] = (message.text or "").strip()
        state["step"] = "genre"
        await message.reply("🎭 ژانر فیلم را وارد کن:")
        return

    if step == "genre":
        state["genre"] = (message.text or "").strip()
        state["step"] = "year"
        await message.reply("📅 سال تولید فیلم را وارد کن (مثلاً 2024):")
        return

    if step == "year":
        state["year"] = (message.text or "").strip()
        state["step"] = "cover"
        await message.reply("🖼 حالا کاور فیلم را ارسال کن (عکس).")
        return

    if step == "cover":
        if message.photo:
            state["cover_id"] = message.photo.file_id
            state["step"] = "awaiting_file"
            await message.reply("📤 ویدیو را ارسال کن. بعد از هر فایل، کیفیت و کپشن پرسیده می‌شود.")
        else:
            await message.reply("❌ لطفاً تصویر معتبر ارسال کن.")
        return

    if step == "awaiting_file":
        if message.video:
            state["current_file_id"] = message.video.file_id
            state["step"] = "ask_quality"
            await message.reply("💎 کیفیت این فایل را بنویس (مثلاً 720p):")
        else:
            await message.reply("❌ لطفاً ویدیو ارسال کن.")
        return

    if step == "ask_quality":
        state["current_quality"] = (message.text or "").strip()
        state["step"] = "ask_caption"
        await message.reply("📝 کپشن این فایل را بنویس (می‌توانی از «متن (https://link)» استفاده کنی):")
        return

    if step == "ask_caption":
        cap = (message.text or "").strip()
        state["files"].append({
            "file_id": state.pop("current_file_id"),
            "quality": state.pop("current_quality", ""),
            "caption": cap
        })
        state["step"] = "more_files"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ بله", callback_data="more_yes"),
             InlineKeyboardButton("❌ خیر", callback_data="more_no")]
        ])
        await message.reply("➕ فایل دیگری هم داری؟", reply_markup=kb)
        return

# ---------------------- ➕/❌ ادامه یا اتمام آپلود ----------------------
@bot.on_callback_query(filters.regex("^more_(yes|no)$") & admin_only())
async def cb_more_files(client: Client, cq: CallbackQuery):
    user_id = cq.from_user.id
    state = upload_state.get(user_id)
    if not state:
        await cq.answer("اطلاعات یافت نشد.", show_alert=True)
        return

    if cq.data.endswith("yes"):
        state["step"] = "awaiting_file"
        await cq.message.reply("📤 ویدیو بعدی را ارسال کن.")
    else:
        film_id = f"DL_{user_id}_{int(datetime.utcnow().timestamp())}"
        doc = {
            "film_id": film_id,
            "title": state.get("title", ""),
            "genre": state.get("genre", ""),
            "year": state.get("year", ""),
            "cover_id": state.get("cover_id"),
            "files": state.get("files", []),
            "created_at": datetime.utcnow(),
            "stats": {"views": 0, "downloads": 0, "shares": 0, "r_like": 0, "r_heart": 0, "r_broken": 0, "r_dislike": 0}
        }
        films_col.insert_one(doc)
        upload_state.pop(user_id, None)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗓 زمان‌بندی ارسال", callback_data=f"sch:{film_id}"),
             InlineKeyboardButton("🔗 دریافت لینک", callback_data=f"link:{film_id}")]
        ])
        await cq.message.reply(
            f"✅ فیلم ذخیره شد.\n\n📍 لینک اختصاصی:\nhttps://t.me/{BOT_USERNAME}?start={film_id}",
            reply_markup=kb
        )
    await cq.answer()

# ---------------------- 🔗 دکمه «دریافت لینک» ----------------------
@bot.on_callback_query(filters.regex("^link:(.+)$") & admin_only())
async def cb_get_link(client: Client, cq: CallbackQuery):
    film_id = cq.matches[0].group(1)
    await cq.message.reply(f"🔗 https://t.me/{BOT_USERNAME}?start={film_id}")
    await cq.answer()

# ---------------------- 🗓 جریان زمان‌بندی (سه‌مرحله‌ای) ----------------------
@bot.on_callback_query(filters.regex("^sch:(.+)$") & admin_only())
async def cb_schedule(client: Client, cq: CallbackQuery):
    user_id = cq.from_user.id
    film_id = cq.matches[0].group(1)
    schedule_draft_by_admin[user_id] = {"film_id": film_id, "step": "date"}
    await cq.message.reply("🗓 تاریخ ارسال را وارد کن (YYYY-MM-DD):")
    await cq.answer()

@bot.on_message(filters.private & admin_only() & ~filters.command(["start","admin","upload"]))
async def schedule_flow(client: Client, message: Message):
    """ اگر ادمین در حالت زمان‌بندی است، اینجا سه مرحله تاریخ/ساعت/هدف را می‌گیرد. """
    user_id = message.from_user.id
    draft = schedule_draft_by_admin.get(user_id)
    if not draft:
        return  # نه در حالت آپلود و نه زمان‌بندی؛ عبور

    text = (message.text or "").strip()

    if draft["step"] == "date":
        try:
            y, m, d = map(int, text.split("-"))
            _ = datetime(y, m, d)  # اعتبارسنجی ساده
            draft["date"] = text
            draft["step"] = "time"
            await message.reply("⏰ ساعت را وارد کن (HH:MM):")
        except Exception:
            await message.reply("❌ فرمت تاریخ نادرست است. نمونه: 2025-08-15")
        return

    if draft["step"] == "time":
        try:
            hh, mm = map(int, text.split(":"))
            draft["time"] = text
            draft["step"] = "target"
            await message.reply("📢 آیدی کانال هدف را وارد کن (مثل @BoxOffice_Irani یا ID عددی):")
        except Exception:
            await message.reply("❌ فرمت ساعت نادرست است. نمونه: 14:30")
        return

    if draft["step"] == "target":
        target = text
        y, m, d = map(int, draft["date"].split("-"))
        hh, mm = map(int, draft["time"].split(":"))
        sched_dt = datetime(y, m, d, hh, mm)  # به زمان سرور/UTC توجه کن
        scheduled_posts.insert_one({
            "film_id": draft["film_id"],
            "target": target,
            "scheduled_time": sched_dt,
            "status": "pending",
            "created_by": user_id,
            "created_at": datetime.utcnow()
        })
        await message.reply(f"✅ زمان‌بندی شد برای {draft['date']} {draft['time']} → {target}")
        schedule_draft_by_admin.pop(user_id, None)
        return

# ---------------------- 📊 آمار و ❤️ ری‌اکشن ----------------------
def _inc_stat(film_id: str, field: str, amount: int = 1):
    films_col.update_one({"film_id": film_id}, {"$inc": {f"stats.{field}": amount}})

@bot.on_callback_query(filters.regex("^st:(view|dwl|shr):(.+)$"))
async def cb_stats(client: Client, cq: CallbackQuery):
    typ = cq.matches[0].group(1)
    film_id = cq.matches[0].group(2)
    field = {"view": "views", "dwl": "downloads", "shr": "shares"}.get(typ)
    if not field:
        await cq.answer(); return
    _inc_stat(film_id, field, 1)
    try:
        await cq.message.edit_reply_markup(reply_markup=stat_keyboard(film_id))
    except Exception:
        pass
    await cq.answer("به‌روزرسانی شد ✅")

@bot.on_callback_query(filters.regex("^rx:(like|heart|broken|dislike):(.+)$"))
async def cb_reactions(client: Client, cq: CallbackQuery):
    typ = cq.matches[0].group(1)
    film_id = cq.matches[0].group(2)
    field = {"like":"r_like","heart":"r_heart","broken":"r_broken","dislike":"r_dislike"}.get(typ)
    if not field:
        await cq.answer(); return
    _inc_stat(film_id, field, 1)
    try:
        await cq.message.edit_reply_markup(reply_markup=stat_keyboard(film_id))
    except Exception:
        pass
    await cq.answer("مرسی از ری‌اکشن ❤️")

# ---------------------- 🛠 پنل ادمین ساده + CSV ----------------------
@bot.on_message(filters.command("admin") & filters.user(ADMIN_IDS) & filters.private)
async def cmd_admin(client, message):
    await message.reply("🛠 به پنل ادمین خوش آمدی.", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 لیست فیلم‌ها", callback_data="ad:list:1")],
        [InlineKeyboardButton("📊 خروجی CSV آمار", callback_data="ad:csv")],
    ]))

@bot.on_callback_query(filters.regex(r"^admin_panel$") & filters.user(ADMIN_IDS))
async def cb_admin_entry(client, cq):
    await cq.message.reply("🛠 پنل ادمین:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 لیست فیلم‌ها", callback_data="ad:list:1")],
        [InlineKeyboardButton("📊 خروجی CSV آمار", callback_data="ad:csv")],
    ]))
    await cq.answer()

def _admin_list_page(page: int = 1, per_page: int = 5):
    total = films_col.count_documents({})
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(pages, page))
    skip = (page - 1) * per_page
    rows = []
    for f in films_col.find({}).sort("created_at", -1).skip(skip).limit(per_page):
        t = f.get("title","")
        fid = f.get("film_id","")
        rows.append([InlineKeyboardButton(f"🎬 {t or fid}", callback_data=f"ad:item:{fid}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"ad:list:{page-1}"))
    if page < pages: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"ad:list:{page+1}"))
    if nav: rows.append(nav)
    return rows, page, pages

@bot.on_callback_query(filters.regex(r"^ad:list:(\d+)$") & filters.user(ADMIN_IDS))
async def cb_admin_list(client, cq):
    page = int(cq.matches[0].group(1))
    rows, p, pages = _admin_list_page(page)
    await cq.message.reply(f"📚 لیست فیلم‌ها (صفحه {p}/{pages})", reply_markup=InlineKeyboardMarkup(rows))
    await cq.answer()

@bot.on_callback_query(filters.regex(r"^ad:item:(.+)$") & filters.user(ADMIN_IDS))
async def cb_admin_item(client, cq):
    fid = cq.matches[0].group(1)
    f = films_col.find_one({"film_id": fid})
    if not f:
        await cq.answer("پیدا نشد.", show_alert=True); return
    cap = (f"🎬 <b>{f.get('title','')}</b>\n"
           f"🎭 {f.get('genre','')} • 📅 {f.get('year','')}\n"
           f"🆔 {fid}\n"
           f"📦 فایل‌ها: {len(f.get('files',[]))}")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 لینک دیپ‌لینک", url=f"https://t.me/{BOT_USERNAME}?start={fid}")],
        [InlineKeyboardButton("🗑 حذف", callback_data=f"ad:del:{fid}")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="ad:list:1")],
    ])
    await cq.message.reply(cap, reply_markup=kb)
    await cq.answer()

@bot.on_callback_query(filters.regex(r"^ad:del:(.+)$") & filters.user(ADMIN_IDS))
async def cb_admin_delete(client, cq):
    fid = cq.matches[0].group(1)
    films_col.delete_one({"film_id": fid})
    await cq.message.reply("✅ حذف شد.")
    await cq.answer()

@bot.on_callback_query(filters.regex(r"^ad:csv$") & filters.user(ADMIN_IDS))
async def cb_admin_csv(client, cq):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["film_id","title","views","downloads","shares","like","heart","broken","dislike","files"])
    for f in films_col.find({}).sort("created_at", -1):
        s = f.get("stats", {})
        writer.writerow([
            f.get("film_id",""),
            (f.get("title","") or "").replace("\n"," "),
            s.get("views",0), s.get("downloads",0), s.get("shares",0),
            s.get("r_like",0), s.get("r_heart",0), s.get("r_broken",0), s.get("r_dislike",0),
            len(f.get("files",[]))
        ])
    data = buf.getvalue().encode("utf-8")
    bio = io.BytesIO(data)
    bio.name = "stats.csv"
    await cq.message.reply_document(document=bio, caption="📊 گزارش CSV")
    await cq.answer()

# ---------------------- ⏱ زمان‌بندی ارسال‌ها ----------------------
scheduler = AsyncIOScheduler()

@scheduler.scheduled_job("interval", seconds=60)
async def send_scheduled_posts():
    now = datetime.utcnow()
    jobs = list(scheduled_posts.find({"status": "pending", "scheduled_time": {"$lte": now}}))
    for job in jobs:
        film = films_col.find_one({"film_id": job["film_id"]})
        if not film:
            scheduled_posts.update_one({"_id": job["_id"]}, {"$set": {"status": "done", "note": "film not found"}})
            continue
        target = job.get("target")
        if not target:
            scheduled_posts.update_one({"_id": job["_id"]}, {"$set": {"status": "done", "note": "no target"}})
            continue

        cap = (
            f"🎬 <b>{film.get('title','')}</b>\n"
            f"🎭 {film.get('genre','')}  •  📅 {film.get('year','')}\n\n"
            f"🔗 دریافت فایل‌ها در ربات:\nhttps://t.me/{BOT_USERNAME}?start={film['film_id']}"
        )
        try:
            kb = stat_keyboard(film['film_id'])
            msg = await bot.send_photo(
                chat_id=target,
                photo=film.get("cover_id") or WELCOME_IMAGE,
                caption=cap,
                reply_markup=kb,
                disable_notification=is_silent_now()
            )
            scheduled_posts.update_one(
                {"_id": job["_id"]},
                {"$set": {"status": "done", "sent_at": now, "channel_chat_id": msg.chat.id, "channel_message_id": msg.id}}
            )
        except Exception as e:
            scheduled_posts.update_one({"_id": job["_id"]}, {"$set": {"status": "error", "error": str(e)}})

# ---------------------- ▶️ اجرا ----------------------
if __name__ == "__main__":
    print("🤖 ربات با موفقیت اجرا شد و آماده دریافت دستورات است.")
    scheduler.start()
    bot.run()
