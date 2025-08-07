import asyncio
from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient

# -------- تنظیمات اولیه ربات --------
API_ID = 27145047
API_HASH = "9e9672f2f920f277daca3d53502e0b34"
BOT_TOKEN = "7696655685:AAFBFhue6YT1uvOSRmoCXNxWpkTWPxYXElg"
BOT_USERNAME = "BoxUp_bot"
ADMIN_ID = 7872708405
REQUIRED_CHANNELS = [
    "BoxOffice_Animation",
    "BoxOfficeMoviiie",
    "BoxOffice_Irani",
    "BoxOfficeGoftegu"
]
WELCOME_IMAGE = "https://i.imgur.com/uZqKsRs.png"
CONFIRM_IMAGE = "https://i.imgur.com/fAGPuXo.png"
DELETE_DELAY = 30

# -------- اتصال به دیتابیس MongoDB --------
mongo_client = MongoClient("mongodb+srv://BoxOffice:136215@boxofficeuploaderbot.2howsv3.mongodb.net/?retryWrites=true&w=majority&appName=BoxOfficeUploaderBot")
db = mongo_client["BoxOfficeDB"]
files_collection = db["files"]
user_sources = db["user_sources"]
upload_data = {}  # وضعیت مراحل آپلود ادمین

# -------- ساخت کلاینت Pyrogram --------
bot = Client("BoxUploader", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ------------------ دستور /start ------------------
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client, message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    film_id = args[1] if len(args) > 1 else None

    # تابع بررسی عضویت کاربر
    async def is_member(uid):
        for channel in REQUIRED_CHANNELS:
            try:
                member = await client.get_chat_member(f"@{channel}", uid)
                if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                    return False
            except:
                return False
        return True

    # اگر از طریق لینک اختصاصی و عضو بود، فایل‌ها رو مستقیماً ارسال کن
    if film_id and await is_member(user_id):
        files = list(files_collection.find({"film_id": film_id}))
        if not files:
            await message.reply("❌ هیچ فایلی برای این فیلم یافت نشد.")
            return

        for f in files:
            msg = await client.send_video(
                chat_id=message.chat.id,
                video=f["file_id"],
                caption=f"🎬 {f['title']} ({f['quality']})\n\n{f['caption']}"
            )
            asyncio.create_task(delete_after_delay(client, msg.chat.id, msg.id))

        warn = await client.send_message(
            chat_id=message.chat.id,
            text="⚠️ فایل‌ها تا ۳۰ ثانیه دیگه حذف می‌شن، فوراً ذخیره کن!"
        )
        asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))
        return

    # اگر عضو نبود یا start بدون لینک بود → ذخیره film_id برای بعد از تأیید عضویت
    if film_id:
        user_sources.update_one(
            {"user_id": user_id},
            {"$set": {"from_film_id": film_id}},
            upsert=True
        )

    # ارسال پیام خوش‌آمد + دکمه‌های عضویت
    buttons = [
        [InlineKeyboardButton("📣 عضویت در @BoxOffice_Animation", url="https://t.me/BoxOffice_Animation")],
        [InlineKeyboardButton("🎬 عضویت در @BoxOfficeMoviiie", url="https://t.me/BoxOfficeMoviiie")],
        [InlineKeyboardButton("🎞 عضویت در @BoxOffice_Irani", url="https://t.me/BoxOffice_Irani")],
        [InlineKeyboardButton("💬 عضویت در گپ @BoxOfficeGoftegu", url="https://t.me/BoxOfficeGoftegu")],
        [InlineKeyboardButton("✅ عضو شدم", callback_data="check_membership")]
    ]

    caption = (
        f"🎬 به ربات UpBox خوش آمدید!\n\n"
        f"ابتدا باید در کانال‌های زیر عضو شوید:"
    )

    await message.reply_photo(
        photo=WELCOME_IMAGE,
        caption=caption,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ------------------ بررسی عضویت کاربر ------------------
@bot.on_callback_query(filters.regex("check_membership"))
async def check_membership(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    missing_channels = []

    for channel in REQUIRED_CHANNELS:
        try:
            member = await client.get_chat_member(f"@{channel}", user_id)
            if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                missing_channels.append(channel)
        except:
            missing_channels.append(channel)

    if missing_channels:
        await callback_query.answer("⛔️ هنوز عضو همه کانال‌ها نشدی!", show_alert=True)
        return

    await callback_query.answer("✅ عضویت تأیید شد!", show_alert=True)
    await client.send_photo(
        chat_id=callback_query.message.chat.id,
        photo=CONFIRM_IMAGE,
        caption="✅ عضویت شما با موفقیت تأیید شد!\nدر حال دریافت فایل‌ها..."
    )

    # 📌 دریافت film_id که قبلاً ذخیره شده
    user_source = user_sources.find_one({"user_id": user_id})
    film_id = user_source.get("from_film_id") if user_source else None

    if not film_id:
        await client.send_message(
            chat_id=callback_query.message.chat.id,
            text="ℹ️ برای دریافت فیلم، لطفاً روی لینک داخل پست کانال کلیک کن."
        )
        return

    # ✅ فقط فایل‌های مرتبط با همون film_id
    files = list(files_collection.find({"film_id": film_id}))
    if not files:
        await client.send_message(chat_id=callback_query.message.chat.id, text="❌ هیچ فایلی برای این فیلم یافت نشد.")
        return

    for f in files:
        msg = await client.send_video(
            chat_id=callback_query.message.chat.id,
            video=f["file_id"],
            caption=f"🎬 {f['title']} ({f['quality']})\n\n{f['caption']}"
        )
        asyncio.create_task(delete_after_delay(client, msg.chat.id, msg.id))

    warn = await client.send_message(
        chat_id=callback_query.message.chat.id,
        text="⚠️ فایل‌ها تا ۳۰ ثانیه دیگه حذف می‌شن، فوراً ذخیره کن!"
    )
    asyncio.create_task(delete_after_delay(client, warn.chat.id, warn.id))

    # 🧹 حذف film_id از دیتابیس کاربر
    user_sources.update_one({"user_id": user_id}, {"$unset": {"from_film_id": ""}})


# ------------------ دستور /upload برای شروع آپلود ------------------
@bot.on_message(filters.command("upload") & filters.private)
async def upload_command(client, message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("⛔️ فقط ادمین می‌تواند فایل آپلود کند.")
    upload_data[message.from_user.id] = {
        "step": "awaiting_file"
    }
    await message.reply("📤 لطفاً فایل ویدیویی، صوتی یا سند را ارسال کنید.")

# ------------------ دریافت فایل و شروع جمع‌آوری اطلاعات ------------------
@bot.on_message(filters.private & (filters.video | filters.document | filters.audio))
async def handle_file_upload(client, message: Message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    if user_id not in upload_data or upload_data[user_id]["step"] != "awaiting_file":
        return

    # گرفتن file_id و ذخیره موقت
    file_id = (
        message.video.file_id if message.video else
        message.document.file_id if message.document else
        message.audio.file_id if message.audio else None
    )
    if not file_id:
        return await message.reply("❌ فایل نامعتبر است.")

    upload_data[user_id]["video_file_id"] = file_id
    upload_data[user_id]["step"] = "awaiting_film_id"
    await message.reply("🎬 لطفاً شناسه فیلم را وارد کن (مثلاً DL_avatar2025):")

# ------------------ مرحله‌ای گرفتن اطلاعات از ادمین ------------------
@bot.on_message(filters.private & filters.text)
async def admin_text_handler(client, message: Message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    if user_id not in upload_data:
        return await message.reply("⚠️ لطفاً ابتدا فایل را ارسال کن.")

    data = upload_data[user_id]

    # گرفتن شناسه فیلم
    if data["step"] == "awaiting_film_id":
        film_id = message.text.strip()
        data["film_id"] = film_id
        data.setdefault("files", [])  # لیست فایل‌ها
        data["step"] = "awaiting_caption"
        return await message.reply("📝 لطفاً کپشن این فایل را وارد کن:")

    # گرفتن کپشن
    if data["step"] == "awaiting_caption":
        caption = message.text.strip()
        data["current_file"] = {"caption": caption}
        data["step"] = "awaiting_quality"
        return await message.reply("📽 لطفاً کیفیت این فایل را وارد کن (مثلاً 720p):")

    # گرفتن کیفیت و رفتن به مرحله تایید ادامه فایل
    if data["step"] == "awaiting_quality":
        quality = message.text.strip()
        file_obj = {
            "film_id": data["film_id"],
            "file_id": data["video_file_id"],
            "caption": data["current_file"]["caption"],
            "quality": quality,
            "title": data["film_id"].replace("DL_", "")
        }
        data["files"].append(file_obj)
        data["step"] = "confirm_more_files"

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ بله", callback_data="upload_more")],
            [InlineKeyboardButton("❌ خیر", callback_data="upload_done")]
        ])

        return await message.reply(
            f"✅ فایل با موفقیت اضافه شد.\n"
            f"🎬 شناسه فیلم: {data['film_id']}\n"
            f"📽 کیفیت: {quality}\n\n"
            f"آیا فایل دیگری برای این عنوان داری؟",
            reply_markup=buttons
        )
# ------------------ دکمه‌های ادامه یا پایان آپلود ------------------
@bot.on_callback_query(filters.regex("upload_more"))
async def upload_more_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id != ADMIN_ID or user_id not in upload_data:
        return await callback_query.answer("⛔️ فقط ادمین مجاز است.", show_alert=True)

    upload_data[user_id]["step"] = "awaiting_file"
    await callback_query.message.reply("📤 لطفاً فایل بعدی را ارسال کن.")
    await callback_query.answer()


@bot.on_callback_query(filters.regex("upload_done"))
async def upload_done_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id != ADMIN_ID or user_id not in upload_data:
        return await callback_query.answer("⛔️ فقط ادمین مجاز است.", show_alert=True)

    data = upload_data[user_id]
    files = data.get("files", [])
    if not files:
        return await callback_query.message.reply("⚠️ هیچ فایلی آپلود نشده است.")

    # ذخیره همه فایل‌ها در MongoDB
    files_collection.insert_many(files)

    # تولید لینک
    film_id = data["film_id"]
    deep_link = f"https://t.me/{BOT_USERNAME}?start={film_id}"

    await callback_query.message.reply(
        f"✅ تمام فایل‌ها با موفقیت ذخیره شدند.\n"
        f"🎬 شناسه فیلم: {film_id}\n"
        f"📁 تعداد فایل‌ها: {len(files)}\n\n"
        f"🔗 لینک اشتراک‌گذاری:\n{deep_link}"
    )

    # پاک کردن داده‌های موقت
    upload_data.pop(user_id, None)
    await callback_query.answer()

# ------------------ تابع حذف پیام با تأخیر ------------------
async def delete_after_delay(bot, chat_id, message_id):
    try:
        await asyncio.sleep(DELETE_DELAY)
        await bot.delete_messages(chat_id, message_id)
    except Exception as e:
        print(f"⚠️ خطا در حذف پیام: {e}")

# ------------------ اجرای نهایی ربات ------------------
print("🤖 ربات با موفقیت راه‌اندازی شد و منتظر دستورات است...")
bot.run()


