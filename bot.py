# ======================= بخش 3: آپلود/ادمین/انتشار/آمار/Scheduler =======================

# استیت‌ها
upload_data: dict[int, dict] = {}
schedule_data: dict[int, dict] = {}
admin_edit_state: dict[int, dict] = {}

# ---------------------- ابزارهای کمکی بخش آپلود/ادمین ----------------------
import unicodedata, string

def slugify(title: str) -> str:
    t = unicodedata.normalize("NFKD", title)
    allowed = string.ascii_letters + string.digits + " _-"
    t = "".join(ch for ch in t if ch in allowed)
    t = t.strip().replace(" ", "_")
    return (t.lower() or "title")[:64]

def _fmt_film_info(film: dict) -> str:
    return (f"🎬 <b>{film.get('title','-')}</b>\n"
            f"🎭 ژانر: {film.get('genre','-')}\n"
            f"📆 سال: {film.get('year','-')}\n"
            f"🖼 کاور: {'✅' if film.get('cover_id') else '❌'}\n"
            f"📂 تعداد فایل: {len(film.get('files', []))}\n"
            f"🆔 {film.get('film_id','-')}")

def _paginate(items, page, page_size=10):
    total = len(items)
    start = (page-1)*page_size
    end = start + page_size
    return items[start:end], total

def kb_admin_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 لیست/جست‌وجوی فیلم‌ها", callback_data="admin_films_1")],
        [InlineKeyboardButton("⏰ مدیریت زمان‌بندی‌ها", callback_data="admin_sched_list_1")],
        [InlineKeyboardButton("📥 خروجی CSV", callback_data="admin_export_csv")],
    ])

# ---------------------- فلو آپلود (ادمین) ----------------------
@bot.on_message(filters.command("upload") & filters.private & filters.user(ADMIN_IDS))
async def upload_command(client: Client, message: Message):
    uid = message.from_user.id
    upload_data[uid] = {"step": "awaiting_title", "files": []}
    await message.reply("🎬 لطفاً <b>عنوان</b> را وارد کن (مثال: آواتار ۲).")

@bot.on_message(filters.private & filters.user(ADMIN_IDS) & filters.text)
async def admin_text_router(client: Client, message: Message):
    uid = message.from_user.id

    # فلو زمان‌بندی
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

    # پنل ادمین (جست‌وجو/ویرایش)
    if uid in admin_edit_state:
        st = admin_edit_state[uid]
        mode = st.get("mode")
        fid  = st.get("film_id")

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
        return

    # فلو آپلود
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

# رسانه‌های ادمین (کاور/فایل‌ها)
@bot.on_message(filters.private & filters.user(ADMIN_IDS) & (filters.photo | filters.video | filters.document | filters.audio))
async def admin_media_router(client: Client, message: Message):
    uid = message.from_user.id

    # حالت‌های ویرایش
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

    # فلو آپلود
    if uid in upload_data:
        data = upload_data[uid]; step = data.get("step")

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
        data.pop("pending_file_id", None); data.pop("current_file", None)
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

# ---------------------- پنل ادمین: ورودی و لیست ----------------------
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
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
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
        [InlineKeyboardButton("🗑 حذف فایل", callback_data=f"file_delete_confirm::{fid}::{idx}")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_files::{fid}")]
    ])
    await cq.message.edit_text(info, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^file_edit_caption::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_caption_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_caption", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("📝 کپشن جدید را بفرست:")

@bot.on_callback_query(filters.regex(r"^file_edit_quality::(.+)::(\d+)$") & filters.user(ADMIN_IDS))
async def file_edit_quality_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1); idx = int(cq.matches[0].group(2))
    admin_edit_state[cq.from_user.id] = {"mode": "file_edit_quality", "film_id": fid, "file_index": idx}
    await cq.message.edit_text("🎞 کیفیت جدید را بفرست (مثلاً 1080p):")

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

# ---------------------- انتشار فوری/زمان‌بندی + آمار زیر پست ----------------------
def build_stats_keyboard(film_id: str, channel_id: int, message_id: int, downloads: int, shares: int, views: int):
    # دکمه دانلود → دیپ‌لینک ساده /start=film_id  (دانلود سراسریِ فیلم)
    row1 = [InlineKeyboardButton("📥 دانلود", url=f"https://t.me/{BOT_USERNAME}?start={film_id}")]
    row2 = [
        InlineKeyboardButton(f"⬇️ دانلود: {downloads}", callback_data="stat:noop"),
        InlineKeyboardButton(f"↗️ اشتراک: {shares}",  callback_data="stat:share"),
        InlineKeyboardButton(f"👁 بازدید: {views}",   callback_data="stat:refresh"),
    ]
    row3 = [InlineKeyboardButton("🔄 بروزرسانی آمار", callback_data="stat:refresh")]
    return InlineKeyboardMarkup([row1, row2, row3])

async def send_channel_post_with_stats(client: Client, film: dict, channel_id: int):
    caption = compose_channel_caption(film)
    # ارسال پست (برای داشتن message_id)
    if film.get("cover_id"):
        sent = await client.send_photo(channel_id, photo=film["cover_id"], caption=caption)
    else:
        sent = await client.send_message(channel_id, text=caption)

    # ثبت رکورد آمار (اگر نبود)
    post_stats.update_one(
        {"film_id": film["film_id"], "channel_id": channel_id, "message_id": sent.id},
        {"$setOnInsert": {
            "downloads": 0, "shares": 0,
            "views": int(getattr(sent, "views", 0) or 0),
            "created_at": datetime.now()
        }},
        upsert=True
    )
    s = post_stats.find_one({"film_id": film["film_id"], "channel_id": channel_id, "message_id": sent.id}) or {}

    # کیبورد آمار
    kb = build_stats_keyboard(
        film["film_id"], channel_id, sent.id,
        downloads=int(s.get("downloads", 0)),
        shares=int(s.get("shares", 0)),
        views=int(s.get("views", 0))
    )
    try:
        await client.edit_message_reply_markup(chat_id=channel_id, message_id=sent.id, reply_markup=kb)
    except Exception as e:
        logging.warning(f"edit_message_reply_markup: {e}")

# انتشار فوری از صفحه فیلم
@bot.on_callback_query(filters.regex(r"^film_pub_pick::(.+)$") & filters.user(ADMIN_IDS))
async def film_pub_pick_channel(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.matches[0].group(1)
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{fid}::{chat_id}")] for title, chat_id in TARGET_CHANNELS.items()]
    await cq.message.edit_text(
        "📣 کانال مقصد برای انتشار فوری را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(rows + [[InlineKeyboardButton("↩️ بازگشت", callback_data=f"film_open::{fid}")]])
    )

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

# زمان‌بندی
@bot.on_callback_query(filters.regex(r"^sched_yes::(.+)$") & filters.user(ADMIN_IDS))
async def ask_schedule_date(client: Client, cq: CallbackQuery):
    await cq.answer()
    fid = cq.data.split("::")[1]
    schedule_data[cq.from_user.id] = {"film_id": fid, "step": "date"}
    await cq.message.reply("📅 تاریخ انتشار را وارد کن (YYYY-MM-DD):")

@bot.on_callback_query(filters.regex(r"^sched_cancel$") & filters.user(ADMIN_IDS))
async def sched_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer()
    schedule_data.pop(cq.from_user.id, None)
    await cq.message.edit_text("⛔️ زمان‌بندی لغو شد.")

@bot.on_callback_query(filters.regex(r"^film_sched_save::(\d{4}-\d{2}-\d2)::(\d{2}:\d{2})::(.+)::(-?\d+)$") & filters.user(ADMIN_IDS))
async def film_sched_save_cb(client: Client, cq: CallbackQuery):
    # (در صورت نیاز می‌تونی همین رِجکس را با قبلی جایگزین کنی؛ در نسخه‌ی بخش ۲ هم وجود داشت)
    pass  # در بخش ۲ پیاده شده بود

@bot.on_callback_query(filters.regex(r"^sched_no::(.+)$") & filters.user(ADMIN_IDS))
async def ask_publish_immediate(client: Client, cq: CallbackQuery):
    await cq.answer()
    film_id = cq.data.split("::")[1]
    rows = [[InlineKeyboardButton(title, callback_data=f"film_pub_go::{film_id}::{chat_id}")] for title, chat_id in TARGET_CHANNELS.items()]
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="pub_cancel")])
    await cq.message.reply("📣 می‌خوای همین الان ارسال کنیم؟ کانال رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(rows))

@bot.on_callback_query(filters.regex(r"^pub_cancel$") & filters.user(ADMIN_IDS))
async def pub_cancel_cb(client: Client, cq: CallbackQuery):
    await cq.answer("لغو شد.")
    await cq.message.edit_text("🚫 ارسال فوری لغو شد.")

# ---------------------- آمار: دکمه‌ها (اشتراک/بروزرسانی) ----------------------
@bot.on_callback_query(filters.regex(r"^stat:share$"))
async def stat_share_cb(client: Client, cq: CallbackQuery):
    ch_id = cq.message.chat.id; msg_id = cq.message.id
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
        logging.warning(f"share/edit kb: {e}")
    # تلاش برای دادن لینک پست
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
    ch_id = cq.message.chat.id; msg_id = cq.message.id
    stat = post_stats.find_one({"channel_id": ch_id, "message_id": msg_id})
    if not stat:
        return await cq.answer("⏳ آمار هنوز ثبت نشده.", show_alert=True)
    # خواندن views فعلی پست
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
        logging.warning(f"refresh/edit kb: {e}")
    await cq.answer("آمار به‌روزرسانی شد ✅", show_alert=False)

@bot.on_callback_query(filters.regex(r"^stat:noop$"))
async def stat_noop_cb(client: Client, cq: CallbackQuery):
    await cq.answer()

# ---------------------- خروجی CSV ----------------------
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

# ---------------------- زمان‌بند خودکار ----------------------
scheduled_posts = db["scheduled_posts"]

async def send_scheduled_posts():
    try:
        now = datetime.now()
        posts = list(scheduled_posts.find({"scheduled_time": {"$lte": now}}))
    except Exception as e:
        logging.error(f"DB unavailable: {e}")
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
            logging.error(f"scheduled send error: {e}")
            scheduled_posts.delete_one({"_id": post["_id"]})

# ---------------------- افزایش دانلود سراسری فیلم بعد از ارسال فایل‌ها ----------------------
# اگر می‌خواهی «دانلود» با هر /start=film_id افزایش یابد، ساده‌ترین جا همین‌جاست:
# یک hook کوچک که بعد از ارسال موفق فایل‌ها اجرا شود.
# برای این کار، یک رپر روی _send_film_files_to_user می‌گذاریم و از این به بعد از آن استفاده می‌کنیم:

async def send_and_count_downloads(client: Client, chat_id: int, film_doc: dict):
    await _send_film_files_to_user(client, chat_id, film_doc)
    # افزایش دانلود سراسری فیلم در همه پست‌ها (فیلد total_downloads داخل سند film)
    films_col.update_one({"film_id": film_doc["film_id"]}, {"$inc": {"total_downloads": 1}})
    # همچنین اگر دوست داشتی آخرین پست کانالی را هم sync کنی، می‌توانیم یک cron دیگر اضافه کنیم.

# اختیاری: اگر خواستی همین حالا هندلر start از بخش ۲ بجای _send_film_files_to_user این تابع را صدا بزند:
# کافیست در آن هندلر، خط:
#     await _send_film_files_to_user(client, message.chat.id, film)
# را به:
#     await send_and_count_downloads(client, message.chat.id, film)
# تغییر بدهی.

# ---------------------- اجرای نهایی (event loop-safe) ----------------------
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import idle

async def _main():
    await bot.start()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_scheduled_posts, "interval", minutes=1, id="send_sched", max_instances=1, coalesce=True)
    scheduler.start()

    print("🤖 ربات با موفقیت راه‌اندازی شد و منتظر دستورات است...")
    try:
        await idle()
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            await bot.stop()
        except Exception:
            pass

if __name__ == "__main__":
    import asyncio
    asyncio.run(_main())
