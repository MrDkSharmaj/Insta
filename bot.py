import asyncio
import os
import sys
import time
import sqlite3
import logging
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler, ChatJoinRequestHandler
)
from telegram.constants import ParseMode, ChatAction

# ── LOGGING CONFIG ──────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── CONFIG (set these as Environment Variables in Render's dashboard) ──
# Render > your service > Environment > Add Environment Variable
#   BOT_TOKEN = <token from @BotFather>
#   ADMIN_ID  = <your numeric Telegram user id>
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()
DB_PATH = os.environ.get("DB_PATH", "bot.db")

if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set. Add it in Render's dashboard.")
    sys.exit(1)

if not ADMIN_ID_RAW.isdigit():
    logger.error("ADMIN_ID environment variable is missing or invalid. Add it in Render's dashboard.")
    sys.exit(1)

ADMIN_ID = int(ADMIN_ID_RAW)

BTN1      = "🎯 Claim Agent"
BTN2      = "📊 Statistics"
BTN3      = "🤝 Refer & Earn"

# ── STATES ──────────────────────────────────────────────────────
(
    S_CH_ID, S_CH_NAME, S_CH_LINK,
    S_WELCOME, S_WELCOME_PHOTO, S_POSTJOIN, S_TOP,
    S_BTN1, S_BTN2, S_BTN3,
    S_BCAST
) = range(11)

# ── DATABASE WITH WAL MODE (HIGH CONCURRENCY) ───────────────────
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    c = db()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_name TEXT, joined_at TEXT);
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT, channel_name TEXT, channel_link TEXT,
            position INTEGER DEFAULT 0, order_num INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS join_requests (
            user_id INTEGER, channel_id TEXT,
            PRIMARY KEY (user_id, channel_id));
        CREATE TABLE IF NOT EXISTS broadcast_msgs (
            bcast_id TEXT, user_id INTEGER, message_id INTEGER);
    """)
    defaults = {
        "welcome": "👋 Welcome!\n\n🛑 Join all channels below.\n\n💣 Then click ✅ Joined",
        "welcome_photo": "",
        "postjoin": "🏛️ Welcome!\n\n📋 Rules\n• One agent per user\n• Permanent assignment",
        "top": "",
        "btn1_msg": "🎯 Agent claim coming soon!",
        "btn2_msg": "📊 Statistics coming soon!",
        "btn3_msg": "🤝 Refer & Earn coming soon!",
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (k, v))
    c.commit()
    c.close()

def gset(key):
    try:
        c = db(); r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone(); c.close()
        return r[0] if r else ""
    except Exception as e:
        logger.error(f"gset error: {e}")
        return ""

def sset(key, val):
    try:
        c = db(); c.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, val)); c.commit(); c.close()
    except Exception as e:
        logger.error(f"sset error: {e}")

def get_channels():
    try:
        c = db(); rows = c.execute("SELECT id,channel_id,channel_name,channel_link,position,order_num FROM channels ORDER BY order_num,id").fetchall(); c.close(); return rows
    except Exception as e:
        logger.error(f"get_channels error: {e}")
        return []

def add_channel(ch_id, name, link):
    c = db()
    mx = c.execute("SELECT MAX(order_num) FROM channels").fetchone()[0] or 0
    c.execute("INSERT INTO channels (channel_id,channel_name,channel_link,position,order_num) VALUES (?,?,?,0,?)", (ch_id, name, link, mx+1))
    c.commit(); c.close()

def del_channel(db_id):
    c = db(); c.execute("DELETE FROM channels WHERE id=?", (db_id,)); c.commit(); c.close()

def move_ch(db_id, pos):
    c = db(); c.execute("UPDATE channels SET position=? WHERE id=?", (pos, db_id)); c.commit(); c.close()

def record_req(user_id, ch_id):
    try:
        c = db(); c.execute("INSERT OR IGNORE INTO join_requests VALUES (?,?)", (user_id, str(ch_id))); c.commit(); c.close()
    except: pass

def has_req(user_id, ch_id):
    try:
        c = db(); r = c.execute("SELECT 1 FROM join_requests WHERE user_id=? AND channel_id=?", (user_id, str(ch_id))).fetchone(); c.close(); return r is not None
    except: return False

def total_users():
    try:
        c = db(); n = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]; c.close(); return n
    except: return 0

def add_user(user_id, name):
    try:
        c = db(); c.execute("INSERT OR IGNORE INTO users VALUES (?,?,?)", (user_id, name, datetime.now().isoformat()))
        new = c.total_changes > 0; c.commit(); c.close(); return new
    except: return False

def all_users():
    try:
        c = db(); rows = c.execute("SELECT user_id FROM users").fetchall(); c.close(); return [r[0] for r in rows]
    except: return []

def save_bcast_msg(bcast_id, user_id, message_id):
    try:
        c = db(); c.execute("INSERT INTO broadcast_msgs VALUES (?,?,?)", (bcast_id, user_id, message_id)); c.commit(); c.close()
    except: pass

def get_bcast_msgs(bcast_id):
    try:
        c = db(); rows = c.execute("SELECT user_id, message_id FROM broadcast_msgs WHERE bcast_id=?", (bcast_id,)).fetchall(); c.close(); return rows
    except: return []

def del_bcast_record(bcast_id):
    try:
        c = db(); c.execute("DELETE FROM broadcast_msgs WHERE bcast_id=?", (bcast_id,)); c.commit(); c.close()
    except: pass

# ── HELPERS ─────────────────────────────────────────────────────
async def typing(bot, chat_id, delay=0.5):
    try:
        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        await asyncio.sleep(delay)
    except: pass

async def check_joined(bot, user_id):
    chs = get_channels()
    if not chs: return True, set()
    joined = set()
    for ch in chs:
        ch_id = ch[1]; ok = False
        try:
            m = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            ok = m.status in ("member","administrator","creator","restricted")
        except: pass
        if not ok: ok = has_req(user_id, ch_id)
        if ok: joined.add(ch_id)
    return len(joined) >= len(chs), joined

async def send_welcome(bot, chat_id, text, kb):
    photo = gset("welcome_photo")
    try:
        if photo:
            await bot.send_photo(chat_id, photo, caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"send_welcome error: {e}")

# ── KEYBOARDS ───────────────────────────────────────────────────
def join_kb(chs, joined=None):
    if joined is None: joined = set()
    left, right = [], []
    for ch in chs:
        _, ch_id, name, link, pos, _ = ch
        if ch_id in joined: continue
        b = InlineKeyboardButton(f" {name}", url=link)
        (left if pos == 0 else right).append(b)
    rows = []
    for i in range(max(len(left), len(right), 0)):
        row = []
        if i < len(left): row.append(left[i])
        if i < len(right): row.append(right[i])
        if row: rows.append(row)
    rows.append([InlineKeyboardButton("✅ Joined", callback_data="check_joined")])
    return InlineKeyboardMarkup(rows)

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BTN1, callback_data="btn1"),
         InlineKeyboardButton(BTN2, callback_data="btn2")],
        [InlineKeyboardButton(BTN3, callback_data="btn3")],
    ])

def back_kb(cb):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=cb)]])

# ── USER FLOW ───────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_new = add_user(user.id, user.first_name or "")
    if is_new:
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"✨ NEW MEMBER\n👤 {user.first_name}\n🆔 {user.id}\n📅 {datetime.now().strftime('%d %b %Y %I:%M %p')}\n👥 Total: {total_users()}",
                parse_mode=ParseMode.HTML
            )
        except: pass
    chs = get_channels()
    all_joined, joined = await check_joined(context.bot, user.id)
    await typing(context.bot, user.id, 0.3)
    if not chs or all_joined:
        await update.message.reply_text(gset("postjoin"), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
    else:
        top = gset("top"); w = gset("welcome")
        txt = (top + "\n\n" + w) if top else w
        await send_welcome(context.bot, user.id, txt, join_kb(chs, joined))

async def cb_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    chs = get_channels(); all_joined, joined = await check_joined(context.bot, q.from_user.id)

    if all_joined:
        await q.answer("✅ Verified!")
        try:
            await q.edit_message_text(gset("postjoin"), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
        except:
            try: await q.message.delete()
            except: pass
            await context.bot.send_message(q.message.chat_id, gset("postjoin"), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
        return

    await q.answer(
        "🚫 You Haven't Joined All Required Channels!\n\nPlease join every channel below, then tap ✅ Joined again.",
        show_alert=True
    )
    top = gset("top"); w = gset("welcome"); txt = (top + "\n\n" + w) if top else w
    try:
        await q.message.delete()
    except: pass
    await typing(context.bot, q.message.chat_id, 0.3)
    await send_welcome(context.bot, q.message.chat_id, txt, join_kb(chs, joined))

async def cb_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    key = {"btn1":"btn1_msg","btn2":"btn2_msg","btn3":"btn3_msg"}.get(q.data)
    if not key: return
    msg = gset(key) or "⚙️ Coming soon!"
    try: await q.edit_message_text(msg, reply_markup=back_kb("back_main"), parse_mode=ParseMode.HTML)
    except: await context.bot.send_message(q.message.chat_id, msg, reply_markup=back_kb("back_main"), parse_mode=ParseMode.HTML)

async def cb_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    try: await q.edit_message_text(gset("postjoin"), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
    except: await context.bot.send_message(q.message.chat_id, gset("postjoin"), reply_markup=main_kb(), parse_mode=ParseMode.HTML)

async def handle_join_req(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = update.chat_join_request
    if r: record_req(r.from_user.id, str(r.chat.id))

# ── ADMIN PANEL ─────────────────────────────────────────────────
def admin_text():
    return (
        "╔═━━━✦ 🤖  FORCE JOINING BOT ✦━━━═╗\n\n"
        f"👑 Owner      ➤  Rᴀᴜsʜᴀɴ Yᴀᴅᴀᴠ\n"
        f"👥 Members    ➤  <b>{total_users()}</b>\n"
        f"🔰 Status     ➤  🟢 Online\n\n"
        "╚═━━━✦━━━━━━━━━━━━━━✦━━━═╝"
    )

def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Channels", callback_data="a_chs"),
         InlineKeyboardButton("✉️ Broadcast", callback_data="a_bcast")],
        [InlineKeyboardButton("📝 Messages", callback_data="a_msgs"),
         InlineKeyboardButton("👥 Members", callback_data="a_total")],
        [InlineKeyboardButton("❌ Close", callback_data="a_close")],
    ])

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await typing(context.bot, update.effective_chat.id, 0.2)
    await update.message.reply_text(admin_text(), reply_markup=admin_kb(), parse_mode=ParseMode.HTML)

async def show_admin(q):
    await q.edit_message_text(admin_text(), reply_markup=admin_kb(), parse_mode=ParseMode.HTML)

async def show_channels(q):
    chs = get_channels()
    txt = "📢 <b>Channels</b>\n\n" + ("".join(f"{i}. <b>{c[2]}</b> [{'⬅️' if c[4]==0 else '➡️'}]\n" for i,c in enumerate(chs,1)) or "None added.\n")
    rows = [[InlineKeyboardButton(f"🗑 {c[2]}", callback_data=f"a_del_{c[0]}"),
             InlineKeyboardButton("⬅️", callback_data=f"a_left_{c[0]}"),
             InlineKeyboardButton("➡️", callback_data=f"a_right_{c[0]}")] for c in chs]
    rows += [[InlineKeyboardButton("➕ Add Channel", callback_data="a_addch")],
             [InlineKeyboardButton("🔙 Back", callback_data="a_back")]]
    await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

async def show_msgs(q):
    photo_status = "✅ Set" if gset("welcome_photo") else "— Not set"
    await q.edit_message_text(
        f"╔═✦ 📝 MESSAGES ✦═╗\n⚠️ Button names are fixed.\n🖼 Welcome Photo: {photo_status}\n╚═✦━━━━━━━━━━━✦═╝",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Top Message", callback_data="a_top")],
            [InlineKeyboardButton("👋 Welcome Message", callback_data="a_welcome")],
            [InlineKeyboardButton("🖼 Welcome Photo", callback_data="a_welcome_photo")],
            [InlineKeyboardButton("🎉 Post-Join Message", callback_data="a_postjoin")],
            [InlineKeyboardButton(f"✉️ {BTN1}", callback_data="a_btn1")],
            [InlineKeyboardButton(f"✉️ {BTN2}", callback_data="a_btn2")],
            [InlineKeyboardButton(f"✉️ {BTN3}", callback_data="a_btn3")],
            [InlineKeyboardButton("🔙 Back", callback_data="a_back")],
        ]),
        parse_mode=ParseMode.HTML
    )

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        await q.answer("❌ Not authorized!", show_alert=True); return
    await q.answer(); d = q.data

    if d == "a_back":    await show_admin(q)
    elif d == "a_chs":   await show_channels(q)
    elif d == "a_msgs":  await show_msgs(q)
    elif d == "a_close":
        try: await q.message.delete()
        except: await q.edit_message_reply_markup(None)

    elif d == "a_total":
        await q.edit_message_text(f"👥 Total Members: <b>{total_users()}</b>",
            reply_markup=back_kb("a_back"), parse_mode=ParseMode.HTML)

    elif d == "a_addch":
        await q.edit_message_text("📢 Send Channel ID\nExample: <code>-1001234567890</code>\n\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_CH_ID

    elif d.startswith("a_del_"):
        del_channel(int(d.split("_")[-1])); await q.answer("✅ Removed!", show_alert=True); await show_channels(q)
    elif d.startswith("a_left_"):
        move_ch(int(d.split("_")[-1]), 0); await show_channels(q)
    elif d.startswith("a_right_"):
        move_ch(int(d.split("_")[-1]), 1); await show_channels(q)

    elif d == "a_bcast":
        await q.edit_message_text(
            "📣 Send <b>text</b>, <b>photo+caption</b>, or <b>video+caption</b> to broadcast.\n\n/cancel to stop.",
            parse_mode=ParseMode.HTML)
        return S_BCAST

    elif d.startswith("a_delbc_"):
        bcast_id = d[len("a_delbc_"):]
        rows = get_bcast_msgs(bcast_id)
        removed = 0
        for uid, mid in rows:
            try:
                await context.bot.delete_message(chat_id=uid, message_id=mid)
                removed += 1
            except: pass
        del_bcast_record(bcast_id)
        await q.edit_message_text(f"🗑 Broadcast deleted from <b>{removed}</b> chats.", reply_markup=back_kb("a_back"), parse_mode=ParseMode.HTML)

    elif d == "a_top":
        await q.edit_message_text(f"✏️ <b>Top Message</b>\nNow: <i>{gset('top') or '(empty)'}</i>\n\nSend new or <code>clear</code>.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_TOP
    elif d == "a_welcome":
        await q.edit_message_text(f"✏️ <b>Welcome Message</b>\nNow:\n<i>{gset('welcome')}</i>\n\nSend new text.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_WELCOME
    elif d == "a_welcome_photo":
        cur = "✅ A photo is currently set." if gset("welcome_photo") else "— No photo currently set."
        await q.edit_message_text(
            f"🖼 <b>Welcome Photo</b>\n{cur}\n\nSend a new photo to set it, or send <code>clear</code> to remove it.\n/cancel to stop.",
            parse_mode=ParseMode.HTML)
        return S_WELCOME_PHOTO
    elif d == "a_postjoin":
        await q.edit_message_text(f"✏️ <b>Post-Join Message</b>\nNow:\n<i>{gset('postjoin')}</i>\n\nSend new text.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_POSTJOIN
    elif d == "a_btn1":
        await q.edit_message_text(f"✏️ <b>{BTN1}</b>\nNow: <i>{gset('btn1_msg')}</i>\n\nSend new reply.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_BTN1
    elif d == "a_btn2":
        await q.edit_message_text(f"✏️ <b>{BTN2}</b>\nNow: <i>{gset('btn2_msg')}</i>\n\nSend new reply.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_BTN2
    elif d == "a_btn3":
        await q.edit_message_text(f"✏️ <b>{BTN3}</b>\nNow: <i>{gset('btn3_msg')}</i>\n\nSend new reply.\n/cancel to stop.", parse_mode=ParseMode.HTML)
        return S_BTN3

    return ConversationHandler.END

# ── CONV STEPS ──────────────────────────────────────────────────
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Cancelled."); return ConversationHandler.END

async def s_ch_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["ch_id"] = update.message.text.strip()
    await update.message.reply_text("✏️ Send Channel Name:"); return S_CH_NAME

async def s_ch_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["ch_name"] = update.message.text.strip()
    await update.message.reply_text("🔗 Send Channel Link (https://t.me/...):"); return S_CH_LINK

async def s_ch_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ch_id = context.user_data.get("ch_id"); name = context.user_data.get("ch_name")
    if not ch_id or not name: await update.message.reply_text("❌ Error."); return ConversationHandler.END
    add_channel(ch_id, name, update.message.text.strip())
    await update.message.reply_text(f"✅ <b>{name}</b> added!", parse_mode=ParseMode.HTML, reply_markup=back_kb("a_chs"))
    context.user_data.clear(); return ConversationHandler.END

async def s_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text_html.strip(); sset("top", "" if update.message.text.strip().lower()=="clear" else t)
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

async def s_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sset("welcome", update.message.text_html.strip())
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

async def s_welcome_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip().lower() == "clear":
        sset("welcome_photo", "")
        await update.message.reply_text("✅ Welcome photo removed!", reply_markup=back_kb("a_msgs"))
        return ConversationHandler.END
    if not update.message.photo:
        await update.message.reply_text("❌ Please send a photo, or send clear to remove it.\n/cancel to stop.")
        return S_WELCOME_PHOTO
    sset("welcome_photo", update.message.photo[-1].file_id)
    await update.message.reply_text("✅ Welcome photo updated!", reply_markup=back_kb("a_msgs"))
    return ConversationHandler.END

async def s_postjoin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sset("postjoin", update.message.text_html.strip())
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

async def s_btn1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sset("btn1_msg", update.message.text_html.strip())
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

async def s_btn2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sset("btn2_msg", update.message.text_html.strip())
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

async def s_btn3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sset("btn3_msg", update.message.text_html.strip())
    await update.message.reply_text("✅ Updated!", reply_markup=back_kb("a_msgs")); return ConversationHandler.END

SPINNER = ["⏳", "🔄", "📡", "🚀"]

async def s_bcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ids = all_users(); ok = fail = 0
    is_photo = bool(update.message.photo)
    is_video = bool(update.message.video)
    bcast_id = str(int(time.time() * 1000))

    st = await update.message.reply_text(f"{SPINNER[0]} Sending to {len(ids)} users...")

    for i, uid in enumerate(ids):
        try:
            if is_photo:
                sent = await context.bot.send_photo(
                    uid, update.message.photo[-1].file_id,
                    caption=update.message.caption_html or "", parse_mode=ParseMode.HTML)
            elif is_video:
                sent = await context.bot.send_video(
                    uid, update.message.video.file_id,
                    caption=update.message.caption_html or "", parse_mode=ParseMode.HTML)
            else:
                sent = await context.bot.send_message(
                    uid, update.message.text_html, parse_mode=ParseMode.HTML)
            save_bcast_msg(bcast_id, uid, sent.message_id)
            ok += 1
        except:
            fail += 1

        if i % 25 == 0 and i > 0:
            frame = SPINNER[(i // 25) % len(SPINNER)]
            try:
                await st.edit_text(f"{frame} Sending... ({i}/{len(ids)})\n✅ {ok}  ❌ {fail}")
            except: pass

        await asyncio.sleep(0.02)

    await st.edit_text(
        f"✅ Broadcast Done!\n✅ Sent: {ok}\n❌ Failed: {fail}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Delete This Broadcast", callback_data=f"a_delbc_{bcast_id}")],
            [InlineKeyboardButton("🔙 Back", callback_data="a_back")],
        ])
    )
    return ConversationHandler.END

# ── HEALTH-CHECK WEB SERVER (required for Render's "Web Service") ──
# Render Web Services must bind to $PORT or the deploy is marked unhealthy
# and gets restarted in a loop. This stdlib-only server just answers
# health-check pings while the bot itself runs on long-polling.
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running")

    def log_message(self, format, *args):
        pass  # silence default per-request logging

def _run_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info(f"Health-check server listening on port {port}")
    server.serve_forever()

# ── MAIN ────────────────────────────────────────────────────────
def main():
    init_db()

    Thread(target=_run_health_server, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).build()
    bf = (filters.TEXT | filters.PHOTO | filters.VIDEO) & ~filters.COMMAND
    tf = filters.TEXT & ~filters.COMMAND
    pf = (filters.PHOTO | filters.TEXT) & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[CommandHandler("admin", admin_cmd), CallbackQueryHandler(admin_cb, pattern="^a_")],
        states={
            S_CH_ID:         [MessageHandler(tf, s_ch_id)],
            S_CH_NAME:       [MessageHandler(tf, s_ch_name)],
            S_CH_LINK:       [MessageHandler(tf, s_ch_link)],
            S_TOP:           [MessageHandler(tf, s_top)],
            S_WELCOME:       [MessageHandler(tf, s_welcome)],
            S_WELCOME_PHOTO: [MessageHandler(pf, s_welcome_photo)],
            S_POSTJOIN:      [MessageHandler(tf, s_postjoin)],
            S_BTN1:          [MessageHandler(tf, s_btn1)],
            S_BTN2:          [MessageHandler(tf, s_btn2)],
            S_BTN3:          [MessageHandler(tf, s_btn3)],
            S_BCAST:         [MessageHandler(bf, s_bcast)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_chat=False, per_user=True, allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(cb_check, pattern="^check_joined$"))
    app.add_handler(CallbackQueryHandler(cb_btn,   pattern="^btn[123]$"))
    app.add_handler(CallbackQueryHandler(cb_back,  pattern="^back_main$"))
    app.add_handler(ChatJoinRequestHandler(handle_join_req))

    logger.info("✅ Bot started successfully with high performance settings!")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
