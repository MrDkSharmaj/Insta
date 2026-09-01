import asyncio
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.constants import ChatType
from telegram.error import (
    TelegramBadRequest,
    TelegramError,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "8801263966:AAHDah0fgMFFPe7TLPtYA0XNNxgqIiVCwJI").strip()
OWNER_IDS_RAW = os.getenv("OWNER_IDS", "8753914631").strip()
DATABASE_PATH = os.getenv("DATABASE_PATH", "join_manager.sqlite3").strip() or "join_manager.sqlite3"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()

try:
    OWNER_IDS = {int(x.strip()) for x in OWNER_IDS_RAW.split(",") if x.strip()}
except ValueError as exc:
    raise SystemExit("OWNER_IDS must be a comma-separated list of numeric Telegram user IDs.") from exc

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is missing. Set BOT_TOKEN before starting the bot.")
if not OWNER_IDS:
    raise SystemExit("OWNER_IDS is missing or empty. Set OWNER_IDS to at least one numeric Telegram user ID.")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("join-request-manager")

MODE_ACCEPT = "accept"
MODE_DECLINE = "decline"
MODE_MANUAL = "manual"
MODE_DISABLED = "disabled"

PAGE_SIZE = 8
MAX_KEYWORD_LENGTH = 64
RATE_WINDOW_SECONDS = 60
RATE_MAX_REQUESTS = 30

BOT_STARTED_AT = time.monotonic()
db_lock = asyncio.Lock()
rate_lock = asyncio.Lock()
user_rate: dict[int, list[float]] = {}


# ============================================================
# DATABASE
# ============================================================

class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(
            self.path,
            timeout=15,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=15000")
        self.initialize()

    def initialize(self) -> None:
        assert self.conn is not None
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS channels (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                chat_type TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                mode TEXT NOT NULL DEFAULT 'manual',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS whitelist (
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                added_by INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, user_id),
                FOREIGN KEY(channel_id) REFERENCES channels(chat_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS blacklist (
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                reason TEXT,
                added_by INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, user_id),
                FOREIGN KEY(channel_id) REFERENCES channels(chat_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                action TEXT NOT NULL,
                reason TEXT,
                mode TEXT,
                timestamp TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                UNIQUE(channel_id, user_id, timestamp),
                FOREIGN KEY(channel_id) REFERENCES channels(chat_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS processed_requests (
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                request_key TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(channel_id, user_id, request_key)
            );

            CREATE TABLE IF NOT EXISTS settings (
                channel_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY(channel_id, key),
                FOREIGN KEY(channel_id) REFERENCES channels(chat_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                added_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_requests_channel_time
                ON requests(channel_id, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_requests_user
                ON requests(user_id);
            CREATE INDEX IF NOT EXISTS idx_requests_action
                ON requests(action);
            CREATE INDEX IF NOT EXISTS idx_whitelist_user
                ON whitelist(user_id);
            CREATE INDEX IF NOT EXISTS idx_blacklist_user
                ON blacklist(user_id);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        assert self.conn is not None
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def executemany(self, sql: str, params) -> None:
        assert self.conn is not None
        self.conn.executemany(sql, params)
        self.conn.commit()

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        assert self.conn is not None
        return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        assert self.conn is not None
        return self.conn.execute(sql, params).fetchall()


DB = Database(DATABASE_PATH)


# ============================================================
# UTILITIES
# ============================================================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def display_name(user) -> str:
    name = " ".join(x for x in [user.first_name, user.last_name] if x).strip()
    return name or (f"@{user.username}" if user.username else str(user.id))


def mode_label(mode: str) -> str:
    return {
        MODE_ACCEPT: "🟢 Auto Accept",
        MODE_DECLINE: "🔴 Auto Decline",
        MODE_MANUAL: "🟡 Manual Approval",
        MODE_DISABLED: "⏸ Disabled",
    }.get(mode, "❓ Unknown")


def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


async def db_call(fn, *args, **kwargs):
    async with db_lock:
        return await asyncio.to_thread(fn, *args, **kwargs)


def _get_setting_sync(channel_id: int, key: str, default: str) -> str:
    row = DB.fetchone("SELECT value FROM settings WHERE channel_id=? AND key=?", (channel_id, key))
    return row["value"] if row else default


async def get_setting(channel_id: int, key: str, default: str) -> str:
    return await db_call(_get_setting_sync, channel_id, key, default)


def _set_setting_sync(channel_id: int, key: str, value: str) -> None:
    DB.execute(
        """
        INSERT INTO settings(channel_id,key,value) VALUES(?,?,?)
        ON CONFLICT(channel_id,key) DO UPDATE SET value=excluded.value
        """,
        (channel_id, key, value),
    )


async def set_setting(channel_id: int, key: str, value: str) -> None:
    await db_call(_set_setting_sync, channel_id, key, value)


def _channel_sync(chat_id: int):
    return DB.fetchone("SELECT * FROM channels WHERE chat_id=?", (chat_id,))


async def get_channel(chat_id: int):
    return await db_call(_channel_sync, chat_id)


async def managed_channels():
    return await db_call(lambda: DB.fetchall("SELECT * FROM channels ORDER BY title COLLATE NOCASE"))


def _save_user_sync(user) -> None:
    DB.execute(
        """
        INSERT INTO users(user_id,username,first_name,last_name,last_seen_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            last_seen_at=excluded.last_seen_at
        """,
        (user.id, user.username, user.first_name, user.last_name, now_iso()),
    )


async def save_user(user) -> None:
    await db_call(_save_user_sync, user)


def _request_exists_sync(channel_id: int, user_id: int, request_key: str) -> bool:
    row = DB.fetchone(
        "SELECT 1 FROM processed_requests WHERE channel_id=? AND user_id=? AND request_key=?",
        (channel_id, user_id, request_key),
    )
    return row is not None


async def request_exists(channel_id: int, user_id: int, request_key: str) -> bool:
    return await db_call(_request_exists_sync, channel_id, user_id, request_key)


def _mark_request_sync(channel_id: int, user_id: int, request_key: str, status: str) -> bool:
    try:
        DB.execute(
            """
            INSERT INTO processed_requests(channel_id,user_id,request_key,status,created_at)
            VALUES(?,?,?,?,?)
            """,
            (channel_id, user_id, request_key, status, now_iso()),
        )
        return True
    except sqlite3.IntegrityError:
        return False


async def mark_request(channel_id: int, user_id: int, request_key: str, status: str) -> bool:
    return await db_call(_mark_request_sync, channel_id, user_id, request_key, status)


def _record_request_sync(
    channel_id: int, user, action: str, reason: str, mode: str, success: bool, error_message: str = ""
) -> None:
    DB.execute(
        """
        INSERT INTO requests(
            channel_id,user_id,username,first_name,last_name,action,reason,mode,
            timestamp,success,error_message
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            channel_id,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            action,
            reason,
            mode,
            now_iso(),
            int(success),
            error_message[:1000] if error_message else None,
        ),
    )


async def record_request(channel_id: int, user, action: str, reason: str, mode: str, success: bool, error_message: str = "") -> None:
    await db_call(_record_request_sync, channel_id, user, action, reason, mode, success, error_message)


def _membership_sync(table: str, channel_id: int, user_id: int):
    if table not in {"whitelist", "blacklist"}:
        raise ValueError("invalid table")
    return DB.fetchone(
        f"SELECT * FROM {table} WHERE channel_id=? AND user_id=?",
        (channel_id, user_id),
    )


async def membership(table: str, channel_id: int, user_id: int):
    return await db_call(_membership_sync, table, channel_id, user_id)


async def within_rate_limit(user_id: int) -> bool:
    now = time.monotonic()
    async with rate_lock:
        values = [x for x in user_rate.get(user_id, []) if now - x < RATE_WINDOW_SECONDS]
        if len(values) >= RATE_MAX_REQUESTS:
            user_rate[user_id] = values
            return False
        values.append(now)
        user_rate[user_id] = values
        return True


def safe_int(text: str) -> Optional[int]:
    try:
        value = int(text.strip())
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def parse_username(text: str) -> Optional[str]:
    text = text.strip()
    if text.startswith("@"):
        text = text[1:]
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", text):
        return None
    return "@" + text


def format_uptime() -> str:
    seconds = int(time.monotonic() - BOT_STARTED_AT)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


# ============================================================
# KEYBOARDS / UI
# ============================================================

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Channels", callback_data="channels"),
         InlineKeyboardButton("⚙️ Auto Mode", callback_data="mode")],
        [InlineKeyboardButton("📋 Requests", callback_data="requests"),
         InlineKeyboardButton("👤 Whitelist", callback_data="whitelist")],
        [InlineKeyboardButton("🚫 Blacklist", callback_data="blacklist"),
         InlineKeyboardButton("📊 Statistics", callback_data="stats")],
        [InlineKeyboardButton("📜 Logs", callback_data="logs"),
         InlineKeyboardButton("🔔 Notifications", callback_data="notify")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast"),
         InlineKeyboardButton("💾 Backup", callback_data="backup")],
        [InlineKeyboardButton("⚡ Bot Status", callback_data="status"),
         InlineKeyboardButton("ℹ️ Help", callback_data="help")],
    ])


def back_keyboard(target: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data=target),
         InlineKeyboardButton("🏠 Main Menu", callback_data="main")]
    ])


def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])


async def selected_channel(context: ContextTypes.DEFAULT_TYPE):
    cid = context.user_data.get("channel_id")
    if not cid:
        channels = await managed_channels()
        return channels[0] if channels else None
    return await get_channel(int(cid))


async def channel_picker(prefix: str) -> InlineKeyboardMarkup:
    channels = await managed_channels()
    rows = []
    for ch in channels[:30]:
        title = ch["title"][:32]
        rows.append([InlineKeyboardButton(title, callback_data=f"{prefix}:{ch['chat_id']}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="main")])
    return InlineKeyboardMarkup(rows)


async def render_main(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False) -> None:
    channels = await managed_channels()
    text = (
        "🤖 *Telegram Join Request Manager*\n\n"
        f"📢 Managed Channels: *{len(channels)}*\n"
        "Choose an option:"
    )
    if edit and update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_keyboard())
    elif update.effective_message:
        await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=main_keyboard())


# ============================================================
# CHANNEL VALIDATION / MANAGEMENT
# ============================================================

async def validate_channel(bot, chat_id: int):
    try:
        chat = await bot.get_chat(chat_id)
        if chat.type not in {ChatType.CHANNEL}:
            return None, "The supplied chat is not a channel."
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if member.status not in {ChatMember.ADMINISTRATOR, ChatMember.OWNER}:
            return None, "The bot is not an administrator in this channel."
        if member.status == ChatMember.ADMINISTRATOR and not getattr(member, "can_invite_users", False):
            return None, "The bot administrator account lacks the required permission to manage join requests."
        return chat, None
    except TelegramForbiddenError:
        return None, "Telegram denied access. Make sure the bot is an administrator."
    except TelegramBadRequest as exc:
        return None, f"Telegram rejected the channel: {exc}"
    except TelegramError as exc:
        return None, f"Telegram error: {exc}"


async def add_channel_from_id(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    chat_id = safe_int(text)
    if chat_id is None:
        await update.effective_message.reply_text(
            "❌ Invalid channel ID.\n\nSend a numeric channel ID such as -1001234567890.",
            reply_markup=cancel_keyboard(),
        )
        return

    chat, error = await validate_channel(context.bot, chat_id)
    if error:
        await update.effective_message.reply_text(f"❌ {error}", reply_markup=cancel_keyboard())
        return

    async with db_lock:
        await asyncio.to_thread(
            DB.execute,
            """
            INSERT INTO channels(chat_id,title,username,chat_type,enabled,mode,created_at)
            VALUES(?,?,?,?,1,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title,
                username=excluded.username,
                chat_type=excluded.chat_type
            """,
            (chat.id, chat.title or "Untitled", chat.username, chat.type, MODE_MANUAL, now_iso()),
        )
    context.user_data["channel_id"] = chat.id
    context.user_data["state"] = None
    await update.effective_message.reply_text(
        f"✅ Channel added.\n\n📢 {chat.title}\n🆔 `{chat.id}`\n"
        f"🔗 @{chat.username}" if chat.username else f"✅ Channel added.\n\n📢 {chat.title}\n🆔 `{chat.id}`",
        parse_mode="Markdown",
        reply_markup=back_keyboard("channels"),
    )


# ============================================================
# REQUEST PROCESSING
# ============================================================

async def notify_owners(context: ContextTypes.DEFAULT_TYPE, channel_id: int, text: str, reply_markup=None, setting_key: str = "notify_new") -> None:
    enabled = await get_setting(channel_id, setting_key, "1")
    if enabled != "1":
        return
    for owner_id in OWNER_IDS:
        try:
            await context.bot.send_message(owner_id, text, reply_markup=reply_markup, parse_mode="Markdown")
        except TelegramError as exc:
            LOGGER.warning("Could not notify owner %s: %s", owner_id, exc)


async def telegram_action_with_retry(bot, action: str, chat_id: int, user_id: int) -> tuple[bool, str]:
    for attempt in range(3):
        try:
            if action == "approve":
                ok = await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            else:
                ok = await bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
            return bool(ok), ""
        except TelegramRetryAfter as exc:
            if attempt == 2:
                return False, f"Rate limited; retry-after={exc.retry_after}s"
            await asyncio.sleep(min(float(exc.retry_after), 10.0))
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNetworkError, TelegramError) as exc:
            return False, str(exc)
    return False, "Unknown Telegram error"


async def process_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    request = update.chat_join_request
    if not request:
        return

    channel = await get_channel(request.chat.id)
    if not channel:
        LOGGER.info("Ignoring unmanaged join request for chat %s", request.chat.id)
        return

    user = request.from_user
    await save_user(user)

    request_key = str(request.date.timestamp())
    if await request_exists(request.chat.id, user.id, request_key):
        LOGGER.debug("Duplicate join request ignored: %s/%s/%s", request.chat.id, user.id, request_key)
        return

    # Reserve this exact incoming event before processing, preventing duplicate handling.
    if not await mark_request(request.chat.id, user.id, request_key, "processing"):
        return

    if not await within_rate_limit(user.id):
        await record_request(
            request.chat.id, user, "IGNORED", "local rate limit", channel["mode"], False,
            "Too many requests from the same user in the local safety window.",
        )
        return

    mode = channel["mode"]
    if not channel["enabled"] or mode == MODE_DISABLED:
        await record_request(request.chat.id, user, "IGNORED", "channel disabled", mode, True)
        return

    black = await membership("blacklist", request.chat.id, user.id)
    if black:
        ok, err = await telegram_action_with_retry(context.bot, "decline", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "DECLINED" if ok else "FAILED", "blacklist", mode, ok, err
        )
        if ok:
            await notify_owners(
                context, request.chat.id,
                f"🚫 Declined blacklisted user\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`",
                setting_key="notify_declined",
            )
        return

    white = await membership("whitelist", request.chat.id, user.id)
    if white:
        ok, err = await telegram_action_with_retry(context.bot, "approve", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "APPROVED" if ok else "FAILED", "whitelist", mode, ok, err
        )
        if ok:
            await notify_owners(
                context, request.chat.id,
                f"✅ Approved whitelisted user\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`",
                setting_key="notify_approved",
            )
        return

    keyword = await get_setting(request.chat.id, "username_keyword", "")
    require_username = await get_setting(request.chat.id, "require_username", "0") == "1"

    if require_username and not user.username:
        ok, err = await telegram_action_with_retry(context.bot, "decline", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "DECLINED" if ok else "FAILED",
            "username required", mode, ok, err
        )
        return

    if keyword and (not user.username or keyword.lower() not in user.username.lower()):
        ok, err = await telegram_action_with_retry(context.bot, "decline", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "DECLINED" if ok else "FAILED",
            "username keyword rule", mode, ok, err
        )
        return

    if mode == MODE_ACCEPT:
        ok, err = await telegram_action_with_retry(context.bot, "approve", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "APPROVED" if ok else "FAILED", "auto accept", mode, ok, err
        )
        if ok:
            await notify_owners(
                context, request.chat.id,
                f"✅ Auto-approved\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`",
                setting_key="notify_approved",
            )
        else:
            await notify_owners(
                context, request.chat.id,
                f"⚠️ Approval failed\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`\nError: `{err[:300]}`",
                setting_key="notify_errors",
            )
        return

    if mode == MODE_DECLINE:
        ok, err = await telegram_action_with_retry(context.bot, "decline", request.chat.id, user.id)
        await record_request(
            request.chat.id, user, "DECLINED" if ok else "FAILED", "auto decline", mode, ok, err
        )
        if ok:
            await notify_owners(
                context, request.chat.id,
                f"❌ Auto-declined\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`",
                setting_key="notify_declined",
            )
        else:
            await notify_owners(
                context, request.chat.id,
                f"⚠️ Decline failed\n📢 {channel['title']}\n👤 {display_name(user)}\n🆔 `{user.id}`\nError: `{err[:300]}`",
                setting_key="notify_errors",
            )
        return

    if mode == MODE_MANUAL:
        text = (
            "🔔 *New Join Request*\n\n"
            f"📢 Channel: *{channel['title']}*\n"
            f"👤 Name: {display_name(user)}\n"
            f"🔗 Username: @{user.username if user.username else 'N/A'}\n"
            f"🆔 ID: `{user.id}`\n\n"
            "What do you want to do?"
        )
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ APPROVE", callback_data=f"reqa:{channel['chat_id']}:{user.id}:{request_key}"),
            InlineKeyboardButton("❌ DECLINE", callback_data=f"reqd:{channel['chat_id']}:{user.id}:{request_key}"),
        ]])
        await record_request(request.chat.id, user, "MANUAL", "manual approval", mode, True)
        await notify_owners(context, request.chat.id, text, markup, setting_key="notify_new")


# ============================================================
# ADMIN PANEL COMMANDS
# ============================================================

async def owner_only(update: Update) -> bool:
    user = update.effective_user
    if not user or not is_owner(user.id):
        if update.callback_query:
            await update.callback_query.answer("Access denied.", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text("⛔ Access denied.")
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await owner_only(update):
        return
    context.user_data.clear()
    await render_main(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await owner_only(update):
        return
    text = (
        "ℹ️ *Help*\n\n"
        "1. Add the bot as an administrator of your channel.\n"
        "2. Grant the administrator permission to manage/invite users (the Bot API exposes this as `can_invite_users`).\n"
        "3. Enable channel join requests in Telegram.\n"
        "4. Open Channels → Add Channel and send the numeric channel ID.\n"
        "5. Choose Auto Accept, Auto Decline, or Manual Approval.\n"
        "6. Use whitelist/blacklist for per-channel exceptions.\n\n"
        "Important: the Bot API delivers incoming join-request updates, but does not provide a general API to fetch an arbitrary backlog of pending requests. This bot therefore never pretends to process a hidden backlog."
    )
    await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=back_keyboard())


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await owner_only(update):
        return
    await show_status(update, context)


# ============================================================
# CALLBACK ROUTER
# ============================================================

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    if not is_owner(query.from_user.id):
        await query.answer("Access denied.", show_alert=True)
        return

    await query.answer()
    data = query.data or ""

    if data == "main":
        await render_main(update, context, edit=True)
    elif data == "channels":
        await show_channels(update, context)
    elif data == "mode":
        await show_mode_picker(update, context)
    elif data == "requests":
        await show_request_channels(update, context)
    elif data == "whitelist":
        await show_list_menu(update, context, "whitelist")
    elif data == "blacklist":
        await show_list_menu(update, context, "blacklist")
    elif data == "stats":
        await show_stats_channels(update, context)
    elif data == "logs":
        await show_logs_channels(update, context)
    elif data == "notify":
        await show_notify_channels(update, context)
    elif data == "broadcast":
        context.user_data["state"] = "broadcast"
        await query.edit_message_text(
            "📢 *Broadcast*\n\nSend the message you want to broadcast.",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(),
        )
    elif data == "backup":
        await create_backup(update, context)
    elif data == "status":
        await show_status(update, context)
    elif data == "help":
        await help_command(update, context)
    elif data == "cancel":
        context.user_data["state"] = None
        await render_main(update, context, edit=True)
    elif data == "add_channel":
        context.user_data["state"] = "add_channel"
        await query.edit_message_text(
            "➕ *Add Channel*\n\n"
            "First add this bot as an administrator with permission to manage/invite users.\n"
            "Then send the numeric channel ID, e.g. `-1001234567890`.",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(),
        )
    elif data.startswith("selmode:"):
        context.user_data["channel_id"] = int(data.split(":", 1)[1])
        await show_channel_settings(update, context)
    elif data.startswith("setmode:"):
        _, channel_id, mode = data.split(":", 2)
        await set_channel_mode(update, context, int(channel_id), mode)
    elif data.startswith("chsettings:"):
        await show_channel_settings(update, context, int(data.split(":", 1)[1]))
    elif data.startswith("remove:"):
        await remove_channel(update, context, int(data.split(":", 1)[1]))
    elif data.startswith("wl:"):
        await whitelist_callback(update, context, data)
    elif data.startswith("bl:"):
        await blacklist_callback(update, context, data)
    elif data.startswith("stats:"):
        await show_stats(update, context, int(data.split(":", 1)[1]))
    elif data.startswith("logs:"):
        await show_logs(update, context, int(data.split(":", 1)[1]), 0)
    elif data.startswith("logpage:"):
        _, cid, page = data.split(":")
        await show_logs(update, context, int(cid), int(page))
    elif data.startswith("notify:"):
        await notification_callback(update, context, data)
    elif data.startswith("reqa:") or data.startswith("reqd:"):
        await manual_action(update, context, data)
    elif data == "broadcast_confirm":
        await broadcast_confirm(update, context)
    elif data == "broadcast_cancel":
        context.user_data["state"] = None
        await render_main(update, context, edit=True)
    elif data.startswith("toggle:"):
        await toggle_rule(update, context, data)
    elif data == "channel_rules":
        await show_channel_rules(update, context)
    else:
        await query.edit_message_text("⚠️ Unknown or expired action.", reply_markup=back_keyboard())


# ============================================================
# CHANNEL UI
# ============================================================

async def show_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = "📢 *Channels*\n\nChoose an action:"
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
        [InlineKeyboardButton("📋 My Channels", callback_data="my_channels")],
        [InlineKeyboardButton("⚙️ Channel Settings", callback_data="channel_settings_list")],
        [InlineKeyboardButton("🗑 Remove Channel", callback_data="remove_channel_list")],
        [InlineKeyboardButton("⬅️ Back", callback_data="main")],
    ])
    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)


async def show_mode_picker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text(
        "⚙️ *Auto Mode*\n\nSelect a channel:",
        parse_mode="Markdown",
        reply_markup=await channel_picker("selmode"),
    )


async def show_channel_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: Optional[int] = None) -> None:
    if channel_id is None:
        ch = await selected_channel(context)
    else:
        ch = await get_channel(channel_id)
        context.user_data["channel_id"] = channel_id
    if not ch:
        await update.callback_query.edit_message_text("📢 No managed channels.", reply_markup=back_keyboard("channels"))
        return

    keyword = await get_setting(ch["chat_id"], "username_keyword", "")
    req_user = await get_setting(ch["chat_id"], "require_username", "0")
    text = (
        f"📢 *{ch['title']}*\n\n"
        f"⚙️ Mode: *{mode_label(ch['mode'])}*\n"
        f"🔔 Notifications: {'ON' if await get_setting(ch['chat_id'], 'notify_new', '1') == '1' else 'OFF'}\n"
        f"👤 Username required: {'ON' if req_user == '1' else 'OFF'}\n"
        f"🔎 Username keyword: `{keyword or 'none'}`"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Change Mode", callback_data=f"selmode:{ch['chat_id']}")],
        [InlineKeyboardButton("👤 Whitelist", callback_data=f"wl:menu:{ch['chat_id']}"),
         InlineKeyboardButton("🚫 Blacklist", callback_data=f"bl:menu:{ch['chat_id']}")],
        [InlineKeyboardButton("🔎 Rules", callback_data="channel_rules")],
        [InlineKeyboardButton("🔔 Notifications", callback_data=f"notify:menu:{ch['chat_id']}")],
        [InlineKeyboardButton("🗑 Remove", callback_data=f"remove:{ch['chat_id']}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="channels")],
    ])
    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)


async def set_channel_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: int, mode: str) -> None:
    if mode not in {MODE_ACCEPT, MODE_DECLINE, MODE_MANUAL, MODE_DISABLED}:
        return
    await db_call(DB.execute, "UPDATE channels SET mode=? WHERE chat_id=?", (mode, channel_id))
    await show_channel_settings(update, context, channel_id)


async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: int) -> None:
    ch = await get_channel(channel_id)
    if not ch:
        await update.callback_query.edit_message_text("Channel not found.", reply_markup=back_keyboard("channels"))
        return
    await db_call(DB.execute, "DELETE FROM channels WHERE chat_id=?", (channel_id,))
    context.user_data.pop("channel_id", None)
    await update.callback_query.edit_message_text(
        f"🗑 Removed *{ch['title']}* and its channel-specific data.",
        parse_mode="Markdown",
        reply_markup=back_keyboard("channels"),
    )


# Dynamic channel lists handled here to keep callback surface small.
async def dynamic_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = update.callback_query.data or ""
    if data == "my_channels":
        channels = await managed_channels()
        if not channels:
            text = "📋 *My Channels*\n\nNo channels are managed yet."
        else:
            lines = []
            for ch in channels:
                lines.append(f"📢 *{ch['title']}*\n🆔 `{ch['chat_id']}`\n⚙️ {mode_label(ch['mode'])}\n")
            text = "📋 *My Channels*\n\n" + "\n".join(lines)
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_keyboard("channels"))
    elif data == "channel_settings_list":
        await update.callback_query.edit_message_text(
            "⚙️ *Channel Settings*\n\nSelect a channel:",
            parse_mode="Markdown",
            reply_markup=await channel_picker("chsettings"),
        )
    elif data == "remove_channel_list":
        await update.callback_query.edit_message_text(
            "🗑 *Remove Channel*\n\nSelect a channel:",
            parse_mode="Markdown",
            reply_markup=await channel_picker("remove"),
        )
    elif data == "request_channels":
        await show_request_channels(update, context)
    elif data == "stats_channels":
        await show_stats_channels(update, context)
    elif data == "logs_channels":
        await show_logs_channels(update, context)
    elif data == "notify_channels":
        await show_notify_channels(update, context)


# ============================================================
# WHITELIST / BLACKLIST
# ============================================================

async def show_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, table: str) -> None:
    label = "Whitelist" if table == "whitelist" else "Blacklist"
    await update.callback_query.edit_message_text(
        f"{'👤' if table == 'whitelist' else '🚫'} *{label}*\n\nSelect a channel:",
        parse_mode="Markdown",
        reply_markup=await channel_picker(f"{'wl' if table == 'whitelist' else 'bl'}:menu"),
    )


async def whitelist_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
    parts = data.split(":")
    if len(parts) < 3:
        return
    action = parts[1]
    cid = int(parts[2])
    context.user_data["channel_id"] = cid
    if action == "menu":
        await update.callback_query.edit_message_text(
            "👤 *Whitelist*\n\nChoose an action:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add User", callback_data=f"wl:add:{cid}")],
                [InlineKeyboardButton("➖ Remove User", callback_data=f"wl:remove:{cid}")],
                [InlineKeyboardButton("📋 List Users", callback_data=f"wl:list:{cid}")],
                [InlineKeyboardButton("🧹 Clear List", callback_data=f"wl:clear:{cid}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="main")],
            ]),
        )
    elif action in {"add", "remove"}:
        context.user_data["state"] = f"{action}_whitelist"
        await update.callback_query.edit_message_text(
            f"{'➕' if action == 'add' else '➖'} Send the numeric Telegram user ID.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "list":
        rows = await db_call(
            DB.fetchall,
            "SELECT user_id,username,first_name,added_at FROM whitelist WHERE channel_id=? ORDER BY added_at DESC LIMIT 100",
            (cid,),
        )
        text = "👤 *Whitelist Users*\n\n"
        text += "\n".join(
            f"• `{r['user_id']}` {('@'+r['username']) if r['username'] else r['first_name'] or ''}"
            for r in rows
        ) or "Empty."
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_keyboard(f"wl:menu:{cid}"))
    elif action == "clear":
        await db_call(DB.execute, "DELETE FROM whitelist WHERE channel_id=?", (cid,))
        await update.callback_query.edit_message_text("🧹 Whitelist cleared.", reply_markup=back_keyboard(f"wl:menu:{cid}"))


async def blacklist_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
    parts = data.split(":")
    if len(parts) < 3:
        return
    action = parts[1]
    cid = int(parts[2])
    context.user_data["channel_id"] = cid
    if action == "menu":
        await update.callback_query.edit_message_text(
            "🚫 *Blacklist*\n\nChoose an action:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add User", callback_data=f"bl:add:{cid}")],
                [InlineKeyboardButton("➖ Remove User", callback_data=f"bl:remove:{cid}")],
                [InlineKeyboardButton("📋 List Users", callback_data=f"bl:list:{cid}")],
                [InlineKeyboardButton("🧹 Clear List", callback_data=f"bl:clear:{cid}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="main")],
            ]),
        )
    elif action in {"add", "remove"}:
        context.user_data["state"] = f"{action}_blacklist"
        await update.callback_query.edit_message_text(
            f"{'➕' if action == 'add' else '➖'} Send the numeric Telegram user ID.\n"
            "For adding a blacklist entry you may append a reason after a space.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "list":
        rows = await db_call(
            DB.fetchall,
            "SELECT user_id,username,first_name,reason,added_at FROM blacklist WHERE channel_id=? ORDER BY added_at DESC LIMIT 100",
            (cid,),
        )
        text = "🚫 *Blacklist Users*\n\n"
        text += "\n".join(
            f"• `{r['user_id']}` {('@'+r['username']) if r['username'] else r['first_name'] or ''}"
            f"{' — '+r['reason'] if r['reason'] else ''}"
            for r in rows
        ) or "Empty."
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_keyboard(f"bl:menu:{cid}"))
    elif action == "clear":
        await db_call(DB.execute, "DELETE FROM blacklist WHERE channel_id=?", (cid,))
        await update.callback_query.edit_message_text("🧹 Blacklist cleared.", reply_markup=back_keyboard(f"bl:menu:{cid}"))


# ============================================================
# STATS / LOGS / NOTIFICATIONS
# ============================================================

async def show_stats_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text(
        "📊 *Statistics*\n\nSelect a channel:",
        parse_mode="Markdown",
        reply_markup=await channel_picker("stats"),
    )


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: int) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    week = (datetime.now(timezone.utc).timestamp() - 7 * 86400)
    month = (datetime.now(timezone.utc).strftime("%Y-%m-01"))
    row = await db_call(
        DB.fetchone,
        """
        SELECT
          COUNT(*) total,
          SUM(CASE WHEN action='APPROVED' AND success=1 THEN 1 ELSE 0 END) approved,
          SUM(CASE WHEN action='DECLINED' AND success=1 THEN 1 ELSE 0 END) declined,
          SUM(CASE WHEN action='MANUAL' THEN 1 ELSE 0 END) manual,
          SUM(CASE WHEN action='FAILED' OR success=0 THEN 1 ELSE 0 END) failed
        FROM requests WHERE channel_id=?
        """,
        (channel_id,),
    )
    day = await db_call(DB.fetchone, "SELECT COUNT(*) c FROM requests WHERE channel_id=? AND timestamp LIKE ?", (channel_id, today + "%"))
    week_row = await db_call(
        DB.fetchone,
        "SELECT COUNT(*) c FROM requests WHERE channel_id=? AND timestamp>=datetime('now','-7 days')",
        (channel_id,),
    )
    month_row = await db_call(
        DB.fetchone,
        "SELECT COUNT(*) c FROM requests WHERE channel_id=? AND timestamp>=?",
        (channel_id, month + "T00:00:00+00:00"),
    )
    ch = await get_channel(channel_id)
    text = (
        f"📊 *Statistics — {ch['title'] if ch else channel_id}*\n\n"
        f"Total Requests: *{row['total'] or 0}*\n"
        f"✅ Approved: *{row['approved'] or 0}*\n"
        f"❌ Declined: *{row['declined'] or 0}*\n"
        f"🟡 Manual: *{row['manual'] or 0}*\n"
        f"⚠️ Failed: *{row['failed'] or 0}*\n\n"
        f"Today: *{day['c']}*\n"
        f"This Week: *{week_row['c']}*\n"
        f"This Month: *{month_row['c']}*"
    )
    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_keyboard("stats"))


async def show_request_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text(
        "📋 *Requests*\n\nSelect a channel to view recent request history:",
        parse_mode="Markdown",
        reply_markup=await channel_picker("logs"),
    )


async def show_logs_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text(
        "📜 *Logs*\n\nSelect a channel:",
        parse_mode="Markdown",
        reply_markup=await channel_picker("logs"),
    )


async def show_logs(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: int, page: int) -> None:
    offset = max(page, 0) * PAGE_SIZE
    rows = await db_call(
        DB.fetchall,
        """
        SELECT * FROM requests
        WHERE channel_id=?
        ORDER BY id DESC LIMIT ? OFFSET ?
        """,
        (channel_id, PAGE_SIZE, offset),
    )
    ch = await get_channel(channel_id)
    if not rows:
        text = f"📜 *Recent Requests — {ch['title'] if ch else channel_id}*\n\nNo records on this page."
    else:
        lines = []
        for r in rows:
            icon = "✅" if r["action"] == "APPROVED" and r["success"] else "❌" if r["action"] == "DECLINED" else "🟡" if r["action"] == "MANUAL" else "⚠️"
            name = "@" + r["username"] if r["username"] else r["first_name"] or str(r["user_id"])
            ts = r["timestamp"].replace("T", " ")[:16]
            lines.append(f"{icon} {name} — `{r['user_id']}`\n{r['action']} · {ts}")
        text = f"📜 *Recent Requests — {ch['title'] if ch else channel_id}*\n\n" + "\n\n".join(lines)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"logpage:{channel_id}:{page-1}"))
    if len(rows) == PAGE_SIZE:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"logpage:{channel_id}:{page+1}"))
    markup_rows = [nav] if nav else []
    markup_rows.append([InlineKeyboardButton("⬅️ Back", callback_data="logs")])
    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(markup_rows))


async def show_notify_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text(
        "🔔 *Notifications*\n\nSelect a channel:",
        parse_mode="Markdown",
        reply_markup=await channel_picker("notify:menu"),
    )


async def notification_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
    parts = data.split(":")
    if len(parts) < 3:
        return
    action, cid = parts[1], int(parts[2])
    if action == "menu":
        rows = []
        for key, label in [
            ("notify_new", "New Requests"),
            ("notify_approved", "Approved"),
            ("notify_declined", "Declined"),
            ("notify_errors", "Errors"),
        ]:
            val = await get_setting(cid, key, "1")
            rows.append([InlineKeyboardButton(
                f"{label}: {'ON' if val == '1' else 'OFF'}",
                callback_data=f"notify:toggle:{cid}:{key}",
            )])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="main")])
        await update.callback_query.edit_message_text(
            "🔔 *Notifications*\n\nTap a setting to toggle it.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
    elif action == "toggle" and len(parts) == 4:
        key = parts[3]
        val = await get_setting(cid, key, "1")
        await set_setting(cid, key, "0" if val == "1" else "1")
        await notification_callback(update, context, f"notify:menu:{cid}")


# ============================================================
# RULES
# ============================================================

async def show_channel_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = context.user_data.get("channel_id")
    if not cid:
        await update.callback_query.edit_message_text("Select a channel first.", reply_markup=back_keyboard())
        return
    req = await get_setting(cid, "require_username", "0")
    keyword = await get_setting(cid, "username_keyword", "")
    await update.callback_query.edit_message_text(
        "🔎 *Rules*\n\n"
        f"Username required: {'ON' if req == '1' else 'OFF'}\n"
        f"Username keyword: `{keyword or 'none'}`\n\n"
        "Choose an option:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👤 Toggle Username Requirement", callback_data=f"toggle:require_username:{cid}")],
            [InlineKeyboardButton("🔎 Set Username Keyword", callback_data=f"toggle:keyword:{cid}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"chsettings:{cid}")],
        ]),
    )


async def toggle_rule(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
    _, key, cid_s = data.split(":")
    cid = int(cid_s)
    context.user_data["channel_id"] = cid
    if key == "require_username":
        val = await get_setting(cid, "require_username", "0")
        await set_setting(cid, "require_username", "0" if val == "1" else "1")
        await show_channel_rules(update, context)
    elif key == "keyword":
        context.user_data["state"] = "username_keyword"
        await update.callback_query.edit_message_text(
            "🔎 Send the username keyword.\nSend `-` to disable it.",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(),
        )


# ============================================================
# MANUAL ACTIONS
# ============================================================

async def manual_action(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
    query = update.callback_query
    parts = data.split(":")
    if len(parts) != 4:
        await query.edit_message_text("⚠️ Invalid request action.", reply_markup=back_keyboard())
        return

    action = "approve" if parts[0] == "reqa" else "decline"
    cid = int(parts[1])
    uid = int(parts[2])
    request_key = parts[3]

    # A successful action is recorded only after Telegram confirms it.
    # The event reservation prevents duplicate button actions in parallel.
    if await request_exists(cid, uid, request_key):
        # The incoming event is already recorded, so we only inspect the latest successful action.
        latest = await db_call(
            DB.fetchone,
            """
            SELECT action,success FROM requests
            WHERE channel_id=? AND user_id=?
            ORDER BY id DESC LIMIT 1
            """,
            (cid, uid),
        )
        if latest and latest["action"] in {"APPROVED", "DECLINED"} and latest["success"]:
            await query.edit_message_text("ℹ️ This request has already been processed.", reply_markup=back_keyboard())
            return

    ch = await get_channel(cid)
    if not ch:
        await query.edit_message_text("❌ Channel is no longer managed.", reply_markup=back_keyboard())
        return

    # Create a distinct manual-action reservation.
    action_key = f"{request_key}:{action}"
    if not await mark_request(cid, uid, action_key, "manual_action"):
        await query.edit_message_text("ℹ️ This action was already submitted.", reply_markup=back_keyboard())
        return

    class SimpleUser:
        def __init__(self, row):
            self.id = row["user_id"]
            self.username = row["username"]
            self.first_name = row["first_name"]
            self.last_name = row["last_name"]

    row = await db_call(
        DB.fetchone,
        """
        SELECT user_id,username,first_name,last_name
        FROM users WHERE user_id=?
        """,
        (uid,),
    )
    if not row:
        await query.edit_message_text("❌ User information is no longer available.", reply_markup=back_keyboard())
        return

    user = SimpleUser(row)
    ok, err = await telegram_action_with_retry(context.bot, action, cid, uid)
    await record_request(
        cid, user, "APPROVED" if action == "approve" and ok else
        "DECLINED" if action == "decline" and ok else "FAILED",
        "manual admin action", MODE_MANUAL, ok, err
    )
    if ok:
        await query.edit_message_text(
            "✅ Request approved successfully." if action == "approve"
            else "❌ Request declined successfully.",
            reply_markup=back_keyboard(),
        )
    else:
        await query.edit_message_text(
            f"⚠️ Telegram did not confirm the action.\n\nError: {err[:500]}",
            reply_markup=back_keyboard(),
        )


# ============================================================
# BACKUP
# ============================================================

async def create_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text("💾 Creating a safe SQLite backup…")
    fd, temp_path = tempfile.mkstemp(prefix="join-manager-", suffix=".sqlite3")
    os.close(fd)
    try:
        async with db_lock:
            await asyncio.to_thread(DB.conn.backup, sqlite3.connect(temp_path))
        with open(temp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=update.effective_user.id,
                document=f,
                filename=f"join-manager-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.sqlite3",
                caption="💾 SQLite database backup",
            )
        await update.effective_message.reply_text("✅ Backup sent to your authorized admin chat.", reply_markup=back_keyboard())
    except Exception as exc:
        LOGGER.exception("Backup failed")
        await update.effective_message.reply_text(f"❌ Backup failed: {exc}", reply_markup=back_keyboard())
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


# ============================================================
# BROADCAST
# ============================================================

async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = context.user_data.get("broadcast_text")
    if not text:
        await update.callback_query.edit_message_text("❌ No broadcast message found.", reply_markup=back_keyboard())
        return
    users = await db_call(DB.fetchall, "SELECT user_id FROM users ORDER BY user_id")
    await update.callback_query.edit_message_text(f"📢 Broadcasting to {len(users)} stored users…")
    sent = 0
    failed = 0
    for row in users:
        try:
            await context.bot.send_message(row["user_id"], text)
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as exc:
            await asyncio.sleep(min(float(exc.retry_after), 10.0))
            try:
                await context.bot.send_message(row["user_id"], text)
                sent += 1
            except TelegramError:
                failed += 1
        except TelegramError:
            failed += 1
    context.user_data["state"] = None
    context.user_data.pop("broadcast_text", None)
    await update.effective_message.reply_text(
        f"📢 Broadcast finished.\n\n✅ Sent: {sent}\n⚠️ Failed/blocked: {failed}",
        reply_markup=back_keyboard(),
    )


# ============================================================
# STATUS
# ============================================================

async def show_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    channels = await managed_channels()
    total = await db_call(lambda: DB.fetchone("SELECT COUNT(*) c FROM requests")["c"])
    modes = "\n".join(f"• {c['title']}: {mode_label(c['mode'])}" for c in channels) or "No channels configured."
    text = (
        "⚡ *Bot Status*\n\n"
        "Status: 🟢 Online\n"
        f"Uptime: {format_uptime()}\n"
        f"Channels: {len(channels)}\n"
        f"Requests Processed: {total}\n"
        "Database: 🟢 OK\n\n"
        "Modes:\n" + modes
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_keyboard())
    else:
        await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=back_keyboard())


# ============================================================
# TEXT INPUT STATE HANDLER
# ============================================================

async def text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await owner_only(update):
        return
    state = context.user_data.get("state")
    text = (update.effective_message.text or "").strip()
    if not state:
        return

    if state == "add_channel":
        await add_channel_from_id(update, context, text)
        return

    cid = context.user_data.get("channel_id")
    if not cid:
        context.user_data["state"] = None
        await update.effective_message.reply_text("❌ No channel selected.", reply_markup=back_keyboard())
        return

    if state in {"add_whitelist", "remove_whitelist"}:
        uid = safe_int(text)
        if uid is None:
            await update.effective_message.reply_text("❌ Invalid user ID.", reply_markup=cancel_keyboard())
            return
        if state == "add_whitelist":
            await db_call(
                DB.execute,
                """
                INSERT INTO whitelist(channel_id,user_id,username,first_name,added_by,added_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(channel_id,user_id) DO UPDATE SET added_by=excluded.added_by,added_at=excluded.added_at
                """,
                (cid, uid, None, None, update.effective_user.id, now_iso()),
            )
            await update.effective_message.reply_text("✅ User added to whitelist.", reply_markup=back_keyboard())
        else:
            await db_call(DB.execute, "DELETE FROM whitelist WHERE channel_id=? AND user_id=?", (cid, uid))
            await update.effective_message.reply_text("✅ User removed from whitelist.", reply_markup=back_keyboard())
        context.user_data["state"] = None
        return

    if state in {"add_blacklist", "remove_blacklist"}:
        first, *rest = text.split(maxsplit=1)
        uid = safe_int(first)
        if uid is None:
            await update.effective_message.reply_text("❌ Invalid user ID.", reply_markup=cancel_keyboard())
            return
        if state == "add_blacklist":
            reason = rest[0][:200] if rest else "admin blacklist"
            await db_call(
                DB.execute,
                """
                INSERT INTO blacklist(channel_id,user_id,username,first_name,reason,added_by,added_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(channel_id,user_id) DO UPDATE SET reason=excluded.reason,added_by=excluded.added_by,added_at=excluded.added_at
                """,
                (cid, uid, None, None, reason, update.effective_user.id, now_iso()),
            )
            await update.effective_message.reply_text("✅ User added to blacklist.", reply_markup=back_keyboard())
        else:
            await db_call(DB.execute, "DELETE FROM blacklist WHERE channel_id=? AND user_id=?", (cid, uid))
            await update.effective_message.reply_text("✅ User removed from blacklist.", reply_markup=back_keyboard())
        context.user_data["state"] = None
        return

    if state == "username_keyword":
        value = "" if text == "-" else text[:MAX_KEYWORD_LENGTH]
        await set_setting(cid, "username_keyword", value)
        context.user_data["state"] = None
        await update.effective_message.reply_text("✅ Username keyword updated.", reply_markup=back_keyboard())
        return

    if state == "broadcast":
        if len(text) > 4096:
            await update.effective_message.reply_text("❌ Telegram messages cannot exceed 4096 characters.", reply_markup=cancel_keyboard())
            return
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = "broadcast_confirm"
        users = await db_call(DB.fetchall, "SELECT user_id FROM users")
        await update.effective_message.reply_text(
            f"⚠️ Broadcast to *{len(users)}* stored users?\n\n{text[:1000]}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="broadcast_confirm"),
                 InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
            ]),
        )
        return


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Unhandled Telegram update error", exc_info=context.error)
    if isinstance(context.error, TelegramRetryAfter):
        LOGGER.warning("Telegram rate limit: retry after %s", context.error.retry_after)
    elif isinstance(context.error, TelegramForbiddenError):
        LOGGER.warning("Telegram forbidden: %s", context.error)
    elif isinstance(context.error, TelegramBadRequest):
        LOGGER.warning("Telegram bad request: %s", context.error)


# ============================================================
# APPLICATION LIFECYCLE
# ============================================================

async def post_init(application: Application) -> None:
    await db_call(lambda: DB.initialize())
    me = await application.bot.get_me()
    LOGGER.info("Started @%s (id=%s)", me.username, me.id)


async def post_shutdown(application: Application) -> None:
    async with db_lock:
        await asyncio.to_thread(DB.close)
    LOGGER.info("Shutdown complete.")


def build_application() -> Application:
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .concurrent_updates(True)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(ChatJoinRequestHandler(process_join_request))
    application.add_handler(CallbackQueryHandler(dynamic_list_callback, pattern=r"^(my_channels|channel_settings_list|remove_channel_list|request_channels|stats_channels|logs_channels|notify_channels)$"))
    application.add_handler(CallbackQueryHandler(callbacks))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input))
    application.add_error_handler(error_handler)
    return application


def main() -> None:
    DB.connect()
    application = build_application()
    LOGGER.info("Starting polling…")
    application.run_polling(
        allowed_updates=["message", "callback_query", "chat_join_request"],
        drop_pending_updates=False,
        close_loop=True,
    )


if __name__ == "__main__":
    main()
