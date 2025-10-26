 import os
import os.path # Using standard library for path joining
import re
import sqlite3
import pytz
import asyncio
import logging
import traceback
import threading
from functools import wraps
from datetime import datetime, time, timedelta
from telegram import Update, ChatPermissions, Chat, ChatMemberUpdated
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ChatMemberHandler
from telegram.error import BadRequest, RetryAfter
# ⚠️ CRITICAL IMPORT: Used to run multiple bots in separate processes
import multiprocessing

# === LOGGING SETUP (MODIFIED FOR HOSTING) ===
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler("bot_multi.log"), # <-- REMOVED: Hosting platforms log stdout
        logging.StreamHandler()
    ]
)


# === CONFIGURATION (MODIFIED FOR KOYEB/FLY.IO) ===

# 1. Read secrets from environment variables
#    We will set these in the Koyeb UI (or via CLI)
OWNER_ID = int(os.environ.get("OWNER_ID", 5409158853)) # Fallback for local testing
BOT_TOKEN_ALPHA = os.environ.get("BOT_TOKEN_ALPHA")
BOT_TOKEN_BETA = os.environ.get("BOT_TOKEN_BETA")
BOT_TOKEN_GAMMA = os.environ.get("BOT_TOKEN_GAMMA")

# 2. Define the persistent volume/disk path
#    Koyeb (and Fly.io) will mount our storage to this exact path: /data
DATA_DIR = "/data"
logger.info(f"Using data directory: {DATA_DIR}")

# 3. Static configs
DEFAULT_OPEN_GIF_FILE_ID = 'CgACAgQAAxkBAAEYabFo4j0mLSA0aMiJNaSwj1_B5RrUiAAC4AIAAhhPDVOzldaDFsYkKjYE'
DEFAULT_TRACKING_LINK = "x.com/your_default_username"
LIST_CHUNK_SIZE = 100 # Max users per list message

# 4. Define configurations using the variables
BOT_CONFIGS = [
    {
        "name": "BotAlpha",
        "token": BOT_TOKEN_ALPHA,
        "db_file": os.path.join(DATA_DIR, "group_data_alpha.db") # Saves to /data/group_data_alpha.db
    },
    {
        "name": "BotBeta",
        "token": BOT_TOKEN_BETA,
        "db_file": os.path.join(DATA_DIR, "group_data_beta.db") # Saves to /data/group_data_beta.db
    },
    {
        "name": "BotGamma",
        "token": BOT_TOKEN_GAMMA,
        "db_file": os.path.join(DATA_DIR, "group_data_gamma.db") # Saves to /data/group_data_gamma.db
    }
]

# === ⏰ AUTOMATED SESSION SCHEDULER CONFIGURATION ⏰ ===
SCHEDULED_CHATS = {
    # --- Group 1 (This group will have 3 sessions per day) ---
    -1002992139767: {
        "times": ["08:50:00", "12:18:00", "19:00:00"],
        "timezone": "Asia/Kolkata"
    },
    # --- Group 2 (This group will have only 2 sessions per day) ---
    -1002373178666: {
        "times": ["09:20:00", "13:50:00", "18:20:00"],
        "timezone": "Asia/Kolkata"
    }
}


# --- CONTEXT-AWARE DATABASE HELPERS ---
# These functions retrieve the correct DB connection/cursor/state for the currently running bot instance.
def get_db_cursor(context: ContextTypes.DEFAULT_TYPE):
    """Retrieves the cursor for the bot's specific database connection."""
    return context.application.bot_data['db_cursor']

def get_db_connection(context: ContextTypes.DEFAULT_TYPE):
    """Retrieves the connection object for the bot's specific database."""
    return context.application.bot_data['db_conn']

def get_session_phases(context: ContextTypes.DEFAULT_TYPE):
    """Retrieves the in-memory session phases dict for this bot."""
    return context.application.bot_data['session_phases']


# === DATABASE SETUP (Modified to accept a file name) ===
def setup_database(db_file):
    # CRITICAL: Since using multiprocessing, set check_same_thread=False
    conn = sqlite3.connect(db_file, timeout=10, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS group_connections (
        chat_id INTEGER PRIMARY KEY, target_chat_id INTEGER NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS group_settings (
        chat_id INTEGER PRIMARY KEY, tracking_link TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS links (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, telegram_user TEXT,
        telegram_name TEXT, twitter_user TEXT, full_link TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS status (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL, completed INTEGER DEFAULT 0,
        last_done DATETIME, PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS srlist (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL, telegram_name TEXT,
        added_on DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS whitelist (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL,
        PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS session_state (
        chat_id INTEGER PRIMARY KEY, phase TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS automated_chats (
        chat_id INTEGER PRIMARY KEY)""")
    c.execute("""CREATE TABLE IF NOT EXISTS group_gifs (
        chat_id INTEGER PRIMARY KEY,
        open_gif_file_id TEXT NOT NULL
    )""")
    conn.commit()
    return conn, c

def load_session_state_from_db(c):
    session_phases = {}
    c.execute("SELECT chat_id, phase FROM session_state")
    rows = c.fetchall()
    session_phases = {chat_id: phase for chat_id, phase in rows}
    if session_phases:
        logger.info(f"Loaded session states from database: {session_phases}")
    return session_phases


# === REGEX & HELPERS (Updated to use Context) ===
twitter_regex = re.compile(
    r"(https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]+)/status/\d+)", re.IGNORECASE
)
username_regex = re.compile(r"@[a-zA-Z0-9_]+", re.IGNORECASE)
done_regex = re.compile(r"\b(done|completed|ad|all done|dn)\b", re.IGNORECASE)

async def is_bot_authorized_in_chat(chat: Chat) -> tuple[bool, str]:
    try:
        owner_member = await chat.get_member(OWNER_ID)
        if owner_member.status in ['left', 'kicked']:
            return (False, "OWNER_NOT_MEMBER")
    except BadRequest:
        return (False, "OWNER_NOT_MEMBER")
    return (True, "AUTHORIZED")

def get_effective_chat_id(chat_id, context: ContextTypes.DEFAULT_TYPE):
    c = get_db_cursor(context)
    c.execute("SELECT target_chat_id FROM group_connections WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    return row[0] if row else chat_id

def tg_mention(name, user_id):
    name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"<a href='tg://user?id={int(user_id)}'>{name}</a>"

def get_main_link(chat_id, telegram_user, context: ContextTypes.DEFAULT_TYPE):
    c = get_db_cursor(context)
    c.execute("""SELECT twitter_user, full_link FROM links WHERE chat_id = ? AND telegram_user = ? ORDER BY id DESC LIMIT 1""", (chat_id, telegram_user,))
    return c.fetchone() or None

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    try:
        await context.bot.delete_message(chat_id=job_data['chat_id'], message_id=job_data['message_id'])
    except BadRequest:
        pass

def to_sans_serif_monospace(text: str) -> str:
    sans_serif_map = {
        'A': '𝙰', 'B': '𝙱', 'C': '𝙲', 'D': '𝙳', 'E': '𝙴', 'F': '𝙵', 'G': '𝙶', 'H': '𝙷', 'I': '𝙸', 'J': '𝙹', 'K': '𝙺', 'L': '𝙻', 'M': '𝙼',
        'N': '𝙽', 'O': '𝙾', 'P': '𝙿', 'Q': '𝚀', 'R': '𝚁', 'S': '𝚂', 'T': '𝚃', 'U': '𝚄', 'V': '𝚅', 'W': '𝚆', 'X': '𝚇', 'Y': '𝚈', 'Z': '𝚉',
        '0': '𝟶', '1': '𝟷', '2': '𝟸', '3': '𝟹', '4': '𝟺', '5': '𝟻', '6': '𝟼', '7': '𝟽', '8': '𝟾', '9': '𝟿', ' ': ' '
    }
    return "".join(sans_serif_map.get(char, char) for char in text.upper())

async def _get_admin_mentions(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    # NOTE: This is for mentioning admins, not for exclusion logic, but kept for reference
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        mentions = [tg_mention(admin.user.full_name, admin.user.id) for admin in admins if not admin.user.is_bot]
        return " ".join(mentions)
    except Exception as e:
        logger.error(f"Could not get admin list for chat {chat_id}: {e}")
        return ""

# === PERMISSION HELPERS (Updated with all requested restrictions) ===
async def set_text_only_permissions(chat: Chat):
    permissions = ChatPermissions(
        can_send_messages=True,
        can_add_web_page_previews=False,  # <-- Disabled previews
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_change_info=False,
        can_invite_users=False, # <-- Disabled 'Add User'
        can_pin_messages=False
    )
    await chat.set_permissions(permissions)
    logger.info(f"Set permissions to TEXT-ONLY (No Previews, No Invites) for chat {chat.id}")

async def set_tracking_phase_permissions(chat: Chat):
    permissions = ChatPermissions(
        can_send_messages=True,
        can_add_web_page_previews=False,  # <-- Disabled previews
        can_send_videos=True,             # <-- ALLOWED: Video
        can_send_other_messages=True,     # <-- ALLOWED: Stickers and GIFs/Animations
        can_send_audios=False,            # <-- DENIED: Music/Audio
        can_send_documents=False,         # <-- DENIED: Files/Documents
        can_send_photos=False,            # <-- DENIED: Photos
        can_send_video_notes=False,       # <-- DENIED: Video Messages
        can_send_voice_notes=False,       # <-- DENIED: Voice Messages
        can_send_polls=False,
        can_change_info=False,
        can_invite_users=False, # <-- Disabled 'Add User'
        can_pin_messages=False
    )
    await chat.set_permissions(permissions)
    logger.info(f"Set permissions to TRACKING (Text, Video, GIF, No Previews, No Invites) for chat {chat.id}")

async def disable_chat(chat):
    permissions = ChatPermissions(
        can_send_messages=False, can_send_audios=False, can_send_documents=False, can_send_photos=False,
        can_send_videos=False, can_send_video_notes=False, can_send_voice_notes=False,
        can_send_polls=False, can_send_other_messages=False, can_add_web_page_previews=False,
        can_change_info=False, can_invite_users=False, can_pin_messages=False )
    await chat.set_permissions(permissions)
    logger.info(f"Disabled all member permissions for chat {chat.id}")


# === REFACTORED SESSION LOGIC (Updated to use Context) ===
async def _run_open_session(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session_phases = get_session_phases(context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    session_phases[chat_id] = "links"
    c.execute("INSERT OR REPLACE INTO session_state (chat_id, phase) VALUES (?, ?)", (chat_id, "links"))
    conn.commit()

    chat = await context.bot.get_chat(chat_id)
    await set_text_only_permissions(chat)
    if "[OPEN]" not in chat.title:
        new_title = chat.title.replace("[CLOSED]", "").strip() + " [OPEN]"
        try: await chat.set_title(new_title)
        except BadRequest as e: logger.warning(f"Could not set title for {chat_id}: {e}")

    c.execute("SELECT open_gif_file_id FROM group_gifs WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    gif_to_send = row[0] if row else DEFAULT_OPEN_GIF_FILE_ID

    await context.bot.send_animation(chat_id, animation=gif_to_send)

    message_text = "<b> start dropping your post </b>"
    try:
        sent_message = await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.HTML)
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
    except BadRequest as e:
        logger.error(f"Error opening session in {chat_id}: {e}")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Could not pin message. Please grant 'Pin Messages' admin rights.")
    logger.info(f"Successfully opened session for chat {chat_id}")

async def _run_tracking(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session_phases = get_session_phases(context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    session_phases[chat_id] = "done"
    c.execute("INSERT OR REPLACE INTO session_state (chat_id, phase) VALUES (?, ?)", (chat_id, "done"))
    conn.commit()

    chat = await context.bot.get_chat(chat_id)
    await set_tracking_phase_permissions(chat)
    if "[OPEN]" in chat.title:
        try: await chat.set_title(chat.title.replace("[OPEN]", "[CLOSED]"))
        except BadRequest as e: logger.warning(f"Could not set title for {chat_id}: {e}")
    elif "[CLOSED]" not in chat.title:
        try: await chat.set_title(chat.title.strip() + " [CLOSED]")
        except BadRequest as e: logger.warning(f"Could not set title for {chat_id}: {e}")

    deadline_str = ""
    try:
        ist_tz = pytz.timezone("Asia/Kolkata")
        now_in_ist = datetime.now(ist_tz)
        deadline_time = now_in_ist + timedelta(hours=1)
        deadline_str = f"\n\n⚠️ DEADLINE: {deadline_time.strftime('%I:%M %p %Z')}"
    except Exception as e:
        logger.error(f"Could not calculate 1-hour deadline for chat {chat_id}: {e}")

    c.execute("SELECT tracking_link FROM group_settings WHERE chat_id = ?", (chat_id,))
    tracking_link = (c.fetchone() or [DEFAULT_TRACKING_LINK])[0]

    message_text = (f"Timeline Updated 👇\n\n{tracking_link}\n\nLike all posts of the TL account and\n"
                    f"Drop 'done' (or 'ad', 'completed') to be marked safe ✅{deadline_str}")
    try:
        # Prevents link preview for the bot's tracking link message
        sent_message = await context.bot.send_message(chat_id=chat_id, text=message_text, disable_web_page_preview=True)
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
    except BadRequest as e:
        logger.error(f"Error starting tracking in {chat_id}: {e}")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Could not start tracking phase. Please check permissions.")
    logger.info(f"Successfully started tracking for chat {chat_id}")

    c.execute("SELECT 1 FROM automated_chats WHERE chat_id = ?", (chat_id,))
    if c.fetchone():
        job_queue = context.job_queue
        # Job names must be unique PER BOT, so we keep the standard naming convention here
        job_queue.run_once(_send_unsafe_list_job, 30 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_1")
        job_queue.run_once(_send_unsafe_list_job, 40 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_2")
        job_queue.run_once(_send_unsafe_list_job, 45 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_3")
        logger.info(f"Scheduled auto-unsafe reminders for chat {chat_id} at 30, 40, and 45 minutes.")

async def _run_muteall(chat_id: int, context: ContextTypes.DEFAULT_TYPE, duration_days: int, is_automated: bool = False):
    c = get_db_cursor(context)

    until_timestamp = int((datetime.utcnow() + timedelta(days=duration_days)).timestamp())
    c.execute("SELECT DISTINCT telegram_user FROM links WHERE chat_id = ?", (chat_id,))
    all_users = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM status WHERE chat_id = ? AND completed=1", (chat_id,))
    completed = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM srlist WHERE chat_id = ?", (chat_id,))
    sr_users = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (chat_id,))
    whitelisted_users = {row[0] for row in c.fetchall()}

    # --- GET ADMINS FOR EXCLUSION ---
    admin_users = set()
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        admin_users = {str(admin.user.id) for admin in admins if not admin.user.is_bot}
    except BadRequest:
        pass # Ignore error if bot lacks permissions
    # ----------------------------------

    unsafe_users = all_users - completed
    final_users_to_mute = (unsafe_users.union(sr_users)) - whitelisted_users - admin_users # Exclude Admins

    if not final_users_to_mute:
        if is_automated:
            await context.bot.send_message(chat_id=chat_id, text="✅ Automated check complete. All active users are safe.")
        else:
            await context.bot.send_message(chat_id=chat_id, text="✅ Check complete. No unsafe users found to mute.")
        logger.info(f"No users to mute in chat {chat_id}")
        return

    chat = await context.bot.get_chat(chat_id)
    muted_users_tags = []
    failed_count = 0
    for tg_user in final_users_to_mute:
        c.execute("SELECT telegram_name FROM links WHERE chat_id = ? AND telegram_user=? ORDER BY id DESC LIMIT 1", (chat_id, tg_user,))
        name_row = c.fetchone()
        name = name_row[0] if name_row else f"ID: {tg_user}"
        try:
            await chat.restrict_member(int(tg_user), ChatPermissions(can_send_messages=False), until_date=until_timestamp)
            muted_users_tags.append(tg_mention(name, tg_user))
        except Exception as e:
            failed_count += 1
            logger.warning(f"Failed to mute user {tg_user} in chat {chat_id}: {e}")

    title_text = "Automated Mute Complete" if is_automated else "Mute Complete"
    msg = f"🔇 {to_sans_serif_monospace(title_text)}\n"
    msg += f"ᴅᴜʀᴀᴛɪᴏɴ: {duration_days} ᴅᴀʏs\n\n"
    if muted_users_tags:
        msg += f"🚫 Muted Users ({len(muted_users_tags)}):\n"
        msg += "\n".join(f"{i}. {user_tag}" for i, user_tag in enumerate(muted_users_tags, 1))
    else:
        msg += "✅ No users were muted in this cycle."
    if failed_count > 0:
        msg += f"\n\n⚠️ Failed to mute {failed_count} user(s) (they may have left or are admins)."
    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    logger.info(f"Muted {len(muted_users_tags)} users in {chat_id}. Failed: {failed_count}.")

async def _run_close_session(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session_phases = get_session_phases(context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    if chat_id in session_phases:
        del session_phases[chat_id]

    c.execute("DELETE FROM session_state WHERE chat_id = ?", (chat_id,))

    job_queue = context.job_queue
    job_prefixes_to_clear = ["reminder_45_", "reminder_90_", "reminder_120_", "unsafe_"]
    # Job names are unique per bot's job queue
    for prefix in job_prefixes_to_clear:
        jobs = job_queue.get_jobs_by_name(f"{prefix}{chat_id}")
        for job in jobs:
            job.schedule_removal()
            logger.info(f"Removed scheduled job {job.name} for chat {chat_id} due to manual close.")

    c.execute("DELETE FROM links WHERE chat_id = ?", (chat_id,))
    c.execute("DELETE FROM status WHERE chat_id = ?", (chat_id,))
    c.execute("DELETE FROM srlist WHERE chat_id = ?", (chat_id,))
    conn.commit()

    chat = await context.bot.get_chat(chat_id)
    await disable_chat(chat)
    new_title = chat.title.replace("[OPEN]", "").replace("[CLOSED]", "").strip() + " [CLOSED]"
    try: await chat.set_title(new_title)
    except BadRequest as e: logger.warning(f"Could not set title for {chat_id}: {e}")
    await context.bot.send_message(chat_id=chat_id, text="🗑️ Previous session closed. All data cleared!")
    logger.info(f"Successfully closed session for chat {chat_id}")

# === AUTOMATED JOBS (Updated to use Context) ===
async def _send_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data['chat_id']
    text = context.job.data['text']

    if get_session_phases(context).get(chat_id) != "links":
        logger.warning(f"Reminder job for chat {chat_id} skipped, session not in 'links' phase.")
        return

    full_text = text
    if "Last 10 mins" in text:
        admin_mentions = await _get_admin_mentions(chat_id, context)
        if admin_mentions:
            full_text += f"\n\n{admin_mentions}"

    try:
        # Prevents link preview for reminder messages
        sent_message = await context.bot.send_message(chat_id=chat_id, text=full_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
        logger.info(f"Sent and pinned reminder '{text}' in chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send/pin reminder in chat {chat_id}: {e}")

async def run_automated_session(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data['chat_id']
    logger.info(f"Starting automated session transition for chat_id: {chat_id}")

    c = get_db_cursor(context)

    try:
        # --- 1. Closing sequence for the PREVIOUS session ---
        logger.info(f"Sending pre-mute unsafe list for chat {chat_id}")
        await _send_unsafe_list_job(context, chat_id=chat_id)

        logger.info(f"Waiting 10 minutes before muting for chat {chat_id}")
        await asyncio.sleep(10 * 60)

        await _run_muteall(chat_id, context, duration_days=2, is_automated=True)
        await asyncio.sleep(5)
        await _run_close_session(chat_id, context)
        await asyncio.sleep(5)

        # --- 2. Opening sequence for the NEW session ---
        await _run_open_session(chat_id, context)

        # --- 3. Schedule reminders for the NEW session if automation is enabled ---
        c.execute("SELECT 1 FROM automated_chats WHERE chat_id = ?", (chat_id,))
        if c.fetchone():
            job_queue = context.job_queue
            job_data = {"chat_id": chat_id}
            job_queue.run_once(_send_reminder_job, 45 * 60, data={**job_data, "text": "keep dropping links🔗"}, name=f"reminder_45_{chat_id}")
            job_queue.run_once(_send_reminder_job, 90 * 60, data={**job_data, "text": "keep dropping links🔗"}, name=f"reminder_90_{chat_id}")
            job_queue.run_once(_send_reminder_job, 120 * 60, data={**job_data, "text": "⚠️ Last 10 mins"}, name=f"reminder_120_{chat_id}")
            logger.info(f"Scheduled new reminders for chat {chat_id} at 45, 90, and 120 mins.")

    except Exception as e:
        logger.error(f"A critical error occurred during automated session transition for {chat_id}: {e}")
        logger.error(traceback.format_exc())
        await context.bot.send_message(chat_id=chat_id, text=f"🚨 A critical error occurred. Please check logs.")
    logger.info(f"Automated session transition finished for chat_id: {chat_id}")


# === AUTHORIZATION DECORATORS (Unchanged) ===
def owner_only(func):
    """Restricts the use of a command to the bot owner."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_user:
            return
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("⚠️ This command can only be used by the bot owner.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

def bot_is_authorized(func):
    """Restricts the use of a command to admins and the owner."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_user or not update.effective_chat: return
        chat = update.effective_chat
        user = update.effective_user
        is_authorized, reason = await is_bot_authorized_in_chat(chat)
        if not is_authorized:
            if reason == "OWNER_NOT_MEMBER": await update.message.reply_text("Bot deactivated: owner not in group.")
            return
        if user.id == OWNER_ID:
            return await func(update, context, *args, **kwargs)
        else:
            try:
                member = await chat.get_member(user.id)
                if member.status not in ["administrator", "creator"]:
                    await update.message.reply_text("⚠️ Only group admins can use this command.")
                    return
            except BadRequest:
                await update.message.reply_text("⚠️ Command must be used in a group where I am an admin.")
                return
            return await func(update, context, *args, **kwargs)
    return wrapper

# === COMMAND HANDLERS & OTHER FUNCTIONS (Updated to use Context) ===

async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_member: return
    if update.chat_member.new_chat_member.user.id == OWNER_ID:
        chat_id = update.effective_chat.id
        if update.chat_member.new_chat_member.status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
            await context.bot.send_message(chat_id, "Bot owner has left. Bot deactivated in this chat.")
            logger.info(f"Bot ops ceased in chat {chat_id} because owner left.")

@bot_is_authorized
async def open_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_open_session(get_effective_chat_id(update.effective_chat.id, context), context)

@bot_is_authorized
async def tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_tracking(get_effective_chat_id(update.effective_chat.id, context), context)

@bot_is_authorized
async def muteall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    duration_str = context.args[0] if context.args else "2d"
    match = re.match(r"(\d+)([dhm])", duration_str.lower())
    if match:
        value, unit = int(match.group(1)), match.group(2)
        if unit == 'd': await _run_muteall(effective_chat_id, context, duration_days=value)
        else: await update.message.reply_text("Manual mute only supports days (e.g., /muteall 2d).")
    else: await update.message.reply_text("⚠️ Invalid duration format. Use 2d, 5d etc.")

@bot_is_authorized
async def close_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_close_session(get_effective_chat_id(update.effective_chat.id, context), context)

async def _generate_unsafe_list_message(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    c = get_db_cursor(context)

    # 1. Get all posted users
    c.execute("SELECT DISTINCT telegram_user FROM links WHERE chat_id = ?", (chat_id,))
    all_users = {u[0] for u in c.fetchall()}

    # 2. Get completed users
    c.execute("SELECT telegram_user FROM status WHERE chat_id = ? AND completed=1", (chat_id,))
    completed_users = {u[0] for u in c.fetchall()}

    # 3. Get whitelisted users
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (chat_id,))
    whitelisted_users = {row[0] for row in c.fetchall()}

    # 4. Get current chat administrators (Asynchronously)
    admin_users = set()
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        # Convert user IDs to strings (matching DB storage)
        admin_users = {str(admin.user.id) for admin in admins if not admin.user.is_bot}
    except BadRequest as e:
        logger.warning(f"Could not fetch admins for chat {chat_id}: {e}")

    # 5. Calculate final unsafe users: (All who posted) - (Completed) - (Whitelisted) - (Admins)
    unsafe_users = all_users - completed_users - whitelisted_users - admin_users

    if not unsafe_users:
        return "✅ All users are safe."

    msg = f"{to_sans_serif_monospace('UNSAFE USERS')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    list_counter = 1

    for tg_user in sorted(list(unsafe_users)):
        c.execute("SELECT telegram_name FROM links WHERE chat_id = ? AND telegram_user=? ORDER BY id DESC LIMIT 1", (chat_id, tg_user,))
        name_row = c.fetchone()
        name = name_row[0] if name_row else f"ID: {tg_user}"
        main = get_main_link(chat_id, tg_user, context)
        if main and main[0]:
            twitter_username = main[0]
            msg += f"{list_counter}. {tg_mention(name, tg_user)} : 𝕏 ɪᴅ @{twitter_username}\n"
            list_counter += 1

    if list_counter == 1 and all_users:
        return f"{to_sans_serif_monospace('UNSAFE USERS')}\n━━━━━━━━━━━━━━━\nCould not retrieve link details for these {len(unsafe_users)} user(s) or they are all admins."
    elif list_counter == 1:
        return "No links or users found in the current session."

    return msg


@bot_is_authorized
async def unsafe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    message_to_send = await _generate_unsafe_list_message(effective_chat_id, context)
    await update.message.reply_text(message_to_send, parse_mode=ParseMode.HTML)

async def _send_unsafe_list_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    effective_chat_id = chat_id
    if effective_chat_id is None and context.job:
        effective_chat_id = context.job.chat_id

    if not effective_chat_id:
        logger.error("Could not determine chat_id for _send_unsafe_list_job")
        return

    is_closing_call = (chat_id is not None)
    if get_session_phases(context).get(effective_chat_id) != "done" and not is_closing_call:
        logger.info(f"Auto-unsafe job for {effective_chat_id} skipped: session phase incorrect.")
        return

    logger.info(f"Running auto-unsafe job for chat {effective_chat_id}.")
    message_to_send = await _generate_unsafe_list_message(effective_chat_id, context)
    if "All users are safe" in message_to_send:
        logger.info(f"Auto-unsafe job for {effective_chat_id}: all users are safe, not sending message.")
        return
    await context.bot.send_message(chat_id=effective_chat_id, text=message_to_send, parse_mode=ParseMode.HTML)

@owner_only
async def enable_automation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)
    c.execute("INSERT OR IGNORE INTO automated_chats (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    await update.message.reply_text("✅ All automated reminders have been ENABLED for this chat.")

@owner_only
async def disable_automation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)
    c.execute("DELETE FROM automated_chats WHERE chat_id = ?", (chat_id,))
    conn.commit()
    await update.message.reply_text("❌ All automated reminders have been DISABLED for this chat.")

@owner_only
async def status_automation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    c.execute("SELECT 1 FROM automated_chats WHERE chat_id = ?", (chat_id,))
    if c.fetchone():
        await update.message.reply_text("ℹ️ Automated reminders are currently ENABLED.")
    else:
        await update.message.reply_text("ℹ️ Automated reminders are currently DISABLED.")

@bot_is_authorized
async def set_open_gif(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets a custom opening GIF for the group by replying to a GIF."""
    if not update.message.reply_to_message or not update.message.reply_to_message.animation:
        await update.message.reply_text("⚠️ Please use this command by replying to a GIF.")
        return

    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    new_gif_id = update.message.reply_to_message.animation.file_id

    c = get_db_cursor(context)
    conn = get_db_connection(context)

    c.execute("INSERT OR REPLACE INTO group_gifs (chat_id, open_gif_file_id) VALUES (?, ?)",
              (effective_chat_id, new_gif_id))
    conn.commit()

    logger.info(f"Custom open GIF set for chat {effective_chat_id}")
    await update.message.reply_animation(
        animation=new_gif_id,
        caption="✅ New open session GIF has been set for this group."
    )

@bot_is_authorized
async def save_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to add them to the safelist.")
        return

    c = get_db_cursor(context)
    conn = get_db_connection(context)

    target_user = update.message.reply_to_message.from_user
    c.execute("INSERT OR IGNORE INTO whitelist (chat_id, telegram_user) VALUES (?, ?)", (get_effective_chat_id(update.effective_chat.id, context), str(target_user.id)))
    conn.commit()
    await update.message.reply_text(f"✅ {tg_mention(target_user.full_name, target_user.id)} added to safelist.", parse_mode=ParseMode.HTML)

@bot_is_authorized
async def unsave_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to remove them from the safelist.")
        return

    c = get_db_cursor(context)
    conn = get_db_connection(context)

    target_user = update.message.reply_to_message.from_user
    c.execute("DELETE FROM whitelist WHERE chat_id = ? AND telegram_user = ?", (get_effective_chat_id(update.effective_chat.id, context), str(target_user.id)))
    conn.commit()
    await update.message.reply_text(f"🗑️ {tg_mention(target_user.full_name, target_user.id)} removed from safelist.", parse_mode=ParseMode.HTML)

@bot_is_authorized
async def list_saved_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (effective_chat_id,))
    rows = c.fetchall()
    if not rows:
        await update.message.reply_text("📝 The safelist is empty.")
        return
    msg = f"📝 {to_sans_serif_monospace('Safelisted Users')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    for idx, (tg_user,) in enumerate(rows, 1):
        c.execute("SELECT telegram_name FROM links WHERE chat_id = ? AND telegram_user=? ORDER BY id DESC LIMIT 1", (effective_chat_id, tg_user,))
        name_row = c.fetchone()
        name = name_row[0] if name_row else f"ID: {tg_user}"
        msg += f"{idx}. {tg_mention(name, tg_user)}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@bot_is_authorized
async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    c = get_db_cursor(context)
    c.execute("SELECT COUNT(DISTINCT telegram_user) FROM links WHERE chat_id = ?", (get_effective_chat_id(update.effective_chat.id, context),))
    total_unique = c.fetchone()[0]
    await update.message.reply_text(f"📊 {to_sans_serif_monospace('Total Unique Users:')} {total_unique}")

@bot_is_authorized
async def multiple_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    c.execute("SELECT telegram_user, telegram_name, twitter_user, full_link FROM links WHERE chat_id = ? ORDER BY telegram_user, id", (effective_chat_id,))
    rows = c.fetchall()
    user_links = {}
    for tg_user, tg_name, tw_user, link in rows:
        user_links.setdefault((tg_user, tg_name), []).append((tw_user, link))
    msg = f"🔗 {to_sans_serif_monospace('Multiple Links by Same User')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    count_multi = 0
    for (tg_user, tg_name), links in user_links.items():
        if len(links) > 1:
            count_multi += 1; msg += f"{count_multi}. 🙍🏻‍♂️ {tg_mention(tg_name, tg_user)}\n"
            for idx, (tw_user, link) in enumerate(links, 1): msg += f"      {idx}. 𝕏 <a href='{link}'>@{tw_user}</a>\n"
            msg += "\n"
    if count_multi == 0: msg += "✅ No user shared multiple links.\n\n"
    tw_map = {}
    for tg_user, tg_name, tw_user, link in rows: tw_map.setdefault(tw_user, []).append((tg_user, tg_name, link))
    msg += f"🚨 {to_sans_serif_monospace('Fraud (Same X by Different Users)')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    count_fraud = 0
    for tw_user, tg_list in tw_map.items():
        if len({u[0] for u in tg_list}) > 1:
            count_fraud += 1; msg += f"{count_fraud}. 𝕏 @{tw_user}\n"
            for i, (tg_user, tg_name, link) in enumerate(tg_list, 1): msg += f"      {i}. 🙍🏻‍♂️ {tg_mention(tg_name, tg_user)} → <a href='{link}'>Link</a>\n"
            msg += "\n"
    if count_fraud == 0: msg += "✅ No fraud cases found."

    # Disable link preview for the entire message
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

@bot_is_authorized
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    c.execute("SELECT DISTINCT telegram_user, telegram_name FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = c.fetchall()

    if not all_users:
        await update.message.reply_text("• No users have shared links yet.")
        return

    users_with_links = []
    for (tg_user, name) in all_users:
        main = get_main_link(effective_chat_id, tg_user, context)
        if main and main[0]:
            twitter_username = main[0]
            users_with_links.append((tg_user, name, twitter_username))

    if not users_with_links:
        await update.message.reply_text("• No users with valid links found in the current session.")
        return

    total_users = len(users_with_links)
    list_counter = 1

    # Split the users into chunks (max 100 per message)
    for i in range(0, total_users, LIST_CHUNK_SIZE):
        chunk = users_with_links[i:i + LIST_CHUNK_SIZE]

        # Build the message header
        if i == 0:
            header = f"📋 {to_sans_serif_monospace(f'USERS LIST (1-{min(LIST_CHUNK_SIZE, total_users)} of {total_users})')}\n"
        else:
            start_num = i + 1
            end_num = min(i + LIST_CHUNK_SIZE, total_users)
            header = f"📋 {to_sans_serif_monospace(f'USERS LIST ({start_num}-{end_num} of {total_users})')}\n"

        msg = header + "━━━━━━━━━━━━━━━\n"

        # Add user entries for the current chunk
        for tg_user, name, twitter_username in chunk:
            msg += f"{list_counter}. {tg_mention(name, tg_user)} : 𝕏 ɪᴅ @{twitter_username}\n"
            list_counter += 1

        # Send the message chunk
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


@bot_is_authorized
async def get_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        main = get_main_link(effective_chat_id, str(target_user.id), context)
        if main: msg = f"🔗 Link for {tg_mention(target_user.full_name, target_user.id)}:\n<a href='{main[1]}'>@{main[0]}</a>"
        else: msg = f"⚠️ No link found for {tg_mention(target_user.full_name, target_user.id)}."
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return
    c.execute("SELECT DISTINCT telegram_user, telegram_name FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = c.fetchall()
    if not all_users:
        await update.message.reply_text("• No links found.")
        return
    msg = f"📋 {to_sans_serif_monospace('User Links')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    for idx, (tg_user, name) in enumerate(all_users, 1):
        main = get_main_link(effective_chat_id, tg_user, context)
        main_msg = f"→ 𝕏 <a href='{main[1]}'>@{main[0]}</a>" if main else ""
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)} {main_msg}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@bot_is_authorized
async def clean_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: count = int(context.args[0]) if context.args else 1000
    except (ValueError, IndexError): count = 1000
    chat_id = update.effective_chat.id
    command_message_id = update.message.message_id
    deleted_count = 0
    # Iterate backwards from the command message ID to delete previous messages
    for message_id in range(command_message_id, command_message_id - count - 1, -1):
        if message_id <= 0: break
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            deleted_count += 1
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try: await context.bot.delete_message(chat_id=chat_id, message_id=message_id); deleted_count += 1
            except (BadRequest, RetryAfter): pass
        except BadRequest: pass
    confirmation_msg = await context.bot.send_message(chat_id=chat_id, text=f"✅️ Chat cleaned. {deleted_count} message(s) deleted.")
    context.job_queue.run_once(delete_message_job, 5, data={'chat_id': chat_id, 'message_id': confirmation_msg.message_id})

@bot_is_authorized
async def set_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    if not context.args:
        await update.message.reply_text("Usage: /set <link>\nExample: /set x.com/your_user")
        return
    new_link = context.args[0]
    c.execute("INSERT OR REPLACE INTO group_settings (chat_id, tracking_link) VALUES (?, ?)", (effective_chat_id, new_link))
    conn.commit()
    # Using MarkdownV2 here prevents link preview for the bot's own response
    await update.message.reply_text(f"✅ Tracking link set to: {new_link}", parse_mode="MarkdownV2")

@bot_is_authorized
async def mark_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Please use this command by replying to a user's message.")
        return

    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    target_user = update.message.reply_to_message.from_user

    c.execute("INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)", (effective_chat_id, str(target_user.id)))
    conn.commit()

    main = get_main_link(effective_chat_id, str(target_user.id), context)

    if main:
        # Simplified reply_text to only show the X ID link
        reply_text = f"𝕏 ɪᴅ :- <a href='{main[1]}'>@{main[0]}</a>"
    else:
        reply_text = f"✅️ Marked {tg_mention(target_user.full_name, target_user.id)} as done, but no link was found for them."

    await update.message.reply_text(
        reply_text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True # Prevents link preview for the reply
    )

@bot_is_authorized
async def sr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to add them to the SR list.")
        return

    c = get_db_cursor(context)
    conn = get_db_connection(context)

    user = update.message.reply_to_message.from_user
    c.execute("INSERT OR REPLACE INTO srlist (chat_id, telegram_user, telegram_name) VALUES (?, ?, ?)", (get_effective_chat_id(update.effective_chat.id, context), str(user.id), user.full_name))
    conn.commit()
    await update.message.reply_text(f"⚠️ {tg_mention(user.full_name, user.id)} your likes are not visible.\nSend a screen recording with a visible profile.", parse_mode=ParseMode.HTML)

@bot_is_authorized
async def rsr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes a user from the SR list."""
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to remove them from the SR list.")
        return

    c = get_db_cursor(context)
    conn = get_db_connection(context)
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    target_user = update.message.reply_to_message.from_user

    deleted_count = c.execute("DELETE FROM srlist WHERE chat_id = ? AND telegram_user = ?",
                                (effective_chat_id, str(target_user.id))).rowcount
    conn.commit()

    if deleted_count > 0:
        await update.message.reply_text(f"✅ {tg_mention(target_user.full_name, target_user.id)} has been **removed from the SR list**.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"ℹ️ {tg_mention(target_user.full_name, target_user.id)} was **not found** in the SR list.", parse_mode=ParseMode.HTML)


@bot_is_authorized
async def srlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    c = get_db_cursor(context)
    c.execute("SELECT telegram_user, telegram_name FROM srlist WHERE chat_id = ?", (effective_chat_id,))
    rows = c.fetchall()
    if not rows:
        await update.message.reply_text("✅ SR list is empty.")
        return
    msg = f"📹 {to_sans_serif_monospace('SR List (Pending Recordings)')}\n"
    msg += "━━━━━━━━━━━━━━━\n"
    for idx, (tg_user, name) in enumerate(rows, 1):
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@bot_is_authorized
async def lock_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await disable_chat(update.effective_chat)
    await update.message.reply_text("🔒 Wait for TL update.")

@bot_is_authorized
async def connect_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    c = get_db_cursor(context)
    conn = get_db_connection(context)

    if not context.args:
        await update.message.reply_text("Usage: /connect <target_group_id>", parse_mode="MarkdownV2")
        return
    try:
        target_chat_id = int(context.args[0])
        if target_chat_id > 0:
            await update.message.reply_text("⚠️ Error: Target ID must be a valid group ID (e.g., -100123456).")
            return
    except (ValueError, IndexError):
        await update.message.reply_text("⚠️ Invalid Target Group ID. It must be a number.")
        return
    c.execute("INSERT OR REPLACE INTO group_connections (chat_id, target_chat_id) VALUES (?, ?)", (chat_id, target_chat_id))
    conn.commit()
    await update.message.reply_text(f"🔗 This group's data is now connected to group {target_chat_id}.", parse_mode=ParseMode.HTML)

@bot_is_authorized
async def disconnect_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    c = get_db_cursor(context)
    conn = get_db_connection(context)
    c.execute("DELETE FROM group_connections WHERE chat_id = ?", (chat_id,))
    conn.commit()
    await update.message.reply_text("🔌 This group is now disconnected and will use its own local data.")

@bot_is_authorized
async def connection_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    c = get_db_cursor(context)
    c.execute("SELECT target_chat_id FROM group_connections WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    if row: await update.message.reply_text(f"🔗 This group shares data with group {row[0]}.", parse_mode=ParseMode.HTML)
    else: await update.message.reply_text("🏠 This group is using its own local data.")

# === MESSAGE HANDLERS (Updated to use Context) ===
async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message or not update.effective_chat: return
        is_authorized, _ = await is_bot_authorized_in_chat(update.effective_chat)
        if not is_authorized: return

        effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
        current_phase = get_session_phases(context).get(effective_chat_id, None)

        if current_phase not in ["links", "done"]: return
        user = update.message.from_user
        text = (update.message.text or "") + " " + (update.message.caption or "")

        c = get_db_cursor(context)
        conn = get_db_connection(context)

        if current_phase == "links":
            if twitter_regex.search(text):
                for match in twitter_regex.finditer(text):
                    c.execute("INSERT INTO links (chat_id, telegram_user, telegram_name, twitter_user, full_link) VALUES (?, ?, ?, ?, ?)",
                            (effective_chat_id, str(user.id), user.full_name, match.group(2), match.group(1)))
                conn.commit()
        elif current_phase == "done":
            if done_regex.search(text):
                c.execute("INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)",
                        (effective_chat_id, str(user.id)))
                conn.commit()
                main = get_main_link(effective_chat_id, str(user.id), context)

                if main:
                    # Create a clickable link using the full_link (main[1]) and show the X ID (main[0]) as text
                    reply_text = f"𝕏 ɪᴅ :- <a href='{main[1]}'>@{main[0]}</a>"
                else:
                    reply_text = f"⚠️ {tg_mention(user.full_name, user.id)}, you are marked done but no link was found from the session."

                await update.message.reply_text(
                    reply_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True # Prevents link preview for the reply
                )
    except Exception as e:
        logger.error(f"Error in track_message for chat {update.effective_chat.id}: {e}")
        logger.error(traceback.format_exc())

async def handle_media_completion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat: return
    is_authorized, _ = await is_bot_authorized_in_chat(update.effective_chat)
    if not is_authorized: return

    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context)
    current_phase = get_session_phases(context).get(effective_chat_id, None)

    if current_phase != "done": return
    user = update.message.from_user

    c = get_db_cursor(context)
    conn = get_db_connection(context)

    c.execute("SELECT 1 FROM srlist WHERE chat_id = ? AND telegram_user=?", (effective_chat_id, str(user.id)))
    is_in_srlist = c.fetchone()
    c.execute("INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)",(effective_chat_id, str(user.id)))

    if is_in_srlist:
        c.execute("DELETE FROM srlist WHERE chat_id = ? AND telegram_user=?", (effective_chat_id, str(user.id)))
        conn.commit()
        await update.message.reply_text("✅ Screen recording received. Marked 'done' and removed from SR list.")
    else:
        conn.commit()
        main = get_main_link(effective_chat_id, str(user.id), context)

        if main:
            # Create a clickable link using the full_link (main[1]) and show the X ID (main[0]) as text
            reply_text = f"𝕏 ɪᴅ :- <a href='{main[1]}'>@{main[0]}</a>"
        else:
            reply_text = "✅️ Media received. Marked as done."

        await update.message.reply_text(
            reply_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True # Prevents link preview for the reply
        )


# === MAIN RUNNER FUNCTION ===

def run_single_bot_instance(config: dict):
    """Initializes, configures, and runs a single Telegram bot application."""
    bot_name = config['name']
    bot_token = config['token']
    db_file = config['db_file']

    # Simple check for placeholder token
    if not bot_token:
        logger.error(f"FATAL: Token for {bot_name} not set in environment variables. Skipping setup.")
        return

    logger.info(f"Setting up {bot_name} with DB: {db_file}...")

    # 1. Setup specific database and load state
    try:
        # Note: setup_database handles check_same_thread=False for multiprocessing
        conn, c = setup_database(db_file)
        session_phases = load_session_state_from_db(c)
    except Exception as e:
        logger.error(f"[{bot_name}] Failed to setup database {db_file}: {e}")
        return

    # 2. Build application and store context data
    app = Application.builder().token(bot_token).build()

    # CRITICAL: Store DB connections and state in bot_data for handlers to access
    app.bot_data['db_conn'] = conn
    app.bot_data['db_cursor'] = c
    app.bot_data['session_phases'] = session_phases
    job_queue = app.job_queue

    # 3. Schedule recurring automated sessions
    for chat_id, schedule in SCHEDULED_CHATS.items():
        tz = pytz.timezone(schedule['timezone'])
        for time_str in schedule['times']:
            try:
                t_aware = time.fromisoformat(time_str).replace(tzinfo=tz)
                # Ensure job name includes the chat_id for uniqueness within this bot's queue
                job_name = f"{bot_name}_AutoSession_{chat_id}_{time_str}"
                job_queue.run_daily(run_automated_session, time=t_aware, data={'chat_id': chat_id}, name=job_name)
                logger.info(f"[{bot_name}] Scheduled session for chat {chat_id} at {time_str}.")
            except ValueError:
                logger.error(f"[{bot_name}] Invalid time format '{time_str}' for chat {chat_id}.")

    # 4. Add all handlers
    app.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(CommandHandler("open", open_session)); app.add_handler(CommandHandler("tracking", tracking))
    app.add_handler(CommandHandler("close", close_session)); app.add_handler(CommandHandler("muteall", muteall))
    app.add_handler(CommandHandler("l", lock_chat)); app.add_handler(CommandHandler("users", users))
    app.add_handler(CommandHandler("list", list_users)); app.add_handler(CommandHandler("link", get_links))
    app.add_handler(CommandHandler("unsafe", unsafe)); app.add_handler(CommandHandler("multiple_link", multiple_link))
    app.add_handler(CommandHandler("srlist", srlist)); app.add_handler(CommandHandler("savelist", list_saved_users))
    app.add_handler(CommandHandler("clean", clean_chat)); app.add_handler(CommandHandler("sr", sr))
    app.add_handler(CommandHandler("rsr", rsr));
    app.add_handler(CommandHandler("ad", mark_done)); app.add_handler(CommandHandler("save", save_user))
    app.add_handler(CommandHandler("unsave", unsave_user)); app.add_handler(CommandHandler("set", set_link))
    app.add_handler(CommandHandler("connect", connect_group)); app.add_handler(CommandHandler("disconnect", disconnect_group))
    app.add_handler(CommandHandler("connection_status", connection_status))
    app.add_handler(CommandHandler("setgif", set_open_gif))

    automation_handler = CommandHandler("automation", status_automation, filters=filters.Regex(r'^/automation$'))
    automation_on_handler = CommandHandler("automation", enable_automation, filters=filters.Regex(r'on'))
    automation_off_handler = CommandHandler("automation", disable_automation, filters=filters.Regex(r'off'))
    app.add_handler(automation_handler)
    app.add_handler(automation_on_handler)
    app.add_handler(automation_off_handler)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_message))
    app.add_handler(MessageHandler(filters.VIDEO | filters.ANIMATION, handle_media_completion))

    # 5. Start Polling
    logger.info(f"{bot_name} is starting polling...")
    try:
        # This is a blocking call and must run in a separate process
        app.run_polling(drop_pending_updates=False)
    except Exception as e:
        logger.error(f"[{bot_name}] Polling failed: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()
        logger.info(f"[{bot_name}] Database connection closed.")

def main():
    """Starts all configured bots concurrently using multiprocessing."""
    processes = []

    # Filter out bots where the token was NOT provided via environment variables
    active_configs = [c for c in BOT_CONFIGS if c['token']]

    if not active_configs:
        logger.error("No valid bot configurations found. Please check your Koyeb secrets (e.g., BOT_TOKEN_ALPHA).")
        return

    for config in active_configs:
        # 💡 CRITICAL FIX: Use multiprocessing.Process instead of threading.Thread
        process = multiprocessing.Process(target=run_single_bot_instance, args=(config,), name=config['name'])
        processes.append(process)
        process.start()

    logger.info(f"Started {len(active_configs)} bot processes. Use Ctrl+C to stop all.")

    # Wait for all processes to complete (keeps the main script alive)
    for process in processes:
        process.join()

# Correct entry point check
if __name__ == "__main__":
    main()