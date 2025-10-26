import os
import os.path
import re
import psycopg2 # Use PostgreSQL driver
from psycopg2 import pool
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
import multiprocessing
from contextlib import contextmanager # Import context manager

# === LOGGING SETUP (MODIFIED FOR HOSTING) ===
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# === CONFIGURATION (MODIFIED FOR HOSTING) ===
OWNER_ID = int(os.environ.get("OWNER_ID", 5409158853))
BOT_TOKEN_ALPHA = os.environ.get("BOT_TOKEN_ALPHA")
BOT_TOKEN_BETA = os.environ.get("BOT_TOKEN_BETA")
BOT_TOKEN_GAMMA = os.environ.get("BOT_TOKEN_GAMMA")
DATABASE_URL = os.environ.get('DATABASE_URL') # Get DB URL from Render

DATA_DIR = "." # Not strictly needed with PostgreSQL, but keep if other parts use it
logger.info(f"Using DATABASE_URL for storage.")
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not found! Bot cannot connect to DB.")
    # Exit or handle fallback if running locally without DATABASE_URL
    # raise ValueError("DATABASE_URL environment variable is required.")

DEFAULT_OPEN_GIF_FILE_ID = 'CgACAgQAAxkBAAEYabFo4j0mLSA0aMiJNaSwj1_B5RrUiAAC4AIAAhhPDVOzldaDFsYkKjYE'
DEFAULT_TRACKING_LINK = "x.com/your_default_username"
LIST_CHUNK_SIZE = 100

BOT_CONFIGS = [
    {"name": "BotAlpha", "token": BOT_TOKEN_ALPHA},
    {"name": "BotBeta", "token": BOT_TOKEN_BETA},
    {"name": "BotGamma", "token": BOT_TOKEN_GAMMA},
]

# === ⏰ AUTOMATED SESSION SCHEDULER CONFIGURATION ⏰ ===
SCHEDULED_CHATS = {
    -1002992139767: {"times": ["08:50:00", "12:18:00", "19:00:00"], "timezone": "Asia/Kolkata"},
    -1002373178666: {"times": ["09:20:00", "13:50:00", "18:20:00"], "timezone": "Asia/Kolkata"},
}

# === DATABASE CONNECTION POOL ===
db_pool = None

def initialize_database_pool():
    global db_pool
    if not DATABASE_URL:
        logger.error("Cannot initialize DB pool: DATABASE_URL is not set.")
        return False
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 5, dsn=DATABASE_URL)
        logger.info("Database connection pool initialized.")
        # Create tables on initialization
        conn = db_pool.getconn()
        cursor = conn.cursor()
        create_tables(cursor)
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        logger.info("Database tables checked/created.")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error initializing database pool: {error}", exc_info=True)
        db_pool = None
        return False

def create_tables(c):
    # Use BIGINT for IDs that might exceed standard integer, SERIAL for auto-incrementing PK
    c.execute("""CREATE TABLE IF NOT EXISTS group_connections (
        chat_id BIGINT PRIMARY KEY, target_chat_id BIGINT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS group_settings (
        chat_id BIGINT PRIMARY KEY, tracking_link TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS links (
        id SERIAL PRIMARY KEY, chat_id BIGINT NOT NULL, telegram_user TEXT,
        telegram_name TEXT, twitter_user TEXT, full_link TEXT,
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)""") # Use TIMESTAMP WITH TIME ZONE
    c.execute("""CREATE TABLE IF NOT EXISTS status (
        chat_id BIGINT NOT NULL, telegram_user TEXT NOT NULL, completed INTEGER DEFAULT 0,
        last_done TIMESTAMP WITH TIME ZONE, PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS srlist (
        chat_id BIGINT NOT NULL, telegram_user TEXT NOT NULL, telegram_name TEXT,
        added_on TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS whitelist (
        chat_id BIGINT NOT NULL, telegram_user TEXT NOT NULL,
        PRIMARY KEY (chat_id, telegram_user))""")
    c.execute("""CREATE TABLE IF NOT EXISTS session_state (
        chat_id BIGINT PRIMARY KEY, phase TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS automated_chats (
        chat_id BIGINT PRIMARY KEY)""")
    c.execute("""CREATE TABLE IF NOT EXISTS group_gifs (
        chat_id BIGINT PRIMARY KEY,
        open_gif_file_id TEXT NOT NULL
    )""")
    logger.info("Executed CREATE TABLE IF NOT EXISTS statements.")

def get_db_connection_from_pool():
    if db_pool is None:
        logger.error("Database pool is not initialized.")
        return None
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}", exc_info=True)
        return None

def return_db_connection_to_pool(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

def close_db_pool():
    if db_pool:
        db_pool.closeall()
        logger.info("Database connection pool closed.")

@contextmanager
def get_db_cursor_context():
    """Provides a database cursor within a context manager."""
    conn = get_db_connection_from_pool()
    if conn is None:
        raise ConnectionError("Failed to get database connection from pool.")
    cursor = None # Define cursor here to ensure it exists in finally block
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Database error: {error}", exc_info=True)
        if conn: conn.rollback()
        raise
    finally:
        if cursor: cursor.close()
        return_db_connection_to_pool(conn)

# --- Keep get_session_phases ---
def get_session_phases(context: ContextTypes.DEFAULT_TYPE):
    if 'session_phases' not in context.application.bot_data:
        context.application.bot_data['session_phases'] = {}
    return context.application.bot_data['session_phases']

# === REGEX & HELPERS ===
twitter_regex = re.compile(r"(https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]+)/status/\d+)", re.IGNORECASE)
username_regex = re.compile(r"@[a-zA-Z0-9_]+", re.IGNORECASE)
done_regex = re.compile(r"\b(done|completed|ad|all done|dn)\b", re.IGNORECASE)

async def is_bot_authorized_in_chat(chat: Chat) -> tuple[bool, str]:
    try:
        owner_member = await chat.get_member(OWNER_ID)
        if owner_member.status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
            return (False, "OWNER_NOT_MEMBER")
    except BadRequest:
        return (False, "OWNER_NOT_MEMBER")
    return (True, "AUTHORIZED")

# --- MODIFIED: Use context manager for DB ---
def get_effective_chat_id(chat_id, context: ContextTypes.DEFAULT_TYPE):
    # This function now needs to handle DB access itself or be removed if not strictly needed
    # For now, let's assume it fetches from DB. If not needed, remove calls to it.
    try:
        with get_db_cursor_context() as c:
            c.execute("SELECT target_chat_id FROM group_connections WHERE chat_id = %s", (chat_id,))
            row = c.fetchone()
            return row[0] if row else chat_id
    except Exception as e:
        logger.error(f"Error getting effective chat ID for {chat_id}: {e}", exc_info=True)
        return chat_id # Fallback to original chat_id on error

def tg_mention(name, user_id):
    name = str(name).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") # Ensure name is string
    try:
        user_id_int = int(user_id)
        return f"<a href='tg://user?id={user_id_int}'>{name}</a>"
    except ValueError:
        logger.warning(f"Invalid user_id for tg_mention: {user_id}")
        return name # Return name without link if ID is invalid

# --- MODIFIED: Use context manager for DB ---
def get_main_link(chat_id, telegram_user, context: ContextTypes.DEFAULT_TYPE):
    try:
        with get_db_cursor_context() as c:
            # Use %s placeholders
            c.execute("""SELECT twitter_user, full_link FROM links
                         WHERE chat_id = %s AND telegram_user = %s
                         ORDER BY id DESC LIMIT 1""", (chat_id, telegram_user))
            return c.fetchone()
    except Exception as e:
        logger.error(f"Error getting main link for user {telegram_user} in chat {chat_id}: {e}", exc_info=True)
        return None

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    try:
        await context.bot.delete_message(chat_id=job_data['chat_id'], message_id=job_data['message_id'])
    except BadRequest:
        pass # Ignore if message already deleted

def to_sans_serif_monospace(text: str) -> str:
    sans_serif_map = {
        'A': '𝙰', 'B': '𝙱', 'C': '𝙲', 'D': '𝙳', 'E': '𝙴', 'F': '𝙵', 'G': '𝙶', 'H': '𝙷', 'I': '𝙸', 'J': '𝙹', 'K': '𝙺', 'L': '𝙻', 'M': '𝙼',
        'N': '𝙽', 'O': '𝙾', 'P': '𝙿', 'Q': '𝚀', 'R': '𝚁', 'S': '𝚂', 'T': '𝚃', 'U': '𝚄', 'V': '𝚅', 'W': '𝚆', 'X': '𝚇', 'Y': '𝚈', 'Z': '𝚉',
        '0': '𝟶', '1': '𝟷', '2': '𝟸', '3': '𝟹', '4': '𝟺', '5': '𝟻', '6': '𝟼', '7': '𝟽', '8': '𝟾', '9': '𝟿', ' ': ' '
    }
    return "".join(sans_serif_map.get(char, char) for char in text.upper())

async def _get_admin_mentions(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        mentions = [tg_mention(admin.user.full_name, admin.user.id) for admin in admins if not admin.user.is_bot]
        return " ".join(mentions)
    except Exception as e:
        logger.error(f"Could not get admin list for chat {chat_id}: {e}", exc_info=True)
        return ""

# === PERMISSION HELPERS (Unchanged) ===
async def set_text_only_permissions(chat: Chat):
    permissions = ChatPermissions(can_send_messages=True, can_add_web_page_previews=False, can_send_audios=False, can_send_documents=False, can_send_photos=False, can_send_videos=False, can_send_video_notes=False, can_send_voice_notes=False, can_send_polls=False, can_send_other_messages=False, can_change_info=False, can_invite_users=False, can_pin_messages=False)
    try: await chat.set_permissions(permissions)
    except Exception as e: logger.error(f"Failed setting text-only permissions for {chat.id}: {e}", exc_info=True)
    logger.info(f"Set permissions to TEXT-ONLY for chat {chat.id}")
async def set_tracking_phase_permissions(chat: Chat):
    permissions = ChatPermissions(can_send_messages=True, can_add_web_page_previews=False, can_send_videos=True, can_send_other_messages=True, can_send_audios=False, can_send_documents=False, can_send_photos=False, can_send_video_notes=False, can_send_voice_notes=False, can_send_polls=False, can_change_info=False, can_invite_users=False, can_pin_messages=False)
    try: await chat.set_permissions(permissions)
    except Exception as e: logger.error(f"Failed setting tracking permissions for {chat.id}: {e}", exc_info=True)
    logger.info(f"Set permissions to TRACKING for chat {chat.id}")
async def disable_chat(chat):
    permissions = ChatPermissions(can_send_messages=False, can_send_audios=False, can_send_documents=False, can_send_photos=False, can_send_videos=False, can_send_video_notes=False, can_send_voice_notes=False, can_send_polls=False, can_send_other_messages=False, can_add_web_page_previews=False, can_change_info=False, can_invite_users=False, can_pin_messages=False)
    try: await chat.set_permissions(permissions)
    except Exception as e: logger.error(f"Failed disabling chat permissions for {chat.id}: {e}", exc_info=True)
    logger.info(f"Disabled all member permissions for chat {chat.id}")

# === REFACTORED SESSION LOGIC (MODIFIED FOR DB ACCESS) ===
async def _run_open_session(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session_phases = get_session_phases(context)
    try:
        with get_db_cursor_context() as c:
            session_phases[chat_id] = "links"
            # Use %s placeholder
            c.execute("INSERT INTO session_state (chat_id, phase) VALUES (%s, %s) ON CONFLICT (chat_id) DO UPDATE SET phase = EXCLUDED.phase",
                      (chat_id, "links"))

            chat = await context.bot.get_chat(chat_id)
            await set_text_only_permissions(chat)
            if "[OPEN]" not in chat.title:
                new_title = chat.title.replace("[CLOSED]", "").strip() + " [OPEN]"
                try: await chat.set_title(new_title)
                except BadRequest as e: logger.warning(f"Could not set title for {chat_id}: {e}")

            c.execute("SELECT open_gif_file_id FROM group_gifs WHERE chat_id = %s", (chat_id,))
            row = c.fetchone()
            gif_to_send = row[0] if row else DEFAULT_OPEN_GIF_FILE_ID

        await context.bot.send_animation(chat_id, animation=gif_to_send)
        message_text = "<b> start dropping your post </b>"
        try:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.HTML)
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
        except BadRequest as e:
            logger.error(f"Error pinning opening message in {chat_id}: {e}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text="⚠️ Could not pin message. Please grant 'Pin Messages' admin rights.")
        logger.info(f"Successfully opened session for chat {chat_id}")

    except (Exception, ConnectionError) as e:
        logger.error(f"Error in _run_open_session for chat {chat_id}: {e}", exc_info=True)
        try: await context.bot.send_message(chat_id=chat_id, text="⚠️ An error occurred opening the session.")
        except: pass # Avoid error loops if sending fails

async def _run_tracking(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session_phases = get_session_phases(context)
    try:
        with get_db_cursor_context() as c:
            session_phases[chat_id] = "done"
            c.execute("INSERT INTO session_state (chat_id, phase) VALUES (%s, %s) ON CONFLICT (chat_id) DO UPDATE SET phase = EXCLUDED.phase",
                      (chat_id, "done"))

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
                logger.error(f"Could not calculate 1-hour deadline for chat {chat_id}: {e}", exc_info=True)

            c.execute("SELECT tracking_link FROM group_settings WHERE chat_id = %s", (chat_id,))
            tracking_link = (c.fetchone() or [DEFAULT_TRACKING_LINK])[0]

            message_text = (f"Timeline Updated 👇\n\n{tracking_link}\n\nLike all posts of the TL account and\n"
                            f"Drop 'done' (or 'ad', 'completed') to be marked safe ✅{deadline_str}")
            try:
                sent_message = await context.bot.send_message(chat_id=chat_id, text=message_text, disable_web_page_preview=True)
                await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
            except BadRequest as e:
                logger.error(f"Error pinning tracking message in {chat_id}: {e}", exc_info=True)
                await context.bot.send_message(chat_id=chat_id, text="⚠️ Could not start tracking phase. Check permissions.")
            logger.info(f"Successfully started tracking for chat {chat_id}")

            # Schedule reminders only if automation is enabled in DB
            c.execute("SELECT 1 FROM automated_chats WHERE chat_id = %s", (chat_id,))
            if c.fetchone():
                job_queue = context.job_queue
                job_queue.run_once(_send_unsafe_list_job, 30 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_1")
                job_queue.run_once(_send_unsafe_list_job, 40 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_2")
                job_queue.run_once(_send_unsafe_list_job, 45 * 60, chat_id=chat_id, name=f"unsafe_{chat_id}_3")
                logger.info(f"Scheduled auto-unsafe reminders for chat {chat_id} at 30, 40, and 45 minutes.")

    except (Exception, ConnectionError) as e:
        logger.error(f"Error in _run_tracking for chat {chat_id}: {e}", exc_info=True)
        try: await context.bot.send_message(chat_id=chat_id, text="⚠️ An error occurred starting the tracking phase.")
        except: pass

# --- MORE MODIFICATIONS NEEDED for _run_muteall, _run_close_session, and ALL OTHER FUNCTIONS that use DB ---
# --- You MUST replace ALL db access (get_db_cursor, get_db_connection, c.execute, conn.commit) ---
# --- with the `with get_db_cursor_context() as c:` pattern and use %s placeholders. ---

# Example Modification for _run_muteall (Partial - you need to complete it)
async def _run_muteall(chat_id: int, context: ContextTypes.DEFAULT_TYPE, duration_days: int, is_automated: bool = False):
    try:
        with get_db_cursor_context() as c:
            until_timestamp_dt = datetime.utcnow() + timedelta(days=duration_days) # Use datetime for DB if possible, or keep timestamp
            until_timestamp = int(until_timestamp_dt.timestamp()) # Keep timestamp for restrict_member

            c.execute("SELECT DISTINCT telegram_user FROM links WHERE chat_id = %s", (chat_id,))
            all_users = {u[0] for u in c.fetchall()}
            c.execute("SELECT telegram_user FROM status WHERE chat_id = %s AND completed=1", (chat_id,))
            completed = {u[0] for u in c.fetchall()}
            c.execute("SELECT telegram_user FROM srlist WHERE chat_id = %s", (chat_id,))
            sr_users = {u[0] for u in c.fetchall()}
            c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = %s", (chat_id,))
            whitelisted_users = {row[0] for row in c.fetchall()}

            admin_users = set()
            try:
                admins = await context.bot.get_chat_administrators(chat_id)
                admin_users = {str(admin.user.id) for admin in admins if not admin.user.is_bot}
            except BadRequest: pass

            unsafe_users = all_users - completed
            final_users_to_mute = (unsafe_users.union(sr_users)) - whitelisted_users - admin_users

            if not final_users_to_mute:
                # Send message outside 'with' block if possible, or handle potential message sending errors
                logger.info(f"No users to mute in chat {chat_id}")
                # Return users_to_mute and message status to send message outside
                return [], "No users found", failed_count # Return empty list, status message, failed_count=0

            muted_users_tags = []
            failed_count = 0
            users_to_mute_details = [] # Store details needed outside the 'with' block

            for tg_user in final_users_to_mute:
                c.execute("SELECT telegram_name FROM links WHERE chat_id = %s AND telegram_user=%s ORDER BY id DESC LIMIT 1", (chat_id, tg_user))
                name_row = c.fetchone()
                name = name_row[0] if name_row else f"ID: {tg_user}"
                users_to_mute_details.append({'id': tg_user, 'name': name})

        # --- Muting logic outside the 'with' block to avoid holding DB connection ---
        chat = await context.bot.get_chat(chat_id)
        for user_detail in users_to_mute_details:
             try:
                 await chat.restrict_member(int(user_detail['id']), ChatPermissions(can_send_messages=False), until_date=until_timestamp)
                 muted_users_tags.append(tg_mention(user_detail['name'], user_detail['id']))
             except Exception as e:
                 failed_count += 1
                 logger.warning(f"Failed to mute user {user_detail['id']} in chat {chat_id}: {e}")

        # --- Send result message ---
        title_text = "Automated Mute Complete" if is_automated else "Mute Complete"
        msg = f"🔇 {to_sans_serif_monospace(title_text)}\n"
        msg += f"ᴅᴜʀᴀᴛɪᴏɴ: {duration_days} ᴅᴀʏs\n\n"
        if muted_users_tags:
            msg += f"🚫 Muted Users ({len(muted_users_tags)}):\n"
            msg += "\n".join(f"{i}. {user_tag}" for i, user_tag in enumerate(muted_users_tags, 1))
        elif not users_to_mute_details: # Check if the list from DB was empty
             msg += "✅ Check complete. No unsafe users found to mute."
        else:
             msg += "✅ No users were successfully muted in this cycle (check logs)."
        if failed_count > 0:
            msg += f"\n\n⚠️ Failed to mute {failed_count} user(s) (they may have left or are admins)."
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
        logger.info(f"Mute cycle complete for {chat_id}. Muted: {len(muted_users_tags)}. Failed: {failed_count}.")


    except (Exception, ConnectionError) as e:
        logger.error(f"Error in _run_muteall for chat {chat_id}: {e}", exc_info=True)
        try: await context.bot.send_message(chat_id=chat_id, text="⚠️ An error occurred during the mute process.")
        except: pass


# === AUTHORIZATION DECORATORS (Unchanged) ===
# ... (owner_only and bot_is_authorized decorators remain the same) ...

# === COMMAND HANDLERS & OTHER FUNCTIONS ===
# !!! IMPORTANT: YOU MUST REFACTOR ALL THESE FUNCTIONS !!!
# Replace ALL database interactions using the old get_db_cursor/conn
# with the `with get_db_cursor_context() as c:` pattern.
# Remember to change SQL placeholders from '?' to '%s'.

# Example: Refactored mark_done
@bot_is_authorized
async def mark_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Please use this command by replying to a user's message.")
        return

    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context) # Ensure this function is refactored too
    target_user = update.message.reply_to_message.from_user
    main_link_info = None # Store link info outside 'with' block

    try:
        with get_db_cursor_context() as c:
            # Use CURRENT_TIMESTAMP directly in SQL for PostgreSQL
            c.execute("""
                INSERT INTO status (chat_id, telegram_user, completed, last_done)
                VALUES (%s, %s, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id, telegram_user)
                DO UPDATE SET completed = 1, last_done = CURRENT_TIMESTAMP
            """, (effective_chat_id, str(target_user.id)))

            # Fetch link info within the same transaction if possible
            c.execute("""SELECT twitter_user, full_link FROM links
                         WHERE chat_id = %s AND telegram_user = %s
                         ORDER BY id DESC LIMIT 1""", (effective_chat_id, str(target_user.id)))
            main_link_info = c.fetchone()

        # Prepare reply text outside the 'with' block
        if main_link_info:
            reply_text = f"𝕏 ɪᴅ :- <a href='{main_link_info[1]}'>@{main_link_info[0]}</a>"
        else:
            reply_text = f"✅️ Marked {tg_mention(target_user.full_name, target_user.id)} as done, but no link was found for them."

        await update.message.reply_text(
            reply_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

    except ConnectionError as e:
        await update.message.reply_text("Database connection error. Please try again later.")
        logger.error(f"DB Connection Error in mark_done: {e}", exc_info=True)
    except (Exception, psycopg2.DatabaseError) as e:
        await update.message.reply_text("An error occurred marking the user as done.")
        logger.error(f"Error in mark_done for user {target_user.id}: {e}", exc_info=True)


# === MESSAGE HANDLERS (MODIFIED FOR DB ACCESS) ===
async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat: return
    # Removed authorization check here, assuming handlers manage it or it's implicitly handled

    effective_chat_id = get_effective_chat_id(update.effective_chat.id, context) # Ensure refactored
    current_phase = get_session_phases(context).get(effective_chat_id)
    user = update.message.from_user
    text = (update.message.text or "") + " " + (update.message.caption or "")

    try:
        if current_phase == "links":
            if twitter_regex.search(text):
                with get_db_cursor_context() as c:
                    for match in twitter_regex.finditer(text):
                        c.execute("""INSERT INTO links (chat_id, telegram_user, telegram_name, twitter_user, full_link)
                                     VALUES (%s, %s, %s, %s, %s)""",
                                  (effective_chat_id, str(user.id), user.full_name, match.group(2), match.group(1)))
        elif current_phase == "done":
            if done_regex.search(text):
                 main_link_info = None # Store link info outside 'with' block
                 with get_db_cursor_context() as c:
                     c.execute("""
                         INSERT INTO status (chat_id, telegram_user, completed, last_done)
                         VALUES (%s, %s, 1, CURRENT_TIMESTAMP)
                         ON CONFLICT (chat_id, telegram_user)
                         DO UPDATE SET completed = 1, last_done = CURRENT_TIMESTAMP
                     """, (effective_chat_id, str(user.id)))

                     # Fetch link info
                     c.execute("""SELECT twitter_user, full_link FROM links
                                  WHERE chat_id = %s AND telegram_user = %s
                                  ORDER BY id DESC LIMIT 1""", (effective_chat_id, str(user.id)))
                     main_link_info = c.fetchone()

                 # Prepare and send reply outside 'with' block
                 if main_link_info:
                     reply_text = f"𝕏 ɪᴅ :- <a href='{main_link_info[1]}'>@{main_link_info[0]}</a>"
                 else:
                     reply_text = f"⚠️ {tg_mention(user.full_name, user.id)}, you are marked done but no link was found from the session."

                 await update.message.reply_text(
                     reply_text,
                     parse_mode=ParseMode.HTML,
                     disable_web_page_preview=True
                 )

    except ConnectionError as e:
        # Avoid replying in message handler to prevent spam on DB errors
        logger.error(f"DB Connection Error in track_message for chat {effective_chat_id}: {e}", exc_info=True)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Error in track_message for chat {effective_chat_id}: {e}", exc_info=True)


# --- YOU MUST REFACTOR handle_media_completion similarly ---


# === MAIN RUNNER FUNCTION ===
def run_single_bot_instance(config: dict):
    bot_name = config['name']
    bot_token = config['token']

    if not bot_token:
        logger.error(f"FATAL: Token for {bot_name} not set in environment variables. Skipping setup.")
        return

    logger.info(f"Setting up {bot_name}...")

    # Initialize DB Pool (happens once per process)
    if not initialize_database_pool():
        logger.error(f"[{bot_name}] Database pool failed to initialize. Exiting.")
        return

    # Load session state from DB if needed (or start fresh)
    initial_session_phases = {}
    try:
        with get_db_cursor_context() as c:
            c.execute("SELECT chat_id, phase FROM session_state")
            rows = c.fetchall()
            initial_session_phases = {chat_id: phase for chat_id, phase in rows}
            if initial_session_phases:
                 logger.info(f"[{bot_name}] Loaded session states from DB: {initial_session_phases}")
    except Exception as e:
        logger.error(f"[{bot_name}] Failed to load session state from DB: {e}. Starting fresh.", exc_info=True)
        initial_session_phases = {}


    app = Application.builder().token(bot_token).build()
    app.bot_data['session_phases'] = initial_session_phases # Store loaded/initial state
    job_queue = app.job_queue

    # Schedule recurring automated sessions
    for chat_id, schedule in SCHEDULED_CHATS.items():
        try:
            tz = pytz.timezone(schedule['timezone'])
            for time_str in schedule['times']:
                t_aware = time.fromisoformat(time_str).replace(tzinfo=tz)
                job_name = f"{bot_name}_AutoSession_{chat_id}_{time_str}"
                # Pass chat_id correctly in data
                job_queue.run_daily(run_automated_session, time=t_aware, data={'chat_id': chat_id}, name=job_name)
                logger.info(f"[{bot_name}] Scheduled session for chat {chat_id} at {time_str} {schedule['timezone']}.")
        except Exception as e:
            logger.error(f"[{bot_name}] Failed to schedule job for chat {chat_id} at {time_str}: {e}", exc_info=True)

    # Add all handlers (Ensure functions called here are fully refactored for PostgreSQL)
    # ... (add_handler calls remain the same, assuming the handler functions are refactored) ...
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


    logger.info(f"{bot_name} is starting polling...")
    try:
        app.run_polling(drop_pending_updates=False)
    except Exception as e:
        logger.error(f"[{bot_name}] Polling failed: {e}", exc_info=True)
    finally:
        close_db_pool() # Attempt to close pool on exit
        logger.info(f"[{bot_name}] Bot process finished.")

def main():
    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set. Cannot start bots without database connection string.")
        return

    processes = []
    active_configs = [c for c in BOT_CONFIGS if c.get('token')]

    if not active_configs:
        logger.error("No valid bot configurations found (missing tokens in env vars?). Please check Render environment variables.")
        return

    logger.info(f"Found {len(active_configs)} bot configurations with tokens.")

    # Filter out bots without tokens *before* creating processes
    valid_bot_configs = [config for config in BOT_CONFIGS if config.get("token")]

    if not valid_bot_configs:
         logger.error("No bot tokens found in environment variables (e.g., BOT_TOKEN_ALPHA). Exiting.")
         return

    for config in valid_bot_configs:
        process = multiprocessing.Process(target=run_single_bot_instance, args=(config,), name=config['name'])
        processes.append(process)
        process.start()

    logger.info(f"Started {len(processes)} bot processes. Use Ctrl+C locally to stop.")
    for process in processes:
        process.join()

if __name__ == "__main__":
    main()