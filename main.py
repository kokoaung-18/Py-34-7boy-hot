from __future__ import annotations

import asyncio
import gc
import hashlib
import html
import io
import os
import random
import re
import sqlite3
import sys
import time
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple, TypeVar

try:
    import resource
except ImportError:
    resource = None  # type: ignore[assignment]

import httpx
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TimedOut,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

T = TypeVar("T")

# ============================================================
# TELEGRAM CONFIG
# ============================================================
BOT_TOKEN = "8498838247:AAF1BmNDJEOj7qsL6sTWJrl5CL6QhC9s9cU"
ADMIN_ID = 7224850261

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise SystemExit(f"Invalid BOT_TOKEN: {BOT_TOKEN!r}")
if ADMIN_ID <= 0:
    raise SystemExit(f"Invalid ADMIN_ID: {ADMIN_ID}")
print(f"[CONFIG] admin_id={ADMIN_ID} token={BOT_TOKEN[:10]}...***", flush=True)

# ============================================================
# TELEGRAM NETWORK TUNING
# ============================================================
TG_CONNECT_TIMEOUT = 20.0
TG_READ_TIMEOUT = 40.0
TG_WRITE_TIMEOUT = 40.0
TG_POOL_TIMEOUT = 20.0

# ============================================================
# FIREBASE REALTIME DATABASE
# ============================================================
FIREBASE_RTDB_URL = "https://kokoaung-41228-default-rtdb.firebaseio.com"
FIREBASE_API_KEY = "AIzaSyALDtmbQq-2Pbz0mOJBi2rPxNPAgjTQ8qo"
FIREBASE_AUTH = ""

_NAMESPACE_SEED = f"{BOT_TOKEN}|{FIREBASE_RTDB_URL}|v1".encode()
FIREBASE_NS = "s_" + hashlib.sha256(_NAMESPACE_SEED).hexdigest()[:24]

FIREBASE_CONNECT_TIMEOUT = 20.0
FIREBASE_READ_TIMEOUT = 45.0
FIREBASE_WRITE_TIMEOUT = 45.0
FIREBASE_POOL_TIMEOUT = 20.0
FIREBASE_MAX_RETRIES = 6
FIREBASE_BACKOFF_BASE = 3.0
FIREBASE_BACKOFF_MAX = 45.0

# ============================================================
# BOOTSTRAP — PARALLEL + RESUMABLE
# ============================================================
BOOTSTRAP_BATCH_SIZE = 10000
BOOTSTRAP_PARALLEL = 8
BOOTSTRAP_BATCH_TIMEOUT = 60.0
BOOTSTRAP_KEYS_TIMEOUT = 30.0

# ============================================================
# LIVE POLLING
# ============================================================
LIVE_POLL_INTERVAL = 5.0
LIVE_OVERLAP_BLOCKS = 50
LIVE_RETRY_BACKOFF_BASE = 5.0
LIVE_RETRY_BACKOFF_MAX = 60.0

LIVE_STATUS_EDIT_INTERVAL = 15.0
LIVE_STATUS_EDIT_TICK = 1.0
LIVE_STATUS_TABLE_ROWS = 10

# ============================================================
# PATTERN SEARCH
# ============================================================
PATTERN_WORKERS = 16
PATTERN_SEARCH_TIMEOUT = 20.0

# ============================================================
# APP CONFIG
# ============================================================
USER_QUERY_COOLDOWN = 3.0
LOG_MAX_LINES = 2000

PATTERN_MAX_LEN = 64
PATTERN_MIN_LEN = 1
ABOVE_DISPLAY_LIMIT = 30

DB_PATH = Path(os.environ.get(
    "TRON_DB_PATH", str(Path(__file__).resolve().with_name("tron_blocks.sqlite3"))
))

# ============================================================
# GLOBAL STATE
# ============================================================
state_lock = asyncio.Lock()
db_lock = asyncio.Lock()
http_client: Optional[httpx.AsyncClient] = None

app_ref_holder: dict = {}

user_last_query: Dict[int, float] = {}
user_query_counts: Dict[int, int] = {}
user_in_flight: Dict[int, int] = {}

allowed_users: Set[int] = set()

db: Optional[sqlite3.Connection] = None
app_log: Deque[str] = deque(maxlen=LOG_MAX_LINES)

history_cache: Dict[int, str] = {}
history_cache_loaded = False

live_task: Optional[asyncio.Task] = None
live_stop = asyncio.Event()
last_seen_max_block: Optional[int] = None
last_new_block_time: float = 0.0
live_new_block_count: int = 0
live_events: Deque[str] = deque(maxlen=20)

live_status_chat_id: Optional[int] = None
live_status_message_id: Optional[int] = None
live_status_last_edit: float = 0.0
live_status_last_error: str = ""

# Pattern cache
_pattern_cache_valid: bool = False
_pattern_items: List[Tuple[int, str]] = []
_pattern_str: str = ""
_pattern_nums: List[int] = []

_pattern_executor: Optional[ThreadPoolExecutor] = None

# Bootstrap state
_bootstrap_active: bool = True
_bootstrap_total_keys: int = 0
_bootstrap_done: int = 0
_bootstrap_started_at: float = 0.0


# ============================================================
# LOGGING
# ============================================================
def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str, *, err: bool = False):
    try:
        safe = msg
        if BOT_TOKEN:
            safe = safe.replace(BOT_TOKEN, "***TOKEN***")
        if FIREBASE_AUTH:
            safe = safe.replace(FIREBASE_AUTH, "***AUTH***")
        if FIREBASE_API_KEY:
            safe = safe.replace(FIREBASE_API_KEY, "***KEY***")
        line = f"[{_ts()}] {safe}"
        app_log.append(line)
        stream = sys.stderr if err else sys.stdout
        print(line, file=stream, flush=True)
    except Exception:
        pass


def dump_log_bytes(header: str) -> bytes:
    try:
        lines = [header, "=" * 60]
        lines.extend(app_log)
        return "\n".join(lines).encode("utf-8", errors="replace")
    except Exception:
        return b"(log dump failed)"


def mem_mb():
    if resource is None:
        return -1.0
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024
    except Exception:
        return -1.0


# ============================================================
# SAFE EVENT LOOP HELPER (Python 3.14+)
# ============================================================
def _ensure_event_loop():
    """
    Python 3.14+ removed the implicit loop creation in
    asyncio.get_event_loop(). This helper guarantees a loop exists
    in the current thread — safe across 3.8 through 3.14+.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("event loop is closed")
        return loop
    except (RuntimeError, DeprecationWarning):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


# ============================================================
# TELEGRAM HELPERS
# ============================================================
async def send_message_safe(bot, chat_id: int, text: str, *,
                            parse_mode=None,
                            document: Optional[io.BytesIO] = None,
                            filename: Optional[str] = None,
                            caption: Optional[str] = None,
                            caption_parse_mode=None):
    if document is not None:
        try:
            return await bot.send_document(
                chat_id=chat_id, document=document, filename=filename,
                caption=caption, parse_mode=caption_parse_mode)
        except RetryAfter as e:
            log(f"[send_document] RetryAfter "
                f"{getattr(e, 'retry_after', '?')}s", err=True)
            raise
        except (TimedOut, NetworkError) as e:
            log(f"[send_document] {type(e).__name__}: {e}", err=True)
            raise

    try:
        return await bot.send_message(
            chat_id=chat_id, text=text, parse_mode=parse_mode)
    except RetryAfter as e:
        log(f"[send_message] RetryAfter "
            f"{getattr(e, 'retry_after', '?')}s", err=True)
        raise
    except (TimedOut, NetworkError) as e:
        log(f"[send_message] {type(e).__name__}: {e}", err=True)
        raise


async def safe_edit(bot, chat_id: int, message_id: int, text: str) -> bool:
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id,
            text=text, parse_mode=ParseMode.HTML)
        return True
    except RetryAfter:
        raise
    except Exception as e:
        err = str(e).lower()
        if "not modified" in err:
            return True
        if ("not found" in err or "message to edit" in err
                or "message_id_invalid" in err):
            log("[edit] message gone", err=True)
            return False
        log(f"[edit] {type(e).__name__}: {e}", err=True)
        return False


async def reply_safe(update: Update, text: str, *, parse_mode=None):
    try:
        return await update.message.reply_text(text, parse_mode=parse_mode)
    except RetryAfter as e:
        log(f"[reply] RetryAfter {getattr(e, 'retry_after', '?')}s",
            err=True)
    except (TimedOut, NetworkError) as e:
        log(f"[reply] {type(e).__name__}: {e}", err=True)
    except Exception as e:
        log(f"[reply] {type(e).__name__}: {e}", err=True)
    return None


async def send_crash_report(bot, reason: str, exc_info: str,
                            extra: Optional[dict] = None):
    try:
        short_tb = exc_info[-1200:] if len(exc_info) > 1200 else exc_info
        lines = [
            "<b>⚠ BOT CRASH</b>",
            f"<b>time</b>: {_ts()}",
            f"<b>reason</b>: <code>{html.escape(reason[:300])}</code>",
            f"<b>mem</b>: {mem_mb():.0f} MB",
            f"<b>last_max</b>: {last_seen_max_block}",
        ]
        if extra:
            for k, v in extra.items():
                lines.append(
                    f"<b>{html.escape(str(k))}</b>: "
                    f"<code>{html.escape(str(v)[:200])}</code>")
        text = "\n".join(lines)
        text += f"\n\n<pre>{html.escape(short_tb)}</pre>"
        if len(text) > 4000:
            text = text[:3950] + "\n…(truncated)</pre>"
        await send_message_safe(
            bot, ADMIN_ID, text, parse_mode=ParseMode.HTML)
        log(f"crash report sent")
    except Exception as e:
        log(f"[crash] send failed: {e}", err=True)


def install_global_excepthook(holder):
    def _hook(exc_type, exc_value, exc_tb):
        try:
            tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            log(f"UNCAUGHT: {exc_type.__name__}: {exc_value}", err=True)
            log(tb, err=True)
            app = holder.get("app")
            if app is None:
                return
            try:
                # ── Python 3.14+ safe loop ──
                loop = _ensure_event_loop()
                # ─────────────────────────────
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        send_crash_report(app.bot, "uncaught exception", tb),
                        loop)
                else:
                    loop.run_until_complete(
                        send_crash_report(app.bot, "uncaught exception", tb))
            except Exception as e:
                log(f"excepthook notify failed: {e}", err=True)
        except Exception:
            pass
    sys.excepthook = _hook


async def safe_task_loop(name: str, coro_factory: Callable[[], Awaitable[None]],
                         restart_delay: float = 3.0,
                         stop_event: Optional[asyncio.Event] = None):
    while True:
        try:
            await coro_factory()
            return
        except asyncio.CancelledError:
            log(f"[{name}] cancelled")
            raise
        except Exception as e:
            tb = traceback.format_exc()
            log(f"[{name}] crashed: {type(e).__name__}: {e} — restarting "
                f"in {restart_delay:.0f}s", err=True)
            log(tb, err=True)
            if stop_event is not None and stop_event.is_set():
                return
            try:
                await asyncio.sleep(restart_delay)
            except asyncio.CancelledError:
                raise


# ============================================================
# FIREBASE CLIENT
# ============================================================
def _fb_auth_params() -> dict:
    if FIREBASE_AUTH:
        return {"auth": FIREBASE_AUTH}
    return {"key": FIREBASE_API_KEY}


def _fb_url(path: str) -> str:
    p = path.lstrip("/")
    return f"{FIREBASE_RTDB_URL}/{p}.json"


async def fb_request(method: str, path: str, *, json_body=None,
                     params: Optional[dict] = None,
                     timeout: Optional[httpx.Timeout] = None,
                     attempts: int = FIREBASE_MAX_RETRIES):
    assert http_client is not None
    q = _fb_auth_params()
    if params:
        q.update(params)
    delay = FIREBASE_BACKOFF_BASE
    last_err: Optional[Exception] = None
    if timeout is None:
        timeout = httpx.Timeout(
            connect=FIREBASE_CONNECT_TIMEOUT,
            read=FIREBASE_READ_TIMEOUT,
            write=FIREBASE_WRITE_TIMEOUT,
            pool=FIREBASE_POOL_TIMEOUT)
    for attempt in range(1, attempts + 1):
        try:
            r = await http_client.request(
                method, _fb_url(path), params=q, json=json_body, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == attempts:
                    r.raise_for_status()
                wait = min(delay, FIREBASE_BACKOFF_MAX)
                log(f"[fb] {r.status_code} attempt {attempt}/{attempts} "
                    f"wait {wait:.1f}s", err=True)
                await asyncio.sleep(wait + random.uniform(0, wait * 0.2))
                delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
                continue
            r.raise_for_status()
            if r.text and r.headers.get("content-type", "").startswith(
                    "application/json"):
                return r.json()
            return None
        except httpx.HTTPError as e:
            last_err = e
            log(f"[fb] {type(e).__name__} attempt {attempt}/{attempts}: {e}",
                err=True)
            if attempt == attempts:
                raise
            await asyncio.sleep(delay + random.uniform(0, delay * 0.2))
            delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
    if last_err:
        raise last_err
    raise RuntimeError("fb_request unreachable")


async def fb_get(path: str):
    return await fb_request("GET", path)


async def fb_put(path: str, value):
    return await fb_request("PUT", path, json_body=value)


def _ns_blocks_path() -> str:
    return f"{FIREBASE_NS}/blocks"


def _ns_users_path() -> str:
    return f"{FIREBASE_NS}/users"


async def fb_get_all_block_keys() -> List[int]:
    assert http_client is not None
    q = _fb_auth_params()
    q["shallow"] = "true"
    url = _fb_url(_ns_blocks_path())

    delay = FIREBASE_BACKOFF_BASE
    last_err: Optional[Exception] = None
    timeout = httpx.Timeout(
        connect=FIREBASE_CONNECT_TIMEOUT,
        read=BOOTSTRAP_KEYS_TIMEOUT,
        write=FIREBASE_WRITE_TIMEOUT,
        pool=FIREBASE_POOL_TIMEOUT)

    for attempt in range(1, FIREBASE_MAX_RETRIES + 1):
        try:
            r = await http_client.get(url, params=q, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == FIREBASE_MAX_RETRIES:
                    r.raise_for_status()
                wait = min(delay, FIREBASE_BACKOFF_MAX)
                log(f"[fb-keys] {r.status_code} attempt {attempt} "
                    f"wait {wait:.1f}s", err=True)
                await asyncio.sleep(wait + random.uniform(0, wait * 0.2))
                delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
                continue
            r.raise_for_status()
            data = r.json()
            keys: List[int] = []
            if isinstance(data, dict):
                for k in data.keys():
                    try:
                        keys.append(int(k))
                    except (TypeError, ValueError):
                        continue
            keys.sort()
            return keys
        except httpx.HTTPError as e:
            last_err = e
            log(f"[fb-keys] {type(e).__name__} attempt {attempt}: {e}",
                err=True)
            if attempt == FIREBASE_MAX_RETRIES:
                raise
            await asyncio.sleep(delay + random.uniform(0, delay * 0.2))
            delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
    if last_err:
        raise last_err
    return []


async def fb_load_blocks_in_range(
        start: int, end: int,
        timeout_read: Optional[float] = None) -> Dict[int, str]:
    assert http_client is not None
    q = _fb_auth_params()
    q.update({
        "orderBy": '"$key"',
        "startAt": f'"{start}"',
        "endAt": f'"{end}"',
    })
    url = _fb_url(_ns_blocks_path())
    delay = FIREBASE_BACKOFF_BASE
    last_err: Optional[Exception] = None
    timeout = httpx.Timeout(
        connect=FIREBASE_CONNECT_TIMEOUT,
        read=timeout_read or FIREBASE_READ_TIMEOUT,
        write=FIREBASE_WRITE_TIMEOUT,
        pool=FIREBASE_POOL_TIMEOUT)
    for attempt in range(1, FIREBASE_MAX_RETRIES + 1):
        try:
            r = await http_client.get(url, params=q, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == FIREBASE_MAX_RETRIES:
                    r.raise_for_status()
                wait = min(delay, FIREBASE_BACKOFF_MAX)
                log(f"[fb-live] {r.status_code} attempt {attempt} "
                    f"wait {wait:.1f}s", err=True)
                await asyncio.sleep(wait + random.uniform(0, wait * 0.2))
                delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
                continue
            r.raise_for_status()
            data = r.json()
            out: Dict[int, str] = {}
            if isinstance(data, dict):
                for k, v in data.items():
                    try:
                        n = int(k)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(v, str) and v:
                        out[n] = v
            return out
        except httpx.HTTPError as e:
            last_err = e
            log(f"[fb-live] {type(e).__name__} attempt {attempt}: {e}", err=True)
            if attempt == FIREBASE_MAX_RETRIES:
                raise
            await asyncio.sleep(delay + random.uniform(0, delay * 0.2))
            delay = min(delay * 2, FIREBASE_BACKOFF_MAX)
    if last_err:
        raise last_err
    raise RuntimeError("fb_load_blocks_in_range unreachable")


async def fb_save_user(chat_id: int, added_at: str):
    await fb_put(f"{_ns_users_path()}/{int(chat_id)}", {"added_at": added_at})


async def fb_delete_user(chat_id: int):
    await fb_request("DELETE", f"{_ns_users_path()}/{int(chat_id)}")


async def fb_load_users_with_meta() -> Dict[int, dict]:
    data = await fb_get(_ns_users_path())
    if not isinstance(data, dict):
        return {}
    out: Dict[int, dict] = {}
    for k, v in data.items():
        try:
            uid = int(k)
        except (TypeError, ValueError):
            continue
        out[uid] = v if isinstance(v, dict) else {}
    return out


# ============================================================
# SQLITE
# ============================================================
def _db_table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)).fetchone()
    return row is not None


def _db_columns(conn, table: str) -> Set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def db_init():
    global db
    db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    if not _db_table_exists(db, "blocks"):
        db.execute("""
            CREATE TABLE blocks (
                block_number INTEGER PRIMARY KEY,
                block_hash TEXT NOT NULL
            )
        """)
    else:
        cols = _db_columns(db, "blocks")
        if "block_hash" not in cols:
            db.execute("DROP TABLE blocks")
            db.execute("""
                CREATE TABLE blocks (
                    block_number INTEGER PRIMARY KEY,
                    block_hash TEXT NOT NULL
                )
            """)

    if not _db_table_exists(db, "allowed_users"):
        db.execute("""
            CREATE TABLE allowed_users (
                chat_id INTEGER PRIMARY KEY,
                added_at TEXT NOT NULL
            )
        """)
    else:
        cols = _db_columns(db, "allowed_users")
        if "added_at" not in cols:
            db.execute(
                "ALTER TABLE allowed_users ADD COLUMN added_at "
                "TEXT NOT NULL DEFAULT ''")

    if not _db_table_exists(db, "meta"):
        db.execute("""
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_blocks_number ON blocks(block_number)")
    db.commit()


def _db_retry(fn: Callable[[], T], attempts: int = 5) -> T:
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last_err = e
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.1 * attempt)
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError("db_retry unreachable")


def _db_replace_all(rows: List[Tuple[int, str]]):
    def _do():
        db.execute("DELETE FROM blocks")
        if rows:
            db.executemany(
                "INSERT INTO blocks(block_number, block_hash) VALUES (?, ?)",
                rows)
        db.commit()
    _db_retry(_do)


def _db_upsert(rows: List[Tuple[int, str]]):
    if not rows:
        return

    def _do():
        db.executemany(
            "INSERT INTO blocks(block_number, block_hash) VALUES (?, ?) "
            "ON CONFLICT(block_number) DO UPDATE SET "
            "block_hash=excluded.block_hash", rows)
        db.commit()
    _db_retry(_do)


def _db_stats():
    return _db_retry(
        lambda: db.execute(
            "SELECT COUNT(*), MIN(block_number), MAX(block_number) "
            "FROM blocks").fetchone())


def _db_latest(limit):
    return _db_retry(
        lambda: db.execute(
            "SELECT block_number, block_hash FROM blocks "
            "ORDER BY block_number DESC LIMIT ?", (limit,)).fetchall())


def _db_max():
    row = _db_retry(
        lambda: db.execute("SELECT MAX(block_number) FROM blocks").fetchone())
    return row[0] if row else None


def _db_all():
    return _db_retry(
        lambda: db.execute(
            "SELECT block_number, block_hash FROM blocks "
            "ORDER BY block_number DESC").fetchall())


def _db_count():
    return _db_retry(
        lambda: db.execute("SELECT COUNT(*) FROM blocks").fetchone()[0])


def _db_meta_get(key: str) -> Optional[str]:
    try:
        row = _db_retry(
            lambda: db.execute(
                "SELECT value FROM meta WHERE key=?", (key,)).fetchone())
        return row[0] if row else None
    except Exception:
        return None


def _db_meta_set(key: str, value: str):
    def _do():
        db.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            (key, str(value)))
        db.commit()
    _db_retry(_do)


def _db_load_users():
    return _db_retry(
        lambda: [r[0] for r in db.execute(
            "SELECT chat_id FROM allowed_users").fetchall()])


def _db_add_user(chat_id, added_at: Optional[str] = None):
    def _do():
        db.execute(
            "INSERT INTO allowed_users(chat_id, added_at) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET added_at=excluded.added_at",
            (chat_id, added_at or _ts()))
        db.commit()
    _db_retry(_do)


def _db_remove_user(chat_id):
    def _do():
        db.execute("DELETE FROM allowed_users WHERE chat_id = ?", (chat_id,))
        db.commit()
    _db_retry(_do)


def _db_list_users():
    return _db_retry(
        lambda: db.execute(
            "SELECT chat_id, added_at FROM allowed_users "
            "ORDER BY added_at").fetchall())


async def get_history_stats():
    async with db_lock:
        return await asyncio.to_thread(_db_stats)


# ============================================================
# USER LOADING
# ============================================================
async def load_users():
    global allowed_users

    fb_meta: Dict[int, dict] = {}
    try:
        fb_meta = await fb_load_users_with_meta()
        log(f"[fb] users loaded from Firebase: {sorted(fb_meta.keys())}")
    except Exception as e:
        log(f"[fb] load users failed: {type(e).__name__}: {e}", err=True)

    try:
        async with db_lock:
            sq_rows = await asyncio.to_thread(_db_list_users)
        sq_users = {uid for uid, _ in sq_rows}
    except Exception as e:
        log(f"[users] sqlite load failed: {e}", err=True)
        sq_users = set()

    merged = set(fb_meta.keys()) | sq_users

    for uid in merged - set(fb_meta.keys()):
        try:
            await fb_save_user(uid, _ts())
        except Exception as e:
            log(f"[fb] user sync {uid}: {e}", err=True)

    for uid in merged - sq_users:
        added_at = ""
        if uid in fb_meta and isinstance(fb_meta[uid], dict):
            added_at = str(fb_meta[uid].get("added_at") or "")
        try:
            async with db_lock:
                await asyncio.to_thread(_db_add_user, uid, added_at or _ts())
        except Exception as e:
            log(f"[users] mirror {uid} failed: {e}", err=True)

    allowed_users = merged
    log(f"[users] total {len(allowed_users)}: {sorted(allowed_users)}")


async def add_user_db(chat_id):
    allowed_users.add(chat_id)
    try:
        async with db_lock:
            await asyncio.to_thread(_db_add_user, chat_id)
    except Exception as e:
        log(f"[users] sqlite add failed: {e}", err=True)
    try:
        await fb_save_user(chat_id, _ts())
    except Exception as e:
        log(f"[fb] add user {chat_id}: {e}", err=True)


async def remove_user_db(chat_id):
    allowed_users.discard(chat_id)
    try:
        async with db_lock:
            await asyncio.to_thread(_db_remove_user, chat_id)
    except Exception as e:
        log(f"[users] sqlite remove failed: {e}", err=True)
    try:
        await fb_delete_user(chat_id)
    except Exception as e:
        log(f"[fb] remove user {chat_id}: {e}", err=True)


# ============================================================
# UTILITIES
# ============================================================
def short_hash(h):
    return h[-10:] if h else "-"


def last_digit(h):
    if not h:
        return None
    for ch in reversed(h):
        if ch.isdigit():
            return ch
    return None


def digit_class(h):
    d = last_digit(h)
    if d is None:
        return "-"
    return "S" if int(d) <= 4 else "B"


def _push_live_event(text: str):
    live_events.append(f"{time.strftime('%H:%M:%S')} {text}")


# ============================================================
# PATTERN CACHE
# ============================================================
def _rebuild_pattern_cache_sync():
    global _pattern_items, _pattern_str, _pattern_nums, _pattern_cache_valid
    items = _db_all()
    seq = [digit_class(h) for _, h in items]
    nums = [n for n, _ in items]
    _pattern_items = items
    _pattern_str = "".join(seq)
    _pattern_nums = nums
    _pattern_cache_valid = True
    return len(items)


def _prepend_to_pattern_cache(truly_new_rows: List[Tuple[int, str]]):
    global _pattern_items, _pattern_str, _pattern_nums
    if not _pattern_cache_valid or not truly_new_rows:
        return
    new_sorted = sorted(truly_new_rows, reverse=True)
    new_seq = [digit_class(h) for _, h in new_sorted]
    new_nums = [n for n, _ in new_sorted]
    _pattern_items = new_sorted + _pattern_items
    _pattern_str = "".join(new_seq) + _pattern_str
    _pattern_nums = new_nums + _pattern_nums


async def _ensure_pattern_cache() -> Tuple[List[Tuple[int, str]], str, List[int]]:
    if _bootstrap_active:
        async with db_lock:
            items = await asyncio.to_thread(_db_all)
        seq = [digit_class(h) for _, h in items]
        nums = [n for n, _ in items]
        return items, "".join(seq), nums

    if _pattern_cache_valid:
        return _pattern_items, _pattern_str, _pattern_nums

    async with db_lock:
        if not _pattern_cache_valid:
            n = await asyncio.to_thread(_rebuild_pattern_cache_sync)
            log(f"[pattern] cache rebuilt ({n:,} blocks, "
                f"str_len={len(_pattern_str):,})")
        return _pattern_items, _pattern_str, _pattern_nums


# ============================================================
# PATTERN SEARCH
# ============================================================
def _find_pattern_with_data(
        pattern: str,
        s: str,
        nums: List[int],
        items: List[Tuple[int, str]],
) -> Tuple[int, List[Tuple[int, str, str, str]], int, int]:
    pattern = pattern.upper()
    p_len = len(pattern)
    if p_len == 0 or p_len > len(s):
        return 0, [], 0, 0

    match_count = 0
    big = 0
    small = 0
    above_rows: List[Tuple[int, str, str, str]] = []

    pos = 0
    while True:
        idx = s.find(pattern, pos)
        if idx < 0:
            break
        if nums[idx] - nums[idx + p_len - 1] == p_len - 1:
            match_count += 1
            above = idx - 1
            if above >= 0 and items[above][0] == items[idx][0] + 1:
                cls = s[above]
                if cls == "B":
                    big += 1
                elif cls == "S":
                    small += 1
                if len(above_rows) < ABOVE_DISPLAY_LIMIT:
                    num, h = items[above]
                    above_rows.append((num, h, last_digit(h) or "-", cls))
        pos = idx + 1

    return match_count, above_rows, big, small


def _fuzzy_find_with_data(
        requested: str,
        items: List[Tuple[int, str]],
        s: str,
        nums: List[int],
) -> Tuple[
    Optional[str], int, List[Tuple[int, str, str, str]], int, int, List[str]
]:
    tried: List[str] = []
    cur = requested.upper()
    if len(cur) > PATTERN_MAX_LEN:
        cur = cur[:PATTERN_MAX_LEN]
        tried.append(f"(truncated to {PATTERN_MAX_LEN})")
    while len(cur) >= PATTERN_MIN_LEN:
        tried.append(cur)
        total, above_rows, big, small = _find_pattern_with_data(
            cur, s, nums, items)
        if total > 0:
            return cur, total, above_rows, big, small, tried
        if len(cur) == PATTERN_MIN_LEN:
            break
        cur = cur[:-1]
    return None, 0, [], 0, 0, tried


async def format_pattern_result(
        requested: str, user_id: Optional[int] = None) -> str:
    requested = requested.upper()
    tag = f"user={user_id}" if user_id is not None else "user=?"
    t_start = time.time()

    items, s, nums = await _ensure_pattern_cache()

    if _pattern_executor is None:
        return "<b>SEARCH ENGINE NOT READY</b>\nTry again in a moment."

    loop = asyncio.get_running_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                _pattern_executor,
                _fuzzy_find_with_data,
                requested, items, s, nums),
            timeout=PATTERN_SEARCH_TIMEOUT)
    except asyncio.TimeoutError:
        log(f"[pattern] timeout {tag} pattern={requested}", err=True)
        return ("<b>SEARCH STILL RUNNING</b>\n"
                "The search took too long. Try a shorter pattern.")
    except Exception as e:
        log(f"[pattern] executor error {tag}: "
            f"{type(e).__name__}: {e}", err=True)
        return "<b>SEARCH INTERRUPTED</b>\nPlease try again."

    matched, total, above_rows, big, small, tried = result
    elapsed_ms = (time.time() - t_start) * 1000.0
    log(f"[pattern] {tag} pattern={requested} matched={matched} "
        f"total={total} ({elapsed_ms:.0f}ms)")

    bootstrap_warning = ""
    if _bootstrap_active:
        bootstrap_warning = (
            f"\n\n⚠️ <b>PARTIAL DATA</b> — "
            f"{_bootstrap_done:,}/{_bootstrap_total_keys:,} blocks loaded. "
            f"Full results after bootstrap completes.")

    if matched is None:
        try:
            total_blocks = (await get_history_stats())[0]
        except Exception:
            total_blocks = 0
        tried_txt = " -> ".join(tried)
        return (f"<b>PATTERN:</b> {html.escape(requested)}\n"
                f"<b>RESULT:</b> NOT FOUND\n"
                f"<b>TRIED:</b> {html.escape(tried_txt)}\n"
                f"<b>STORED BLOCKS:</b> {total_blocks}"
                f"{bootstrap_warning}")

    SEP = "-" * 37

    if matched == requested:
        first_line = f"(PATTERN: {matched})"
    else:
        first_line = f"(MATCHED: {matched})"

    head_lines = [
        first_line,
        SEP,
        f"MATCHES: {total}",
        SEP,
        f"BIG : {big} times",
        SEP,
        f"SMALL : {small} times",
        SEP,
    ]

    table_lines = [
        "",
        f"{'BLOCK':>10}  |  {'LAST10':<10} | {'N':^1} | {'S/B':^3}",
        SEP,
    ]
    for num, h, nd, cls in above_rows:
        table_lines.append(
            f"{num:>10} | {short_hash(h):<10} | {nd:^1} |  {cls} ")
    if total > len(above_rows):
        table_lines.append(
            f"... and {total - len(above_rows)} more (counts include all)")

    out = "<pre>"
    out += "\n".join(html.escape(x) for x in head_lines)
    out += "\n" + "\n".join(html.escape(x) for x in table_lines)
    out += "</pre>"
    return out + bootstrap_warning


# ============================================================
# BOOTSTRAP — PARALLEL + RESUMABLE
# ============================================================
async def bootstrap_history():
    global history_cache, history_cache_loaded, last_seen_max_block
    global _bootstrap_active, _bootstrap_total_keys, _bootstrap_done
    global _bootstrap_started_at

    _bootstrap_started_at = time.time()

    # ── Check completion marker ──────────────────────────
    try:
        async with db_lock:
            complete = await asyncio.to_thread(
                _db_meta_get, "bootstrap_complete")
    except Exception:
        complete = None

    # ── FAST PATH — already bootstrapped ─────────────────
    if complete == "1":
        _bootstrap_active = False
        t0 = time.time()
        try:
            async with db_lock:
                sq_rows = await asyncio.to_thread(_db_all)
        except Exception as e:
            log(f"[history] SQLite read failed: {e}", err=True)
            sq_rows = []

        if sq_rows:
            history_cache = {n: h for n, h in sq_rows}
            local_max = max(history_cache.keys())
            last_seen_max_block = local_max
            history_cache_loaded = True
            log(f"[history] FAST: SQLite {len(sq_rows):,} blocks, "
                f"max={local_max} ({time.time()-t0:.1f}s)")

            try:
                new_blocks = await fb_load_blocks_in_range(
                    local_max + 1, local_max + 1000000)
                if new_blocks:
                    async with db_lock:
                        await asyncio.to_thread(
                            _db_upsert, sorted(new_blocks.items()))
                    history_cache.update(new_blocks)
                    last_seen_max_block = max(new_blocks.keys())
                    log(f"[history] caught up {len(new_blocks)} blocks "
                        f"in {time.time()-t0:.1f}s")
            except Exception as e:
                log(f"[history] catch-up failed (will catch via live): "
                    f"{e}", err=True)
        else:
            log("[history] marked complete but SQLite empty — "
                "resetting bootstrap flag", err=True)
            async with db_lock:
                await asyncio.to_thread(
                    _db_meta_set, "bootstrap_complete", "0")
            complete = None

        if complete == "1":
            return

    # ── SLOW PATH — full bootstrap ───────────────────────
    _bootstrap_active = True
    log("[history] ═══ FULL BOOTSTRAP START ═══")

    # STEP 1: Get all keys via shallow fetch
    t0 = time.time()
    try:
        keys = await fb_get_all_block_keys()
    except Exception as e:
        log(f"[history] key fetch failed: {type(e).__name__}: {e}",
            err=True)
        _bootstrap_active = False
        return

    if not keys:
        log("[history] Firebase is empty")
        async with db_lock:
            await asyncio.to_thread(
                _db_meta_set, "bootstrap_complete", "1")
        _bootstrap_active = False
        history_cache = {}
        last_seen_max_block = None
        history_cache_loaded = True
        return

    _bootstrap_total_keys = len(keys)
    log(f"[history] got {len(keys):,} keys in {time.time()-t0:.1f}s")

    # STEP 2: Load existing SQLite (for resume)
    async with db_lock:
        sq_rows = await asyncio.to_thread(_db_all)
    existing = {n: h for n, h in sq_rows}

    # STEP 3: Find missing keys
    missing = [k for k in keys if k not in existing]
    _bootstrap_done = len(keys) - len(missing)

    if not missing:
        log(f"[history] all {len(keys):,} blocks already in SQLite")
    else:
        log(f"[history] {len(missing):,} blocks to fetch, "
            f"{len(existing):,} already present")

    # STEP 4: Parallel chunk fetch
    if missing:
        sem = asyncio.Semaphore(BOOTSTRAP_PARALLEL)
        chunks = [
            missing[i:i + BOOTSTRAP_BATCH_SIZE]
            for i in range(0, len(missing), BOOTSTRAP_BATCH_SIZE)
        ]
        total_chunks = len(chunks)
        completed = 0
        total_fetched = 0
        failed = 0

        async def fetch_one(chunk_idx, chunk_keys):
            # nonlocal for locals, global for module vars
            nonlocal completed, total_fetched, failed
            global _bootstrap_done
            start = chunk_keys[0]
            end = chunk_keys[-1]
            async with sem:
                try:
                    batch = await fb_load_blocks_in_range(
                        start, end,
                        timeout_read=BOOTSTRAP_BATCH_TIMEOUT)
                    if batch:
                        async with db_lock:
                            await asyncio.to_thread(
                                _db_upsert, sorted(batch.items()))
                        total_fetched += len(batch)
                        _bootstrap_done += len(batch)
                    completed += 1
                    if completed % 5 == 0 or completed == total_chunks:
                        pct = 100 * completed / total_chunks
                        log(f"[history] {completed}/{total_chunks} chunks "
                            f"({pct:.0f}%) — {total_fetched:,} fetched, "
                            f"{time.time()-t0:.1f}s")
                    return len(batch)
                except Exception as e:
                    failed += 1
                    completed += 1
                    log(f"[history] chunk [{start}..{end}] FAILED: "
                        f"{type(e).__name__}: {e}", err=True)
                    return 0

        tasks = [fetch_one(i, c) for i, c in enumerate(chunks)]
        await asyncio.gather(*tasks, return_exceptions=True)

        log(f"[history] fetch complete: {total_fetched:,} blocks "
            f"in {time.time()-t0:.1f}s ({failed} failed chunks)")

    # STEP 5: Load full cache from SQLite
    async with db_lock:
        final_rows = await asyncio.to_thread(_db_all)
    history_cache = {n: h for n, h in final_rows}
    if history_cache:
        last_seen_max_block = max(history_cache.keys())
    history_cache_loaded = True

    log(f"[history] total in SQLite: {len(history_cache):,} blocks, "
        f"max={last_seen_max_block}")

    # STEP 6: Mark complete + build cache
    async with db_lock:
        await asyncio.to_thread(_db_meta_set, "bootstrap_complete", "1")
    _bootstrap_active = False

    try:
        async with db_lock:
            n = await asyncio.to_thread(_rebuild_pattern_cache_sync)
        log(f"[pattern] cache built ({n:,} blocks, "
            f"str_len={len(_pattern_str):,})")
    except Exception as e:
        log(f"[pattern] cache build failed: {e}", err=True)

    elapsed = time.time() - t0
    log(f"[history] ═══ BOOTSTRAP COMPLETE ═══ "
        f"{len(history_cache):,} blocks in {elapsed:.1f}s")
    log(f"[history] next restarts will be FAST (~0.5s)")


# ============================================================
# LIVE POLLING
# ============================================================
async def live_poll_once() -> int:
    global last_seen_max_block, last_new_block_time, live_new_block_count

    if _bootstrap_active:
        return 0

    try:
        async with db_lock:
            db_max = await asyncio.to_thread(_db_max)
    except Exception as e:
        log(f"[live] db_max failed: {e}", err=True)
        db_max = None
    base = db_max if db_max is not None else 0
    if last_seen_max_block is not None and last_seen_max_block > base:
        base = last_seen_max_block

    start = max(0, base - LIVE_OVERLAP_BLOCKS)
    upper = base + 100000

    try:
        data = await fb_load_blocks_in_range(start, upper)
    except Exception as e:
        log(f"[live] fb range failed [{start}..{upper}]: "
            f"{type(e).__name__}: {e}", err=True)
        raise

    if not data:
        return 0

    new_max = max(data.keys())
    rows = sorted(data.items())
    try:
        async with db_lock:
            await asyncio.to_thread(_db_upsert, rows)
    except Exception as e:
        log(f"[live] db upsert failed: {e}", err=True)
    for n, h in rows:
        history_cache[n] = h

    old_max = last_seen_max_block or 0
    if new_max > old_max:
        delta = new_max - old_max
        last_seen_max_block = new_max
        last_new_block_time = time.time()
        live_new_block_count += delta

        truly_new = [(n, h) for n, h in rows if n > old_max]
        if truly_new:
            _prepend_to_pattern_cache(truly_new)

        _push_live_event(f"+{delta} blocks → max {new_max}")
        log(f"[live] {delta} new blocks, max now {new_max}")
        return delta
    return 0


async def live_loop():
    log(f"[live] starting (interval={LIVE_POLL_INTERVAL}s)")
    fails = 0
    while not live_stop.is_set():
        try:
            n = await live_poll_once()
            fails = 0
            if n == 0:
                await asyncio.sleep(LIVE_POLL_INTERVAL)
            else:
                await asyncio.sleep(max(1.0, LIVE_POLL_INTERVAL / 2))
        except asyncio.CancelledError:
            log("[live] cancelled")
            raise
        except Exception as e:
            fails += 1
            wait = min(LIVE_RETRY_BACKOFF_BASE * (2 ** (fails - 1)),
                       LIVE_RETRY_BACKOFF_MAX)
            log(f"[live] error #{fails}: {type(e).__name__}: {e} — "
                f"retry in {wait:.0f}s", err=True)
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                raise
    log("[live] stopped")


# ============================================================
# LIVE STATUS
# ============================================================
def _build_status_parts(total, oldest, newest, big_cnt, small_cnt,
                        latest_rows, alive) -> str:
    range_txt = f"{oldest} .. {newest}" if total else "empty"
    last_ago = ("-" if last_new_block_time == 0
                else f"{time.time() - last_new_block_time:.0f}s ago")

    SEP = "-" * 37
    hist_lines = [
        f"HISTORY (last {len(latest_rows)} blocks)",
        SEP,
        f"{'BLOCK':>10}  |  {'LAST10':<10} | {'N':^1} | {'S/B':^3}",
        SEP,
    ]
    if latest_rows:
        for num, h in latest_rows:
            nd = last_digit(h) or "-"
            hist_lines.append(
                f"{num:>10} | {short_hash(h):<10} | {nd:^1} |  "
                f"{digit_class(h)} ")
    else:
        hist_lines.append("(no blocks)")
    hist_lines += [
        SEP,
        f"Ton Txt History = {total}",
    ]

    parts = [
        "<b>BOT ONLINE</b>",
        "<pre>",
        f"Firebase history : {total:,} blocks",
        f"Range            : {range_txt}",
        f"BIG total        : {big_cnt:,}",
        f"SMALL total      : {small_cnt:,}",
        f"Users            : {len(allowed_users)}",
        f"Memory           : {mem_mb():.0f} MB",
        f"FB_NS            : {FIREBASE_NS}",
        "</pre>",
    ]

    if _bootstrap_active:
        elapsed = time.time() - _bootstrap_started_at
        pct = (100 * _bootstrap_done / _bootstrap_total_keys
               if _bootstrap_total_keys else 0)
        parts.append(
            f"⏳ <b>BOOTSTRAP IN PROGRESS</b>\n"
            f"Progress: {_bootstrap_done:,}/{_bootstrap_total_keys:,} "
            f"({pct:.0f}%)\n"
            f"Elapsed: {elapsed:.0f}s\n"
            f"<i>Users can search partial data — full results on completion</i>")
    else:
        parts.append(
            f"Live polling: {'ON' if alive else 'OFF'} "
            f"(interval={LIVE_POLL_INTERVAL:.0f}s)")
        parts.append(f"Last new block: {last_ago}")
        parts.append(f"New since start: {live_new_block_count:,}")

    parts.append("")
    parts.append("<pre>" + "\n".join(hist_lines) + "</pre>")
    return "\n".join(parts)


async def format_live_status() -> str:
    try:
        total, oldest, newest = await get_history_stats()
    except Exception:
        total, oldest, newest = 0, None, None
    big_cnt = 0
    small_cnt = 0
    for h in history_cache.values():
        c = digit_class(h)
        if c == "B":
            big_cnt += 1
        elif c == "S":
            small_cnt += 1
    alive = live_task is not None and not live_task.done()

    try:
        async with db_lock:
            latest_rows = await asyncio.to_thread(
                _db_latest, LIVE_STATUS_TABLE_ROWS)
    except Exception:
        latest_rows = []

    return _build_status_parts(
        total, oldest, newest, big_cnt, small_cnt, latest_rows, alive)


async def live_status_editor():
    global live_status_message_id, live_status_last_edit
    global live_status_last_error

    log(f"[live-status] editor started "
        f"(edit interval={LIVE_STATUS_EDIT_INTERVAL:.0f}s)")

    cooldown_until = 0.0

    while not live_stop.is_set():
        try:
            await asyncio.sleep(LIVE_STATUS_EDIT_TICK)
            if live_status_chat_id is None:
                continue
            if live_status_message_id is None:
                continue
            now = time.time()
            if now < cooldown_until:
                continue
            if _bootstrap_active:
                if now - live_status_last_edit < 5.0:
                    continue
            else:
                if now - live_status_last_edit < LIVE_STATUS_EDIT_INTERVAL:
                    continue

            app = app_ref_holder.get("app")
            if app is None:
                continue

            try:
                text = await format_live_status()
            except Exception as e:
                log(f"[live-status] format failed: {e}", err=True)
                continue

            try:
                ok = await safe_edit(
                    app.bot, live_status_chat_id,
                    live_status_message_id, text)
                if ok:
                    live_status_last_edit = time.time()
                    live_status_last_error = ""
                else:
                    live_status_last_error = "message gone (not resending)"
                    log(f"[live-status] {live_status_last_error}", err=True)
                    live_status_last_edit = time.time()
            except RetryAfter as e:
                wait = float(getattr(e, "retry_after", 30.0))
                cooldown_until = time.time() + wait + 1.0
                live_status_last_error = f"RetryAfter {wait:.0f}s"
                log(f"[live-status] cooldown {wait:.0f}s", err=True)
            except Exception as e:
                live_status_last_error = f"{type(e).__name__}: {e}"
                log(f"[live-status] edit failed: {live_status_last_error}",
                    err=True)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            live_status_last_error = f"{type(e).__name__}: {e}"
            log(f"[live-status] editor error: {live_status_last_error}",
                err=True)
            await asyncio.sleep(3.0)

    log("[live-status] editor stopped")


# ============================================================
# /status
# ============================================================
async def format_status():
    try:
        total, oldest, newest = await get_history_stats()
    except Exception:
        total, oldest, newest = 0, None, None
    big_cnt = 0
    small_cnt = 0
    for h in history_cache.values():
        c = digit_class(h)
        if c == "B":
            big_cnt += 1
        elif c == "S":
            small_cnt += 1
    alive = live_task is not None and not live_task.done()
    last_ago = ("-" if last_new_block_time == 0
                else f"{time.time() - last_new_block_time:.0f}s ago")

    try:
        qsize = (_pattern_executor._work_queue.qsize()
                 if _pattern_executor else 0)
    except Exception:
        qsize = 0

    head = [
        "BOT STATUS",
        "=" * 30,
        f"HISTORY TOTAL : {total:,}",
        f"BIG total     : {big_cnt:,}",
        f"SMALL total   : {small_cnt:,}",
    ]
    if total:
        head.append(f"RANGE         : {oldest} .. {newest}")
    head += [
        f"MEM           : {mem_mb():.0f} MB",
        f"USERS         : {len(allowed_users)}",
        f"FB_NS         : {FIREBASE_NS[:12]}...",
        "=" * 30,
        f"BOOTSTRAP     : {'ACTIVE' if _bootstrap_active else 'COMPLETE'}",
    ]
    if _bootstrap_active and _bootstrap_total_keys:
        pct = 100 * _bootstrap_done / _bootstrap_total_keys
        head.append(
            f"BOOT PROGRESS : {_bootstrap_done:,}/{_bootstrap_total_keys:,} "
            f"({pct:.0f}%)")
    head += [
        f"LIVE          : {'ON' if alive else 'OFF'}",
        f"LAST MAX      : {last_seen_max_block}",
        f"LAST NEW      : {last_ago}",
        f"NEW SINCE START: {live_new_block_count:,}",
        f"POLL INTERVAL : {LIVE_POLL_INTERVAL:.0f}s",
        f"STATUS MSG ID : {live_status_message_id}",
        f"PATTERN CACHE : {'valid' if _pattern_cache_valid else 'stale'}",
        f"CACHE SIZE    : {len(_pattern_items):,}",
        f"PATTERN WORKERS: {PATTERN_WORKERS}",
        f"QUEUE SIZE    : {qsize}",
    ]
    lse = list(live_events)[-5:][::-1]
    if lse:
        head.append("=" * 30)
        head.append("RECENT EVENTS:")
        head.extend(lse)

    return "<pre>" + html.escape("\n".join(head)) + "</pre>"


def _format_history_sync(limit: int, total_blocks: int) -> str:
    rows = _db_latest(limit)

    SEP = "-" * 37
    lines = [
        f"HISTORY (last {len(rows)} blocks)",
        SEP,
        f"{'BLOCK':>10}  |  {'LAST10':<10} | {'N':^1} | {'S/B':^3}",
        SEP,
    ]
    if rows:
        for num, h in rows:
            nd = last_digit(h) or "-"
            lines.append(
                f"{num:>10} | {short_hash(h):<10} | {nd:^1} |  "
                f"{digit_class(h)} ")
    else:
        lines.append("(no blocks)")
    lines += [
        SEP,
        f"Ton Txt History = {total_blocks}",
    ]
    return "\n".join(lines)


# ============================================================
# ACCESS
# ============================================================
def is_admin(chat_id):
    return str(chat_id) == str(ADMIN_ID)


def is_allowed(chat_id):
    return is_admin(chat_id) or chat_id in allowed_users


def check_rate_limit(chat_id):
    now = time.time()
    last = user_last_query.get(chat_id, 0.0)
    if now - last < USER_QUERY_COOLDOWN:
        return False
    user_last_query[chat_id] = now
    user_query_counts[chat_id] = user_query_counts.get(chat_id, 0) + 1
    return True


# ============================================================
# HANDLERS
# ============================================================
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_allowed(chat_id):
            await reply_safe(update,
                "ACCESS DENIED\n"
                f"Your chat_id: {chat_id}\n"
                "Send this to the admin to request access.")
            return
        if is_admin(chat_id):
            await reply_safe(update,
                "ADMIN COMMANDS\n"
                "/User <chat_id> - authorize a user\n"
                "/Revoke <chat_id> - revoke a user (including ADMIN)\n"
                "/Users - list authorized users\n"
                "/status - bot state + bootstrap progress\n"
                "/history [N] - show last N blocks (default 30, max 500)\n"
                "/live - live polling info\n"
                "/mem - memory usage\n"
                "/log - get full log as .txt\n"
                "/testcrash - simulate crash\n"
                "\nUSER COMMAND\n"
                "Send a B/S pattern (e.g. BBS) to search history")
        else:
            await reply_safe(update,
                "You are authorized. Send a B/S pattern (e.g. BBS) "
                "to search history.")
    except Exception as e:
        log(f"[start] {type(e).__name__}: {e}", err=True)


async def user_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        if not ctx.args or not ctx.args[0].lstrip("-").isdigit():
            await reply_safe(update, "USAGE: /User <chat_id>")
            return
        uid = int(ctx.args[0])
        await add_user_db(uid)
        await reply_safe(update, f"USER {uid} AUTHORIZED (persistent)")
        try:
            await send_message_safe(
                ctx.bot, uid,
                "YOU ARE AUTHORIZED. Send a B/S pattern (e.g. BBS) "
                "to search history.")
        except Exception as e:
            log(f"[user] notify {uid}: {e}", err=True)
    except Exception as e:
        log(f"[user] {type(e).__name__}: {e}", err=True)


async def revoke_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        if not ctx.args or not ctx.args[0].lstrip("-").isdigit():
            await reply_safe(update, "USAGE: /Revoke <chat_id>")
            return
        uid = int(ctx.args[0])
        await remove_user_db(uid)
        if uid == ADMIN_ID:
            await reply_safe(update,
                f"USER {uid} REVOKED (ADMIN is still protected)")
        else:
            await reply_safe(update, f"USER {uid} REVOKED")
    except Exception as e:
        log(f"[revoke] {type(e).__name__}: {e}", err=True)


async def users_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        await load_users()
        users = sorted(allowed_users)
        if not users:
            await reply_safe(update, "NO AUTHORIZED USERS")
            return
        try:
            async with db_lock:
                rows = await asyncio.to_thread(_db_list_users)
        except Exception:
            rows = []
        timestamps = {uid: added for uid, added in rows}
        lines = [
            f"AUTHORIZED USERS ({len(users)})",
            "=" * 30,
        ]
        for uid in users:
            added = timestamps.get(uid) or "(from Firebase)"
            marker = "  [ADMIN]" if is_admin(uid) else ""
            lines.append(f"{uid}  {added}{marker}")
        await reply_safe(
            update,
            "<pre>" + html.escape("\n".join(lines)) + "</pre>",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        log(f"[users] {type(e).__name__}: {e}", err=True)


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_allowed(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        await reply_safe(
            update, await format_status(), parse_mode=ParseMode.HTML)
    except Exception as e:
        log(f"[status] {type(e).__name__}: {e}", err=True)


async def live_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        alive = live_task is not None and not live_task.done()
        last_ago = ("-" if last_new_block_time == 0
                    else f"{time.time() - last_new_block_time:.0f}s ago")
        now = time.time()
        next_in = max(0.0, LIVE_STATUS_EDIT_INTERVAL
                      - (now - live_status_last_edit))
        ev = list(live_events)[-10:][::-1]
        try:
            qsize = (_pattern_executor._work_queue.qsize()
                     if _pattern_executor else 0)
        except Exception:
            qsize = 0
        lines = [
            "LIVE POLLING",
            "=" * 30,
            f"STATUS         : {'ON' if alive else 'OFF'}",
            f"BOOTSTRAP      : "
            f"{'ACTIVE' if _bootstrap_active else 'COMPLETE'}",
        ]
        if _bootstrap_active and _bootstrap_total_keys:
            pct = 100 * _bootstrap_done / _bootstrap_total_keys
            lines.append(
                f"BOOT PROGRESS  : {_bootstrap_done:,}/"
                f"{_bootstrap_total_keys:,} ({pct:.0f}%)")
        lines += [
            f"POLL INTERVAL  : {LIVE_POLL_INTERVAL:.0f}s",
            f"OVERLAP        : {LIVE_OVERLAP_BLOCKS} blocks",
            f"LAST SEEN MAX  : {last_seen_max_block}",
            f"LAST NEW BLOCK : {last_ago}",
            f"NEW SINCE START: {live_new_block_count:,}",
            "=" * 30,
            f"STATUS MSG ID  : {live_status_message_id}",
            f"EDIT INTERVAL  : {LIVE_STATUS_EDIT_INTERVAL:.0f}s",
            f"NEXT EDIT IN   : {next_in:.0f}s",
            f"PATTERN CACHE  : "
            f"{'valid' if _pattern_cache_valid else 'stale'}",
            f"CACHE SIZE     : {len(_pattern_items):,}",
            f"PATTERN WORKERS: {PATTERN_WORKERS}",
            f"QUEUE SIZE     : {qsize}",
        ]
        if live_status_last_error:
            lines.append(f"LAST ERR       : {live_status_last_error[:120]}")
        if ev:
            lines.append("=" * 30)
            lines.append("RECENT EVENTS:")
            lines.extend(ev)
        await reply_safe(
            update,
            "<pre>" + html.escape("\n".join(lines)) + "</pre>",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        log(f"[live] {type(e).__name__}: {e}", err=True)


async def history_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_allowed(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        if not check_rate_limit(chat_id):
            await reply_safe(update, "SLOW DOWN. Wait a few seconds.")
            return

        limit = 30
        if ctx.args:
            try:
                limit = int(ctx.args[0])
            except ValueError:
                await reply_safe(
                    update, "USAGE: /history [N]  (default 30, max 500)")
                return
        limit = max(1, min(limit, 500))

        try:
            total, _, _ = await get_history_stats()
        except Exception:
            total = 0

        try:
            async with db_lock:
                text = await asyncio.to_thread(
                    _format_history_sync, limit, total)
        except Exception as e:
            log(f"[history] format failed: {e}", err=True)
            await reply_safe(update, "HISTORY QUERY FAILED, TRY AGAIN.")
            return

        if limit <= 50 and len(text) < 3500:
            await reply_safe(
                update,
                f"<pre>{html.escape(text)}</pre>",
                parse_mode=ParseMode.HTML)
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            payload = text.encode("utf-8")
            caption = (f"<b>HISTORY</b> — last {limit} blocks\n"
                       f"<pre>Ton Txt History = {total}</pre>")
            try:
                await send_message_safe(
                    ctx.bot, chat_id, "",
                    document=io.BytesIO(payload),
                    filename=f"history_last_{limit}_{ts}.txt",
                    caption=caption,
                    caption_parse_mode=ParseMode.HTML)
            except RetryAfter as e:
                log(f"[history] RetryAfter "
                    f"{getattr(e, 'retry_after', '?')}s", err=True)
    except Exception as e:
        log(f"[history] {type(e).__name__}: {e}", err=True)


async def mem_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        gc.collect()
        try:
            qsize = (_pattern_executor._work_queue.qsize()
                     if _pattern_executor else 0)
        except Exception:
            qsize = 0
        try:
            async with db_lock:
                boot_done = (await asyncio.to_thread(
                    _db_meta_get, "bootstrap_complete") == "1")
        except Exception:
            boot_done = False
        await reply_safe(update,
            f"MEM: {mem_mb():.0f}MB\n"
            f"TASKS: {len(asyncio.all_tasks())}\n"
            f"FB_NS: {FIREBASE_NS}\n"
            f"CACHE: {len(history_cache):,}\n"
            f"USERS: {len(allowed_users)}\n"
            f"LIVE: {'ON' if (live_task and not live_task.done()) else 'OFF'}\n"
            f"LAST MAX: {last_seen_max_block}\n"
            f"STATUS MSG ID: {live_status_message_id}\n"
            f"PATTERN CACHE: "
            f"{'valid' if _pattern_cache_valid else 'stale'}\n"
            f"CACHE SIZE: {len(_pattern_items):,}\n"
            f"BOOTSTRAP ACTIVE: {_bootstrap_active}\n"
            f"BOOTSTRAP DONE: {boot_done}\n"
            f"BOOT PROGRESS: {_bootstrap_done:,}/"
            f"{_bootstrap_total_keys:,}\n"
            f"PATTERN WORKERS: {PATTERN_WORKERS}\n"
            f"PATTERN QUEUE: {qsize}\n"
            f"TRACKED USERS: {len(user_last_query)}\n"
            f"LOG LINES: {len(app_log)}")
    except Exception as e:
        log(f"[mem] {type(e).__name__}: {e}", err=True)


async def log_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        payload = dump_log_bytes(
            f"BOT LOG — {_ts()}\nmem={mem_mb():.1f}MB fb_ns={FIREBASE_NS} "
            f"last_max={last_seen_max_block}")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        await send_message_safe(
            ctx.bot, ADMIN_ID, "",
            document=io.BytesIO(payload),
            filename=f"bot_log_{ts}.txt",
            caption=f"<b>BOT LOG</b>\nlines: {len(app_log)}\n"
                    f"mem: {mem_mb():.0f}MB",
            caption_parse_mode=ParseMode.HTML)
    except Exception as e:
        log(f"[log] {type(e).__name__}: {e}", err=True)
        try:
            await reply_safe(update, f"FAILED: {html.escape(str(e))}")
        except Exception:
            pass


async def testcrash_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_admin(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return
        await reply_safe(update, "Simulating handler crash...")
        raise RuntimeError("testcrash: intentional exception")
    except RuntimeError:
        raise
    except Exception as e:
        log(f"[testcrash] {type(e).__name__}: {e}", err=True)


async def pattern_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = None
    text = ""
    try:
        chat_id = update.effective_chat.id
        if not is_allowed(chat_id):
            await reply_safe(update, "ACCESS DENIED")
            return

        text = (update.message.text or "").strip()
        if not re.fullmatch(r"[BSbs]+", text):
            await reply_safe(update,
                "Send ONLY B/S letters (e.g. BBS, SSB, BBSS). "
                "No spaces or other characters.")
            return
        if len(text) > PATTERN_MAX_LEN:
            await reply_safe(update,
                f"PATTERN TOO LONG (max {PATTERN_MAX_LEN} chars).")
            return
        if not check_rate_limit(chat_id):
            await reply_safe(update, "SLOW DOWN. Wait a few seconds.")
            return

        t0 = time.time()
        user_in_flight[chat_id] = user_in_flight.get(chat_id, 0) + 1
        try:
            result = await format_pattern_result(text.upper(), chat_id)
        finally:
            user_in_flight[chat_id] = max(
                0, user_in_flight.get(chat_id, 1) - 1)
            if user_in_flight[chat_id] == 0:
                user_in_flight.pop(chat_id, None)
        elapsed_ms = (time.time() - t0) * 1000.0
        log(f"[pattern] user={chat_id} {text.upper()} ({elapsed_ms:.0f}ms)")

        await reply_safe(update, result, parse_mode=ParseMode.HTML)

    except asyncio.TimeoutError:
        log(f"[pattern] timeout on '{text}' user={chat_id}", err=True)
        await reply_safe(update, "SEARCH TIMED OUT. Try a shorter pattern.")
    except RetryAfter as e:
        log(f"[pattern] RetryAfter {getattr(e, 'retry_after', '?')}s — "
            f"dropping reply user={chat_id}", err=True)
    except Exception as e:
        tb = traceback.format_exc()
        log(f"[pattern] {type(e).__name__}: {e} (pattern='{text}' "
            f"user={chat_id})", err=True)
        log(tb, err=True)
        await reply_safe(update, f"SEARCH ERROR: {type(e).__name__}")


async def fallback_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        if not is_allowed(chat_id):
            await reply_safe(update,
                "ACCESS DENIED\n"
                f"Your chat_id: {chat_id}")
            return
        if is_admin(chat_id):
            await reply_safe(update, "UNKNOWN COMMAND. /help")
        else:
            await reply_safe(update,
                "Send a B/S pattern (e.g. BBS) to search history. "
                "Use ONLY B and S letters, no spaces.")
    except Exception as e:
        log(f"[fallback] {type(e).__name__}: {e}", err=True)


# ============================================================
# ERROR HANDLER
# ============================================================
async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        err = ctx.error
        if isinstance(err, (TimedOut, NetworkError, RetryAfter)):
            log(f"[handler] transient {type(err).__name__}: {err}",
                err=True)
            return
        tb = "".join(traceback.format_exception(
            type(err), err, err.__traceback__))
        log(f"[handler] {type(err).__name__}: {err}", err=True)
        log(tb, err=True)
        try:
            await send_crash_report(
                ctx.bot, reason=f"handler: {type(err).__name__}: {err}",
                exc_info=tb,
                extra={"update_id": getattr(update, "update_id", "?")})
        except Exception as e:
            log(f"[handler] crash report failed: {e}", err=True)
    except Exception as e:
        log(f"[handler] error_handler itself failed: {e}", err=True)


# ============================================================
# WATCHDOG
# ============================================================
async def watchdog_loop():
    while not live_stop.is_set():
        try:
            await asyncio.sleep(30.0)
            if live_stop.is_set():
                return
            global live_task
            if live_task is None or live_task.done():
                log("[watchdog] live_task dead — restarting", err=True)
                live_task = asyncio.create_task(
                    safe_task_loop("live-loop", live_loop, 3.0, live_stop),
                    name="live-poll")
        except asyncio.CancelledError:
            return
        except Exception as e:
            log(f"[watchdog] error: {e}", err=True)


# ============================================================
# LIFECYCLE
# ============================================================
async def on_startup(app: Application):
    global http_client, live_task, _pattern_executor
    global live_status_chat_id, live_status_message_id, live_status_last_edit

    try:
        await asyncio.to_thread(db_init)
    except Exception as e:
        log(f"[startup] db_init failed: {e}", err=True)
        raise

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=FIREBASE_CONNECT_TIMEOUT,
            read=FIREBASE_READ_TIMEOUT,
            write=FIREBASE_WRITE_TIMEOUT,
            pool=FIREBASE_POOL_TIMEOUT),
        limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        headers={"User-Agent": "tron-history-bot/14.0-py314fix"},
    )

    _pattern_executor = ThreadPoolExecutor(
        max_workers=PATTERN_WORKERS,
        thread_name_prefix="pattern-worker")

    log(f"[BOT] started mem={mem_mb():.0f}MB fb_ns={FIREBASE_NS} "
        f"pattern_workers={PATTERN_WORKERS} "
        f"bootstrap_batch={BOOTSTRAP_BATCH_SIZE} "
        f"bootstrap_parallel={BOOTSTRAP_PARALLEL} "
        f"py={sys.version.split()[0]}")

    try:
        await fb_get(f"{FIREBASE_NS}/meta")
        log("[warmup] firebase ok")
    except Exception as e:
        log(f"[warmup] firebase warn: {type(e).__name__}: {e}", err=True)

    try:
        await load_users()
    except Exception as e:
        log(f"[startup] load_users failed: {e}", err=True)

    live_stop.clear()
    live_task = asyncio.create_task(
        safe_task_loop("live-loop", live_loop, 3.0, live_stop),
        name="live-poll")
    asyncio.create_task(watchdog_loop(), name="watchdog")

    await asyncio.sleep(0.2)
    try:
        text = await format_live_status()
        msg = await send_message_safe(
            app.bot, ADMIN_ID, text, parse_mode=ParseMode.HTML)
        live_status_chat_id = ADMIN_ID
        live_status_message_id = msg.message_id
        live_status_last_edit = time.time()
        log(f"[BOT] admin notified — message_id={msg.message_id}")
    except RetryAfter as e:
        wait = float(getattr(e, "retry_after", 30.0))
        log(f"[BOT] startup RetryAfter {wait:.0f}s — retry once", err=True)
        try:
            await asyncio.sleep(wait + 1.0)
            text = await format_live_status()
            msg = await send_message_safe(
                app.bot, ADMIN_ID, text, parse_mode=ParseMode.HTML)
            live_status_chat_id = ADMIN_ID
            live_status_message_id = msg.message_id
            live_status_last_edit = time.time()
            log(f"[BOT] admin notified — message_id={msg.message_id}")
        except Exception as e2:
            log(f"[BOT] cannot message admin: {e2}", err=True)
    except Exception as e:
        log(f"[BOT] cannot message admin: {e}", err=True)

    asyncio.create_task(
        safe_task_loop("live-status", live_status_editor, 3.0, live_stop),
        name="live-status")

    asyncio.create_task(
        safe_task_loop("bootstrap", bootstrap_history, 5.0),
        name="bootstrap")


async def on_shutdown(app: Application):
    global http_client, _pattern_executor
    try:
        live_stop.set()
        tasks_to_stop = []
        for task in asyncio.all_tasks():
            if task.get_name() in (
                    "live-poll", "live-status", "watchdog", "bootstrap"):
                tasks_to_stop.append(task)
        for task in tasks_to_stop:
            if not task.done():
                try:
                    await asyncio.wait_for(task, timeout=5)
                except (asyncio.TimeoutError, Exception):
                    task.cancel()

        if _pattern_executor is not None:
            try:
                _pattern_executor.shutdown(wait=False)
            except Exception:
                pass
            _pattern_executor = None

        if http_client is not None:
            try:
                await http_client.aclose()
            except Exception:
                pass
        if db is not None:
            try:
                await asyncio.to_thread(db.close)
            except Exception:
                pass
        log("[BOT] shutdown complete")
    except Exception as e:
        log(f"[shutdown] {type(e).__name__}: {e}", err=True)


def main():
    global app_ref_holder
    app_ref_holder = {}
    request = HTTPXRequest(
        connect_timeout=TG_CONNECT_TIMEOUT,
        read_timeout=TG_READ_TIMEOUT,
        write_timeout=TG_WRITE_TIMEOUT,
        pool_timeout=TG_POOL_TIMEOUT,
    )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(request)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    app_ref_holder["app"] = app
    install_global_excepthook(app_ref_holder)

    app.add_handler(CommandHandler(["start", "help"], start_cmd))
    app.add_handler(CommandHandler("user", user_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("live", live_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("mem", mem_cmd))
    app.add_handler(CommandHandler("log", log_cmd))
    app.add_handler(CommandHandler("testcrash", testcrash_cmd))
    app.add_handler(MessageHandler(filters.Regex(r"^[BSbs]+$"), pattern_cmd))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, fallback_cmd))
    app.add_error_handler(error_handler)

    log("[BOT] starting polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True)


# ============================================================
# ENTRY POINT — Python 3.14 SAFE
# ============================================================
if __name__ == "__main__":
    # ── CRITICAL: ensure event loop exists for Python 3.14+ ──
    _ensure_event_loop()
    # ─────────────────────────────────────────────────────────
    try:
        main()
    except KeyboardInterrupt:
        log("[BOT] stopped by user")
    except Exception:
        tb = traceback.format_exc()
        log(tb, err=True)
        try:
            import requests as _rq
            payload = dump_log_bytes(f"BOT FATAL — {_ts()}\n\n{tb}")
            _rq.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data={"chat_id": ADMIN_ID,
                      "caption": "BOT FATAL — startup/run failed"},
                files={"document": (f"bot_fatal_{int(time.time())}.txt",
                                    payload)},
                timeout=15)
        except Exception as e:
            print(f"fatal notify failed: {e}", file=sys.stderr, flush=True)
        if os.name == "nt":
            input("Press Enter to close...")