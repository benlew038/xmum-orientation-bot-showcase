import os
from dotenv import load_dotenv
from telegram.ext import MessageHandler, filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

load_dotenv()

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import gspread
from google.oauth2.service_account import Credentials

from datetime import datetime
from zoneinfo import ZoneInfo
from telegram.error import RetryAfter

MY_TZ = ZoneInfo("Asia/Kuala_Lumpur")

from collections import defaultdict

import asyncio

RESET_SEMAPHORE = asyncio.Semaphore(5)  
GROUP_UPDATE_SEMAPHORE = asyncio.Semaphore(2)
AUTH_WRITE_SEMAPHORE = asyncio.Semaphore(1)
GROUP_LOCKS = defaultdict(asyncio.Lock)
USER_FLOW_LOCKS = defaultdict(asyncio.Lock)
FLOW_STATE = {
    "day1": {},
    "day2": {}
}

ATTENDANCE_INDEX = {
    "facis": [],
    "gm": []
}


# =====================
# Google Sheets setup
# =====================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

CREDS = Credentials.from_service_account_file(
    "service_account.json",
    scopes=SCOPES
)

gc = gspread.authorize(CREDS)

SPREADSHEET_ID = "1ksmqUbylTPFvP658jY_zCnizKqttG8hLy66pijPAhR4"
SHEET_NAME = "XMUM Orientation DB"

GM_GROUP_ID = -5098182838
FACI_GROUP_ID = -5223184026

sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

auth_sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("AUTH")
coins_sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("COINS_PRIVATE")
logs_sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("LOGS")
best_faci_sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("BEST_FACILITATOR")
att_sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("ATTENDANCE")


PERMISSIONS = {
    "Advisor": ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "OC": ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "HOF": ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "HOGM": ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "Facis and Freshies (Game Test)": ["update_group", "check_own_coin", "update_group_data"],
    "Facilitators": ["update_group", "check_own_coin", "update_group_data"],
    "Game Masters": ["manage_coins", "view_all_coins"],
}

# =====================
# Telegram bot setup
# =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise Exception("❌ BOT_TOKEN not set")

# =====================
# Helpers
# =====================
import time

FACI_OTP = None
FACI_OTP_EXPIRE = None
GM_OTP = None
GM_OTP_EXPIRE = None
FACI_ATTENDANCE_START = None
GM_ATTENDANCE_START = None
RESET_ENABLED = False
AUTH_CACHE = None
AUTH_CACHE_TIME = 0
AUTH_CACHE_TTL = 30   # 秒（30 秒内不重新读）
AUTH_CACHE_LOCK = asyncio.Lock()
AUTH_INDEX = {}
COINS_CACHE_LOCK = asyncio.Lock()
COINS_CACHE = {}
COINS_CACHE_TIME = 0
COINS_CACHE_TTL = 30  
GROUP_INFO_CACHE = {}
GROUP_INFO_CACHE_LOCK = asyncio.Lock()
LOG_LOCK = asyncio.Lock()
LOG_BUFFER = []
LOG_LAST_FLUSH = 0
LOG_FLUSH_INTERVAL = 5  
LOGS_CACHE = []
LOGS_CACHE_TIME = 0
LOGS_CACHE_TTL = 10
LOGS_CACHE_LOCK = asyncio.Lock()
ATTENDANCE_SIGNED = {
    "facis": set(),
    "gm": set()
}
ATTENDANCE_LOCK = asyncio.Lock()
ATTENDANCE_QUEUE = None
ATTENDANCE_BATCH_WINDOW = 1.0     
ATTENDANCE_BATCH_MAX = 25         
ATTENDANCE_RETRY_MAX = 3
ATTENDANCE_RETRY_BACKOFF = 2      
ATTENDANCE_CACHE = []
ATTENDANCE_CACHE_TIME = 0
ATTENDANCE_CACHE_TTL = 10
ATTENDANCE_CACHE_LOCK = asyncio.Lock()
ATTENDANCE_WRITE_SEMAPHORE = asyncio.Semaphore(1)

WRITE_QUEUE = None
WRITE_BATCH_WINDOW = 0.5  
WRITE_BATCH_MAX = 50       
COINS_WRITE_SEMAPHORE = asyncio.Semaphore(1)
WRITE_RETRY_MAX = 3
WRITE_RETRY_BACKOFF = 0.5  
WRITE_RATE_PER_SEC = 3.0
WRITE_INTERVAL = 1 / WRITE_RATE_PER_SEC
GLOBAL_APP = None

TELEGRAM_SEMAPHORE = asyncio.Semaphore(25)
LOG_WRITE_SEMAPHORE = asyncio.Semaphore(1)
LOGIN_SEMAPHORE = asyncio.Semaphore(25)
TELEGRAM_QUEUE = asyncio.Queue()
TELEGRAM_RATE = 20   
TX_REGISTRY = {}
UNDONE_TX = set()
UNDO_LOCK = asyncio.Lock()  

# =========================
# UPDATE GROUP V2
# =========================

UPDATE_QUEUE = asyncio.Queue()

UPDATE_BATCH_WINDOW = 0.5   
UPDATE_BATCH_MAX = 50          
UPDATE_WRITE_RATE = 4          

LAST_UPDATE_WRITE = 0

from telegram.error import BadRequest

async def safe_answer(query, text=None):

    try:
        if text:
            await query.answer(text)
        else:
            await query.answer()
    except BadRequest:
        pass

async def cache_health_check():
    """Periodically check cache health, auto-refresh if needed"""
    while True:
        await asyncio.sleep(300)  
        
        now = time.time()
        
        # Check AUTH cache
        if now - AUTH_CACHE_TIME > AUTH_CACHE_TTL * 0.8:  
            print("🔄 Auto-refreshing AUTH cache...")
            await get_auth_records_async()
        
        # Check COINS cache
        if now - COINS_CACHE_TIME > COINS_CACHE_TTL * 0.8:
            print("🔄 Auto-refreshing COINS cache...")
            records = await asyncio.to_thread(coins_sheet.get_all_records)
            async with COINS_CACHE_LOCK:
                COINS_CACHE.clear()
                COINS_CACHE.update({int(r["Group"]): int(r["Atlantis Coins"]) for r in records})
                COINS_CACHE_TIME = time.time()

async def build_group_summary_from_cache(group):
    async with GROUP_INFO_CACHE_LOCK:
        raw = GROUP_INFO_CACHE.get(group)
        g = raw.copy() if raw else None

    if not g:
        return f"Group {group} not found"

    return (
        f"📋 Group {group} Info\n\n"
        f"🏷 Group Name: {g['name']}\n"
        f"💬 Group Slogan: {g['slogan']}\n"
        f"🌍 International: {g['intl']}\n"
        f"🏠 Local: {g['local']}\n"
        f"🎓 Degree: {g['degree']}\n"
        f"📘 Foundation: {g['foundation']}\n"
        f"👥 Total: {g['total']}\n"
        f"📍 Location: {g['location']}"
    )

async def refresh_cache(update, context):

    if context.user_data.get("role") not in ["Advisor", "OC", "HOF", "HOGM"]:
        await update.message.reply_text("❌ Permission denied.")
        return

    msg = await update.message.reply_text("🔄 Refreshing caches...")

    try:
        await warmup_all_caches()

        await msg.edit_text(
            "✅ Cache refreshed successfully."
        )

    except Exception as e:
        await msg.edit_text(
            f"❌ Cache refresh failed:\n{e}"
        )

async def load_group_info_cache():
    global GROUP_INFO_CACHE

    rows = await asyncio.to_thread(sheet.get_all_values)
    

    cache = {}

    for row_index, r in enumerate(rows[1:], start=2):

        group = row_index - 1

        intl = int(r[2]) if len(r) > 2 and r[2] else 0
        local = int(r[3]) if len(r) > 3 and r[3] else 0
        degree = int(r[7]) if len(r) > 7 and r[7] else 0
        foundation = int(r[8]) if len(r) > 8 and r[8] else 0

        cache[group] = {
            "location": r[1] if len(r) > 1 else "-",
            "intl": intl,
            "local": local,
            "total": intl + local,
            "name": r[5] if len(r) > 5 else "-",
            "slogan": r[6] if len(r) > 6 else "-",
            "degree": degree,
            "foundation": foundation
        }

    async with GROUP_INFO_CACHE_LOCK:
        GROUP_INFO_CACHE.clear()
        GROUP_INFO_CACHE.update(cache)

async def post_init(app: Application):
    global WRITE_QUEUE, GLOBAL_APP, ATTENDANCE_QUEUE

    WRITE_QUEUE = asyncio.Queue()
    ATTENDANCE_QUEUE = asyncio.Queue()
    GLOBAL_APP = app

    # Start background workers
    app.bot_data["coin_worker"] = asyncio.create_task(
        coin_write_worker()
    )

    app.bot_data["attendance_worker"] = asyncio.create_task(
        attendance_write_worker()
    )

    app.bot_data["log_flusher"] = asyncio.create_task(
        _log_flusher()
    )

    # 🔥 NEW: Warm up caches (non-blocking)
    app.bot_data["cache_warmup"] = asyncio.create_task(
        warmup_all_caches()
    )

    app.bot_data["health_checker"] = asyncio.create_task(cache_health_check())

    app.bot_data["update_worker"] = asyncio.create_task(
        update_group_write_worker()
    )

    app.bot_data["telegram_worker"] = asyncio.create_task(
        telegram_send_worker()
    )
    
    print("✅ Bot initialization complete, cache warmup started...")

async def update_group_write_worker():
    global LAST_UPDATE_WRITE

    while True:
        try:
            first = await UPDATE_QUEUE.get()
        except asyncio.CancelledError:
            break

        batch = [first]
        start = asyncio.get_event_loop().time()

        while len(batch) < UPDATE_BATCH_MAX:
            timeout = UPDATE_BATCH_WINDOW - (
                asyncio.get_event_loop().time() - start
            )
            if timeout <= 0:
                break

            try:
                item = await asyncio.wait_for(
                    UPDATE_QUEUE.get(),
                    timeout
                )
                batch.append(item)
            except asyncio.TimeoutError:
                break

        now = time.time()
        min_interval = 1 / UPDATE_WRITE_RATE
        if now - LAST_UPDATE_WRITE < min_interval:
            await asyncio.sleep(min_interval - (now - LAST_UPDATE_WRITE))

        updates = []

        # 记录 batch 内最新值
        batch_vals = {(g, f): v for g, f, v in batch}

        for group, field, value in batch:
            row = group + 1
            col = FIELD_COLUMN[field]

            updates.append({
                "range": f"{col}{row}",
                "values": [[value]]
            })

            if field in ("intl", "local"):

                async with GROUP_INFO_CACHE_LOCK:
                    g_data = GROUP_INFO_CACHE.get(group, {})
                    cache_intl = g_data.get("intl", 0)
                    cache_local = g_data.get("local", 0)

                intl = int(batch_vals.get((group, "intl"), cache_intl))
                local = int(batch_vals.get((group, "local"), cache_local))

                updates.append({
                    "range": f"E{row}",
                    "values": [[intl + local]]
                })

        for attempt in range(3):
            try:
                await asyncio.to_thread(sheet.batch_update, updates)
                LAST_UPDATE_WRITE = time.time()
                break
            except Exception as e:
                if attempt == 2:
                    print("[UpdateWorker] batch failed permanently:", e)
                else:
                    await asyncio.sleep(1.5 * (attempt + 1))

        for _ in range(len(batch)):
            UPDATE_QUEUE.task_done()
    
async def get_attendance_records_cached():
    """
    Get attendance records with caching to avoid repeated full table scans
    """
    global ATTENDANCE_CACHE, ATTENDANCE_CACHE_TIME
    
    now = time.time()

    # Cache hit
    if ATTENDANCE_CACHE and now - ATTENDANCE_CACHE_TIME < ATTENDANCE_CACHE_TTL:
        return ATTENDANCE_CACHE

    async with ATTENDANCE_CACHE_LOCK:
        # Double-check after acquiring lock
        now = time.time()
        if ATTENDANCE_CACHE and now - ATTENDANCE_CACHE_TIME < ATTENDANCE_CACHE_TTL:
            return ATTENDANCE_CACHE

        # Read from sheets (expensive operation)
        raw = await asyncio.to_thread(att_sheet.get_all_records)
        ATTENDANCE_CACHE = raw
        ATTENDANCE_CACHE_TIME = time.time()
        return raw
    
async def warmup_all_caches():
    """Warm up all caches in background, non-blocking"""
    try:
        print("🔥 Starting cache warmup...")
        start_time = time.time()
        
        print("  📚 Loading auth data...")
        await get_auth_records_async()
        print(f"  ✅ AUTH cache ready: {len(AUTH_INDEX)} records")
        
        # Warm up COINS cache
        print("  💰 Loading coins data...")
        records = await asyncio.to_thread(coins_sheet.get_all_records)
        async with COINS_CACHE_LOCK:
            COINS_CACHE.clear()
            COINS_CACHE.update({int(r["Group"]): int(r["Atlantis Coins"]) for r in records})
            COINS_CACHE_TIME = time.time()
        print(f"  ✅ COINS cache ready: {len(COINS_CACHE)} groups")
        
        # Warm up COIN_ROW_MAP
        print("  🗺️  Loading row mapping...")
        await asyncio.to_thread(load_coin_map)
        print(f"  ✅ Row map ready: {len(COIN_ROW_MAP)} groups")

        #  Warm up GROUP INFO cache
        print("  📦 Loading group info...")
        await load_group_info_cache()
        print(f"  ✅ GROUP INFO cache ready: {len(GROUP_INFO_CACHE)} groups")
        
        # Warm up ATTENDANCE cache
        print("  📋 Loading attendance records...")
        await get_attendance_records_cached()  # Make sure to use your cached version
        print(f"  ✅ ATTENDANCE cache ready")
        
        # Warm up LOGS cache
        print("  📊 Loading logs...")
        global LOGS_CACHE, LOGS_CACHE_TIME
        LOGS_CACHE = await asyncio.to_thread(logs_sheet.get_all_records)
        LOGS_CACHE_TIME = time.time()
        print(f"  ✅ LOGS cache ready")
        
        elapsed = time.time() - start_time
        print(f"🔥 All caches warmed up! Time: {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"⚠️ Cache warmup error: {e}")
        import traceback
        traceback.print_exc()

async def get_logs_cached():
    global LOGS_CACHE, LOGS_CACHE_TIME

    now = time.time()

    # cache hit
    if LOGS_CACHE and now - LOGS_CACHE_TIME < LOGS_CACHE_TTL:
        return LOGS_CACHE

    async with LOGS_CACHE_LOCK:
        now = time.time()
        if LOGS_CACHE and now - LOGS_CACHE_TIME < LOGS_CACHE_TTL:
            return LOGS_CACHE

        records = await asyncio.to_thread(logs_sheet.get_all_records)

        LOGS_CACHE = records
        LOGS_CACHE_TIME = time.time()

        return records

async def post_shutdown(app: Application):
    if ATTENDANCE_QUEUE:
        await ATTENDANCE_QUEUE.join()

    for key in ["coin_worker", "attendance_worker"]:
        task = app.bot_data.get(key)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

async def get_attendance_records():
    global ATTENDANCE_CACHE, ATTENDANCE_CACHE_TIME
    now = time.time()

    if ATTENDANCE_CACHE and now - ATTENDANCE_CACHE_TIME < ATTENDANCE_CACHE_TTL:
        return ATTENDANCE_CACHE

    async with ATTENDANCE_CACHE_LOCK:
        raw = await asyncio.to_thread(att_sheet.get_all_records)
        ATTENDANCE_CACHE = raw
        ATTENDANCE_CACHE_TIME = time.time()
        return raw

async def safe_edit(query, text, reply_markup=None):
    """
    callback 过期就 fallback 用 send_message
    """
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest:
        await query.message.reply_text(text, reply_markup=reply_markup)


async def add_log_async(operator_name, operator_role, target, action, before, after):
    global LOG_BUFFER, LOG_LAST_FLUSH
    now = datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")

    async with LOG_LOCK:
        LOG_BUFFER.append([now, operator_name, operator_role, target, action, before, after])
        t = time.time()
        if len(LOG_BUFFER) >= 10 or t - LOG_LAST_FLUSH >= LOG_FLUSH_INTERVAL:
            rows = LOG_BUFFER.copy()
            LOG_BUFFER.clear()
            LOG_LAST_FLUSH = t
            try:
                async with LOG_WRITE_SEMAPHORE:
                    await asyncio.to_thread(logs_sheet.append_rows, rows)
            except Exception as e:
                LOG_BUFFER = rows + LOG_BUFFER
                LOG_LAST_FLUSH = 0
                print("add_log_async append_rows failed:", e)

async def _log_flusher():
    global LOG_BUFFER
    while True:
        await asyncio.sleep(LOG_FLUSH_INTERVAL)
        async with LOG_LOCK:
            if not LOG_BUFFER:
                continue
            rows = LOG_BUFFER.copy()
            LOG_BUFFER.clear()
            try:
                async with LOG_WRITE_SEMAPHORE:
                    await asyncio.to_thread(logs_sheet.append_rows, rows)
            except Exception as e:
                LOG_BUFFER = rows + LOG_BUFFER
                print("log_flusher append_rows failed:", e)

async def undo_transaction(context, tx_id):

    success = []
    failed = []

    if tx_id not in TX_REGISTRY:
        await notify_gm_group(context.application, f"❌ Transaction not found, please try again: {tx_id}")
        return

    async with UNDO_LOCK:
        if tx_id in UNDONE_TX:
            return
        
        UNDONE_TX.add(tx_id)

        if len(UNDONE_TX) > 300:
            UNDONE_TX.discard(next(iter(UNDONE_TX)))

    role = context.user_data.get("role", "")
    name = context.user_data.get("name", "")

    summary_gm = f"↩️ UNDO APPLIED\n\nTX: {tx_id}\n\n"
    summary_faci = f"↩️ UNDO APPLIED\n\nTX: {tx_id}\n\n"

    for item in TX_REGISTRY[tx_id]:

        group = item["group"]
        before = item["before"]
        after = item["after"]

        async with GROUP_LOCKS[group]:
            current = await get_coins_async(group)

            if current != after:
                print(f"[UNDO BLOCKED] {group} changed: {current} != {after}")
                failed.append(group)
                continue

            success.append(group)

            await WRITE_QUEUE.put((
                group,
                before,
                {
                    "user_id": context._chat_id,
                    "before": current,
                    "after": before
                }
            ))

        delta = after - before

        await add_log_async(
            name,
            role,
            f"Group {group}",
            f"UNDO (TX:{tx_id})",
            current,
            before
        )

        summary_gm += f"Group {group}\n{current} → {before} (-{delta}🪙)\n\n"
        summary_faci += f"Group {group} (-{delta}🪙)\n"
    
    if failed:
        summary_gm += f"\n⚠️ Skipped (state changed): {failed}\n"

    await notify_gm_group(context.application, summary_gm)
    await notify_faci_group(context.application, summary_faci)

    TX_REGISTRY.pop(tx_id, None)

async def coin_write_worker():
    last_write_ts = 0.0

    while True:
        got = 0  

        group, value, meta = await WRITE_QUEUE.get()
        got += 1

        pending = {group: (value, meta)}
        start = asyncio.get_event_loop().time()

        try:
            while True:
                timeout = WRITE_BATCH_WINDOW - (asyncio.get_event_loop().time() - start)
                if timeout <= 0 or len(pending) >= WRITE_BATCH_MAX:
                    break
                try:
                    g, v, m = await asyncio.wait_for(WRITE_QUEUE.get(), timeout)
                    got += 1                     
                    pending[g] = (v, m)
                except asyncio.TimeoutError:
                    break

            now = asyncio.get_event_loop().time()
            sleep_time = WRITE_INTERVAL - (now - last_write_ts)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            if not COIN_ROW_MAP:
                await asyncio.to_thread(load_coin_map)

            updates = []
            for g, (v, _) in pending.items():
                row = COIN_ROW_MAP.get(str(g))
                if row:
                    updates.append({
                        "range": f"B{row}",
                        "values": [[int(v)]]
                    })

            if updates:
                async with COINS_WRITE_SEMAPHORE:
                    await asyncio.to_thread(coins_sheet.batch_update, updates)
                    last_write_ts = asyncio.get_event_loop().time()

            # ④ 通知用户
            for g, (_, m) in pending.items():
                try:
                    if not m.get("user_id"):
                        continue

                    before = m.get("before")
                    if before is not None:
                        msg = f"✅ Group {g}: {before} → {m['after']} 🪙"
                    else:
                        msg = f"✅ Group {g}: updated to {m['after']} 🪙"

                    await TELEGRAM_QUEUE.put((
                        m["user_id"],
                        msg,
                        None,
                        0
                    ))
                except Exception:
                    pass

        finally:
            for _ in range(got):
                WRITE_QUEUE.task_done()

async def attendance_write_worker():
    while True:
        try:
            first = await ATTENDANCE_QUEUE.get()
        except asyncio.CancelledError:
            break

        batch = [first]
        start = asyncio.get_event_loop().time()

        while len(batch) < ATTENDANCE_BATCH_MAX:
            timeout = ATTENDANCE_BATCH_WINDOW - (
                asyncio.get_event_loop().time() - start
            )
            if timeout <= 0:
                break

            try:
                row = await asyncio.wait_for(
                    ATTENDANCE_QUEUE.get(), timeout
                )
                batch.append(row)
            except asyncio.TimeoutError:
                break

        success = False

        for attempt in range(ATTENDANCE_RETRY_MAX):
            try:
                async with ATTENDANCE_WRITE_SEMAPHORE:
                    await asyncio.to_thread(
                        att_sheet.append_rows,
                        batch
                    )
                    success = True
                    break

            except Exception as e:
                if "429" in str(e):
                    await asyncio.sleep(
                        ATTENDANCE_RETRY_BACKOFF * (attempt + 1)
                    )
                else:
                    print("attendance worker error:", e)
                    break

        # fallback
        if not success:
            print("⚠ Fallback to single writes")

            for row in batch:
                try:
                    await asyncio.to_thread(
                        safe_append_row,
                        att_sheet,
                        row
                    )
                except Exception as e:
                    print("❌ FATAL attendance write failure:", e)

        for _ in batch:
            ATTENDANCE_QUEUE.task_done()


async def _requeue_later(group, val, meta, delay=2):
    await asyncio.sleep(delay)
    await WRITE_QUEUE.put((group, val, meta))


async def get_auth_records_async():
    global AUTH_CACHE, AUTH_CACHE_TIME, AUTH_INDEX

    now = time.time()

    if AUTH_CACHE and now - AUTH_CACHE_TIME < AUTH_CACHE_TTL:
        return AUTH_CACHE

    async with AUTH_CACHE_LOCK:
        now = time.time()
        if AUTH_CACHE and now - AUTH_CACHE_TIME < AUTH_CACHE_TTL:
            return AUTH_CACHE

        raw = await asyncio.to_thread(
            auth_sheet.get_all_values
        )
        headers = raw[0]
        AUTH_CACHE = [
            dict(zip(headers, row))
            for row in raw[1:]
            if any(row)
        ]

        global AUTH_INDEX
        AUTH_INDEX = {
            str(r["Password"]).strip().lstrip("'"): r
            for r in AUTH_CACHE
            if r.get("Password")
        }

        AUTH_CACHE_TIME = time.time()
        return AUTH_CACHE

def now_my_str():
    return datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")


async def search_user_by_name_async(keyword):
    keyword = keyword.lower().strip().split()

    records = await get_auth_records_async()
    matches = []

    for r in records:
        name = r["Name"].lower()
        if all(k in name for k in keyword):
            matches.append(r)

    return matches


DAY1_RANK = {1:1000, 2:750, 3:500, 4:250}
DAY2_RANK = {"win":750, "lose":250}

FIELD_COLUMN = {
    "location": "B",
    "intl": "C",
    "local": "D",
    "groupname": "F",
    "groupslogan": "G",
    "degree": "H",
    "foundation": "I"
}

FIELD_LABEL = {
    "location": "Current Location",
    "intl": "International Students Amount",
    "local": "Local Students Amount",
    "groupname": "Group Name",
    "groupslogan": "Group Slogan",

    "degree": "Degree Students",
    "foundation": "Foundation Students",
}

COIN_ROW_MAP = {}

def load_coin_map():
    records = coins_sheet.get_all_records()
    for i, r in enumerate(records, start=2):
        COIN_ROW_MAP[str(r["Group"])] = i

async def get_coins_async(group):
    global COINS_CACHE, COINS_CACHE_TIME
    now = time.time()
    if COINS_CACHE and now - COINS_CACHE_TIME < COINS_CACHE_TTL:
        return COINS_CACHE.get(int(group), 0)

    async with COINS_CACHE_LOCK:
        if COINS_CACHE and time.time() - COINS_CACHE_TIME < COINS_CACHE_TTL:
            return COINS_CACHE.get(int(group), 0)

        records = await asyncio.to_thread(coins_sheet.get_all_records)
        COINS_CACHE = {int(r["Group"]): int(r["Atlantis Coins"]) for r in records}
        COINS_CACHE_TIME = time.time()
        return COINS_CACHE.get(int(group), 0)

async def update_coins_async(group, new_amount, user_id=None):

    global COINS_CACHE, COINS_CACHE_TIME

    async with COINS_CACHE_LOCK:
        COINS_CACHE[int(group)] = int(new_amount)
        COINS_CACHE_TIME = time.time()

    if WRITE_QUEUE is None:
        raise RuntimeError("WRITE_QUEUE not initialized")

    # enqueue
    await WRITE_QUEUE.put((
        group,
        int(new_amount),
        {
            "user_id": user_id,
            "before": None,
            "after": new_amount
        }
    ))

def safe_append_row(sheet, row, retries=3):
    for i in range(retries):
        try:
            sheet.append_row(row)
            return
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(0.5 * (2 ** i))

def get_update_submenu():
    keyboard = [
        [InlineKeyboardButton("📍 Current Location", callback_data="field_location")],
        [InlineKeyboardButton("🌍 International Students Amount", callback_data="field_intl")],
        [InlineKeyboardButton("🏠 Local Students Amount", callback_data="field_local")],
        [InlineKeyboardButton("🎓 Degree Students", callback_data="field_degree")],
        [InlineKeyboardButton("📘 Foundation Students", callback_data="field_foundation")],
        [InlineKeyboardButton("🏷 Group Name", callback_data="field_groupname")],
        [InlineKeyboardButton("💬 Group Slogan", callback_data="field_groupslogan")],
        [InlineKeyboardButton("⬅ Back", callback_data="back_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_menu_by_role(role):

    if role in ["Advisor", "OC", "HOF", "HOGM"]:   # admin
        keyboard = [
            [InlineKeyboardButton("💰 Manage Atlantis Coins", callback_data="manage_coins")],
            [InlineKeyboardButton("➕➖ Edit Atlantis Coins Amount", callback_data="edit_coins")],
            [InlineKeyboardButton("📊 View All Groups' Atlantis Coins", callback_data="view_all_coins")],
            [InlineKeyboardButton("✏ Update Group Data", callback_data="update_group")],
            [InlineKeyboardButton("📩 Message Facis", callback_data="msg_faci")],
            [InlineKeyboardButton("📝 Comment for Best Facis", callback_data="best_faci_comment")],
            [InlineKeyboardButton("📊 View Logs", callback_data="view_logs")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]

    elif role == "Facilitators":
        keyboard = [
            [InlineKeyboardButton("✏ Update Group Data", callback_data="update_group")],
            [InlineKeyboardButton("💰 Check My Atlantis Coins", callback_data="check_my_coins")],
            [InlineKeyboardButton("📩 Message Facis", callback_data="msg_faci")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]

    elif role == "Facis and Freshies (Game Test)":
        keyboard = [
            [InlineKeyboardButton("✏ Update Group Data", callback_data="update_group")],
            [InlineKeyboardButton("💰 Check My Atlantis Coins", callback_data="check_my_coins")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]

    elif role == "Game Masters":
        keyboard = [
            [InlineKeyboardButton("💰 Manage Atlantis Coins", callback_data="manage_coins")],
            [InlineKeyboardButton("➕➖ Edit Atlantis Coins Amount", callback_data="edit_coins")],
            [InlineKeyboardButton("📊 View All Atlantis Coins", callback_data="view_all_coins")],
            [InlineKeyboardButton("📩 Message Facis", callback_data="msg_faci")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]

    else:
        keyboard = [[InlineKeyboardButton("🚪 Logout", callback_data="logout")]]

    return InlineKeyboardMarkup(keyboard)

async def get_user_by_password_async(password):
    await get_auth_records_async()   # 确保 cache & index 已加载

    password = str(password).strip().lstrip("'")
    r = AUTH_INDEX.get(password)
    if not r:
        return None

    return {
        "role": r["Roles"],
        "group": int(r["Group"]) if r["Group"] else None,
        "name": r["Name"],
        "is_proxy": str(r.get("Is_Proxy", "")).upper() == "TRUE"
    }

    return None
    
def acquire_action_lock(context, key, ttl=3):
    now = time.time()
    lock = context.user_data.get(f"_lock_{key}")

    if lock and now - lock < ttl:
        return False

    context.user_data[f"_lock_{key}"] = now
    return True
    
async def show_telegram_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user

    # 群组 / 超级群
    if chat.type in ["group", "supergroup"]:
        await update.message.reply_text(
            f"👥 Group Chat ID:\n{chat.id}"
        )
    else:
        # 私聊
        await update.message.reply_text(
            f"🆔 Your Telegram User ID:\n{user.id}"
        )

async def notify_gm_group(app, text):

    await TELEGRAM_QUEUE.put((
        GM_GROUP_ID,
        text,
        None,
        0
    ))

async def notify_faci_group(app, text):
   
    await TELEGRAM_QUEUE.put((
        FACI_GROUP_ID,
        text,
        None,
        0
    ))

class TelegramRateLimiter:
    def __init__(self, rate=20):
        self.rate = rate         
        self.tokens = rate         
        self.updated_at = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.updated_at

                self.tokens = min(
                    self.rate,
                    self.tokens + elapsed * self.rate
                )
                self.updated_at = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                wait_time = (1 - self.tokens) / self.rate

            await asyncio.sleep(wait_time)


TELEGRAM_LIMITER = TelegramRateLimiter(rate=20)

async def telegram_send_worker():

    while True:
        try:
            chat_id, text, markup, retry = await TELEGRAM_QUEUE.get()
        except asyncio.CancelledError:
            break

        try:
            # 等待发送许可
            await TELEGRAM_LIMITER.acquire()

            await GLOBAL_APP.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup
            )

            TELEGRAM_QUEUE.task_done()

        except RetryAfter as e:
            wait_time = e.retry_after + 1

            if retry < 3:
                await asyncio.sleep(wait_time)

                await TELEGRAM_QUEUE.put(
                    (chat_id, text, markup, retry + 1)
                )
            else:
                print(f"❌ Telegram failed permanently: {chat_id}")
                TELEGRAM_QUEUE.task_done()

        except Exception as e:
            print("Telegram send error:", e)
            TELEGRAM_QUEUE.task_done()

async def do_reset_logic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with RESET_SEMAPHORE: 
        try:
            for k in [
                "search_name_mode",
                "awaiting_password",
                "awaiting_old_password",
                "awaiting_new_password",
                "reset_row",
                "reset_target_name",
            ]:
                context.user_data.pop(k, None)

            context.user_data["search_name_mode"] = True

            await update.message.reply_text(
                "🔑 Password Reset\n\n"
                "Please type your full name or keyword:"
            )

        except Exception as e:
            await update.message.reply_text(
                "❌ Reset failed. Please try again later."
            )

async def reset_password_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not RESET_ENABLED:
        await update.message.reply_text(
            "🔒 Password reset is currently DISABLED.\n"
            "Please contact admin (HOF)."
        )
        return

    await update.message.reply_text(
        "🔄 Reset request received.\nPlease wait..."
    )

    asyncio.create_task(do_reset_logic(update, context))
    
async def _update_telegram_id_bg(name, tg_id):
    async with AUTH_WRITE_SEMAPHORE:
        def find_and_update():
            cell = auth_sheet.find(name)
            auth_sheet.update(range_name=f"E{cell.row}", values=[[tg_id]])

        try:
            await asyncio.to_thread(find_and_update)
        except Exception as e:
            print("auth find/update failed:", e)

async def process_login(update, context, password):
    async with LOGIN_SEMAPHORE:
        user = await get_user_by_password_async(password)  

        if not user:
            context.user_data["awaiting_password"] = True
            await update.message.reply_text(
                "❌ Invalid password.\nPlease try again:"
            )
            return
        
        for k in list(context.user_data.keys()):
            if k not in ["role", "group", "name", "is_proxy"]:
                context.user_data.pop(k, None)

        context.user_data["role"] = user["role"]
        context.user_data["group"] = user["group"]
        context.user_data["name"] = user["name"]
        context.user_data["is_proxy"] = user.get("is_proxy", False)

        telegram_id = update.effective_user.id

        asyncio.create_task(
            _update_telegram_id_bg(user["name"], telegram_id)
        )

        menu = get_menu_by_role(user["role"])
        await update.message.reply_text(
            f"🦢 Honk Honk!\n🎉 Welcome {user['role']} {user['name']}",
            reply_markup=menu
        )

async def process_rank_coins(context, rank, group):

    add = DAY1_RANK[rank]

    async with GROUP_LOCKS[group]:
        before = await get_coins_async(group)
        after = before + add

        await update_coins_async(group, after, context._chat_id)

    await add_log_async(
        context.user_data.get("name", ""),
        context.user_data.get("role", ""),
        f"Group {group}",
        f"Day 1 Rank {rank}",
        before,
        after
    )

async def process_day1_ranking(context):

        results = context.user_data.get("day1_results", {})
        role = context.user_data.get("role", "")
        name = context.user_data.get("name", "")

        medals = {1:"🥇", 2:"🥈", 3:"🥉", 4:"🎖"}
        summary_gm = f"🏆 Day 1 Ranking Result\n\nBy: {role} {name}\n\n"
        summary_faci = f"🏆 Day 1 Ranking Result\n\nBy: {role} {name}\n\n"

        for rank, data in results.items():
            group = data["group"]
            add = data["add"]

            if group is None:
                summary_gm += f"{medals[rank]} Rank {rank}: 🚫 No Group\n"
                summary_faci += f"{medals[rank]} Rank {rank}: 🚫 No Group\n"
                continue

            async with GROUP_LOCKS[group]:
                before = await get_coins_async(group)
                after = before + add

                await update_coins_async(group, after, context._chat_id)

            await add_log_async(
                name,
                role,
                f"Group {group}",
                f"Day 1 Rank {rank}",
                before,
                after
            )

            summary_gm += f"{medals[rank]} Rank {rank}: Group {group}\n{before} → {after} (+{add}🪙)\n\n"
            summary_faci += f"{medals[rank]} Rank {rank}: Group {group}\n"

        await notify_gm_group(context.application, summary_gm)
        await notify_faci_group(context.application, summary_faci)

async def process_day1_ranking_from_state(context, state, tx_id):

    if len(TX_REGISTRY) > 200:
        TX_REGISTRY.pop(next(iter(TX_REGISTRY)))

    TX_REGISTRY[tx_id] = []

    results = state["results"]
    role = context.user_data.get("role", "")
    name = context.user_data.get("name", "")

    medals = {1:"🥇", 2:"🥈", 3:"🥉", 4:"🎖"}

    summary_gm = f"🏆 Day 1 Ranking Result\n\nBy: {role} {name}\n\n"
    summary_faci = f"🏆 Day 1 Ranking Result\n\nBy: {role} {name}\n\n"

    for rank, data in results.items():

        group = data["group"]
        add = data["add"]

        if group is None:
            summary_gm += f"{medals[rank]} Rank {rank}: 🚫 No Group\n"
            summary_faci += f"{medals[rank]} Rank {rank}: 🚫 No Group\n"
            continue

        async with GROUP_LOCKS[group]:
            before = await get_coins_async(group)
            after = before + add

            await update_coins_async(group, after, context._chat_id)
            TX_REGISTRY[tx_id].append({
                "group": group,
                "before": before,
                "after": after
            })

        await add_log_async(
            name,
            role,
            f"Group {group}",
            f"Day 1 Rank {rank}",
            before,
            after
        )

        summary_gm += f"{medals[rank]} Rank {rank}: Group {group}\n{before} → {after} (+{add}🪙)\n\n"
        summary_faci += f"{medals[rank]} Rank {rank}: Group {group}\n"

    await notify_gm_group(context.application, summary_gm)
    await notify_faci_group(context.application, summary_faci)

async def show_next_rank(query, state):

    rank = state["current_rank"]

    keyboard = []
    row = []

    for i in range(1, 29):
        if i in state["used"]:
            continue

        row.append(
            InlineKeyboardButton(str(i), callback_data=f"rank_group_{i}")
        )

        if len(row) == 4:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🚫 No group", callback_data="rank_none")])
    keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="manage_coins")])

    await query.edit_message_text(
        f"🏅 Select Group for Rank {rank} (+{DAY1_RANK[rank]}🪙):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_day1_summary(query, state):

    medals = {1:"🥇", 2:"🥈", 3:"🥉", 4:"🎖"}

    text = "🏆 Day 1 Ranking Summary\n\n"

    for r in range(1, 5):
        data = state["results"].get(r)
        group = data["group"]

        if group is None:
            text += f"{medals[r]} Rank {r}: 🚫 No Group\n"
        else:
            text += (
                f"{medals[r]} Rank {r}: "
                f"Group {group} (+{DAY1_RANK[r]}🪙)\n"
            )

    text += "\nPlease confirm to apply coins."

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm Ranking", callback_data="confirm_day1")],
        [InlineKeyboardButton("❌ Cancel", callback_data="manage_coins")]
    ])

    await query.edit_message_text(text, reply_markup=keyboard)

async def process_day2_pk(context, win, lose, tx_id):

    if len(TX_REGISTRY) > 200:
        TX_REGISTRY.pop(next(iter(TX_REGISTRY)))

    TX_REGISTRY[tx_id] = []
    role = context.user_data.get("role", "")
    name = context.user_data.get("name", "")

    summary_gm = f"⚔ Day 2 PK Result\n\nBy: {role} {name}\n\n"
    summary_faci = f"⚔ Day 2 PK Result\n\n"

    # ===== WINNER =====
    if win is not None:

        async with GROUP_LOCKS[win]:
            before_win = await get_coins_async(win)
            after_win = before_win + DAY2_RANK["win"]

            await update_coins_async(win, after_win, context._chat_id)

        await add_log_async(
            name, role,
            f"Group {win}",
            f"PK Win (TX:{tx_id})",
            before_win,
            after_win
        )

        TX_REGISTRY[tx_id].append({
            "group": win,
            "before": before_win,
            "after": after_win
        })

        summary_gm += (
            f"🏆 Winner: Group {win}\n"
            f"{before_win} → {after_win} (+{DAY2_RANK['win']}🪙)\n\n"
        )

        summary_faci += f"🏆 Winner: Group {win}\n"

    else:
        summary_gm += "🏆 Winner: 🚫 No Group\n\n"
        summary_faci += "🏆 Winner: 🚫 No Group\n"

    # ===== LOSER =====
    if lose is not None:

        async with GROUP_LOCKS[lose]:
            before_lose = await get_coins_async(lose)
            after_lose = before_lose + DAY2_RANK["lose"]

            await update_coins_async(lose, after_lose, context._chat_id)

        await add_log_async(
            name, role,
            f"Group {lose}",
            f"PK Lose (TX:{tx_id})",
            before_lose,
            after_lose
        )

        TX_REGISTRY[tx_id].append({
            "group": lose,
            "before": before_lose,
            "after": after_lose
        })

        summary_gm += (
            f"💪 Runner-up: Group {lose}\n"
            f"{before_lose} → {after_lose} (+{DAY2_RANK['lose']}🪙)\n\n"
        )

        summary_faci += f"💪 Runner-up: Group {lose}\n"

    else:
        summary_gm += "💪 Runner-up: 🚫 No Group\n"
        summary_faci += "💪 Runner-up: 🚫 No Group\n"

    await notify_gm_group(context.application, summary_gm)
    await notify_faci_group(context.application, summary_faci)

async def process_manual_coins(update, context, group, change):
    
    global COINS_CACHE, COINS_CACHE_TIME

    async with COINS_CACHE_LOCK:
        current = COINS_CACHE.get(int(group))
        if current is None:
            records = await asyncio.to_thread(coins_sheet.get_all_records)
            COINS_CACHE = {int(r["Group"]): int(r["Atlantis Coins"]) for r in records}
            COINS_CACHE_TIME = time.time()
            current = COINS_CACHE.get(int(group), 0)

        before = int(current)
        change = int(change)

        if change < 0 and abs(change) > before:
            await update.message.reply_text(
                f"❌ Cannot deduct {abs(change)} coins.\n"
                f"Group {group} only has {before} coins.\n\n"
                "Please try again."
            )
            return

        after = before + change
        COINS_CACHE[int(group)] = int(after)
        COINS_CACHE_TIME = time.time()

    await WRITE_QUEUE.put((
        group,
        after,
        {
            "user_id": update.effective_chat.id,
            "name": context.user_data.get("name", ""),
            "role": context.user_data.get("role", ""),
            "before": before,
            "after": after,
        }
    ))

    await add_log_async(
        context.user_data.get("name", ""),
        context.user_data.get("role", ""),
        f"Group {group}",
        "manual edit coins (queued)",
        before,
        after
    )


def process_update_group_info_sync(context, group, field, value):
    row = group + 1
    col = FIELD_COLUMN[field]

    try:
        before = sheet.acell(f"{col}{row}").value
    except Exception:
        before = "-"

    updates = [
        {
            "range": f"{col}{row}",
            "values": [[value]]
        }
    ]

    if field in ("intl", "local"):
        try:
            if field == "intl":
                local = sheet.acell(f"D{row}").value or 0
                total = int(value) + int(local)
            else:
                intl = sheet.acell(f"C{row}").value or 0
                total = int(value) + int(intl)
        except Exception:
            total = value  

        updates.append({
            "range": f"E{row}",
            "values": [[total]]
        })

    sheet.batch_update(updates)

    return before, value

def new_ui_page(context):
    context.user_data["_ui_version"] = str(time.time())
    return context.user_data["_ui_version"]

def ui_valid(context, version):
    return context.user_data.get("_ui_version") == version

def get_name_by_telegram_id(tg_id, records):
    for r in records:
        if str(r.get("Telegram_ID")) == str(tg_id):
            return r["Name"]
    return str(tg_id)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text = update.message.text.strip()

    if update.message.text == "/fake_click_attendance_check":
        fake_query = type("obj", (), {})()
        fake_query.data = "attendance_check_facis"
        fake_query.message = update.message

        class Dummy:
            async def answer(self, *args, **kwargs):
                pass

        fake_query.answer = Dummy().answer

        update.callback_query = fake_query
        await button_handler(update, context)
        return
    
    if context.user_data.get("search_name_mode"):

        results = await search_user_by_name_async(text)

        if not results:
            await update.message.reply_text("❌ No matching name found. Try again:")
            return

        if len(results) > 1:
            msg = "⚠ Multiple matches found:\n\n"
            for r in results:
                msg += f"{r['Name']}\n"
            msg += "\nPlease type more specific keyword:"
            await update.message.reply_text(msg)
            return

        user = results[0]

        cell = await asyncio.to_thread(auth_sheet.find, user["Name"])
        cell_val = await asyncio.to_thread(auth_sheet.cell, cell.row, 3)
        current_password = cell_val.value

        context.user_data["reset_target_name"] = user["Name"]
        context.user_data["reset_row"] = cell.row
        context.user_data["has_old_password"] = bool(current_password)
        context.user_data.pop("search_name_mode")
        context.user_data.pop("awaiting_password", None)

        if current_password:
            context.user_data["awaiting_old_password"] = True
            await update.message.reply_text(
                f"🔐 Account: {user['Name']}\n\n"
                f"This account already has a password.\n"
                f"Please enter your OLD password:"
            )
        else:
            context.user_data["awaiting_new_password"] = True
            await update.message.reply_text(
                f"🔐 Account: {user['Name']}\n\n"
                f"This account has no password yet.\n"
                f"Please enter your NEW password:"
            )
        return
    
    if context.user_data.get("awaiting_password"):

        context.user_data.pop("awaiting_password", None)

        await update.message.reply_text(
            "⏳ Checking password, please wait..."
        )

        await process_login(update, context, text)
        return

    if context.user_data.get("reply_mode"):
        auth_records = await get_auth_records_async()

        # 🚫 proxy 不能 reply
        if context.user_data.get("is_proxy"):
            await update.message.reply_text(
                "❌ Game test account cannot reply messages."
            )
            context.user_data.pop("reply_mode", None)
            context.user_data.pop("reply_target", None)
            return

        text = update.message.text.strip()

        if text.lower() == "cancel":
            context.user_data.pop("reply_mode", None)
            context.user_data.pop("reply_target", None)
            await update.message.reply_text("❎ Reply cancelled.")
            return

        if not context.user_data.get("role"):
            await update.message.reply_text("❌ Please /start and login.")
            return

        sender_id = context.user_data["reply_target"]
        faci_name = context.user_data.get("name", "Facilitator")

        await TELEGRAM_QUEUE.put((
            sender_id,
            f"📩 Reply from Facilitator {faci_name}:\n\n{text}",
            None,
            0
        ))

        after_text = text.strip()
        after_text = after_text[:60] + "..." if len(after_text) > 60 else after_text

        target_name = get_name_by_telegram_id(sender_id, auth_records)

        await add_log_async(
            faci_name,
            context.user_data["role"],
            f"Reply to {target_name}",
            "MSG_REPLY_FACI",
            "-",
            after_text
        )

        context.user_data.pop("reply_mode", None)
        context.user_data.pop("reply_target", None)

        await update.message.reply_text("✅ Reply sent.")
        return

    text = update.message.text.strip()

    # ===== ADMIN TOGGLE RESET MODE =====
    global RESET_ENABLED

    if (
        context.user_data.get("role") in ["Advisor", "OC", "HOF", "HOGM"]
        and not context.user_data.get("awaiting_password")
        and not context.user_data.get("awaiting_old_password")
        and not context.user_data.get("awaiting_new_password")
    ):
        if text.lower().strip() == "enable reset":

            RESET_ENABLED = not RESET_ENABLED
            status = "🟢 ENABLED" if RESET_ENABLED else "🔴 DISABLED"

            await update.message.reply_text(
                f"🔐 Reset Password Function is now {status}"
            )
            return
    
    # ===== VERIFY OLD PASSWORD =====
    if context.user_data.get("awaiting_old_password"):

        row = context.user_data["reset_row"]
        cell_val = await asyncio.to_thread(auth_sheet.cell, row, 3)

        real_password = str(cell_val.value).strip().lstrip("'")
        input_password = text.strip().lstrip("'")

        if input_password != real_password:
            await update.message.reply_text(
                "❌ Incorrect old password. Please try again:"
            )
            return

        context.user_data.pop("awaiting_old_password")
        context.user_data["awaiting_new_password"] = True

        await update.message.reply_text(
            "✅ Old password verified.\nPlease enter your NEW password:"
        )
        return
    
    # ===== SET NEW PASSWORD =====
    if context.user_data.get("awaiting_new_password"):

        new_pass = text.strip()
        row = context.user_data["reset_row"]

        if len(new_pass) < 5:
            await update.message.reply_text(
                "❌ Password too short (min 5 chars). Try again:"
            )
            return

        records = await get_auth_records_async()
        all_passwords = [
            str(r.get("Password")).strip().lstrip("'")
            for r in records
            if r.get("Password")
        ]
        cell_val = await asyncio.to_thread(auth_sheet.cell, row, 3)
        current_password = str(cell_val.value).strip().lstrip("'")
        new_pass_clean = new_pass.strip().lstrip("'")

        if new_pass_clean == current_password:
            await update.message.reply_text(
                "⚠️ New password cannot be the same as old password."
            )
            return

        if new_pass_clean in all_passwords:
            await update.message.reply_text(
                "❌ This password is already used by another user.\n"
                "Please choose a different one:"
            )
            return

        async with AUTH_WRITE_SEMAPHORE:
            await asyncio.to_thread(auth_sheet.update, range_name=f"C{row}", values=[[f"'{new_pass_clean}"]])

        global AUTH_CACHE, AUTH_CACHE_TIME
        AUTH_CACHE = None
        AUTH_CACHE_TIME = 0
        AUTH_INDEX.clear()

        name = context.user_data["reset_target_name"]

        await update.message.reply_text(
            f"✅ Password reset successful for {name}!\n\n"
            "You can now /start to login."
        )
        return

    if context.user_data.get("attendance_mode"):

        if context.user_data.get("is_proxy"):
            await update.message.reply_text(
                "❌ This account is for game testing and cannot take attendance."
            )
            context.user_data.pop("attendance_mode", None)
            return

        role = context.user_data["role"]
        otp_input = update.message.text.strip()


        if role in ["Facilitators", "Facis and Freshies (Game Test)"]:
            if not FACI_OTP or time.time() > FACI_OTP_EXPIRE:
                await update.message.reply_text("❌ Faci OTP expired.")
                context.user_data.pop("attendance_mode", None)
                return

            if otp_input != FACI_OTP:
                await update.message.reply_text("❌ Invalid Faci OTP.")
                return

            att_type = "facis"
            identifier = context.user_data["name"]

            start_time = FACI_ATTENDANCE_START

        elif role == "Game Masters":
            if not GM_OTP or time.time() > GM_OTP_EXPIRE:
                await update.message.reply_text("❌ GM OTP expired.")
                context.user_data.pop("attendance_mode", None)
                return

            if otp_input != GM_OTP:
                await update.message.reply_text("❌ Invalid GM OTP.")
                return

            att_type = "gm"
            identifier = context.user_data["name"]

            start_time = GM_ATTENDANCE_START

        else:
            await update.message.reply_text("❌ You are not allowed to take attendance.")
            context.user_data.pop("attendance_mode", None)
            return

        key = (att_type, identifier)

        async with ATTENDANCE_LOCK:
            if key in ATTENDANCE_SIGNED[att_type]:
                await update.message.reply_text("⚠️ Attendance already marked.")
                context.user_data.pop("attendance_mode", None)
                return

        records = await get_attendance_records_cached()

        already = False
        for r in records:
            if r["Type"] != att_type or r["Group_Or_Id"] != identifier:
                continue

            record_time = datetime.strptime(
                r["Time"], "%Y-%m-%d %H:%M:%S"
            ).timestamp()

            if start_time and record_time >= start_time:
                already = True
                break

        if already:
            async with ATTENDANCE_LOCK:
                ATTENDANCE_SIGNED[att_type].add(key)
                ATTENDANCE_INDEX[att_type].append({
                    "identifier": identifier,
                    "time": time.time()
                })

            await update.message.reply_text("⚠️ Attendance already marked.")
            context.user_data.pop("attendance_mode", None)
            return

        async with ATTENDANCE_LOCK:
            if key in ATTENDANCE_SIGNED[att_type]:
                await update.message.reply_text("⚠️ Attendance already marked.")
                context.user_data.pop("attendance_mode", None)
                return

            ATTENDANCE_SIGNED[att_type].add(key)
            ATTENDANCE_INDEX[att_type].append({
                "identifier": identifier,
                "time": time.time()
            })

        now_str = datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")

        await ATTENDANCE_QUEUE.put(
            [att_type, role, identifier, "present", now_str]
        )

        await update.message.reply_text("✅ Attendance marked!")
        context.user_data.pop("attendance_mode", None)
        return


    # ===== COIN RANK MODE =====
    if context.user_data.get("coin_mode") == "rank":

        try:
            group, rank = text.split()
            group = int(group)
            rank = int(rank)

            await update.message.reply_text(
                "⏳ Ranking recorded.\nPlease wait..."
            )

            asyncio.create_task(
                process_rank_coins(context, rank, group)
            )

            context.user_data.pop("coin_mode", None)

        except:
            await update.message.reply_text(
                "❌ Format error. Example: 23 1"
            )

        return
    
    # ===== COIN MANUAL MODE =====
    if context.user_data.get("coin_mode") == "manual":
        try:
            change = int(text)
            group = context.user_data["edit_group"]

            context.user_data.pop("coin_mode", None)
            context.user_data.pop("edit_group", None)

            await update.message.reply_text("⏳ Updating coins...")

            await process_manual_coins(update, context, group, change)
            menu = get_menu_by_role(context.user_data["role"])
            await update.message.reply_text(
                "✅ Coins updated successfully.",
                reply_markup=menu
            )

        except ValueError:
            await update.message.reply_text(
                "❌ Invalid format.\nExample:\n+500\n-200"
            )

        except Exception as e:
            await update.message.reply_text(
                f"❌ Update failed:\n{e}"
            )

        return
    
    if context.user_data.get("bf_mode"):

        comment = update.message.text.strip()
        faci = context.user_data["bf_faci"]
        commenter = context.user_data["bf_commenter"]

        try:
            cell = await asyncio.to_thread(best_faci_sheet.find, faci)
            row = cell.row

            headers = await asyncio.to_thread(best_faci_sheet.row_values, 1)
            col = headers.index(commenter) + 1
            
            await asyncio.to_thread(best_faci_sheet.update_cell, row, col, comment)

            context.user_data.pop("bf_mode")
            context.user_data.pop("bf_faci")
            context.user_data.pop("bf_commenter")

            menu = get_menu_by_role(context.user_data["role"])

            await update.message.reply_text(
                "✅ Comment stored successfully!",
                reply_markup=menu
            )

        except Exception as e:
            await update.message.reply_text(f"❌ Failed to save comment:\n{e}")

        return
    
    if context.user_data.get("msg_mode"):
        auth_records = await get_auth_records_async()
        message = update.message.text
        target = context.user_data["msg_group"] 

        sender_role = context.user_data["role"]
        sender_name = context.user_data["name"]
        sender_group = context.user_data.get("group")
        sender_id = update.effective_user.id

    
        if target == "ALL":
            action = "MSG_ALL_FACI"
            log_target = "Facilitators (ALL)"
            all_facis = [
                r for r in auth_records
                if r["Roles"] == "Facilitators"
                and str(r.get("Is_Proxy", "")).upper() != "TRUE"
            ]
        else:
            action = "MSG_GROUP_FACI"
            log_target = f"Facilitators (Group {target})"
            group = int(target)
            all_facis = [
                r for r in auth_records
                if r["Roles"] == "Facilitators"
                and int(r["Group"]) == group
                and str(r.get("Is_Proxy", "")).upper() != "TRUE"
            ]

        targets = [r for r in all_facis if r.get("Telegram_ID")]
        missing = [r["Name"] for r in all_facis if not r.get("Telegram_ID")]

        if sender_group:
            header = (
                f"‼️ ALERT ‼️\n"
                f"📩 Message from {sender_role} {sender_name} (Group {sender_group}):"
            )
        else:
            header = (
                f"‼️ ALERT ‼️\n"
                f"📩 Message from {sender_role} {sender_name}:"
            )

        for i,t in enumerate(targets):
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✉ Reply", callback_data=f"reply_to_{sender_id}")]
            ])

            await TELEGRAM_QUEUE.put((
                int(t["Telegram_ID"]),
                f"{header}\n\n{message}",
                keyboard,
                0
            ))

        after_text = message.strip()
        after_text = after_text[:60] + "..." if len(after_text) > 60 else after_text

        await add_log_async(
            sender_name,
            sender_role,
            log_target,
            action,
            "-",
            after_text
        )

        context.user_data.pop("msg_mode", None)
        context.user_data.pop("msg_group", None)

        menu = get_menu_by_role(sender_role)

        reply = "✅ Message sent successfully."

        if missing:
            reply += (
                "\n\n⚠️ These facilitators have not logged in yet, "
                "so they did NOT receive the message:\n"
                + ", ".join(missing)
            )

        await update.message.reply_text(
            reply,
            reply_markup=menu
        )
        
        return
    
    if "updating_field" in context.user_data:

        field = context.user_data["updating_field"]
        group = int(context.user_data["target_group"])

        if field in ["intl", "local"]:
            try:
                value = int(text)
            except ValueError:
                await update.message.reply_text(
                    "❌ Please enter a valid number."
                )
                return
        else:
            value = text

        if not acquire_action_lock(context, f"update_{group}_{field}", ttl=5):
            await update.message.reply_text("⏳ Please wait...")
            return

        try:
            context.user_data.pop("updating_field", None)
            context.user_data.pop("target_group", None)

            await update.message.reply_text("⏳ Updating group info...")

            async with GROUP_INFO_CACHE_LOCK:

                g = GROUP_INFO_CACHE.get(group)

                if g:

                    if field == "location":
                        g["location"] = value

                    elif field == "intl":
                        g["intl"] = value
                        g["total"] = g["intl"] + g["local"]

                    elif field == "local":
                        g["local"] = value
                        g["total"] = g["intl"] + g["local"]

                    elif field == "groupname":
                        g["name"] = value

                    elif field == "groupslogan":
                        g["slogan"] = value

                    elif field == "degree":
                        g["degree"] = value

                    elif field == "foundation":
                        g["foundation"] = value

            async with GROUP_LOCKS[group]:
                await UPDATE_QUEUE.put((group, field, value))

            await add_log_async(
                context.user_data.get("name", ""),
                context.user_data.get("role", ""),
                f"Group {group}",
                f"Update {FIELD_LABEL[field]}",
                "-",
                value
            )

            summary = await build_group_summary_from_cache(group)

            await update.message.reply_text(
                f"✅ Update queued successfully!\n\n{summary}",
                reply_markup=get_menu_by_role(
                    context.user_data["role"]
                )
            )

        except Exception as e:
            print("Update error:", e)
            await update.message.reply_text(
                "❌ Update failed. Please try again."
            )

        return
    

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for key in ["role", "group", "name", "is_proxy"]:
        context.user_data.pop(key, None)
    await update.message.reply_text("👋 Logged out successfully.")

def has_permission(context, permission):
    role = context.user_data.get("role")

    if not role:
        return False

    return permission in PERMISSIONS.get(role, [])


# =====================
# Handlers
# =====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    import random, string, time

    query = update.callback_query
    
    await safe_answer(query)

    user_id = update.effective_user.id

    async with USER_FLOW_LOCKS[user_id]:
        if not context.user_data.get("role"):
            await safe_edit(query, "⚠ Session expired. Please /start again.")
            return

        action = query.data
        role = context.user_data.get("role")

        if action == "back_menu":
            context.user_data.pop("attendance_mode", None)
            context.user_data.pop("coin_mode", None)
            context.user_data.pop("edit_group", None)

            menu = get_menu_by_role(role)
            await query.edit_message_text("📋 Main Menu", reply_markup=menu)
            return

        if action == "msg_all_faci":

            context.user_data["msg_mode"] = True
            context.user_data["msg_group"] = "ALL"

            await query.edit_message_text(
                "✏ Please enter the message for ALL Facilitators:"
            )
            return
        
        if action.startswith("reply_to_"):

            sender_id = int(action.split("_")[-1])

            context.user_data["reply_mode"] = True
            context.user_data["reply_target"] = sender_id

            await query.message.reply_text(
                "✏ Please type your reply message:"
            )
            return

        if action == "rank_none":

            state = FLOW_STATE["day1"].get(user_id)
            if not state:
                await query.answer("Session expired.", show_alert=True)
                return

            rank = state["current_rank"]

            state["results"][rank] = {
                "group": None,
                "add": 0
            }

            rank += 1
            state["current_rank"] = rank

            if rank > 4:
                await show_day1_summary(query, state)
                return

            await show_next_rank(query, state)
            return
            
        if action.startswith("update_group_"):

            group = int(action.split("_")[-1])

            context.user_data["target_group"] = group

            submenu = get_update_submenu()

            await query.edit_message_text(
                f"✏ Updating Group {group}\n\nSelect field to update:",
                reply_markup=submenu
            )
            return

        if action.startswith("rank_group_"):

            state = FLOW_STATE["day1"].get(user_id)
            if not state:
                await query.answer("Session expired. Please restart.", show_alert=True)
                return

            group = int(action.split("_")[-1])

            if group in state["used"]:
                await query.answer("This group already selected!", show_alert=True)
                return

            rank = state["current_rank"]
            add = DAY1_RANK[rank]

            state["used"].add(group)

            state["results"][rank] = {
                "group": group,
                "add": add
            }

            rank += 1
            state["current_rank"] = rank

            if rank > 4:
                await show_day1_summary(query, state)
                return

            await show_next_rank(query, state)
            return
        
        if action.startswith("undo_"):

            tx_id = action.split("_")[1]

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes Undo", callback_data=f"confirm_undo_{tx_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="back_menu")]
            ])

            await safe_edit(
                query,
                f"⚠ Undo this transaction?\n\nTX: {tx_id}",
                reply_markup=keyboard
            )
            return
        
        if action.startswith("confirm_undo_"):

            tx_id = action.split("_")[2]

            if tx_id in UNDONE_TX:
                await safe_edit(query, "❌ Already undone.")
                return

            await safe_edit(query, "⏳ Reverting...")

            await undo_transaction(context, tx_id)

            menu = get_menu_by_role(context.user_data["role"])
            await query.message.reply_text("📋 Main Menu", reply_markup=menu)
            return
        
        if action == "confirm_day1":

            state = FLOW_STATE["day1"].pop(user_id, None)

            await safe_edit(query, "⏳ Applying Day 1 Ranking...")

            if state:
                import uuid
                tx_id = uuid.uuid4().hex[:6]

                await process_day1_ranking_from_state(context, state, tx_id)

                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("↩ Undo", callback_data=f"undo_{tx_id}")]
                ])

                await query.message.reply_text(
                    f"✅ Day 1 applied!\n\nTX: {tx_id}",
                    reply_markup=keyboard
                )

            menu = get_menu_by_role(context.user_data["role"])
            await query.message.reply_text("📋 Main Menu", reply_markup=menu)
            return
        
        if action == "confirm_day2":

            if not acquire_action_lock(context, "day2_confirm", ttl=3):
                await safe_edit(query, "⏳ Processing...")
                return

            state = FLOW_STATE["day2"].pop(user_id, None)

            await safe_edit(query, "⏳ Applying Day 2 PK...")

            if state:
                import uuid
                tx_id = uuid.uuid4().hex[:6]

                await process_day2_pk(context, state["win"], state["lose"], tx_id)

                # ✅ 回 user + Undo 按钮
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("↩ Undo", callback_data=f"undo_{tx_id}")]
                ])

                await query.message.reply_text(
                    f"✅ PK queued successfully!\n\nTX: {tx_id}",
                    reply_markup=keyboard
                )

            menu = get_menu_by_role(context.user_data["role"])
            await query.message.reply_text("📋 Main Menu", reply_markup=menu)
            return

        if action == "attendance":

            if not has_permission(context, "attendance"):
                await query.edit_message_text("❌ You don't have permission.")
                return

            keyboard = [
                [InlineKeyboardButton("📋 Faci Attendance (Generate OTP)", callback_data="att_facis")],
                [InlineKeyboardButton("🎮 GM Attendance (Generate OTP)", callback_data="att_gm")],
                [InlineKeyboardButton("❌ Check Faci Attendance", callback_data="attendance_check_facis")],
                [InlineKeyboardButton("❌ Check GM Attendance", callback_data="attendance_check_gm")],
                [InlineKeyboardButton("⬅ Back", callback_data="back_menu")]
            ]

            await query.edit_message_text(
                "📋 Select attendance type:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        if action == "user_attendance":

            context.user_data["attendance_mode"] = True

            await query.edit_message_text(
                "📋 Please enter the OTP for attendance:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅ Back to Menu", callback_data="back_menu")]
                ])
            )
            return

        if action == "att_facis":
            global FACI_OTP, FACI_OTP_EXPIRE, FACI_ATTENDANCE_START

            FACI_OTP = ''.join(random.choices(string.digits, k=5))
            FACI_OTP_EXPIRE = time.time() + 1200
            FACI_ATTENDANCE_START = time.time()
            ATTENDANCE_SIGNED["facis"].clear()
            ATTENDANCE_INDEX["facis"].clear()

            await query.edit_message_text(
                f"🔐 OTP for FACI attendance:\n\n{FACI_OTP}\n\nValid 20 minutes",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅ Back", callback_data="attendance")]
                ])
            )
            return


        if action == "att_gm":
            global GM_OTP, GM_OTP_EXPIRE, GM_ATTENDANCE_START

            GM_OTP = ''.join(random.choices(string.digits, k=5))
            GM_OTP_EXPIRE = time.time() + 1200
            GM_ATTENDANCE_START = time.time()
            ATTENDANCE_SIGNED["gm"].clear()
            ATTENDANCE_INDEX["gm"].clear()

            await query.edit_message_text(
                f"🔐 OTP for GM attendance:\n\n{GM_OTP}\n\nValid 20 minutes",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅ Back", callback_data="attendance")]
                ])
            )
            return
        
        if action == "attendance_check_facis":

            await safe_answer(query, "Loading...")

            start_time = FACI_ATTENDANCE_START

            if not start_time:
                menu = get_menu_by_role(context.user_data["role"])
                await query.message.reply_text(
                    "❌ No faci attendance session started yet.",
                    reply_markup=menu
                )
                return

            present_facis = {
                r["identifier"]
                for r in ATTENDANCE_INDEX["facis"]
                if r["time"] >= start_time
            }

            groups = {}

            for r in await get_auth_records_async():
                if (
                    r["Roles"] == "Facilitators"
                    and str(r.get("Is_Proxy", "")).upper() != "TRUE"
                ):
                    g = int(r["Group"])
                    groups.setdefault(g, []).append(r["Name"])

            text = "📋 Faci Attendance Status\n\n"

            for g in range(1, 29):
                faci_list = groups.get(g, [])
                total = len(faci_list)

                if total == 0:
                    continue

                present_names = [n for n in faci_list if n in present_facis]
                absent_names = [n for n in faci_list if n not in present_facis]

                present = len(present_names)

                if present == total:
                    status = "✅"
                elif present == 0:
                    status = "❌"
                else:
                    status = "⚠️"

                text += f"Group {g} {status} ({present}/{total})\n"

                if absent_names:
                    text += "❌ Absent: " + ", ".join(absent_names) + "\n"

                text += "\n"

            menu = get_menu_by_role(role)
            await query.message.reply_text(text, reply_markup=menu)
            return

        
        if action == "attendance_check_gm":

            await safe_answer(query, "Loading...")

            start_time = GM_ATTENDANCE_START

            if not start_time:
                menu = get_menu_by_role(context.user_data["role"])
                await query.message.reply_text(
                    "❌ No GM attendance session started yet.",
                    reply_markup=menu
                )
                return

            present_gm = {
                r["identifier"]
                for r in ATTENDANCE_INDEX["gm"]
                if r["time"] >= start_time
            }

            all_gms = [
                r["Name"]
                for r in await get_auth_records_async()
                if r["Roles"] == "Game Masters"
            ]

            text = "🎮 GM Attendance Status\n\n"

            for gm in all_gms:
                text += f"{gm} {'✅' if gm in present_gm else '❌'}\n"

            menu = get_menu_by_role(context.user_data["role"])

            await query.message.reply_text(
                text,
                reply_markup=menu
            )
            return
        
        if action == "msg_faci":

            keyboard = []
            row = []

            for i in range(1, 29):
                row.append(
                    InlineKeyboardButton(
                        f"Group {i}",
                        callback_data=f"msg_group_{i}"
                    )
                )
                if len(row) == 4:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back_menu")])

            await query.edit_message_text(
                "📩 Select group to message:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if action.startswith("msg_group_"):
            await query.answer("Loading facilitators...")

            auth_records = await get_auth_records_async()
            group = int(action.split("_")[-1])

            faci_names = [
                r["Name"]
                for r in auth_records
                if r["Roles"] == "Facilitators" and int(r["Group"]) == group
            ]

            names_text = ", ".join(faci_names) if faci_names else "No facilitators found"

            if not faci_names:
                await query.edit_message_text(
                    f"❌ No facilitators found in Group {group}.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅ Back", callback_data="msg_faci")]
                    ])
                )
                return

            context.user_data["msg_group"] = group
            context.user_data["msg_mode"] = True

            await query.edit_message_text(
                f"✏ Please enter the message for Group {group} Facilitators:\n\n"
                f"👥 {names_text}"
            )
            return


        if action == "best_faci_comment":

            await query.answer("Loading facilitators...")

            records = await asyncio.to_thread(best_faci_sheet.get_all_records)

            keyboard = []
            row = []

            for r in records:
                name = r["Facilitator"]
                group = r["Group"]

                safe_name = name.replace(" ", "__")

                row.append(

                    InlineKeyboardButton(
                        f"{name} (Group {group})",
                        callback_data=f"bf_faci_{safe_name}"
                    )
                )

                if len(row) == 2:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back_menu")])

            await query.edit_message_text(
                "📝 Select a Facilitator:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        if action.startswith("bf_faci_"):

            faci_name = action.replace("bf_faci_", "").replace("__", " ")
            context.user_data["bf_faci"] = faci_name

            headers = await asyncio.to_thread(best_faci_sheet.row_values, 1)

            commenters = headers[2:]  

            keyboard = []
            row = []

            for c in commenters:
                row.append(
                    InlineKeyboardButton(
                        c,
                        callback_data=f"bf_by_{c}"
                    )
                )

                if len(row) == 2:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="best_faci_comment")])

            await query.edit_message_text(
                f"✍ Select commenter for {faci_name}:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        if action.startswith("bf_by_"):

            commenter = action.replace("bf_by_", "")

            context.user_data["bf_commenter"] = commenter
            context.user_data["bf_mode"] = True

            await query.edit_message_text(
                f"✏ Please type your comment:\n\n"
                f"Facilitator: {context.user_data['bf_faci']}\n"
                f"From: {commenter}"
            )
            return

        # ================= LOGOUT =================
        if action == "logout":
            for key in ["role", "group", "name", "is_proxy"]:
                context.user_data.pop(key, None)
            await query.edit_message_text("👋 Logged out successfully.")
            return

        # ================= OPEN UPDATE SUB MENU =================
        if action == "update_group":

            if not has_permission(context, "update_group_data"):
                await query.edit_message_text("❌ You don't have permission.")
                return

            role = context.user_data["role"]
            
            if role in ["Advisor", "OC", "HOF", "HOGM"]:

                keyboard = []
                row = []

                for i in range(1, 29):
                    row.append(InlineKeyboardButton(f"Group {i}", callback_data=f"update_group_{i}"))
                    if len(row) == 4:
                        keyboard.append(row)
                        row = []

                if row:
                    keyboard.append(row)

                keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back_menu")])

                await query.edit_message_text(
                    "✏ Select group to update:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            context.user_data["target_group"] = context.user_data["group"]

            submenu = get_update_submenu()
            await query.edit_message_text(
                "✏ Select field to update:",
                reply_markup=submenu
            )
            return

        if action.startswith("field_"):
            field = action.replace("field_", "")
            context.user_data["updating_field"] = field

            label = FIELD_LABEL.get(field, field)

            await query.edit_message_text(
                f"✏ Please enter the new {label}:"
            )

            return

        if action == "check_my_coins":

            group = context.user_data["group"]
            coins = await get_coins_async(group)

            menu = get_menu_by_role(role)

            await query.edit_message_text(
                f"💰 Group {group} Atlantis Coins:\n{coins}",
                reply_markup=menu
            )
            return

        # ================= GM EDIT COINS MANUAL =================
        if action == "edit_coins":

            if not has_permission(context, "manage_coins"):
                await query.edit_message_text("❌ You don't have permission.")
                return

            keyboard = []
            row = []

            for i in range(1, 29):

                row.append(
                    InlineKeyboardButton(str(i), callback_data=f"edit_group_{i}")
                )

                if len(row) == 4:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)


            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="manage_coins")])

            await query.edit_message_text(
                "📥 Select group to edit coins:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        if action.startswith("edit_group_"):

            group = int(action.split("_")[-1])

            context.user_data["edit_group"] = group
            context.user_data["coin_mode"] = "manual"

            keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅ Back to Menu", callback_data="back_menu")]
                    ])

            await query.edit_message_text(
                f"➕➖ Enter amount for Group {group}:\n\nExample:\n+500\n-200",
                reply_markup=keyboard
            )
            return

        # ================= ADMIN VIEW ALL COINS =================
        if action == "view_all_coins":

            if not has_permission(context, "view_all_coins"):
                await query.edit_message_text("❌ You don't have permission.")
                return

            await get_coins_async(1)

            records = [
                {"Group": g, "Atlantis Coins": c}
                for g, c in sorted(COINS_CACHE.items())
            ]
            
            await safe_answer(query, "Loading coins...")
            
            text = "📊 Atlantis Coins Summary:\n\n"

            for r in records:
                group = r["Group"]
                coins = r["Atlantis Coins"]

                text += f"Group {group}: {coins} Atlantis Coins\n"

            menu = get_menu_by_role(role)

            await query.edit_message_text(
                text,
                reply_markup=menu
            )

            context.user_data.pop("viewing_coins", None)
            return
        
        if action == "manage_coins":

            if not has_permission(context, "manage_coins"):
                await query.edit_message_text("❌ You don't have permission.")
                return

            keyboard = [
                [InlineKeyboardButton("🏆 Day 1 Ranking", callback_data="coin_day1")],
                [InlineKeyboardButton("⚔ Day 2 PK", callback_data="coin_day2")],
                [InlineKeyboardButton("⬅ Back", callback_data="back_menu")]
            ]

            version = new_ui_page(context)

            await query.edit_message_text(
                "💰 Select Coins Mode:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        if action == "coin_day1":

            FLOW_STATE["day1"][user_id] = {
                "current_rank": 1,
                "results": {},
                "used": set()
            }

            state = FLOW_STATE["day1"][user_id]

            keyboard = []
            row = []

            for i in range(1, 29):
                row.append(
                    InlineKeyboardButton(str(i), callback_data=f"rank_group_{i}")
                )
                if len(row) == 4:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("🚫 No group", callback_data="rank_none")])
            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="manage_coins")])

            await query.edit_message_text(
                "🥇 Select Group for Rank 1 (+1000🪙):",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return


        if action == "coin_day2":

            FLOW_STATE["day2"][user_id] = {
                "win": None,
                "lose": None
            }

            keyboard = []
            row = []

            for i in range(1, 29):
                row.append(
                    InlineKeyboardButton(str(i), callback_data=f"pk_win_{i}")
                )
                if len(row) == 4:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("🚫 No group", callback_data="pk_win_none")])
            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="manage_coins")])

            await query.edit_message_text(
                "⚔ Day 2 PK\n\n🏆 Select WINNER group:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if action.startswith("pk_win_"):

            state = FLOW_STATE["day2"].get(user_id)
            if not state:
                await query.answer("Session expired.", show_alert=True)
                return

            win = None if action == "pk_win_none" else int(action.split("_")[-1])
            state["win"] = win

            keyboard = []
            row = []

            for i in range(1, 29):
                if i == win:
                    continue
                row.append(
                    InlineKeyboardButton(str(i), callback_data=f"pk_lose_{i}")
                )
                if len(row) == 4:
                    keyboard.append(row)
                    row = []

            if row:
                keyboard.append(row)

            keyboard.append([InlineKeyboardButton("🚫 No group", callback_data="pk_lose_none")])
            keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="manage_coins")])

            await query.edit_message_text(
                "📉 Select LOSER group:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if action.startswith("pk_lose_"):

            state = FLOW_STATE["day2"].get(user_id)
            if not state:
                await query.answer("Session expired.", show_alert=True)
                return

            lose = None if action == "pk_lose_none" else int(action.split("_")[-1])
            state["lose"] = lose

            win = state["win"]

            summary = "⚔ Day 2 PK Summary\n\n"
            summary += f"🏆 Winner: {'🚫 No Group' if win is None else f'Group {win}'}\n"
            summary += f"💪 Runner-up: {'🚫 No Group' if lose is None else f'Group {lose}'}\n\n"
            summary += "Please confirm to apply coins."

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm PK Result", callback_data="confirm_day2")],
                [InlineKeyboardButton("❌ Cancel", callback_data="manage_coins")]
            ])

            await query.edit_message_text(summary, reply_markup=keyboard)
            return


        if action == "view_logs":

            await query.answer("Loading logs...")

            records = await get_logs_cached()

            text = "📊 Recent Logs:\n\n"

            for r in records[-30:]:   
                text += (
                    f"{r['Timestamp']} | {r['Operator']} ({r['Operator Role']})\n"
                    f"{r['Target']} | {r['Action']} : {r['Before']} → {r['After']}\n\n"
                )

            menu = get_menu_by_role(role)

            await safe_edit(query, text, reply_markup=menu)
            return
        
        await query.edit_message_text("⚠ Unknown action.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id

    context.user_data.clear()

    FLOW_STATE["day1"].pop(user_id, None)
    FLOW_STATE["day2"].pop(user_id, None)

    context.user_data["_ui_version"] = str(time.time())
    context.user_data["awaiting_password"] = True

    await update.message.reply_text(
        "🔄 Session restarted.\n\n"
        "🦢 Honk Honk!\n"
        "🔐 Please enter your password:"
    )

def main():
    from telegram.ext import CallbackQueryHandler

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)  
        .build()
    )
    
    app.add_handler(CommandHandler("reset", reset_password_start))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("logout", logout))
    app.add_handler(CommandHandler("refresh_cache", refresh_cache))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    app.add_handler(CommandHandler("id", show_telegram_id))

    print("🤖 Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
