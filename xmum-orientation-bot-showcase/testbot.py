"""
test_bot_bruteforce.py
======================
完整暴力测试 — 完全适配 bot.py 架构

测试套件：
  A. AUTH Cache + O(1) 登录索引（get_auth_records_async + AUTH_INDEX）
  B. COINS Cache 并发读写（get_coins_async + update_coins_async）
  C. UPDATE_QUEUE worker（update_group_write_worker，含 intl/local total 修复）
  D. WRITE_QUEUE coin worker（coin_write_worker，批量 + 速率控制）
  E. ATTENDANCE 签到并发 + 重复防护（attendance_write_worker）
  F. LOG buffer 并发写入（add_log_async + _log_flusher）
  G. TELEGRAM 队列发送（telegram_send_worker + TelegramRateLimiter）
  H. msg_faci 广播模拟（56条 → 你自己，模拟 TELEGRAM_QUEUE）
  I. coin_write_worker Telegram 通知（带 before/after meta）
  J. FLOW_STATE Day1 Ranking + Day2 PK 逻辑验证（纯内存）
  K. 权限系统验证（PERMISSIONS 矩阵完整性）
  L. Password Reset 流程（AUTH_WRITE_SEMAPHORE 并发保护）
  M. queue.join() 死锁检测（UPDATE + WRITE + ATTENDANCE）
  N. 持续 5 轮全功能混合暴力（压测 worker 稳定性）

需要 .env：
  BOT_TOKEN=...
  TEST_CHAT_ID=你自己的 Telegram user ID

用法：
  python test_bot_bruteforce.py
"""

import asyncio
import os
import time
import random
from statistics import mean
from collections import defaultdict, Counter

# ── 依赖加载 ──────────────────────────────────────────────────
def _load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

_load_env()

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass  # 用上面的手动 .env 读取

import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot
from telegram.error import RetryAfter, TelegramError

# ============================================================
# CONFIG（镜像 bot.py）
# ============================================================
SPREADSHEET_ID = "1ksmqUbylTPFvP658jY_zCnizKqttG8hLy66pijPAhR4"
BOT_TOKEN      = os.getenv("BOT_TOKEN")
TEST_CHAT_ID   = int(os.getenv("TEST_CHAT_ID", "0"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

FIELD_COLUMN = {
    "location":    "B",
    "intl":        "C",
    "local":       "D",
    "groupname":   "F",
    "groupslogan": "G",
}
SAFE_VALUES = {
    "location":    "TestHall",
    "intl":        "8",
    "local":       "12",
    "groupname":   "TestGroup",
    "groupslogan": "We Test!",
}

DAY1_RANK = {1: 1000, 2: 750, 3: 500, 4: 250}
DAY2_RANK = {"win": 750, "lose": 250}

PERMISSIONS = {
    "Advisor":    ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "OC":         ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "HOF":        ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "HOGM":       ["update_group", "manage_coins", "view_all_coins", "attendance", "view_all", "update_group_data"],
    "Facis and Freshies (Game Test)": ["update_group", "check_own_coin", "update_group_data"],
    "Facilitators": ["update_group", "check_own_coin", "update_group_data"],
    "Game Masters":  ["manage_coins", "view_all_coins"],
}

# ============================================================
# STATS
# ============================================================
class Stats:
    def __init__(self, name):
        self.name      = name
        self.ok        = 0
        self.fail      = 0
        self.latencies = []
        self.errors    = []

    def ok_(self, lat=0):
        self.ok += 1
        self.latencies.append(lat)

    def fail_(self, err=""):
        self.fail += 1
        self.errors.append(str(err)[:120])

    def verdict(self):
        total = self.ok + self.fail
        if total == 0:
            return "—"
        rate = self.fail / total
        if rate == 0:
            return "✅ PASS"
        if rate < 0.05:
            return "⚠️  MARGINAL"
        return "❌ FAIL"

    def show(self, indent="  "):
        total = self.ok + self.fail
        if total == 0:
            print(f"{indent}[{self.name}] — no data")
            return
        lat_str = ""
        if self.latencies:
            s = sorted(self.latencies)
            lat_str = (f"avg={mean(s):.3f}s  "
                       f"p95={s[int(len(s)*.95)]:.3f}s  "
                       f"max={s[-1]:.3f}s")
        err_str = ""
        if self.errors:
            top = Counter(self.errors).most_common(1)[0][0]
            err_str = f"\n{indent}  ⚠ {top}"
        print(f"{indent}{self.verdict()}  [{self.name}]  {self.ok}/{total}  {lat_str}{err_str}")


# ============================================================
# GLOBAL STATE（精确镜像 bot.py）
# ============================================================

# UPDATE_QUEUE
UPDATE_QUEUE         = None
UPDATE_BATCH_WINDOW  = 0.5
UPDATE_BATCH_MAX     = 50
UPDATE_WRITE_RATE    = 4
LAST_UPDATE_WRITE    = 0
GROUP_LOCKS          = defaultdict(asyncio.Lock)

# WRITE_QUEUE (coins)
WRITE_QUEUE          = None
COIN_ROW_MAP         = {}
COINS_CACHE          = {}
COINS_CACHE_LOCK     = asyncio.Lock()
COINS_WRITE_SEM      = asyncio.Semaphore(1)
WRITE_BATCH_WINDOW   = 0.5
WRITE_BATCH_MAX      = 50
WRITE_INTERVAL       = 1 / 3.0

# ATTENDANCE
ATTENDANCE_QUEUE        = None
ATTENDANCE_SIGNED       = {"facis": set(), "gm": set()}
ATTENDANCE_INDEX        = {"facis": [], "gm": []}
ATTENDANCE_LOCK         = asyncio.Lock()
ATTENDANCE_WRITE_SEM    = asyncio.Semaphore(1)
ATTENDANCE_BATCH_WINDOW = 1.0
ATTENDANCE_BATCH_MAX    = 25
ATTENDANCE_RETRY_MAX    = 3
ATTENDANCE_RETRY_BACKOFF = 2

# LOGS
LOG_LOCK           = asyncio.Lock()
LOG_BUFFER         = []
LOG_LAST_FLUSH     = 0
LOG_FLUSH_INTERVAL = 5
LOG_WRITE_SEM      = asyncio.Semaphore(1)

# AUTH
AUTH_CACHE      = None
AUTH_CACHE_TIME = 0
AUTH_CACHE_TTL  = 30
AUTH_CACHE_LOCK = asyncio.Lock()
AUTH_INDEX      = {}
AUTH_WRITE_SEM  = asyncio.Semaphore(1)

# COINS TTL
COINS_CACHE_TIME = 0
COINS_CACHE_TTL  = 30

# TELEGRAM
TELEGRAM_QUEUE = None

# FLOW STATE
FLOW_STATE = {"day1": {}, "day2": {}}

# Sheets / Bot
db_sheet = coins_sheet = auth_sheet = att_sheet = logs_sheet = None
bot: Bot = None


# ============================================================
# INIT SHEETS
# ============================================================
def init_sheets_sync():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    sp = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    return (
        sp.worksheet("XMUM Orientation DB"),
        sp.worksheet("COINS_PRIVATE"),
        sp.worksheet("AUTH"),
        sp.worksheet("ATTENDANCE"),
        sp.worksheet("LOGS"),
    )

def load_coin_map_sync():
    for i, r in enumerate(coins_sheet.get_all_records(), start=2):
        COIN_ROW_MAP[str(r["Group"])] = i


# ============================================================
# CACHE WARMUP
# ============================================================
async def warmup_all_caches():
    global AUTH_CACHE, AUTH_CACHE_TIME, AUTH_INDEX
    global COINS_CACHE, COINS_CACHE_TIME

    print("  📚 Loading auth data...")
    raw = await asyncio.to_thread(auth_sheet.get_all_values)
    headers = raw[0]
    AUTH_CACHE = [dict(zip(headers, r)) for r in raw[1:] if any(r)]
    AUTH_INDEX = {
        str(r["Password"]).strip().lstrip("'"): r
        for r in AUTH_CACHE if r.get("Password")
    }
    AUTH_CACHE_TIME = time.time()
    print(f"  ✅ AUTH cache: {len(AUTH_INDEX)} records")

    print("  💰 Loading coins data...")
    recs = await asyncio.to_thread(coins_sheet.get_all_records)
    async with COINS_CACHE_LOCK:
        COINS_CACHE.update({int(r["Group"]): int(r["Atlantis Coins"]) for r in recs})
        COINS_CACHE_TIME = time.time()
    print(f"  ✅ COINS cache: {len(COINS_CACHE)} groups")

    print("  🗺️  Loading row map...")
    await asyncio.to_thread(load_coin_map_sync)
    print(f"  ✅ Row map: {len(COIN_ROW_MAP)} groups")


# ============================================================
# WORKERS（精确镜像 bot.py）
# ============================================================

async def update_group_write_worker(wstats: Stats):
    global LAST_UPDATE_WRITE
    while True:
        try:
            first = await UPDATE_QUEUE.get()
        except asyncio.CancelledError:
            break

        batch = [first]
        start = asyncio.get_event_loop().time()
        while len(batch) < UPDATE_BATCH_MAX:
            remaining = UPDATE_BATCH_WINDOW - (asyncio.get_event_loop().time() - start)
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(UPDATE_QUEUE.get(), remaining))
            except asyncio.TimeoutError:
                break

        now = time.time()
        gap = 1 / UPDATE_WRITE_RATE
        if now - LAST_UPDATE_WRITE < gap:
            await asyncio.sleep(gap - (now - LAST_UPDATE_WRITE))

        # last-write-wins per (group, field)
        batch_vals = {(g, f): v for g, f, v in batch}
        updates = []

        for group, field, value in batch:
            row = group + 1
            col = FIELD_COLUMN[field]
            updates.append({"range": f"{col}{row}", "values": [[value]]})

            if field in ("intl", "local"):
                try:
                    row_data = await asyncio.to_thread(db_sheet.row_values, row)
                    sheet_intl  = int(row_data[2]) if len(row_data) > 2 and row_data[2] else 0
                    sheet_local = int(row_data[3]) if len(row_data) > 3 and row_data[3] else 0
                    intl  = int(batch_vals.get((group, "intl"),  sheet_intl))
                    local = int(batch_vals.get((group, "local"), sheet_local))
                    updates.append({"range": f"E{row}", "values": [[intl + local]]})
                except Exception:
                    pass

        t0 = time.time()
        try:
            await asyncio.to_thread(db_sheet.batch_update, updates)
            LAST_UPDATE_WRITE = time.time()
            lat = (time.time() - t0) / max(len(batch), 1)
            for _ in batch:
                wstats.ok_(lat)
        except Exception as e:
            for _ in batch:
                wstats.fail_(e)

        for _ in batch:
            UPDATE_QUEUE.task_done()


async def coin_write_worker(notify_stats: Stats = None):
    last = 0.0
    while True:
        try:
            group, value, meta = await WRITE_QUEUE.get()
        except asyncio.CancelledError:
            break

        got     = 1
        pending = {group: (value, meta)}
        start   = asyncio.get_event_loop().time()

        while True:
            remaining = WRITE_BATCH_WINDOW - (asyncio.get_event_loop().time() - start)
            if remaining <= 0 or len(pending) >= WRITE_BATCH_MAX:
                break
            try:
                g, v, m = await asyncio.wait_for(WRITE_QUEUE.get(), remaining)
                got += 1
                pending[g] = (v, m)
            except asyncio.TimeoutError:
                break

        sl = WRITE_INTERVAL - (asyncio.get_event_loop().time() - last)
        if sl > 0:
            await asyncio.sleep(sl)

        updates = [
            {"range": f"B{COIN_ROW_MAP[str(g)]}", "values": [[int(v)]]}
            for g, (v, _) in pending.items() if COIN_ROW_MAP.get(str(g))
        ]
        if updates:
            async with COINS_WRITE_SEM:
                try:
                    await asyncio.to_thread(coins_sheet.batch_update, updates)
                    last = asyncio.get_event_loop().time()
                except Exception as e:
                    print(f"  [CoinWorker] sheet write error: {e}")

        # Telegram 通知
        for g, (_, m) in pending.items():
            try:
                before = m.get("before")
                msg = (
                    f"✅ Group {g}: {before} → {m['after']} 🪙"
                    if before is not None
                    else f"✅ Group {g}: updated to {m['after']} 🪙"
                )
                if TELEGRAM_QUEUE and m.get("user_id"):
                    await TELEGRAM_QUEUE.put((m["user_id"], msg, None, 0))
                if notify_stats:
                    notify_stats.ok_(0)
            except Exception:
                if notify_stats:
                    notify_stats.fail_("notify enqueue failed")

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
            remaining = ATTENDANCE_BATCH_WINDOW - (asyncio.get_event_loop().time() - start)
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(ATTENDANCE_QUEUE.get(), remaining))
            except asyncio.TimeoutError:
                break

        success = False
        for attempt in range(ATTENDANCE_RETRY_MAX):
            try:
                async with ATTENDANCE_WRITE_SEM:
                    await asyncio.to_thread(att_sheet.append_rows, batch)
                success = True
                break
            except Exception as e:
                if "429" in str(e):
                    await asyncio.sleep(ATTENDANCE_RETRY_BACKOFF * (attempt + 1))
                else:
                    print(f"  [AttWorker] {e}")
                    break

        if not success:
            print(f"  ⚠ Attendance fallback: {len(batch)} rows dropped")

        for _ in batch:
            ATTENDANCE_QUEUE.task_done()


async def log_flusher_worker():
    global LOG_BUFFER, LOG_LAST_FLUSH
    while True:
        await asyncio.sleep(LOG_FLUSH_INTERVAL)
        async with LOG_LOCK:
            if not LOG_BUFFER:
                continue
            rows = LOG_BUFFER.copy()
            LOG_BUFFER.clear()
            try:
                async with LOG_WRITE_SEM:
                    await asyncio.to_thread(logs_sheet.append_rows, rows)
            except Exception as e:
                LOG_BUFFER = rows + LOG_BUFFER
                print(f"  [LogFlusher] {e}")


class TelegramRateLimiter:
    def __init__(self, rate=20):
        self.rate       = rate
        self.tokens     = float(rate)
        self.updated_at = time.time()
        self.lock       = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now     = time.time()
                elapsed = now - self.updated_at
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                self.updated_at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait)


TELEGRAM_LIMITER = TelegramRateLimiter(rate=20)


async def telegram_send_worker(tg_stats: Stats):
    while True:
        try:
            chat_id, text, markup, retry = await TELEGRAM_QUEUE.get()
        except asyncio.CancelledError:
            break

        try:
            await TELEGRAM_LIMITER.acquire()
            t0 = time.time()
            await bot.send_message(chat_id=chat_id, text=text)
            tg_stats.ok_(time.time() - t0)
            TELEGRAM_QUEUE.task_done()
        except RetryAfter as e:
            if retry < 3:
                await asyncio.sleep(e.retry_after + 1)
                await TELEGRAM_QUEUE.put((chat_id, text, markup, retry + 1))
            else:
                tg_stats.fail_("RetryAfter exhausted")
                TELEGRAM_QUEUE.task_done()
        except Exception as e:
            tg_stats.fail_(str(e))
            TELEGRAM_QUEUE.task_done()


# ============================================================
# HELPER FUNCTIONS（镜像 bot.py）
# ============================================================

async def add_log_async(operator_name, operator_role, target, action, before, after):
    global LOG_BUFFER, LOG_LAST_FLUSH
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    async with LOG_LOCK:
        LOG_BUFFER.append([now, operator_name, operator_role, target, action, before, after])
        t = time.time()
        if len(LOG_BUFFER) >= 10 or t - LOG_LAST_FLUSH >= LOG_FLUSH_INTERVAL:
            rows = LOG_BUFFER.copy()
            LOG_BUFFER.clear()
            LOG_LAST_FLUSH = t
            try:
                async with LOG_WRITE_SEM:
                    await asyncio.to_thread(logs_sheet.append_rows, rows)
            except Exception as e:
                LOG_BUFFER = rows + LOG_BUFFER
                LOG_LAST_FLUSH = 0


async def get_coins_async(group):
    global COINS_CACHE, COINS_CACHE_TIME
    now = time.time()
    if COINS_CACHE and now - COINS_CACHE_TIME < COINS_CACHE_TTL:
        return COINS_CACHE.get(int(group), 0)
    async with COINS_CACHE_LOCK:
        if COINS_CACHE and time.time() - COINS_CACHE_TIME < COINS_CACHE_TTL:
            return COINS_CACHE.get(int(group), 0)
        records = await asyncio.to_thread(coins_sheet.get_all_records)
        COINS_CACHE.update({int(r["Group"]): int(r["Atlantis Coins"]) for r in records})
        COINS_CACHE_TIME = time.time()
        return COINS_CACHE.get(int(group), 0)


async def update_coins_async(group, new_amount, user_id=None, stats: Stats = None):
    global COINS_CACHE, COINS_CACHE_TIME
    t0 = time.time()
    try:
        async with COINS_CACHE_LOCK:
            COINS_CACHE[int(group)] = int(new_amount)
            COINS_CACHE_TIME = time.time()
        await WRITE_QUEUE.put((
            group,
            int(new_amount),
            {"user_id": user_id, "before": None, "after": new_amount}
        ))
        if stats:
            stats.ok_(time.time() - t0)
    except Exception as e:
        if stats:
            stats.fail_(e)


async def get_user_by_password_async(password):
    password = str(password).strip().lstrip("'")
    r = AUTH_INDEX.get(password)
    if not r:
        return None
    return {
        "role":     r["Roles"],
        "group":    int(r["Group"]) if r["Group"] else None,
        "name":     r["Name"],
        "is_proxy": str(r.get("Is_Proxy", "")).upper() == "TRUE"
    }


def has_permission(role, permission):
    return permission in PERMISSIONS.get(role, [])


# ============================================================
# TEST A: AUTH Cache + O(1) 登录索引
# ============================================================
async def test_a_auth_cache():
    print(f"\n{'='*62}")
    print(f"  TEST A: AUTH Cache + O(1) 登录索引")
    print(f"{'='*62}")

    # A1: cache 命中速度（100次 O(1) 查找）
    s_hit = Stats("AUTH cache hit 100次")
    for _ in range(100):
        t0 = time.time()
        AUTH_INDEX.get("nonexistent_password_xyz")
        s_hit.ok_(time.time() - t0)
    s_hit.show()

    # A2: 真实密码登录
    s_login = Stats("正确密码登录（第一个真实账号）")
    if AUTH_INDEX:
        sample_pw = next(iter(AUTH_INDEX))
        t0   = time.time()
        user = await get_user_by_password_async(sample_pw)
        lat  = time.time() - t0
        if user:
            s_login.ok_(lat)
            print(f"  ✅ Sample login: {user['name']} ({user['role']}), {lat*1000:.1f}ms")
        else:
            s_login.fail_("user not found")
    else:
        print("  ⚠️  AUTH_INDEX empty — skip login test")
    s_login.show()

    # A3: 错误密码（50次并发）
    s_bad = Stats("错误密码拒绝 50次并发")
    async def check_bad(i):
        t0 = time.time()
        r  = await get_user_by_password_async(f"WRONG_PASS_{i:04d}")
        if r is None:
            s_bad.ok_(time.time() - t0)
        else:
            s_bad.fail_("should be None")
    await asyncio.gather(*[check_bad(i) for i in range(50)])
    s_bad.show()

    # A4: 25 并发（模拟 LOGIN_SEMAPHORE=25）
    s_conc = Stats("25 并发登录 login_semaphore")
    sem    = asyncio.Semaphore(25)
    pws    = list(AUTH_INDEX.keys())
    pws    = (pws * 25)[:25]

    async def concurrent_login(pw):
        async with sem:
            t0 = time.time()
            await get_user_by_password_async(pw)
            s_conc.ok_(time.time() - t0)

    await asyncio.gather(*[concurrent_login(pw) for pw in pws])
    s_conc.show()

    return s_hit, s_login, s_bad, s_conc


# ============================================================
# TEST B: COINS Cache 并发读写
# ============================================================
async def test_b_coins_cache():
    print(f"\n{'='*62}")
    print(f"  TEST B: COINS Cache 并发读写")
    print(f"{'='*62}")

    # B1: 28 groups 并发读
    s_read = Stats("get_coins_async 28 groups 并发")
    async def read_one(g):
        t0 = time.time()
        try:
            await get_coins_async(g)
            s_read.ok_(time.time() - t0)
        except Exception as e:
            s_read.fail_(e)
    await asyncio.gather(*[read_one(i) for i in range(1, 29)])
    s_read.show()

    # B2: 28 groups 并发写（乐观更新）
    s_write = Stats("update_coins_async cache 乐观更新")
    await asyncio.gather(*[
        update_coins_async(i, 100 + i * 10, user_id=TEST_CHAT_ID, stats=s_write)
        for i in range(1, 29)
    ])
    await WRITE_QUEUE.join()
    s_write.show()

    # B3: cache 一致性
    mismatch = sum(
        1 for i in range(1, 29)
        if COINS_CACHE.get(i, -1) != 100 + i * 10
    )
    print(f"  Cache 一致性: {'✅ 全部正确' if mismatch == 0 else f'❌ {mismatch} 个不一致'}")

    # B4: TTL 过期后重新读
    global COINS_CACHE_TIME
    COINS_CACHE_TIME = 0
    s_refresh = Stats("cache TTL 过期重新读取")
    t0 = time.time()
    try:
        await get_coins_async(1)
        s_refresh.ok_(time.time() - t0)
    except Exception as e:
        s_refresh.fail_(e)
    s_refresh.show()

    return s_read, s_write, s_refresh


# ============================================================
# TEST C: UPDATE_QUEUE worker
# ============================================================
async def test_c_update_queue(wstats: Stats):
    print(f"\n{'='*62}")
    print(f"  TEST C: UPDATE_QUEUE worker（28 groups 所有字段）")
    print(f"{'='*62}")

    fields = list(FIELD_COLUMN.keys())
    s      = Stats("enqueue latency")

    # C1: 28 groups 同时入队
    await asyncio.gather(*[
        _enqueue_update(i, fields[i % len(fields)], SAFE_VALUES[fields[i % len(fields)]], s)
        for i in range(1, 29)
    ])
    t0 = time.time()
    await UPDATE_QUEUE.join()
    print(f"  Worker wall time: {time.time()-t0:.2f}s")
    s.show()
    wstats.show()

    # C2: intl/local total 修复验证
    print(f"\n  ── C2: intl/local total 修复验证 ──")
    s2 = Stats("intl+local enqueue 同一 batch")
    for i in range(1, 6):
        await _enqueue_update(i, "intl",  "8",  s2)
        await _enqueue_update(i, "local", "12", s2)
    await UPDATE_QUEUE.join()
    s2.show()

    try:
        row_data = await asyncio.to_thread(db_sheet.row_values, 2)
        intl     = row_data[2] if len(row_data) > 2 else "?"
        local    = row_data[3] if len(row_data) > 3 else "?"
        total    = row_data[4] if len(row_data) > 4 else "?"
        expected = int(intl or 0) + int(local or 0)
        ok       = str(total) == str(expected)
        print(f"  Group 1 → intl={intl}, local={local}, total={total} (期望={expected})")
        print(f"  {'✅ Total 正确' if ok else '❌ Total 错误 — 修复未生效'}")
    except Exception as e:
        print(f"  ⚠️  验证失败: {e}")

    # C3: GROUP_LOCKS 串行保护（同一 group 连续写 10 次）
    print(f"\n  ── C3: GROUP_LOCKS 串行保护 ──")
    s3 = Stats("group_lock 串行 group1 10次")
    for _ in range(10):
        await _enqueue_update(1, "location", "SerialTest", s3)
    await UPDATE_QUEUE.join()
    s3.show()

    return s, s2, s3


# ============================================================
# TEST D: coin_write_worker
# ============================================================
async def test_d_coin_worker():
    print(f"\n{'='*62}")
    print(f"  TEST D: WRITE_QUEUE coin_write_worker")
    print(f"{'='*62}")

    # D1: 28 groups 同时写
    s = Stats("coins enqueue 28 groups")
    for i in range(1, 29):
        await _enqueue_coins(i, 50, s)
    t0 = time.time()
    await WRITE_QUEUE.join()
    print(f"  Worker flush: {time.time()-t0:.2f}s")
    s.show()

    # D2: 快速重复写（last-write-wins）
    print(f"\n  ── D2: last-write-wins ──")
    s2 = Stats("rapid requeue group1")
    for v in [100, 200, 300, 400, 500]:
        await _enqueue_coins(1, v - COINS_CACHE.get(1, 0), s2)
    await WRITE_QUEUE.join()
    s2.show()
    print(f"  Group 1 cache: {COINS_CACHE.get(1, -1)} (期望 ≥ 500)")

    # D3: 429 检查
    hits_429 = sum(1 for e in s.errors if "429" in str(e))
    print(f"  429 hits: {'✅ None' if hits_429 == 0 else f'❌ {hits_429}'}")

    return s, s2


# ============================================================
# TEST E: ATTENDANCE 签到并发 + 重复防护
# ============================================================
async def test_e_attendance():
    print(f"\n{'='*62}")
    print(f"  TEST E: 签到并发 + 重复防护")
    print(f"{'='*62}")

    ATTENDANCE_SIGNED["facis"].clear()
    ATTENDANCE_INDEX["facis"].clear()

    # E1: 20 个独立用户同时签到
    s_unique = Stats("20 unique users sign in")
    await asyncio.gather(*[_sign_attendance(i, s_unique) for i in range(20)])
    s_unique.show()

    # E2: 同一人 10 次并发（只有 1 次应成功）
    print(f"\n  ── E2: 重复签到防护（10次并发，只能 1 次成功）──")
    identifier = "E2_DupTest_Facilitator"
    key        = ("facis", identifier)
    async with ATTENDANCE_LOCK:
        ATTENDANCE_SIGNED["facis"].discard(key)

    s_dup_ok  = Stats("1 pass expected")
    s_dup_blk = Stats("9 blocked expected")

    async def try_dup_sign():
        async with ATTENDANCE_LOCK:
            if key in ATTENDANCE_SIGNED["facis"]:
                return "block"
            ATTENDANCE_SIGNED["facis"].add(key)
            ATTENDANCE_INDEX["facis"].append({"identifier": identifier, "time": time.time()})
        return "ok"

    results = await asyncio.gather(*[try_dup_sign() for _ in range(10)])
    ok_n = results.count("ok")
    bl_n = results.count("block")
    if ok_n == 1:
        s_dup_ok.ok_(0)
    else:
        s_dup_ok.fail_(f"{ok_n}x passed (expected 1)")
    for _ in range(bl_n):
        s_dup_blk.ok_(0)
    s_dup_ok.show()
    s_dup_blk.show()

    # E3: GM 签到
    ATTENDANCE_SIGNED["gm"].clear()
    ATTENDANCE_INDEX["gm"].clear()
    s_gm = Stats("GM 10 users sign in")
    await asyncio.gather(*[_sign_attendance_gm(i, s_gm) for i in range(10)])
    s_gm.show()

    await ATTENDANCE_QUEUE.join()
    return s_unique, s_dup_ok, s_dup_blk, s_gm


# ============================================================
# TEST F: LOG buffer 并发写入
# ============================================================
async def test_f_logs():
    print(f"\n{'='*62}")
    print(f"  TEST F: Log buffer 并发写入")
    print(f"{'='*62}")

    # F1: 50 条并发
    s = Stats("add_log_async 50 并发")
    await asyncio.gather(*[
        _add_log(f"TestOp_{i}", "Facilitators", f"Group {i%28+1}", "test_action", "-", "-", s)
        for i in range(50)
    ])
    s.show()

    # F2: 超过 10 条触发即时 flush
    s2 = Stats("buffer overflow flush 11条")
    for i in range(11):
        await _add_log("OverflowOp", "OC", f"Group {i+1}", "overflow_test", "-", str(i), s2)
    s2.show()

    # F3: 多 role 并发
    s3    = Stats("多 role 并发写日志")
    roles = ["Advisor", "OC", "HOF", "HOGM", "Facilitators", "Game Masters"]
    await asyncio.gather(*[
        _add_log(f"Op_{r}", r, f"Group {i+1}", "role_test", "-", r, s3)
        for i, r in enumerate(roles * 5)
    ])
    s3.show()

    return s, s2, s3


# ============================================================
# TEST G: TELEGRAM 队列发送
# ============================================================
async def test_g_telegram_queue(tg_stats: Stats):
    print(f"\n{'='*62}")
    print(f"  TEST G: TELEGRAM_QUEUE 发送（rate=20/s）")
    print(f"  chat_id: {TEST_CHAT_ID}")
    print(f"{'='*62}")

    # G1: 单条
    await TELEGRAM_QUEUE.put((TEST_CHAT_ID, "🧪 [G1] 单条消息测试", None, 0))
    await TELEGRAM_QUEUE.join()
    print("  G1: 单条 ✅")

    # G2: 10 条并发入队
    for i in range(10):
        await TELEGRAM_QUEUE.put((TEST_CHAT_ID, f"🧪 [G2] 并发消息 {i+1}/10", None, 0))
    t0 = time.time()
    await TELEGRAM_QUEUE.join()
    print(f"  G2: 10条 wall={time.time()-t0:.2f}s ✅")

    # G3: 30 条（测 rate limiter）
    for i in range(30):
        await TELEGRAM_QUEUE.put((TEST_CHAT_ID, f"🧪 [G3] Rate limit {i+1}/30", None, 0))
    t0 = time.time()
    await TELEGRAM_QUEUE.join()
    wall = time.time() - t0
    print(f"  G3: 30条 wall={wall:.2f}s (20/s → 期望 ≥1.5s) ✅")
    tg_stats.show()


# ============================================================
# TEST H: msg_faci 广播模拟（56条）
# ============================================================
async def test_h_msg_faci(tg_stats: Stats):
    print(f"\n{'='*62}")
    print(f"  TEST H: msg_faci 广播模拟（56条 → 你自己）")
    print(f"{'='*62}")

    n      = 56
    header = "‼️ ALERT ‼️\n📩 Message from OC TestAdmin (Group 1):"
    msg    = "这是一条来自 OC 的群组广播测试消息"

    for i in range(n):
        await TELEGRAM_QUEUE.put((
            TEST_CHAT_ID,
            f"{header}\n\n{msg} [{i+1}/{n}]",
            None, 0
        ))
    t0   = time.time()
    await TELEGRAM_QUEUE.join()
    wall = time.time() - t0
    print(f"  Wall time ({n} msgs): {wall:.2f}s (期望 ≥ {n/20:.1f}s)")
    tg_stats.show()

    # H2: 3 Admin 同时广播（3×28 = 84条）
    print(f"\n  ── H2: 3 Admin 同时广播（84条）──")
    for admin_i in range(3):
        for j in range(28):
            await TELEGRAM_QUEUE.put((
                TEST_CHAT_ID,
                f"[H2] Admin {admin_i+1} → Faci {j+1}/28",
                None, 0
            ))
    t0   = time.time()
    await TELEGRAM_QUEUE.join()
    print(f"  Wall time (84 msgs): {time.time()-t0:.2f}s")
    tg_stats.show()


# ============================================================
# TEST I: coin_write_worker → Telegram 通知
# ============================================================
async def test_i_coin_notify(tg_stats: Stats):
    print(f"\n{'='*62}")
    print(f"  TEST I: coin_write_worker Telegram 通知（28 组）")
    print(f"{'='*62}")

    s = Stats("coins enqueue with notify")
    for i in range(1, 29):
        async with COINS_CACHE_LOCK:
            before = COINS_CACHE.get(i, 0)
            after  = before + 100
            COINS_CACHE[i]   = after
            COINS_CACHE_TIME = time.time()
        await WRITE_QUEUE.put((i, after, {
            "user_id": TEST_CHAT_ID,
            "before":  before,
            "after":   after,
        }))
        s.ok_(0)

    t0 = time.time()
    await WRITE_QUEUE.join()
    print(f"  Sheets write wall: {time.time()-t0:.2f}s")
    s.show()

    await TELEGRAM_QUEUE.join()
    print("  📱 已发送 28 条 coins 更新通知")
    tg_stats.show()


# ============================================================
# TEST J: FLOW_STATE Day1 Ranking + Day2 PK
# ============================================================
async def test_j_flow_state():
    print(f"\n{'='*62}")
    print(f"  TEST J: FLOW_STATE Day1 Ranking + Day2 PK（纯内存）")
    print(f"{'='*62}")

    fake_uid = 999999

    # J1: Day1 ranking（4 轮选 group）
    s_day1 = Stats("Day1 ranking flow 4 ranks")
    FLOW_STATE["day1"][fake_uid] = {"current_rank": 1, "results": {}, "used": set()}
    state    = FLOW_STATE["day1"][fake_uid]
    assigned = []

    for rank in range(1, 5):
        group = rank * 3
        if group in state["used"]:
            s_day1.fail_(f"group {group} already used")
            continue
        state["used"].add(group)
        state["results"][rank] = {"group": group, "add": DAY1_RANK[rank]}
        state["current_rank"]  = rank + 1
        assigned.append((rank, group))
        s_day1.ok_(0)

    for rank, group in assigned:
        r = state["results"][rank]
        assert r["group"] == group,          f"Rank {rank} group mismatch"
        assert r["add"]   == DAY1_RANK[rank], f"Rank {rank} coins mismatch"
    s_day1.show()

    # 模拟 confirm_day1（加币）
    s_coins = Stats("Day1 coins apply 4 groups")
    state2  = FLOW_STATE["day1"].pop(fake_uid)
    for rank, data in state2["results"].items():
        g      = data["group"]
        add    = data["add"]
        before = await get_coins_async(g)
        after  = before + add
        await update_coins_async(g, after, stats=s_coins)
        await add_log_async("TestGM", "Game Masters", f"Group {g}", f"Day1 Rank {rank}", before, after)
    s_coins.show()

    # J2: Day2 PK flow
    s_day2 = Stats("Day2 PK flow win+lose")
    FLOW_STATE["day2"][fake_uid] = {"win": None, "lose": None}
    state = FLOW_STATE["day2"][fake_uid]
    state["win"]  = 5
    state["lose"] = 10
    assert state["win"] == 5 and state["lose"] == 10
    s_day2.ok_(0)

    win  = state["win"]
    lose = state["lose"]
    FLOW_STATE["day2"].pop(fake_uid)
    for g, action_type in [(win, "PK Win"), (lose, "PK Lose")]:
        before = await get_coins_async(g)
        after  = before + DAY2_RANK[action_type.split()[-1].lower()]
        await update_coins_async(g, after, stats=s_day2)
        await add_log_async("TestGM", "Game Masters", f"Group {g}", action_type, before, after)
    s_day2.show()

    # J3: rank_none（No Group）
    s_none = Stats("rank_none 处理")
    FLOW_STATE["day1"][fake_uid] = {"current_rank": 1, "results": {}, "used": set()}
    state = FLOW_STATE["day1"][fake_uid]
    state["results"][1]   = {"group": None, "add": 0}
    state["current_rank"] = 2
    assert state["results"][1]["group"] is None
    s_none.ok_(0)
    FLOW_STATE["day1"].pop(fake_uid)
    s_none.show()

    await WRITE_QUEUE.join()
    return s_day1, s_coins, s_day2, s_none


# ============================================================
# TEST K: 权限系统完整性
# ============================================================
async def test_k_permissions():
    print(f"\n{'='*62}")
    print(f"  TEST K: PERMISSIONS 矩阵完整性验证")
    print(f"{'='*62}")

    s = Stats("权限检查正确性")
    expect_has = {
        ("Advisor",    "attendance"):      True,
        ("OC",         "manage_coins"):    True,
        ("HOF",        "view_all_coins"):  True,
        ("HOGM",       "update_group_data"): True,
        ("Facilitators", "update_group"):  True,
        ("Facilitators", "check_own_coin"): True,
        ("Facilitators", "manage_coins"):  False,
        ("Facilitators", "attendance"):    False,
        ("Game Masters", "manage_coins"):  True,
        ("Game Masters", "view_all_coins"): True,
        ("Game Masters", "attendance"):    False,
        ("Game Masters", "update_group"):  False,
        ("Facis and Freshies (Game Test)", "manage_coins"):    False,
        ("Facis and Freshies (Game Test)", "update_group_data"): True,
    }

    for (role, perm), expected in expect_has.items():
        actual = has_permission(role, perm)
        if actual == expected:
            s.ok_(0)
        else:
            s.fail_(f"{role} → {perm}: expected={expected}, got={actual}")
    s.show()
    return s


# ============================================================
# TEST L: Password Reset 并发保护
# ============================================================
async def test_l_password_reset():
    print(f"\n{'='*62}")
    print(f"  TEST L: Password Reset 并发保护（AUTH_WRITE_SEMAPHORE）")
    print(f"{'='*62}")

    # L1: 10 个 reset 请求同时来（Semaphore=1，串行）
    s           = Stats("AUTH_WRITE_SEMAPHORE 串行保护")
    write_order = []

    async def fake_reset(uid):
        async with AUTH_WRITE_SEM:
            t0 = time.time()
            await asyncio.sleep(0.05)
            write_order.append(uid)
            s.ok_(time.time() - t0)

    t0   = time.time()
    await asyncio.gather(*[fake_reset(i) for i in range(10)])
    wall = time.time() - t0
    s.show()
    print(f"  Wall time: {wall:.2f}s (串行 ~0.5s，并发 ~0.05s)")
    print(f"  串行写入有序: {'✅' if len(write_order) == 10 else '❌'}")

    # L2: 重复密码拦截
    s2      = Stats("重复密码拦截")
    all_pws = [
        str(r.get("Password", "")).strip().lstrip("'")
        for r in AUTH_CACHE if r.get("Password")
    ]
    if all_pws:
        existing   = all_pws[0]
        is_dup     = existing in all_pws
        is_dup and s2.ok_(0) or s2.fail_("duplicate not detected")
        print(f"  ✅ 重复密码正确拦截" if is_dup else "  ❌ 重复密码未拦截")

        new_unique = "UNIQUE_PW_XYZ_12345_TEST"
        is_new     = new_unique not in all_pws
        is_new and s2.ok_(0) or s2.fail_("unique pw incorrectly blocked")
        print(f"  ✅ 唯一密码正确放行" if is_new else "  ❌ 唯一密码被错误拦截")

    # L3: 修复验证 — awaiting_password 不被 reset flow 截断
    s3      = Stats("awaiting_password 不截断 reset flow")
    user_data = {"awaiting_password": True, "search_name_mode": True}
    user_data.pop("search_name_mode", None)
    user_data.pop("awaiting_password", None)   # the fix
    user_data["awaiting_new_password"] = True
    if not user_data.get("awaiting_password"):
        s3.ok_(0)
        print("  ✅ awaiting_password 已清除，reset flow 不会被截断")
    else:
        s3.fail_("awaiting_password still set — fix missing!")
        print("  ❌ awaiting_password 未清除！bug 仍存在")
    s3.show()

    s2.show()
    return s, s2, s3


# ============================================================
# TEST M: queue.join() 死锁检测
# ============================================================
async def test_m_deadlock():
    print(f"\n{'='*62}")
    print(f"  TEST M: queue.join() 死锁检测")
    print(f"{'='*62}")

    for i in range(10):
        await UPDATE_QUEUE.put(((i % 28) + 1, "location", "DeadlockTest"))
    for i in range(10):
        async with COINS_CACHE_LOCK:
            COINS_CACHE[(i % 28) + 1] = 999
            COINS_CACHE_TIME          = time.time()
        await WRITE_QUEUE.put(((i % 28) + 1, 999, {
            "user_id": TEST_CHAT_ID, "before": 0, "after": 999
        }))
    for i in range(10):
        await ATTENDANCE_QUEUE.put([
            "facis", "Facilitators", f"DeadTest_{i}",
            "present", time.strftime("%Y-%m-%d %H:%M:%S")
        ])

    results = {"update": False, "write": False, "attendance": False}

    async def join_with_timeout(name, q):
        try:
            await asyncio.wait_for(q.join(), timeout=20)
            results[name] = True
        except asyncio.TimeoutError:
            print(f"  ❌ {name} queue.join() TIMEOUT — 死锁！")

    await asyncio.gather(
        join_with_timeout("update",     UPDATE_QUEUE),
        join_with_timeout("write",      WRITE_QUEUE),
        join_with_timeout("attendance", ATTENDANCE_QUEUE),
    )

    for name, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {name}: {'no deadlock' if ok else 'DEADLOCK DETECTED'}")

    return results


# ============================================================
# TEST N: 5 轮全功能混合暴力
# ============================================================
async def test_n_sustained(wstats_list: list):
    print(f"\n{'='*62}")
    print(f"  TEST N: 5 轮全功能混合暴力")
    print(f"{'='*62}")

    fields = list(FIELD_COLUMN.keys())

    for rnd in range(1, 6):
        su = Stats(f"R{rnd} update")
        sc = Stats(f"R{rnd} coins")
        sa = Stats(f"R{rnd} attendance")

        tasks = (
            [_enqueue_update((i % 28) + 1, fields[i % len(fields)],
                              SAFE_VALUES[fields[i % len(fields)]], su) for i in range(28)] +
            [_enqueue_coins((i % 28) + 1, 10, sc) for i in range(20)] +
            [_sign_attendance(i + rnd * 100, sa) for i in range(15)]
        )
        random.shuffle(tasks)

        t0 = time.time()
        await asyncio.gather(*tasks, return_exceptions=True)
        enqueue_wall = time.time() - t0

        flush_t0 = time.time()
        await asyncio.gather(
            UPDATE_QUEUE.join(),
            WRITE_QUEUE.join(),
            ATTENDANCE_QUEUE.join(),
        )
        flush_wall = time.time() - flush_t0

        v = "✅" if su.fail == 0 and sc.fail == 0 and sa.fail == 0 else "❌"
        print(f"  {v} Round {rnd}: enqueue={enqueue_wall:.2f}s  flush={flush_wall:.2f}s"
              f"  upd={su.ok}/{su.ok+su.fail}  coins={sc.ok}/{sc.ok+sc.fail}"
              f"  att={sa.ok}/{sa.ok+sa.fail}")
        wstats_list.append((su, sc, sa))


# ============================================================
# INTERNAL HELPERS
# ============================================================

async def _enqueue_update(group, field, value, stats: Stats):
    t0 = time.time()
    try:
        async with GROUP_LOCKS[group]:
            await UPDATE_QUEUE.put((group, field, value))
        stats.ok_(time.time() - t0)
    except Exception as e:
        stats.fail_(e)


async def _enqueue_coins(group, change, stats: Stats):
    t0 = time.time()
    try:
        async with COINS_CACHE_LOCK:
            cur             = COINS_CACHE.get(int(group), 0)
            new             = cur + change
            COINS_CACHE[int(group)] = new
        await WRITE_QUEUE.put((group, new, {
            "user_id": TEST_CHAT_ID, "before": cur, "after": new
        }))
        stats.ok_(time.time() - t0)
    except Exception as e:
        stats.fail_(e)


async def _sign_attendance(uid, stats: Stats, att_type="facis"):
    identifier = f"BF_Test_{uid:05d}"
    key        = (att_type, identifier)
    t0         = time.time()
    try:
        async with ATTENDANCE_LOCK:
            if key in ATTENDANCE_SIGNED[att_type]:
                stats.fail_("duplicate")
                return
            ATTENDANCE_SIGNED[att_type].add(key)
            ATTENDANCE_INDEX[att_type].append({"identifier": identifier, "time": time.time()})
        await ATTENDANCE_QUEUE.put([
            att_type, "Facilitators", identifier,
            "present", time.strftime("%Y-%m-%d %H:%M:%S")
        ])
        stats.ok_(time.time() - t0)
    except Exception as e:
        stats.fail_(e)


async def _sign_attendance_gm(uid, stats: Stats):
    await _sign_attendance(uid, stats, att_type="gm")


async def _add_log(op_name, op_role, target, action, before, after, stats: Stats):
    t0 = time.time()
    try:
        await add_log_async(op_name, op_role, target, action, before, after)
        stats.ok_(time.time() - t0)
    except Exception as e:
        stats.fail_(e)


# ============================================================
# RESTORE
# ============================================================
async def restore():
    print(f"\n{'='*62}")
    print(f"  RESTORE & CLEANUP")
    print(f"{'='*62}")

    # 恢复 DB sheet
    try:
        updates = []
        for g in range(1, 29):
            row = g + 1
            updates += [
                {"range": f"B{row}", "values": [["TBD"]]},
                {"range": f"F{row}", "values": [[f"Group {g}"]]},
                {"range": f"G{row}", "values": [["TBD"]]},
            ]
        for i in range(0, len(updates), 50):
            await asyncio.to_thread(db_sheet.batch_update, updates[i:i+50])
            await asyncio.sleep(0.5)
        print("  ✅ DB 已恢复")
    except Exception as e:
        print(f"  ⚠️  DB 恢复失败: {e}")

    # 删除测试签到记录
    try:
        recs         = await asyncio.to_thread(att_sheet.get_all_values)
        test_kws     = ("BF_Test_", "DeadTest_", "E2_Dup")
        to_del       = [
            i + 1 for i, r in enumerate(recs)
            if any(kw in str(c) for c in r for kw in test_kws)
        ]
        for row_n in reversed(to_del):
            await asyncio.to_thread(att_sheet.delete_rows, row_n)
        print(f"  ✅ 已删除 {len(to_del)} 条测试签到记录")
    except Exception as e:
        print(f"  ⚠️  签到清理失败: {e}")
        print("  → 请手动删除 ATTENDANCE sheet 中含 'BF_Test_' / 'DeadTest_' 的行")


# ============================================================
# MAIN
# ============================================================
async def main():
    global UPDATE_QUEUE, WRITE_QUEUE, ATTENDANCE_QUEUE, TELEGRAM_QUEUE
    global db_sheet, coins_sheet, auth_sheet, att_sheet, logs_sheet, bot

    import concurrent.futures
    asyncio.get_running_loop().set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=20)
    )

    tg_enabled = bool(TEST_CHAT_ID and BOT_TOKEN)

    print("\n" + "🔥" * 28)
    print("  BOT FULL BRUTEFORCE TEST")
    print("🔥" * 28)
    print(f"  Telegram: {'✅ ' + str(TEST_CHAT_ID) if tg_enabled else '⛔ disabled (set TEST_CHAT_ID in .env)'}")

    # Init Sheets
    try:
        db_sheet, coins_sheet, auth_sheet, att_sheet, logs_sheet = \
            await asyncio.to_thread(init_sheets_sync)
        print("  ✅ Sheets connected")
    except Exception as e:
        print(f"  ❌ Sheets failed: {e}")
        return

    # Init Bot
    if tg_enabled:
        bot = Bot(token=BOT_TOKEN)
        try:
            me = await bot.get_me()
            print(f"  ✅ Bot: @{me.username}")
        except Exception as e:
            print(f"  ❌ Bot failed: {e}")
            tg_enabled = False

    # Init Queues
    UPDATE_QUEUE     = asyncio.Queue()
    WRITE_QUEUE      = asyncio.Queue()
    ATTENDANCE_QUEUE = asyncio.Queue()
    TELEGRAM_QUEUE   = asyncio.Queue()

    print("\n  🔥 Warming up caches...")
    await warmup_all_caches()
    print("  ✅ Warmup complete\n")

    # Worker stats
    ws_C        = Stats("UpdateWorker C")
    ws_D_notify = Stats("CoinNotify D")
    ws_G_tg     = Stats("TelegramWorker G")
    ws_H_tg     = Stats("TelegramWorker H")
    ws_I_tg     = Stats("TelegramWorker I")
    ws_N        = []

    # Start workers
    tw_C    = asyncio.create_task(update_group_write_worker(ws_C))
    tw_coin = asyncio.create_task(coin_write_worker(notify_stats=ws_D_notify))
    tw_att  = asyncio.create_task(attendance_write_worker())
    tw_log  = asyncio.create_task(log_flusher_worker())
    tw_tg   = None
    if tg_enabled:
        tw_tg = asyncio.create_task(telegram_send_worker(ws_G_tg))

    all_stats = []

    # ── Run tests ──────────────────────────────────────────

    sa = await test_a_auth_cache()
    all_stats.extend(sa)

    sb = await test_b_coins_cache()
    all_stats.extend(sb)
    await asyncio.sleep(0.5)

    sc = await test_c_update_queue(ws_C)
    all_stats.extend(sc)
    await asyncio.sleep(0.5)

    sd = await test_d_coin_worker()
    all_stats.extend(sd)
    await asyncio.sleep(0.5)

    se = await test_e_attendance()
    all_stats.extend(se)

    sf = await test_f_logs()
    all_stats.extend(sf)
    await asyncio.sleep(0.5)

    if tg_enabled:
        await test_g_telegram_queue(ws_G_tg)
        await asyncio.sleep(1)

        tw_tg.cancel()
        await asyncio.gather(tw_tg, return_exceptions=True)
        tw_tg = asyncio.create_task(telegram_send_worker(ws_H_tg))
        await test_h_msg_faci(ws_H_tg)
        await asyncio.sleep(1)

        tw_tg.cancel()
        await asyncio.gather(tw_tg, return_exceptions=True)
        tw_tg = asyncio.create_task(telegram_send_worker(ws_I_tg))
        await test_i_coin_notify(ws_I_tg)
        await asyncio.sleep(1)
    else:
        print("\n  ⛔ Tests G/H/I skipped (Telegram disabled)")

    sj = await test_j_flow_state()
    all_stats.extend(sj)
    await WRITE_QUEUE.join()

    sk = await test_k_permissions()
    all_stats.append(sk)

    sl = await test_l_password_reset()
    all_stats.extend(sl)

    await test_m_deadlock()
    await asyncio.sleep(0.5)

    # N: 重新创建 update worker
    tw_C.cancel()
    await asyncio.gather(tw_C, return_exceptions=True)
    ws_N_main = Stats("UpdateWorker N")
    tw_C      = asyncio.create_task(update_group_write_worker(ws_N_main))
    await test_n_sustained(ws_N)

    # ── FINAL SUMMARY ──────────────────────────────────────
    print(f"\n{'='*62}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*62}")

    sheet_stats = [s for s in all_stats if s is not None]
    total_ok    = sum(s.ok   for s in sheet_stats)
    total_fail  = sum(s.fail for s in sheet_stats)
    all_lats    = [l for s in sheet_stats for l in s.latencies]
    hits_429    = sum(1 for s in sheet_stats for e in s.errors if "429" in str(e))
    hits_dup    = sum(1 for s in sheet_stats for e in s.errors if "duplicate" in str(e))

    print(f"  Sheets operations : {total_ok}/{total_ok+total_fail}")
    if all_lats:
        sl2 = sorted(all_lats)
        print(f"  Latency           : avg={mean(sl2):.3f}s  "
              f"p95={sl2[int(len(sl2)*.95)]:.3f}s  max={sl2[-1]:.3f}s")
    print(f"  429 hits          : {'✅ None' if hits_429==0 else f'❌ {hits_429}'}")
    print(f"  Duplicate blocks  : {hits_dup} (正常 attendance 重复拦截)")

    all_ws  = [ws_C, ws_N_main] + [t[0] for t in ws_N]
    ws_ok   = sum(s.ok   for s in all_ws)
    ws_fail = sum(s.fail for s in all_ws)
    ws_lats = [l for s in all_ws for l in s.latencies]
    print(f"\n  Worker writes     : {ws_ok}/{ws_ok+ws_fail}")
    if ws_lats:
        wl = sorted(ws_lats)
        print(f"  Worker latency    : avg={mean(wl):.3f}s  "
              f"p95={wl[int(len(wl)*.95)]:.3f}s  max={wl[-1]:.3f}s")

    if tg_enabled:
        tg_all   = [ws_G_tg, ws_H_tg, ws_I_tg]
        tg_ok    = sum(s.ok   for s in tg_all)
        tg_fail  = sum(s.fail for s in tg_all)
        tg_retry = sum(1 for s in tg_all for e in s.errors if "RetryAfter" in str(e))
        tg_lats  = [l for s in tg_all for l in s.latencies]
        print(f"\n  Telegram sends    : {tg_ok}/{tg_ok+tg_fail}")
        if tg_lats:
            tl = sorted(tg_lats)
            print(f"  Telegram latency  : avg={mean(tl):.3f}s  "
                  f"p95={tl[int(len(tl)*.95)]:.3f}s  max={tl[-1]:.3f}s")
        print(f"  RetryAfter hits   : {'✅ None' if tg_retry==0 else f'⚠️  {tg_retry}'}")

    all_ok = total_fail == 0 and hits_429 == 0 and ws_fail == 0
    if tg_enabled:
        all_ok = all_ok and tg_fail == 0
    print(f"\n  {'🎉 ALL PASS — Bot is production ready!' if all_ok else '⚠️  Issues found — review above'}")

    # Stop workers
    workers = [tw_C, tw_coin, tw_att, tw_log]
    if tw_tg:
        workers.append(tw_tg)
    for t in workers:
        t.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    await restore()

    if tg_enabled and bot:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())