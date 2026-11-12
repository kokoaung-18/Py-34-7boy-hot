"""
Tron History Search - Pure Python 3 (HTML logic + Auto + Telegram)
Run:  pip install requests
      python app.py
"""

import hashlib
import json
import os
import time
import threading
from datetime import datetime

import requests

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════
BOT_TOKEN  = "8498838247:AAF1BmNDJEOj7qsL6sTWJrl5CL6QhC9s9cU"
FB_URL     = "https://kokoaung-41228-default-rtdb.firebaseio.com"
FB_KEY     = "AIzaSyALDtmbQq-2Pbz0mOJBi2rPxNPAgjTQ8qo"

TG_BOT_TOKEN = "8683983171:AAECYjaamxLs0288Nsn9b0xIOueCpdJHKMw"
TG_CHAT_ID   = "6385759989"
TG_STICKER   = "CAACAgUAAxkBA6-TYGrvAVRZrnBEeMyvsm5AQfvx3LneAAIWIQACptoYV_YCGFUFwQoCPQQ"

BATCH_SIZE       = 5000
BATCH_DELAY      = 0.15
CACHE_FILE       = "tron_cache.json"
HISTORY_API      = "https://draw.ar-lottery01.com/TrxWinGo/TrxWinGo_1M/GetHistoryIssuePage.json"
HISTORY_FILE     = "lottery_history.json"
PRED_FILE        = "predictions.json"
LIVE_POLL_SEC    = 10
LIVE_FETCH_RNG   = 100000
HISTORY_POLL_SEC = 5

LOSS_TRIGGER      = 1
PRED_HISTORY_MAX  = 10

PATTERN_MAX_LEN = 64
PATTERN_MIN_LEN = 1

# ═══════════════════════════════════════════════════════
# ANSI COLORS
# ═══════════════════════════════════════════════════════
class C:
    RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
    RED="\033[91m"; GREEN="\033[92m"; YELLOW="\033[93m"
    BLUE="\033[94m"; CYAN="\033[96m"; WHITE="\033[97m"; GRAY="\033[90m"


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


# ═══════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════
block_map: dict[int, str] = {}
signals_str: str = ""
signals_nums: list[int] = []
signals_dirty: bool = True
loaded_count: int = 0
live_max_known: int = 0
ready: bool = False
lock = threading.Lock()

latest_history: list[dict] = []
prediction_history: list[dict] = []
last_auto_pattern: str = ""
last_search_result: dict | None = None
last_search_prediction: str | None = None
last_search_elapsed: float = 0.0
last_status: str = "Initializing…"
last_error_msg: str = ""
log_lines: list[str] = []

consecutive_losses: int = 0
alert_active: bool = False


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    log_lines.append(f"[{ts}] {msg}")
    if len(log_lines) > 200:
        log_lines.pop(0)


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg_post(method: str, payload: dict) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
        r = requests.post(url, json=payload, timeout=15)
        ok = r.ok and r.json().get("ok")
        if not ok:
            log(f"TG {method} failed: {r.text[:120]}")
        return bool(ok)
    except Exception as e:
        log(f"TG {method} error: {e}")
        return False


def tg_send_message(text: str) -> bool:
    return tg_post("sendMessage", {
        "chat_id": TG_CHAT_ID, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    })


def tg_send_sticker() -> bool:
    if not TG_STICKER:
        return False
    return tg_post("sendSticker", {"chat_id": TG_CHAT_ID, "sticker": TG_STICKER})


def tg_alert_prediction(period, prediction, pattern, big=0, small=0):
    if not alert_active:
        log(f"TG skip prediction {period} (alert off)")
        return
    text = (
        "🎯 <b>TRX 1MIN  PREDICTION</b>\n"
        f"📊 : <code>{pattern}</code>\n"
        "-----------------------------------------\n"
        f"Period: <code>{period}</code>\n"
        f"Prediction: <b>{prediction}</b>"
    )
    if tg_send_message(text):
        log(f"TG → prediction {prediction} for {period}")


def tg_alert_win(period, prediction, actual, pattern):
    global alert_active
    if not alert_active:
        return
    if tg_send_sticker():
        log(f"TG → WIN sticker ({period})")
    alert_active = False
    log(f"TG alert stopped after WIN ({period})")


# ═══════════════════════════════════════════════════════
# NAMESPACE
# ═══════════════════════════════════════════════════════
def compute_namespace() -> str:
    seed = f"{BOT_TOKEN}|{FB_URL}|v1"
    h = hashlib.sha256(seed.encode()).hexdigest()
    return "s_" + h[:24]


FB_NS = compute_namespace()


# ═══════════════════════════════════════════════════════
# SIGNALS
# ═══════════════════════════════════════════════════════
def last_digit(h):
    if not h: return None
    for c in reversed(h):
        if c.isdigit(): return c
    return None


def digit_class(h):
    d = last_digit(h)
    if d is None: return "-"
    return "S" if int(d) <= 4 else "B"


def rebuild_signals():
    global signals_str, signals_nums, signals_dirty
    with lock:
        nums = sorted(block_map.keys(), reverse=True)
        classes = [digit_class(block_map[n]) for n in nums]
        signals_nums = nums
        signals_str = "".join(classes)
        signals_dirty = False


def ensure_signals():
    if signals_dirty or not signals_str:
        rebuild_signals()
    return signals_str, signals_nums


# ═══════════════════════════════════════════════════════
# FIREBASE
# ═══════════════════════════════════════════════════════
def fb_fetch_keys() -> list[int]:
    url = f"{FB_URL}/{FB_NS}/blocks.json?auth={FB_KEY}&shallow=true"
    r = requests.get(url, timeout=30); r.raise_for_status()
    data = r.json()
    if not data: return []
    return sorted(int(k) for k in data.keys() if k.isdigit())


def fb_fetch_range(start, end) -> dict[int, str]:
    params = {"auth": FB_KEY, "orderBy": '"$key"',
              "startAt": f'"{start}"', "endAt": f'"{end}"'}
    url = f"{FB_URL}/{FB_NS}/blocks.json"
    r = requests.get(url, params=params, timeout=30); r.raise_for_status()
    data = r.json() or {}
    out = {}
    for k, v in data.items():
        if k.isdigit() and isinstance(v, str) and v:
            out[int(k)] = v
    return out


# ═══════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════
def load_cache():
    global block_map, loaded_count, signals_dirty
    if not os.path.exists(CACHE_FILE): return
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        block_map = {int(k): v for k, v in data.items()}
        loaded_count = len(block_map); signals_dirty = True
        log(f"Cache restored: {loaded_count} blocks")
    except Exception as e:
        log(f"Cache load failed: {e}")


def save_cache():
    try:
        with lock:
            snap = {str(k): v for k, v in block_map.items()}
        with open(CACHE_FILE, "w") as f:
            json.dump(snap, f)
    except Exception as e:
        log(f"Cache save failed: {e}")


def load_history_cache():
    global latest_history
    if not os.path.exists(HISTORY_FILE): return
    try:
        with open(HISTORY_FILE) as f:
            latest_history = json.load(f)
        log(f"History restored: {len(latest_history)}")
    except Exception as e:
        log(f"History load failed: {e}")


def save_history_cache():
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(latest_history, f)
    except Exception as e:
        log(f"History save failed: {e}")


# ═══════════════════════════════════════════════════════
# PREDICTION PERSISTENCE
# ═══════════════════════════════════════════════════════
def reset_predictions():
    global prediction_history, consecutive_losses, alert_active
    prediction_history = []
    consecutive_losses = 0
    alert_active = False
    try:
        if os.path.exists(PRED_FILE):
            os.remove(PRED_FILE)
        log("Prediction history cleared (fresh start)")
    except Exception as e:
        log(f"Pred clear failed: {e}")


def save_predictions():
    try:
        with open(PRED_FILE, "w") as f:
            json.dump(prediction_history, f)
    except Exception as e:
        log(f"Pred save failed: {e}")


# ═══════════════════════════════════════════════════════
# SEARCH — HTML findPattern/fuzzyFind အတိုင်း
# ═══════════════════════════════════════════════════════
def find_pattern(pattern: str, s: str, nums: list[int]):
    p_len = len(pattern)
    if p_len == 0 or p_len > len(s):
        return {"count": 0, "big": 0, "small": 0}
    count = big = small = 0
    pos = 0
    while True:
        idx = s.find(pattern, pos)
        if idx < 0: break
        end = idx + p_len - 1
        if nums[idx] - nums[end] == p_len - 1:
            count += 1
            above = idx - 1
            if above >= 0 and nums[above] == nums[idx] + 1:
                cls = s[above]
                if cls == "B": big += 1
                elif cls == "S": small += 1
        pos = idx + 1
    return {"count": count, "big": big, "small": small}


def fuzzy_find(requested: str, s: str, nums: list[int]):
    cur = requested.upper()[:PATTERN_MAX_LEN]
    while len(cur) >= PATTERN_MIN_LEN:
        r = find_pattern(cur, s, nums)
        if r["count"] > 0:
            return {"matched": cur, **r}
        if len(cur) == PATTERN_MIN_LEN:
            break
        cur = cur[:-1]
    return {"matched": None, "count": 0, "big": 0, "small": 0}


# ═══════════════════════════════════════════════════════
# SYNC
# ═══════════════════════════════════════════════════════
def sync_blocks() -> bool:
    global loaded_count, live_max_known, ready, signals_dirty, last_status
    try:
        last_status = "Fetching…"
        keys = fb_fetch_keys()
        if not keys:
            ready = True; last_status = "Empty"; return True
        missing = [k for k in keys if k not in block_map]
        if not missing:
            ready = True; last_status = "Ready"
            log(f"Already up to date ({loaded_count})")
            return True
        log(f"Fetching {len(missing)} blocks…")
        for i in range(0, len(missing), BATCH_SIZE):
            batch = missing[i:i+BATCH_SIZE]
            start, end = batch[0], batch[-1]
            data = None
            for attempt in range(3):
                try:
                    data = fb_fetch_range(start, end); break
                except Exception as e:
                    log(f"retry {attempt+1}: {e}")
                    time.sleep(2*(attempt+1))
            if data:
                with lock:
                    block_map.update(data)
                    loaded_count = len(block_map)
                    live_max_known = max(live_max_known, max(data.keys()))
                    signals_dirty = True
                last_status = f"{loaded_count}/{len(keys)}"
            time.sleep(BATCH_DELAY)
        save_cache()
        ready = True; last_status = "Ready"; return True
    except Exception as e:
        log(f"Sync failed: {e}")
        last_status = "Cache only" if loaded_count > 0 else "Error"
        return loaded_count > 0


def live_poller():
    global live_max_known, loaded_count, signals_dirty, last_status
    while not ready: time.sleep(0.5)
    log("Live polling started")
    while True:
        try:
            data = fb_fetch_range(live_max_known+1, live_max_known+LIVE_FETCH_RNG)
            if data:
                new_max = max(data.keys())
                with lock:
                    block_map.update(data)
                    loaded_count = len(block_map)
                    live_max_known = max(live_max_known, new_max)
                    signals_dirty = True
                save_cache()
                log(f"+{len(data)} new blocks")
                last_status = "Live"
        except Exception as e:
            log(f"Live failed: {e}")
        time.sleep(LIVE_POLL_SEC)


# ═══════════════════════════════════════════════════════
# LOTTERY HISTORY
# ═══════════════════════════════════════════════════════
def increment_issue(issue: str) -> str:
    d = str(issue or "")
    if not d.isdigit(): return ""
    out = list(d)
    for i in range(len(out)-1, -1, -1):
        if out[i] != "9":
            out[i] = str(int(out[i])+1)
            return "".join(out)
        out[i] = "0"
    return "1" + "".join(out)


def load_history_once():
    global latest_history
    try:
        r = requests.get(HISTORY_API, params={"ts": int(time.time()*1000)},
                         headers={"Accept": "application/json"}, timeout=20)
        r.raise_for_status()
        payload = r.json()
        lst = (payload or {}).get("data", {}).get("list")
        if not isinstance(lst, list): return
        by_issue = {}
        for it in latest_history + lst:
            if it and it.get("issueNumber") is not None:
                by_issue[str(it["issueNumber"])] = it
        latest_history = sorted(by_issue.values(),
                                key=lambda x: str(x.get("issueNumber","")),
                                reverse=True)[:30]
        save_history_cache()
        grade_predictions()
        auto_search()
    except Exception as e:
        log(f"History error: {e}")


def history_poller():
    while True:
        load_history_once()
        time.sleep(HISTORY_POLL_SEC)


def grade_predictions():
    global consecutive_losses, alert_active
    actual = {}
    for it in latest_history:
        try: n = int(it.get("number"))
        except (TypeError, ValueError): continue
        if it.get("issueNumber") is not None:
            actual[str(it["issueNumber"])] = "BIG" if n >= 5 else "SMALL"
    changed = False
    for p in prediction_history:
        if p["outcome"] != "WAIT": continue
        a = actual.get(str(p["period"]))
        if a:
            p["actual"] = a
            p["outcome"] = "WIN" if p["prediction"] == a else "LOSE"
            changed = True
            if p["outcome"] == "LOSE":
                consecutive_losses += 1
                log(f"LOSE #{consecutive_losses} at {p['period']}")
                if consecutive_losses == LOSS_TRIGGER and not alert_active:
                    alert_active = True
                    log(f"TG alert ACTIVATED (after {consecutive_losses} LOSE)")
            else:
                consecutive_losses = 0
                if alert_active:
                    threading.Thread(
                        target=tg_alert_win,
                        args=(p["period"], p["prediction"], a, p.get("pattern","")),
                        daemon=True,
                    ).start()
    if changed:
        save_predictions()


def save_prediction(pattern, prediction, big=0, small=0) -> bool:
    if not latest_history: return False
    target = increment_issue(latest_history[0].get("issueNumber"))
    if not target or not prediction: return False
    if any(str(p["period"]) == target for p in prediction_history): return False
    prediction_history.insert(0, {
        "period": target, "prediction": prediction,
        "actual": "", "outcome": "WAIT", "pattern": pattern,
        "big": big, "small": small,
    })
    prediction_history[:] = prediction_history[:PRED_HISTORY_MAX]
    save_predictions()
    log(f"Prediction saved → {prediction} for {target} "
        f"({len(prediction_history)}/{PRED_HISTORY_MAX})")
    if alert_active:
        threading.Thread(
            target=tg_alert_prediction,
            args=(target, prediction, pattern, big, small),
            daemon=True,
        ).start()
    else:
        log(f"TG skip prediction {target} (alert off)")
    return True


# ═══════════════════════════════════════════════════════
# AUTO SEARCH — HTML tryAutoSearchHistoryPattern() အတိုင်း
# ═══════════════════════════════════════════════════════
def auto_search():
    global last_auto_pattern, last_error_msg
    global last_search_result, last_search_prediction, last_search_elapsed

    if len(latest_history) < 10:
        last_error_msg = f"Auto: waiting for 10 history ({len(latest_history)}/10)"
        return
    s, nums = ensure_signals()
    if not s:
        last_error_msg = "Auto: waiting for block data"
        return

    pattern = ""
    for it in latest_history[:20]:
        try: n = int(it.get("number"))
        except (TypeError, ValueError):
            last_error_msg = "Auto: invalid history number"
            return
        pattern += "B" if n >= 5 else "S"

    if len(pattern) < 10:
        last_error_msg = f"Auto: pattern too short ({len(pattern)}/10)"
        return
    if pattern == last_auto_pattern:
        return

    last_auto_pattern = pattern
    last_error_msg = ""
    last_search_prediction = None

    t0 = time.perf_counter()
    r = fuzzy_find(pattern, s, nums)
    last_search_elapsed = (time.perf_counter() - t0) * 1000
    last_search_result = r

    if r["matched"] is None:
        last_error_msg = f"Not found: {pattern}"
        log(f"Auto → not found ({pattern})")
        return

    if r["big"] > 0 and r["small"] == 0:
        last_search_prediction = "BIG"
    elif r["small"] > 0 and r["big"] == 0:
        last_search_prediction = "SMALL"

    if last_search_prediction:
        save_prediction(pattern, last_search_prediction,
                        r["big"], r["small"])
        log(f"Auto → {last_search_prediction} ({pattern}) "
            f"matched={r['matched']} hits={r['count']} B={r['big']} S={r['small']}")
    else:
        log(f"Auto → no 100% result ({pattern}) "
            f"matched={r['matched']} hits={r['count']} B={r['big']} S={r['small']}")


# ═══════════════════════════════════════════════════════
# RENDER
# ═══════════════════════════════════════════════════════
def render():
    clear_screen()
    W = 72; line = "─"*W

    print(f"{C.CYAN}{C.BOLD}{'𝐋𝐎𝐓𝐓𝐄𝐑𝐘  𝐅𝐀𝐓𝐇𝐄𝐑  [ AUTO + TG ]'.center(W)}{C.RESET}")
    print(f"{C.GRAY}{'𝚃𝙾𝙽_𝙻𝙾𝚃𝚃𝙴𝚁𝚈_𝙱𝙻𝙾𝙲𝙺_𝚆𝙰𝚅𝙴  (Py3 Terminal)'.center(W)}{C.RESET}")
    print(line)

    rng = "—"
    if signals_nums:
        rng = f"{signals_nums[-1]} – {signals_nums[0]}"
    sc = C.GREEN if ready else C.YELLOW
    if last_status in ("Error", "Cache only"): sc = C.RED
    print(f"{C.BOLD}Status:{C.RESET} {sc}{last_status}{C.RESET}    "
          f"{C.BOLD}Blocks:{C.RESET} {loaded_count}    "
          f"{C.BOLD}Range:{C.RESET} {rng}    "
          f"{C.BOLD}Sync:{C.RESET} {datetime.now().strftime('%H:%M:%S')}")
    print(line)

    print(f"{C.BOLD}Latest Blocks:{C.RESET}")
    if signals_nums:
        for i in range(min(5, len(signals_nums))):
            n = signals_nums[i]; h = block_map[n]; cls = digit_class(h)
            col = C.GREEN if cls == "B" else (C.YELLOW if cls == "S" else C.GRAY)
            print(f"  {C.DIM}{n}{C.RESET}  {h[-10:]:<12}  "
                  f"{last_digit(h) or '-':>2}  {col}{cls}{C.RESET}")
    else:
        print(f"  {C.DIM}(no blocks loaded){C.RESET}")
    print(line)

    print(f"{C.BOLD}Lottery History ({len(latest_history)}/30):{C.RESET}")
    if latest_history:
        for it in latest_history[:5]:
            issue = it.get("issueNumber", "—")
            num = it.get("number", "—")
            try: t = "BIG" if int(num) >= 5 else "SMALL"
            except (TypeError, ValueError): t = "—"
            col = C.GREEN if t == "BIG" else (C.YELLOW if t == "SMALL" else C.GRAY)
            print(f"  {issue:<14}  {str(num):>4}  {col}{t}{C.RESET}")
    else:
        print(f"  {C.DIM}(waiting for history…){C.RESET}")
    print(line)

    if prediction_history:
        print(f"{C.BOLD}Predictions ({len(prediction_history)}/{PRED_HISTORY_MAX}):{C.RESET}")
        for p in prediction_history[:PRED_HISTORY_MAX]:
            oc = p["outcome"]
            ocol = C.GREEN if oc == "WIN" else (C.RED if oc == "LOSE" else C.GRAY)
            pcol = C.GREEN if p["prediction"] == "BIG" else C.YELLOW
            tag = " 📤" if oc == "WIN" else (" ⏳" if oc == "WAIT" else "")
            print(f"  {p['period']:<18}  {pcol}{p['prediction']:<5}{C.RESET}  →  {ocol}{oc}{C.RESET}{tag}")
        print(line)

    print(f"{C.BOLD}Auto Search:{C.RESET}")
    if last_auto_pattern:
        colored = "".join(
            f"{C.GREEN}B{C.RESET}" if c == "B" else f"{C.YELLOW}S{C.RESET}"
            for c in last_auto_pattern)
        print(f"  Pattern: {colored}")
    else:
        print(f"  {C.DIM}(waiting for enough history){C.RESET}")

    if last_search_result:
        r = last_search_result
        if r["matched"] is not None:
            head = (f"Pattern: {C.WHITE}{r['matched']}{C.RESET}"
                    if r["matched"] == last_auto_pattern
                    else f"Matched: {C.WHITE}{r['matched']}{C.RESET}")
            print(f"  {head} · {last_search_elapsed:.1f} ms")
            print(f"  Hits: {r['count']}")
            tot = r["big"] + r["small"]
            if tot > 0:
                bp = round(r["big"]/tot*100); sp = 100 - bp
                bar = (f"{C.YELLOW}{'█'*(bp//5):<20}{C.RESET}"
                       f"{C.BLUE}{'█'*(sp//5):<20}{C.RESET}")
                print(f"  {C.YELLOW}BIG {bp}%{C.RESET} | "
                      f"{C.BLUE}SMALL {sp}%{C.RESET}  {bar}")
            if last_search_prediction:
                print(f"  → {C.GREEN}{C.BOLD}Prediction: {last_search_prediction}{C.RESET}")
            else:
                print(f"  → {C.DIM}No 100% BIG/SMALL result{C.RESET}")
        else:
            print(f"  Not found")
    if last_error_msg:
        print(f"  {C.YELLOW}{last_error_msg}{C.RESET}")
    print(line)

    if alert_active:
        ts = f"{C.GREEN}ACTIVE (sending until WIN){C.RESET}"
    elif consecutive_losses == 0:
        ts = f"{C.GRAY}idle (0/{LOSS_TRIGGER} LOSE){C.RESET}"
    else:
        ts = f"{C.YELLOW}waiting ({consecutive_losses}/{LOSS_TRIGGER} LOSE){C.RESET}"
    print(f"{C.BOLD}TG Alert:{C.RESET} {ts}")
    print(line)

    print(f"{C.BOLD}Log (last 5):{C.RESET}")
    for l in log_lines[-5:]:
        print(f"  {C.DIM}{l}{C.RESET}")
    print(line)
    print(f"{C.GRAY}Telegram: {TG_CHAT_ID} · Ctrl+C to quit{C.RESET}")


# ═══════════════════════════════════════════════════════
# BACKGROUND STARTUP
# ═══════════════════════════════════════════════════════
def start_background():
    load_cache()
    load_history_cache()
    reset_predictions()

    def runner():
        while True:
            if sync_blocks(): break
            log("Sync failed — retry in 30s…")
            time.sleep(30)
        live_poller()

    threading.Thread(target=runner, daemon=True).start()
    threading.Thread(target=history_poller, daemon=True).start()


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def main():
    print(f"{C.CYAN}[namespace]{C.RESET} {FB_NS}")
    print(f"{C.CYAN}[telegram]{C.RESET} chat={TG_CHAT_ID}")
    start_background()
    try:
        while True:
            render()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nBye 👋")


if __name__ == "__main__":
    main()