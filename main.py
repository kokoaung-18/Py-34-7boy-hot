import os
import json
import time
import signal
import asyncio
import requests

from datetime import datetime
from pathlib import Path
from telegram import Bot
from telegram.error import TelegramError

# ============================================================
# CONFIGURATION
# ============================================================
# Set these environment variables before running:
#   export TELEGRAM_TOKEN="your-new-bot-token"
#   export YOUR_CHAT_ID="your-chat-id"
#
# Do not put your bot token directly in this file.
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8971898524:AAGbQExBbu4wnvmAc9BCDoxtZ_gmIPAhmdE").strip()
YOUR_CHAT_ID = os.getenv("YOUR_CHAT_ID", "-1004322725776").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "6385759989").strip()

BASE_API_URL = (
    "https://draw.ar-lottery01.com/TrxWinGo/TrxWinGo_1M/"
    "GetHistoryIssuePage.json"
)

POLL_INTERVAL = 10
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
MAX_RESULTS_HISTORY = 10
SIGNALS_PER_PAGE = 10
PATTERN_BATCH_SIZE = 2
WAIT_MAX_RESULTS = 3
RECOVERY_STAKE_INDEX = 2  # Resume at 500ks after Wait ends.
STAKE_LEVELS = (
    100, 200, 500, 1200, 2500, 5500, 12000, 25000, 55000,
    120000, 250000, 550000,
)
WIN_STICKER_FILE_ID = (
    "CAACAgUAAxkBA6-TYGrvAVRZrnBEeMyvsm5AQfvx3LneAAIWIQACptoYV_YCGFUFwQoCPQQ"
)

# The two new rules use positions 6 and 10 in the admin history table.
# History display is newest-first, so these are zero-based indexes 5 and 9.
PREDICTION_RULES = ("Pattern 1", "Pattern 2")
STATE_FILE = Path(__file__).with_name("lottery_bot_state.json")

running = True
bot = None
last_results_history = []
admin_history_message_id = None
signal_page_records = []
signal_page_message_id = None
signal_page_number = 1
active_pattern = 0
predictions_in_batch = 0
current_stake_index = 0
waiting_for_recovery = False
wait_attempts = 0

# Terminal colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"


def print_color(text, color=RESET):
    print(f"{color}{text}{RESET}", flush=True)


def save_bot_state():
    """Persist the active three-signal pattern block and stake step."""
    state = {
        "active_pattern": active_pattern,
        "predictions_in_batch": predictions_in_batch,
        "current_stake_index": current_stake_index,
        "waiting_for_recovery": waiting_for_recovery,
        "wait_attempts": wait_attempts,
    }
    temporary_file = STATE_FILE.with_suffix(".tmp")
    try:
        temporary_file.write_text(json.dumps(state), encoding="utf-8")
        temporary_file.replace(STATE_FILE)
    except OSError as error:
        print_color(f"[!] Could not save pattern/stake state: {error}", YELLOW)


def load_bot_state():
    """Resume the pattern block and stake step after a process restart."""
    global active_pattern
    global predictions_in_batch
    global current_stake_index
    global waiting_for_recovery
    global wait_attempts

    active_pattern = 0
    predictions_in_batch = 0
    current_stake_index = 0
    waiting_for_recovery = False
    wait_attempts = 0
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        active_pattern = int(state.get("active_pattern", 0)) % len(PREDICTION_RULES)
        predictions_in_batch = max(0, int(state.get("predictions_in_batch", 0)))
        while predictions_in_batch >= PATTERN_BATCH_SIZE:
            active_pattern = (active_pattern + 1) % len(PREDICTION_RULES)
            predictions_in_batch -= PATTERN_BATCH_SIZE
        current_stake_index = int(state.get("current_stake_index", 0))
        current_stake_index = max(0, min(current_stake_index, len(STAKE_LEVELS) - 1))
        waiting_for_recovery = bool(state.get("waiting_for_recovery", False))
        wait_attempts = max(0, int(state.get("wait_attempts", 0)))
        if waiting_for_recovery and wait_attempts >= WAIT_MAX_RESULTS:
            waiting_for_recovery = False
            wait_attempts = 0
            current_stake_index = RECOVERY_STAKE_INDEX
        print_color(
            f"[*] Resumed {PREDICTION_RULES[active_pattern]} block "
            f"({predictions_in_batch}/{PATTERN_BATCH_SIZE} already sent).",
            CYAN,
        )
        if waiting_for_recovery:
            print_color(
                f"[*] Recovery Wait active ({wait_attempts}/{WAIT_MAX_RESULTS}); "
                f"resume stake is {STAKE_LEVELS[RECOVERY_STAKE_INDEX]}ks.",
                YELLOW,
            )
    except FileNotFoundError:
        print_color("[*] Starting with Pattern 1 and the first stake level.", CYAN)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        print_color(f"[!] Could not read saved state; starting fresh: {error}", YELLOW)


def validate_config():
    """Validate required environment variables before starting."""
    missing = []

    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not YOUR_CHAT_ID:
        missing.append("YOUR_CHAT_ID")
    if not ADMIN_CHAT_ID:
        missing.append("ADMIN_CHAT_ID")

    if missing:
        print_color(
            "[!] Missing environment variable(s): " + ", ".join(missing),
            RED,
        )
        print_color(
            'Example: export TELEGRAM_TOKEN="your-token"',
            YELLOW,
        )
        print_color(
            '         export YOUR_CHAT_ID="your-chat-id"',
            YELLOW,
        )
        return False

    return True


def fetch_history():
    """Fetch the latest lottery history from the API."""
    for attempt in range(MAX_RETRIES):
        try:
            timestamp = int(time.time() * 1000)
            url = f"{BASE_API_URL}?ts={timestamp}"
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                )
            }

            response = requests.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()
            if data.get("code") != 0:
                raise ValueError(data.get("msg", "Unknown API error"))

            if "data" not in data or "list" not in data["data"]:
                raise ValueError("Invalid API response format")

            history_list = data["data"]["list"]
            history_list.sort(
                key=lambda item: int(item.get("issueNumber", 0)),
                reverse=True,
            )
            return history_list[:10]

        except Exception as error:
            print_color(
                f"[!] API error (attempt {attempt + 1}/{MAX_RETRIES}): {error}",
                YELLOW,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)

    return None


def get_rule_predictions(results_history):
    """Return both predictions from the sixth and tenth newest results."""
    if len(results_history) < MAX_RESULTS_HISTORY:
        return None

    newest_first = list(reversed(results_history))
    sixth = get_result_letter(newest_first[5][2])
    tenth = get_result_letter(newest_first[9][2])
    if sixth not in ("B", "S") or tenth not in ("B", "S"):
        return None

    pair = (sixth, tenth)
    rule_one = {
        ("S", "B"): "BIG",
        ("B", "S"): "SMALL",
        ("B", "B"): "BIG",
        ("S", "S"): "SMALL",
    }
    rule_two = {
        ("S", "B"): "SMALL",
        ("B", "S"): "BIG",
        ("B", "B"): "SMALL",
        ("S", "S"): "BIG",
    }
    return (rule_one[pair], rule_two[pair])


def determine_result_type(number):
    try:
        return "B" if int(number) >= 5 else "S"
    except (TypeError, ValueError):
        return "?"


def get_result_letter(number):
    return determine_result_type(number)


def get_emoji(prediction, actual):
    predicted_type = "B" if prediction == "BIG" else "S"
    actual_type = determine_result_type(actual)
    return "✅" if predicted_type == actual_type else "❌"


def format_issue_number(issue_num, full=False):
    issue_string = str(issue_num)

    if full or len(issue_string) < 8:
        return issue_string

    return f"{issue_string[:4]}*****{issue_string[-4:]}"


def create_prediction_message(issue_num, pattern_idx, prediction, stake, is_wait=False):
    """Create a compact TRX signal, or a no-stake Wait notice."""
    del pattern_idx  # Pattern identity is tracked separately.
    period_suffix = format_issue_number(issue_num, full=True)[-4:]
    if is_wait:
        return f"<b>TRX ☞ {period_suffix} ( Wait )</b>"
    signal_letter = "B" if prediction == "BIG" else "S"
    return f"<b>TRX ☞ {period_suffix} ( {signal_letter} ➤ {stake}ks )</b>"


def create_results_message(results_history, title="📊 TRX HISTORY — LAST 10"):
    if not results_history:
        return ""

    rows = [
        f"<b>{title}</b>",
        "<pre>",
        " #  PERIOD  RESULT  SIGNAL  W/L",
        "",
    ]

    for serial, record in enumerate(reversed(results_history), start=1):
        issue_num, _emoji, actual_num = record[:3]
        win_loss = record[4] if len(record) > 4 else "—"
        if win_loss == "❌":
            win_loss = "☑️"
        signal = record[5] if len(record) > 5 else "—"
        issue = format_issue_number(issue_num, full=True)[-6:]
        letter = get_result_letter(actual_num)
        result_label = "BIG" if letter == "B" else "SMALL" if letter == "S" else "UNKNOWN"
        signal = signal.upper() if signal != "—" else "—"
        rows.append(
            f"{serial:>2}. {issue:<6}  {result_label:<6}  "
            f"{signal:<6}  {win_loss}"
        )

    rows.append("</pre>")
    return "\n".join(rows)


async def send_telegram_message(text, parse_mode=None, chat_id=None):
    global bot

    if bot is None:
        return None

    destination = chat_id if chat_id is not None else YOUR_CHAT_ID
    try:
        return await bot.send_message(
            chat_id=destination,
            text=text,
            parse_mode=parse_mode,
        )

    except TelegramError as error:
        print_color(f"[!] Telegram error: {error}", RED)
        return None


async def send_win_sticker():
    """Send the configured win sticker to the user/prediction chat."""
    if bot is None:
        return False
    try:
        await bot.send_sticker(
            chat_id=YOUR_CHAT_ID,
            sticker=WIN_STICKER_FILE_ID,
        )
        return True
    except TelegramError as error:
        print_color(f"[!] Could not send win sticker: {error}", RED)
        return False


async def update_admin_history():
    """Create or refresh the single rolling history message for the admin."""
    global admin_history_message_id

    if bot is None or not last_results_history:
        return

    text = create_results_message(last_results_history)
    if admin_history_message_id is not None:
        try:
            await bot.edit_message_text(
                chat_id=ADMIN_CHAT_ID,
                message_id=admin_history_message_id,
                text=text,
                parse_mode="HTML",
            )
            return
        except TelegramError as error:
            print_color(f"[!] Admin history edit failed; sending a new message: {error}", YELLOW)
            admin_history_message_id = None

    sent = await send_telegram_message(
        text,
        parse_mode="HTML",
        chat_id=ADMIN_CHAT_ID,
    )
    if sent is not None:
        admin_history_message_id = sent.message_id
    else:
        print_color(
            "[!] Admin history was not delivered. Ask the admin to open "
            "@lottery_King_Hot_bot and press /start, then verify that "
            "ADMIN_CHAT_ID is the admin's Telegram user ID.",
            RED,
        )


async def update_admin_signal_history(record):
    """Update a page of ten completed signals, then archive and start a new page."""
    global signal_page_records
    global signal_page_message_id
    global signal_page_number

    if bot is None:
        return

    signal_page_records.append(record)
    title = f"📊 BET HISTORY — PAGE {signal_page_number}"
    text = create_results_message(signal_page_records, title=title)

    if signal_page_message_id is not None:
        try:
            await bot.edit_message_text(
                chat_id=ADMIN_CHAT_ID,
                message_id=signal_page_message_id,
                text=text,
                parse_mode="HTML",
            )
        except TelegramError as error:
            print_color(
                f"[!] Signal page edit failed; sending a new page message: {error}",
                YELLOW,
            )
            signal_page_message_id = None

    if signal_page_message_id is None:
        sent = await send_telegram_message(
            text,
            parse_mode="HTML",
            chat_id=ADMIN_CHAT_ID,
        )
        if sent is None:
            print_color(
                "[!] Signal history page was not delivered; check Admin chat and /start.",
                RED,
            )
            return
        signal_page_message_id = sent.message_id

    if len(signal_page_records) >= SIGNALS_PER_PAGE:
        print_color(
            f"[*] BET HISTORY page {signal_page_number} completed with "
            f"{len(signal_page_records)} signals; archived for Admin.",
            GREEN,
        )
        signal_page_records = []
        signal_page_message_id = None
        signal_page_number += 1


async def prepare_rule_prediction(issue_num, results_history, selected_pattern):
    """Send one prediction for the active rule and current multiplier step."""
    predictions = get_rule_predictions(results_history)
    if predictions is None:
        print_color("[*] Need ten valid results before predicting.", YELLOW)
        return []

    predicted_issue = str(int(issue_num) + 1)
    newest_first = list(reversed(results_history))
    sixth = get_result_letter(newest_first[5][2])
    tenth = get_result_letter(newest_first[9][2])
    prediction = predictions[selected_pattern]
    print_color(
        f"[*] Row 6={sixth}, row 10={tenth}; "
        f"sending {PREDICTION_RULES[selected_pattern]} ({prediction}), "
        f"batch {predictions_in_batch + 1}/{PATTERN_BATCH_SIZE}.",
        CYAN,
    )

    text = create_prediction_message(
        predicted_issue,
        selected_pattern,
        prediction,
        STAKE_LEVELS[current_stake_index],
        is_wait=waiting_for_recovery,
    )
    print_color(text, YELLOW if waiting_for_recovery else GREEN)
    sent = await send_telegram_message(text, parse_mode="HTML")
    if sent is None:
        return []

    return [(selected_pattern, prediction, sent.message_id, predicted_issue, waiting_for_recovery)]


def advance_pattern_cycle():
    """Switch to the other rule after the configured delivered predictions."""
    global active_pattern
    global predictions_in_batch

    predictions_in_batch += 1
    if predictions_in_batch >= PATTERN_BATCH_SIZE:
        old_pattern = active_pattern
        active_pattern = (active_pattern + 1) % len(PREDICTION_RULES)
        predictions_in_batch = 0
        print_color(
            f"[*] Finished {PATTERN_BATCH_SIZE} {PREDICTION_RULES[old_pattern]} predictions; "
            f"switching to {PREDICTION_RULES[active_pattern]}.",
            YELLOW,
        )
    save_bot_state()


async def initialize_bot():
    global bot

    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        await bot.initialize()
        bot_info = await bot.get_me()
        print_color(
            f"[+] Bot initialized: @{bot_info.username}",
            GREEN,
        )
        return True

    except Exception as error:
        print_color(f"[!] Bot initialization failed: {error}", RED)
        if bot is not None:
            try:
                await bot.shutdown()
            except Exception:
                pass
            bot = None
        return False


async def process_lottery_data():
    global last_results_history
    global admin_history_message_id
    global signal_page_records
    global signal_page_message_id
    global signal_page_number
    global running
    global active_pattern
    global predictions_in_batch
    global current_stake_index
    global waiting_for_recovery
    global wait_attempts

    last_results_history = []
    admin_history_message_id = None
    signal_page_records = []
    signal_page_message_id = None
    signal_page_number = 1
    load_bot_state()
    previous_issue = None
    pending_prediction = None

    print_color("\n" + "=" * 50, CYAN)
    print_color("[*] Starting Lottery Bot - Alternating Rules", CYAN)
    print_color(
        f"[*] Pattern 1: {PATTERN_BATCH_SIZE} predictions, then "
        f"Pattern 2: {PATTERN_BATCH_SIZE} predictions",
        CYAN,
    )
    print_color("[*] The 3-prediction cycle repeats until stopped", CYAN)
    print_color("=" * 50, CYAN)

    while running:
        try:
            history = await asyncio.to_thread(fetch_history)
            if not history:
                print_color("[!] Failed to fetch data. Retrying...", YELLOW)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            latest_item = history[0]
            latest_issue = latest_item.get("issueNumber")
            latest_number = latest_item.get("number")
            if latest_issue is None:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            try:
                int(latest_number)
            except (TypeError, ValueError):
                print_color(f"[!] Invalid latest result: {latest_number}", YELLOW)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if previous_issue is None:
                # API results arrive newest-first; store chronologically.
                for item in reversed(history):
                    issue = item.get("issueNumber")
                    number = item.get("number")
                    try:
                        int(number)
                        if issue is not None:
                            last_results_history.append((issue, "📌", number, "—", "—", "—"))
                    except (TypeError, ValueError):
                        print_color(f"[!] Skipping invalid startup result: {number}", YELLOW)

                last_results_history = last_results_history[-MAX_RESULTS_HISTORY:]
                previous_issue = latest_issue
                print_color(
                    f"[*] Loaded {len(last_results_history)} startup history record(s). "
                    f"Latest issue: {latest_issue}",
                    CYAN,
                )
                # Keep startup API history internal for rule calculations only.
                predictions = await prepare_rule_prediction(
                    latest_issue,
                    last_results_history,
                    active_pattern,
                )
                if predictions:
                    pending_prediction = predictions[0]
                    advance_pattern_cycle()
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if latest_issue != previous_issue:
                print_color("\n[!] NEW RESULT DETECTED!", GREEN)

                pattern_used = "—"
                win_loss = "—"
                prediction_label = "—"
                signal_was_sent = pending_prediction is not None
                if pending_prediction is not None:
                    pattern_idx, prediction, message_id, predicted_issue, was_wait = pending_prediction
                    pattern_used = PREDICTION_RULES[pattern_idx]
                    is_win = get_emoji(prediction, latest_number) == "✅"
                    prediction_label = "WAIT" if was_wait else prediction.upper()
                    win_loss = "✅" if is_win else "☑️"

                    if was_wait:
                        # Wait outcomes are tracked for the three-result limit, but never
                        # change stake as a win/loss and never send a win sticker.
                        wait_attempts += 1
                        if is_win:
                            waiting_for_recovery = False
                            wait_attempts = 0
                            current_stake_index = RECOVERY_STAKE_INDEX
                            print_color(
                                f"[*] Wait prediction matched; resume at "
                                f"{STAKE_LEVELS[current_stake_index]}ks.",
                                GREEN,
                            )
                        elif wait_attempts >= WAIT_MAX_RESULTS:
                            waiting_for_recovery = False
                            wait_attempts = 0
                            current_stake_index = RECOVERY_STAKE_INDEX
                            print_color(
                                f"[*] {WAIT_MAX_RESULTS} Wait results completed; resume at "
                                f"{STAKE_LEVELS[current_stake_index]}ks.",
                                YELLOW,
                            )
                        else:
                            print_color(
                                f"[*] Wait result {wait_attempts}/{WAIT_MAX_RESULTS} missed; "
                                "no stake was sent.",
                                YELLOW,
                            )
                    elif is_win:
                        current_stake_index = 0
                        waiting_for_recovery = False
                        wait_attempts = 0
                        await send_win_sticker()
                        print_color(
                            f"[*] Bet win: reset stake to {STAKE_LEVELS[0]}ks.",
                            GREEN,
                        )
                    else:
                        # Wait is triggered specifically by consecutive losses at 100ks
                        # and 400ks. After recovery, larger stakes progress normally.
                        if current_stake_index == 1:
                            waiting_for_recovery = True
                            wait_attempts = 0
                            current_stake_index = RECOVERY_STAKE_INDEX
                            print_color(
                                "[*] 100ks and 400ks both lost; entering Wait for up to "
                                f"{WAIT_MAX_RESULTS} results.",
                                YELLOW,
                            )
                        else:
                            current_stake_index = min(
                                current_stake_index + 1,
                                len(STAKE_LEVELS) - 1,
                            )
                        print_color(
                            f"[*] Loss: next stake is {STAKE_LEVELS[current_stake_index]}ks."
                            if not waiting_for_recovery
                            else "[*] Loss at 400ks: next signals will be Wait.",
                            RED,
                        )

                    save_bot_state()
                    print_color(
                        f"   {PREDICTION_RULES[pattern_idx]}: {prediction} | "
                        f"actual {get_result_letter(latest_number)} | "
                        f"{'WIN' if is_win else 'LOSS'}"
                        f"{' (WAIT; no stake)' if was_wait else ''}",
                        GREEN if is_win else RED,
                    )
                pending_prediction = None

                result_record = (
                    latest_issue,
                    "📌",
                    latest_number,
                    pattern_used,
                    win_loss,
                    prediction_label,
                )
                last_results_history.append(result_record)
                last_results_history = last_results_history[-MAX_RESULTS_HISTORY:]
                if signal_was_sent:
                    await update_admin_signal_history(result_record)

                predictions = await prepare_rule_prediction(
                    latest_issue,
                    last_results_history,
                    active_pattern,
                )
                if predictions:
                    pending_prediction = predictions[0]
                    advance_pattern_cycle()

                previous_issue = latest_issue
                print_color("-" * 50, GREEN)
            else:
                current_time = datetime.now().strftime("%H:%M:%S")
                print_color(
                    f"[{current_time}] Monitoring... Latest: {latest_issue}",
                    RESET,
                )

            await asyncio.sleep(POLL_INTERVAL)

        except Exception as error:
            print_color(f"[!] Loop error: {error}", RED)
            await asyncio.sleep(POLL_INTERVAL)


async def main():
    if not validate_config():
        return

    if not await initialize_bot():
        print_color("[!] Cannot start the bot.", RED)
        return

    try:
        await process_lottery_data()
    except KeyboardInterrupt:
        print_color("\n[!] Stopped by user.", YELLOW)
    finally:
        if bot is not None:
            try:
                await bot.shutdown()
            except Exception as error:
                print_color(f"[!] Bot shutdown warning: {error}", YELLOW)
        print_color("\n[+] Shutdown complete.", GREEN)


def signal_handler(sig, frame):
    del sig, frame
    global running
    running = False
    print_color("\n[!] Stopping...", YELLOW)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print_color("=" * 50, CYAN)
    print_color("LOTTERY BOT - TWO HISTORY RULES", CYAN)
    print_color(
        f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        CYAN,
    )
    print_color("=" * 50, CYAN)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print_color(f"\n[!] Crash: {error}", RED)
