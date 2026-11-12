import logging
import zipfile
import io
import os
import subprocess
import sqlite3
import shutil
import signal
import re
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes,
    ConversationHandler, CallbackQueryHandler
)
from telegram.error import BadRequest

# ======================== CONFIGURATION ========================
BOT_TOKEN = "8200197901:AAHr36BIl4W-K7evFgmcbxHbCg7pmwUKnyY"   # သင့် Bot Token
ADMIN_ID = 6385759989   # သင့် Telegram User ID

BOT_TOKEN = os.environ.get("BOT_TOKEN", BOT_TOKEN)
ADMIN_ID = int(os.environ.get("ADMIN_ID", ADMIN_ID))

PROJECTS_ROOT = os.path.abspath("./user_projects")
os.makedirs(PROJECTS_ROOT, exist_ok=True)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

NAMING_PROJECT, UPLOADING_ZIP = range(2)
running_processes = {}
project_start_times = {}

# ======================== DATABASE ========================
def init_db():
    conn = sqlite3.connect('hoster.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS allowed_users (user_id INTEGER PRIMARY KEY)')
    c.execute('''CREATE TABLE IF NOT EXISTS projects (
        user_id INTEGER,
        project_name TEXT,
        sanitized_key TEXT,
        folder_path TEXT,
        PRIMARY KEY (user_id, sanitized_key)
    )''')
    c.execute("PRAGMA table_info(projects)")
    columns = [col[1] for col in c.fetchall()]
    if 'sanitized_key' not in columns:
        c.execute("ALTER TABLE projects ADD COLUMN sanitized_key TEXT")
    conn.commit()
    conn.close()

# ======================== RESET FUNCTION (အသစ်ပြန်စရန်) ========================
def reset_all_data():
    """Bot စတင်တိုင်း အရင်အချက်အလက်တွေ အကုန်ရှင်းပစ်မယ်"""
    global running_processes, project_start_times
    
    # ၁။ Memory ထဲက process စာရင်းရှင်းမယ်
    running_processes.clear()
    project_start_times.clear()
    
    # ၂။ ကျန်နေတဲ့ orphan process တွေကိုသတ်မယ်
    try:
        result = subprocess.run(['ps', '-A', '-o', 'pid,args'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines[1:]:
                if not line.strip(): continue
                parts = line.strip().split(' ', 1)
                if len(parts) < 2: continue
                try:
                    pid = int(parts[0])
                    if 'python' in parts[1]:
                        cwd_path = f'/proc/{pid}/cwd'
                        if os.path.exists(cwd_path):
                            try:
                                cwd = os.readlink(cwd_path)
                                if cwd and cwd.startswith(PROJECTS_ROOT):
                                    os.kill(pid, signal.SIGKILL)
                                    logging.info(f"Killed orphaned process {pid}")
                            except:
                                pass
                except:
                    continue
    except:
        pass

    # ၃။ user_projects ဖိုလ်ဒါတစ်ခုလုံးဖျက်မယ်
    if os.path.exists(PROJECTS_ROOT):
        shutil.rmtree(PROJECTS_ROOT)
    os.makedirs(PROJECTS_ROOT, exist_ok=True)
    
    # ၄။ Database ဖိုင်ကိုဖျက်မယ်
    if os.path.exists("hoster.db"):
        os.remove("hoster.db")
    
    # ၅။ Database အသစ်ပြန်ဆောက်မယ်
    init_db()
    logging.info("✅ All previous data has been wiped. Fresh start!")

# ======================== HELPERS ========================
def is_allowed(user_id):
    if user_id == ADMIN_ID: return True
    conn = sqlite3.connect('hoster.db')
    c = conn.cursor()
    c.execute("SELECT 1 FROM allowed_users WHERE user_id=?", (user_id,))
    res = c.fetchone()
    conn.close()
    return res is not None

def get_projects(uid):
    conn = sqlite3.connect('hoster.db')
    c = conn.cursor()
    c.execute("SELECT project_name, sanitized_key FROM projects WHERE user_id=?", (uid,))
    data = c.fetchall()
    conn.close()
    return data

def get_project_by_key(uid, safe_key):
    conn = sqlite3.connect('hoster.db')
    c = conn.cursor()
    c.execute("SELECT project_name, folder_path FROM projects WHERE user_id=? AND sanitized_key=?", (uid, safe_key))
    row = c.fetchone()
    conn.close()
    return row

def sanitize_callback(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def cleanup_orphaned_processes():
    # ဒီ function ကို reset ထဲမှာ ထည့်သုံးပြီးပြီမို့ အခုတော့ ဘာမှမလုပ်တော့ဘူး
    return 0

def check_and_cleanup_orphans():
    return 0

# ======================== ADMIN COMMANDS ========================
async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        new_id = int(context.args[0])
        conn = sqlite3.connect('hoster.db')
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO allowed_users VALUES (?)", (new_id,))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"✅ User {new_id} added.")
    except:
        await update.message.reply_text("Usage: /add <chat_id>")

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    conn = sqlite3.connect('hoster.db')
    c = conn.cursor()
    c.execute("SELECT user_id FROM allowed_users")
    users = c.fetchall()
    conn.close()
    if not users:
        await update.message.reply_text("👥 No users found.")
        return
    msg = "👥 Allowed Users:\n_______________________\n"
    for (uid,) in users:
        try:
            chat = await context.bot.get_chat(uid)
            name = chat.first_name
        except:
            name = "Unknown"
        active = sum(1 for pid in running_processes if pid.startswith(f"{uid}_"))
        msg += f"Name: {name}\nID: {uid}\nActive: {active}\n________________________\n"
    await update.message.reply_text(msg)

async def cleanup_orphans_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ℹ️ ဒီ Bot က စတင်တိုင်း အလိုအလျောက် ရှင်းပေးတယ်။")

# ======================== USER HANDLERS ========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        await update.message.reply_text("🚫 Access Denied. Contact Admin.")
        return
    kb = [
        [InlineKeyboardButton("📝 MY PROJECTS", callback_data="list_projects")],
        [InlineKeyboardButton("🧑‍💻 OWNER", url="https://t.me/Lotteryfather100000000")]
    ]
    text = (
        "🧑‍💻 Lottery Father 24/7 Hoster\n\n"
        "Welcome! Upload your Python project as .zip\n"
        "🆓 Free tier: 50 projects max.\n"
        "⚠️ Note: Bot restart ဖြစ်ရင် Project အကုန်ပျက်သွားမယ်။"
    )
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest as e:
        logging.error(f"Start error: {e}")

async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.callback_query.from_user.id
    projs = get_projects(uid)
    if len(projs) >= 50:
        await update.callback_query.message.reply_text("❌ You already have 50 projects. Delete one first.")
        return ConversationHandler.END
    await update.callback_query.message.reply_text("Enter a name for your new project:")
    return NAMING_PROJECT

async def name_rcv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['pname'] = update.message.text
    await update.message.reply_text(f"✅ Name: {update.message.text}\nNow upload your .zip archive:")
    return UPLOADING_ZIP

async def zip_rcv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    pname = context.user_data['pname']
    msg = await update.message.reply_text("⏳ Extracting and setting up...")
    try:
        projs = get_projects(uid)
        if len(projs) >= 50:
            await msg.edit_text("❌ Project limit reached (50).")
            return ConversationHandler.END

        file = await context.bot.get_file(update.message.document.file_id)
        file_bytes = await file.download_as_bytearray()

        base_safe = sanitize_callback(f"{uid}_{pname}")
        safe_key = base_safe
        counter = 1
        path = os.path.join(PROJECTS_ROOT, safe_key)
        while os.path.exists(path):
            safe_key = f"{base_safe}_{counter}"
            path = os.path.join(PROJECTS_ROOT, safe_key)
            counter += 1
        os.makedirs(path, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            z.extractall(path)

        items = [f for f in os.listdir(path) if not f.startswith('.')]
        if len(items) == 1:
            single_item = os.path.join(path, items[0])
            if os.path.isdir(single_item):
                for item in os.listdir(single_item):
                    src = os.path.join(single_item, item)
                    dst = os.path.join(path, item)
                    shutil.move(src, dst)
                os.rmdir(single_item)

        main_file = None
        if os.path.exists(os.path.join(path, "main.py")):
            main_file = "main.py"
        elif os.path.exists(os.path.join(path, "bot.py")):
            main_file = "bot.py"

        if not main_file:
            shutil.rmtree(path)
            await msg.edit_text("❌ No main.py or bot.py found.")
            return ConversationHandler.END

        if os.path.exists(os.path.join(path, "requirements.txt")):
            await msg.edit_text("⚙️ Installing dependencies...")
            subprocess.run(["pip", "install", "-r", os.path.join(path, "requirements.txt")], check=False)

        log_path = os.path.join(path, "logs.txt")
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"Project '{pname}' created at {datetime.now()}\n")

        conn = sqlite3.connect('hoster.db')
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO projects (user_id, project_name, sanitized_key, folder_path) VALUES (?,?,?,?)",
                  (uid, pname, safe_key, path))
        conn.commit()
        conn.close()

        await msg.edit_text(f"✅ Project '{pname}' setup complete! Use /start to manage.")
    except zipfile.BadZipFile:
        await msg.edit_text("❌ Invalid zip file.")
    except Exception as e:
        logging.error(f"Zip error: {e}")
        await msg.edit_text(f"❌ Error: {str(e)}")
    return ConversationHandler.END

# ======================== UI ========================
async def show_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    projs = get_projects(uid)
    kb = []
    row = []
    for pname, safe_key in projs:
        row.append(InlineKeyboardButton(f"📁 {pname}", callback_data=f"manage_{safe_key}"))
        if len(row) == 3:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    
    kb.append([
        InlineKeyboardButton("➕ New Project", callback_data="add_new"),
        InlineKeyboardButton("🗑 Delete All", callback_data="delete_all")
    ])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="back")])
    
    if not projs:
        kb = [
            [InlineKeyboardButton("➕ New Project", callback_data="add_new")],
            [InlineKeyboardButton("🔙 Back", callback_data="back")]
        ]
        await query.edit_message_text("📭 No active projects.", reply_markup=InlineKeyboardMarkup(kb))
        return
    await query.edit_message_text("📂 Your Active Projects:", reply_markup=InlineKeyboardMarkup(kb))

async def manage_proj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    try:
        raw = query.data
        if not raw.startswith("manage_"):
            await query.answer("Invalid.")
            return
        safe_key = raw[7:]
        proj_data = get_project_by_key(uid, safe_key)
        if not proj_data:
            await query.answer("❌ Not found.")
            return
        pname, path = proj_data
    except Exception:
        await query.answer("Error.")
        return

    is_running = False
    if safe_key in running_processes:
        proc = running_processes[safe_key]
        try:
            if proc.poll() is None:
                is_running = True
            else:
                del running_processes[safe_key]
                if safe_key in project_start_times:
                    del project_start_times[safe_key]
        except:
            pass

    status = "🟢 Running" if is_running else "🔴 Stopped"
    start_time = project_start_times.get(safe_key, "Not Started")
    txt = f"📁 Project: {pname}\nStatus: {status}\nStarted: {start_time}"

    kb = [
        [InlineKeyboardButton("▶️ Start", callback_data=f"run_{safe_key}"),
         InlineKeyboardButton("⏹ Stop", callback_data=f"stop_{safe_key}"),
         InlineKeyboardButton("📄 Logs", callback_data=f"log_{safe_key}")],
        [InlineKeyboardButton("🗑 Delete", callback_data=f"del_{safe_key}"),
         InlineKeyboardButton("🔙 Back", callback_data="list_projects")]
    ]
    await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def view_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    try:
        _, safe_key = query.data.split("_", 1)
        proj_data = get_project_by_key(uid, safe_key)
        if not proj_data:
            await query.answer("❌ Not found.")
            return
        pname, path = proj_data
    except Exception:
        await query.answer("Error.")
        return

    log_path = os.path.join(path, "logs.txt")
    if not os.path.exists(log_path):
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("No logs yet.\n")
    try:
        await query.answer("📄 Sending logs...")
        with open(log_path, 'rb') as f:
            await query.message.reply_document(document=f, filename=f"logs_{pname}.txt", caption=f"Logs for {pname}")
        try:
            await query.message.delete()
        except:
            pass
    except Exception as e:
        logging.error(f"Log send error: {e}")
        await query.answer("Error sending log.")

# ======================== DELETE ALL ========================
async def delete_all_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = [
        [InlineKeyboardButton("✅ Yes, Delete All", callback_data="confirm_delete_all")],
        [InlineKeyboardButton("❌ Cancel", callback_data="list_projects")]
    ]
    await query.edit_message_text(
        "⚠️ Are you sure you want to delete **ALL** your projects?\nThis action cannot be undone!",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def confirm_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer("🗑 Deleting all projects...")

    projs = get_projects(uid)
    deleted_count = 0
    
    for pname, safe_key in projs:
        if safe_key in running_processes:
            proc = running_processes[safe_key]
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except:
                try:
                    proc.kill()
                except:
                    pass
            del running_processes[safe_key]
            if safe_key in project_start_times:
                del project_start_times[safe_key]
        
        proj_data = get_project_by_key(uid, safe_key)
        if proj_data:
            _, path = proj_data
            if os.path.exists(path):
                shutil.rmtree(path)
        
        conn = sqlite3.connect('hoster.db')
        c = conn.cursor()
        c.execute("DELETE FROM projects WHERE user_id=? AND sanitized_key=?", (uid, safe_key))
        conn.commit()
        conn.close()
        deleted_count += 1

    await show_list(update, context)
    if deleted_count > 0:
        await query.message.reply_text(f"✅ Successfully deleted {deleted_count} project(s).")
    else:
        await query.message.reply_text("ℹ️ You had no projects to delete.")

# ======================== CONTROL ========================
async def control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    uid = query.from_user.id

    try:
        action, safe_key = data.split("_", 1)
        proj_data = get_project_by_key(uid, safe_key)
        if not proj_data:
            await query.answer("❌ Not found.")
            return
        pname, path = proj_data
    except Exception:
        await query.answer("Error.")
        return

    try:
        if action == "run":
            if not os.path.exists(path):
                await query.answer("❌ Folder missing.")
                return
            if safe_key in running_processes:
                proc = running_processes[safe_key]
                try:
                    if proc.poll() is not None:
                        del running_processes[safe_key]
                        if safe_key in project_start_times:
                            del project_start_times[safe_key]
                except:
                    pass
            if safe_key not in running_processes:
                main_file = None
                if os.path.exists(os.path.join(path, "main.py")):
                    main_file = "main.py"
                elif os.path.exists(os.path.join(path, "bot.py")):
                    main_file = "bot.py"
                if not main_file:
                    await query.answer("❌ No main.py/bot.py")
                    return
                log_path = os.path.join(path, "logs.txt")
                log_file = open(log_path, "a", encoding='utf-8')
                log_file.write(f"\n--- Started at {datetime.now()} ---\n")
                log_file.flush()
                running_processes[safe_key] = subprocess.Popen(
                    ["python3", "-u", main_file],
                    cwd=path,
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True
                )
                project_start_times[safe_key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await query.answer(f"🚀 Started {pname}")
                await manage_proj(update, context)
                return
            else:
                await query.answer("Already running.")

        elif action == "stop":
            if safe_key in running_processes:
                proc = running_processes[safe_key]
                log_path = os.path.join(path, "logs.txt")
                try:
                    with open(log_path, "a", encoding='utf-8') as f:
                        f.write(f"--- Stopped at {datetime.now()} ---\n")
                except:
                    pass
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except:
                    try:
                        proc.terminate()
                    except:
                        pass
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                del running_processes[safe_key]
                if safe_key in project_start_times:
                    del project_start_times[safe_key]
                await query.answer(f"⏹ Stopped {pname}")
                await manage_proj(update, context)
                return
            else:
                await query.answer("Not running.")

        elif action == "del":
            if safe_key in running_processes:
                proc = running_processes[safe_key]
                try:
                    log_path = os.path.join(path, "logs.txt")
                    with open(log_path, "a", encoding='utf-8') as f:
                        f.write(f"--- Deleted at {datetime.now()} ---\n")
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except:
                    try:
                        proc.kill()
                    except:
                        pass
                del running_processes[safe_key]
                if safe_key in project_start_times:
                    del project_start_times[safe_key]
            conn = sqlite3.connect('hoster.db')
            c = conn.cursor()
            c.execute("DELETE FROM projects WHERE user_id=? AND sanitized_key=?", (uid, safe_key))
            conn.commit()
            conn.close()
            if os.path.exists(path):
                shutil.rmtree(path)
            await query.answer(f"🗑 Deleted {pname}")
            await show_list(update, context)
            return

        await manage_proj(update, context)
    except Exception as e:
        logging.error(f"Control error: {e}")
        await query.answer(f"Error: {str(e)[:50]}")

# ======================== MAIN ========================
def main():
    # 🔥 အရေးကြီးဆုံးအပိုင်း - Bot စတင်တိုင်း အချက်အလက်အကုန်ရှင်းမယ်
    reset_all_data()
    
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_user))
    app.add_handler(CommandHandler("users", list_users))
    app.add_handler(CommandHandler("cleanup", cleanup_orphans_cmd))

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(ask_name, pattern="^add_new$")],
        states={
            NAMING_PROJECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, name_rcv)],
            UPLOADING_ZIP: [MessageHandler(filters.Document.ALL, zip_rcv)]
        },
        fallbacks=[],
        per_message=False,
        per_chat=False
    )
    app.add_handler(conv_handler)

    app.add_handler(CallbackQueryHandler(show_list, pattern="^list_projects$"))
    app.add_handler(CallbackQueryHandler(manage_proj, pattern="^manage_"))
    app.add_handler(CallbackQueryHandler(view_logs, pattern="^log_"))
    app.add_handler(CallbackQueryHandler(control, pattern="^(run_|stop_|del_)"))
    app.add_handler(CallbackQueryHandler(start, pattern="^back$"))
    
    app.add_handler(CallbackQueryHandler(delete_all_projects, pattern="^delete_all$"))
    app.add_handler(CallbackQueryHandler(confirm_delete_all, pattern="^confirm_delete_all$"))

    print("✅ Hoster Bot started successfully. (Fresh state - all old data wiped)")
    app.run_polling()

if __name__ == '__main__':
    main()