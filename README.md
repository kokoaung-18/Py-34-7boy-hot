# 🤖 Telegram Hoster Bot

Python Telegram bot for hosting user projects (24/7).

## Features
- Upload `.zip` Python projects
- Start / Stop / Delete projects
- View live logs
- Admin user management (allowlist)
- Per-user project limit (10)

## Deploy on Render (Background Worker)

### 1. GitHub
1. Create a **PRIVATE** repo
2. Upload all files (`bot.py`, `requirements.txt`, `Procfile`, `runtime.txt`, `render.yaml`, `.gitignore`)
3. Commit & push

### 2. Render
1. Go to https://render.com → Sign up with GitHub
2. Dashboard → **New +** → **Background Worker**
3. Connect your repo
4. Render auto-detects `render.yaml` (or fill manually):
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot.py`
   - **Plan**: Free
5. Click **Create**

### 3. Verify
- Open Render **Logs** tab
- Look for: `✅ Hoster Bot started successfully.`
- Send `/start` to your bot on Telegram

## ⚠️ Important Warnings

- **Free tier filesystem is ephemeral** — data lost on restart
- **Token is hardcoded in `bot.py`** — keep repo PRIVATE
- **Executed projects run as bot user** — no sandbox, use trusted users only

## Admin Commands
- `/add <user_id>` — allow a user
- `/users` — list allowed users
- `/cleanup` — info only

## Persistence (Optional, paid)
- Add **Render Disk** → mount at `/var/data`
- Set `DATA_DIR=/var/data` env var