#!/usr/bin/env python3
"""
ToolifyX — social media downloader bot with built-in stats + admin dashboard.

New in this version:
  • Annual premium plan (365 days) alongside the 30-day plan
  • Admin notification on every successful payment
  • Payment history logged and shown in /stats + dashboard
  • /payments admin command for quick payment review
  • /api/payments endpoint for the Spck dashboard
  • Welcoming /start with personal greeting + group CTA
  • Emoji diet — 1 emoji max per message, cleaner button labels
  • Per-user tracking (jobs, fails, cmds, bytes, per-platform usage)
  • Debounced persistence — writes are batched, zero impact on download speed
"""

import os
import json
import time
import logging
import asyncio
import secrets
import datetime
import aiohttp
import re
from pathlib import Path
from aiohttp import web
from supabase import create_client, Client
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, PreCheckoutQueryHandler, filters, ContextTypes
)

# --- CONFIG ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "toolifyx_social_dld_bot")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
MAX_FILE_SIZE_MB = 50
DAILY_LIMIT = 3
PREMIUM_PRICE_STARS = 150
PREMIUM_ANNUAL_PRICE_STARS = 1200
PREMIUM_DAILY_DAYS = 30
PREMIUM_ANNUAL_DAYS = 365
MONETAG_LINK = "https://omg10.com/4/10177672"
TOOLIFYX_LINK = "https://toolifyx.netlify.app/"
os.makedirs('downloads', exist_ok=True)

# --- STATS CONFIG ---
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_PATH = DATA_DIR / "users_toolifyx.json"
PAYMENTS_PATH = DATA_DIR / "payments_toolifyx.json"
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or "0")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()
if not ADMIN_TOKEN:
    ADMIN_TOKEN = "mf_" + secrets.token_hex(16)
    logging.warning(f"ADMIN_TOKEN not set — generated: {ADMIN_TOKEN}")
if not ADMIN_ID:
    logging.warning("ADMIN_ID not set — /stats disabled.")

# --- SUPABASE CLIENT ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- PLATFORM MAP ---
PLATFORMS = {
    "tiktok": {"backend": "https://tiktok-video-downloader-2-e3lu.onrender.com", "endpoint": "/download", "type": "video", "site": "https://toolifyx.netlify.app/tiktok", "name": "TikTok", "emoji": "🎵"},
    "facebook": {"backend": "https://fb-downloader-3.onrender.com", "endpoint": "/download", "type": "video", "site": "https://toolifyx.netlify.app/facebook", "name": "Facebook", "emoji": "📘"},
    "fb.watch": {"backend": "https://fb-downloader-3.onrender.com", "endpoint": "/download", "type": "video", "site": "https://toolifyx.netlify.app/facebook", "name": "Facebook", "emoji": "📘"},
    "pinterest": {"backend": "https://pinterest-downloder-kfjx.onrender.com", "endpoint": "/api/download", "type": "media", "site": "https://toolifyx.netlify.app/pinterest", "name": "Pinterest", "emoji": "📌"},
    "pin.it": {"backend": "https://pinterest-downloder-kfjx.onrender.com", "endpoint": "/api/download", "type": "media", "site": "https://toolifyx.netlify.app/pinterest", "name": "Pinterest", "emoji": "📌"},
    "twitter": {"backend": "https://twitter-downloader-vc7w.onrender.com", "endpoint": "/download", "type": "twitter", "site": "https://toolifyx.netlify.app/twitter", "name": "Twitter", "emoji": "🐦"},
    "x.com": {"backend": "https://twitter-downloader-vc7w.onrender.com", "endpoint": "/download", "type": "twitter", "site": "https://toolifyx.netlify.app/twitter", "name": "Twitter", "emoji": "🐦"},
    "snapchat": {"backend": "https://snapcd.onrender.com", "endpoint": "/download", "type": "video", "site": "https://toolifyx.netlify.app/snapchat.html", "name": "Snapchat", "emoji": "👻"},
}

# --- AD TIMESTAMPS ---
ad_timestamps = {}

logging.basicConfig(level=logging.INFO)

# ══════════════════════════════════════════════════════════════════════════════
#  STATS + USER TRACKING
# ══════════════════════════════════════════════════════════════════════════════

STATS = {
    "start_time": time.time(),
    "downloads_done": 0,
    "downloads_failed": 0,
    "bytes_out": 0,
    "downloads_today": 0,
    "today_date": str(datetime.date.today()),
    "payments_count": 0,
    "payments_stars": 0,
    "payments_stars_today": 0,
    "payments_today_date": str(datetime.date.today()),
}

USERS: dict[str, dict] = {}
PAYMENTS: list[dict] = []
_save_lock = asyncio.Lock()
_pay_save_lock = asyncio.Lock()
_dirty = False
_pay_dirty = False


def _load_users():
    global USERS
    if not USERS_PATH.exists():
        return
    try:
        raw = json.loads(USERS_PATH.read_text())
        if isinstance(raw, dict):
            USERS.update(raw)
            logging.info(f"Loaded {len(USERS)} tracked users.")
    except Exception as e:
        logging.warning(f"Could not read users: {e}")


def _load_payments():
    global PAYMENTS
    if not PAYMENTS_PATH.exists():
        return
    try:
        raw = json.loads(PAYMENTS_PATH.read_text())
        if isinstance(raw, list):
            PAYMENTS.extend(raw)
            logging.info(f"Loaded {len(PAYMENTS)} payment records.")
            # Recompute stats from stored payments
            STATS["payments_count"] = len(PAYMENTS)
            STATS["payments_stars"] = sum(p.get("amount", 0) for p in PAYMENTS)
    except Exception as e:
        logging.warning(f"Could not read payments: {e}")


async def _flush_users():
    global _dirty
    async with _save_lock:
        if not _dirty:
            return
        try:
            tmp = USERS_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(USERS, separators=(",", ":")))
            tmp.replace(USERS_PATH)
            _dirty = False
        except Exception as e:
            logging.warning(f"Could not write users: {e}")


async def _flush_payments():
    global _pay_dirty
    async with _pay_save_lock:
        if not _pay_dirty:
            return
        try:
            tmp = PAYMENTS_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(PAYMENTS, separators=(",", ":")))
            tmp.replace(PAYMENTS_PATH)
            _pay_dirty = False
        except Exception as e:
            logging.warning(f"Could not write payments: {e}")


async def _flusher_loop():
    while True:
        await asyncio.sleep(3)
        await _flush_users()
        await _flush_payments()


def _mark_dirty():
    global _dirty
    _dirty = True


def _mark_pay_dirty():
    global _pay_dirty
    _pay_dirty = True


def _rollover_day():
    today = str(datetime.date.today())
    if STATS["today_date"] != today:
        STATS["today_date"] = today
        STATS["downloads_today"] = 0
    if STATS["payments_today_date"] != today:
        STATS["payments_today_date"] = today
        STATS["payments_stars_today"] = 0


def track_seen(user) -> None:
    if user is None:
        return
    uid = str(user.id)
    now = time.time()
    u = USERS.get(uid)
    if not u:
        u = {
            "id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "first_seen": now,
            "last_seen": now,
            "jobs": 0,
            "fails": 0,
            "cmds": 0,
            "bytes_out": 0,
            "tools": {},
            "premium": False,
            "referrals": 0,
        }
        USERS[uid] = u
    if user.username:
        u["username"] = user.username
    if user.first_name:
        u["first_name"] = user.first_name
    u["last_seen"] = now
    _mark_dirty()


def track_cmd(user) -> None:
    if user is None:
        return
    track_seen(user)
    u = USERS[str(user.id)]
    u["cmds"] = u.get("cmds", 0) + 1
    _mark_dirty()


def track_download(user_id, platform_key: str, bytes_out: int, ok: bool = True) -> None:
    _rollover_day()
    if not user_id:
        return
    uid = str(user_id)
    u = USERS.get(uid)
    if not u:
        return
    if ok:
        u["jobs"] = u.get("jobs", 0) + 1
        u["tools"][platform_key] = u["tools"].get(platform_key, 0) + 1
        u["bytes_out"] = u.get("bytes_out", 0) + bytes_out
        STATS["downloads_done"] += 1
        STATS["downloads_today"] += 1
        STATS["bytes_out"] += bytes_out
    else:
        u["fails"] = u.get("fails", 0) + 1
        STATS["downloads_failed"] += 1
    _mark_dirty()


def track_premium(user_id, is_premium: bool) -> None:
    uid = str(user_id)
    u = USERS.get(uid)
    if u:
        u["premium"] = bool(is_premium)
        _mark_dirty()


def track_referral(referrer_id: int) -> None:
    uid = str(referrer_id)
    u = USERS.get(uid)
    if u:
        u["referrals"] = u.get("referrals", 0) + 1
        _mark_dirty()


def log_payment(user_id: int, amount: int, plan: str, charge_id: str, username: str = ""):
    """Append a payment record and bump the payment counters."""
    _rollover_day()
    entry = {
        "user_id": user_id,
        "username": username,
        "amount": amount,
        "plan": plan,
        "charge_id": charge_id,
        "timestamp": time.time(),
    }
    PAYMENTS.append(entry)
    # Cap stored history at 500
    if len(PAYMENTS) > 500:
        del PAYMENTS[:-500]
    STATS["payments_count"] += 1
    STATS["payments_stars"] += amount
    STATS["payments_stars_today"] += amount
    _mark_pay_dirty()


def is_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(ADMIN_ID) and u is not None and u.id == ADMIN_ID


def _fmt_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit != "B" else f"{n:.0f} B"
        n /= 1024
    return f"{n:.1f} TB"


def _fmt_duration(sec: float) -> str:
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def aggregate_stats() -> dict:
    now = time.time()
    day, week = 86400, 7 * 86400
    platforms: dict[str, int] = {}
    users_today = users_week = 0
    premium_count = 0
    referrals_total = 0
    for u in USERS.values():
        last = u.get("last_seen", 0)
        if now - last < day:
            users_today += 1
        if now - last < week:
            users_week += 1
        if u.get("premium"):
            premium_count += 1
        referrals_total += u.get("referrals", 0)
        for k, v in (u.get("tools") or {}).items():
            platforms[k] = platforms.get(k, 0) + v
    return {
        "bot": "toolifyx",
        "uptime_seconds": round(now - STATS["start_time"], 1),
        "jobs_done": STATS["downloads_done"],
        "jobs_failed": STATS["downloads_failed"],
        "bytes_out": STATS["bytes_out"],
        "users_total": len(USERS),
        "users_today": users_today,
        "users_week": users_week,
        "tools": platforms,
        "generated_at": now,
        "downloads_today": STATS["downloads_today"],
        "premium_users": premium_count,
        "referrals_total": referrals_total,
        # Payment stats
        "payments_count": STATS["payments_count"],
        "payments_stars": STATS["payments_stars"],
        "payments_stars_today": STATS["payments_stars_today"],
        "recent_payments": PAYMENTS[-10:],
    }


def first_name_of(user) -> str:
    if user is None:
        return "there"
    return (user.first_name or user.username or "there").split()[0]


# ══════════════════════════════════════════════════════════════════════════════
#  SUPABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def db_get_daily(user_id):
    today = str(datetime.date.today())
    try:
        res = supabase.table("daily_usage").select("*").eq("user_id", user_id).execute()
        if res.data and res.data[0]["usage_date"] == today:
            return res.data[0]["count"]
        supabase.table("daily_usage").upsert({"user_id": user_id, "usage_date": today, "count": 0}).execute()
        return 0
    except Exception as e:
        logging.error(f"DB daily get error: {e}")
        return 0

def db_increment_daily(user_id):
    today = str(datetime.date.today())
    try:
        current = db_get_daily(user_id)
        supabase.table("daily_usage").upsert({"user_id": user_id, "usage_date": today, "count": current + 1}).execute()
    except Exception as e:
        logging.error(f"DB daily inc error: {e}")

def db_decrement_daily(user_id, amount=2):
    today = str(datetime.date.today())
    try:
        current = db_get_daily(user_id)
        new_count = max(0, current - amount)
        supabase.table("daily_usage").upsert({"user_id": user_id, "usage_date": today, "count": new_count}).execute()
    except Exception as e:
        logging.error(f"DB daily dec error: {e}")

def db_is_premium(user_id):
    try:
        res = supabase.table("premium_users").select("*").eq("user_id", user_id).execute()
        if res.data:
            expiry = datetime.date.fromisoformat(res.data[0]["expires_at"])
            return expiry > datetime.date.today()
        return False
    except Exception as e:
        logging.error(f"DB premium error: {e}")
        return False

def db_set_premium(user_id, days=30):
    expiry = str(datetime.date.today() + datetime.timedelta(days=days))
    try:
        supabase.table("premium_users").upsert({"user_id": user_id, "expires_at": expiry}).execute()
    except Exception as e:
        logging.error(f"DB set premium error: {e}")

def db_get_bonus(user_id):
    try:
        res = supabase.table("bonus_downloads").select("*").eq("user_id", user_id).execute()
        return res.data[0]["count"] if res.data else 0
    except Exception as e:
        logging.error(f"DB bonus get error: {e}")
        return 0

def db_add_bonus(user_id, amount=1):
    try:
        current = db_get_bonus(user_id)
        supabase.table("bonus_downloads").upsert({"user_id": user_id, "count": current + amount}).execute()
    except Exception as e:
        logging.error(f"DB bonus add error: {e}")

def db_consume_bonus(user_id):
    try:
        current = db_get_bonus(user_id)
        if current > 0:
            supabase.table("bonus_downloads").upsert({"user_id": user_id, "count": current - 1}).execute()
            return True
    except Exception as e:
        logging.error(f"DB bonus consume error: {e}")
    return False

def db_get_referral_count(user_id):
    try:
        res = supabase.table("referral_counts").select("*").eq("user_id", user_id).execute()
        return res.data[0]["count"] if res.data else 0
    except Exception as e:
        logging.error(f"DB referral count error: {e}")
        return 0

def db_register_referral(user_id, referrer_id):
    try:
        existing = supabase.table("referrals").select("*").eq("user_id", user_id).execute()
        if existing.data:
            return False
        supabase.table("referrals").insert({"user_id": user_id, "referrer_id": referrer_id}).execute()
        current = db_get_referral_count(referrer_id)
        supabase.table("referral_counts").upsert({"user_id": referrer_id, "count": current + 1}).execute()
        db_add_bonus(referrer_id, 1)
        return True
    except Exception as e:
        logging.error(f"DB referral register error: {e}")
        return False

# --- HELPERS ---
def get_platform(url: str):
    url_lower = url.lower()
    for key, info in PLATFORMS.items():
        if key in url_lower:
            return info
    return None

async def delete_file(filepath, delay=600):
    await asyncio.sleep(delay)
    if os.path.exists(filepath):
        os.remove(filepath)
        logging.info(f"Deleted {filepath}")

# --- BACKEND WAKE + RETRY ---
async def try_backend(session, info, url, max_retries=2):
    for attempt in range(max_retries):
        try:
            try:
                await session.get(info["backend"] + "/", timeout=aiohttp.ClientTimeout(total=60))
            except Exception:
                pass
            async with session.post(
                info["backend"] + info["endpoint"],
                json={"url": url},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=90)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        return data
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}")
        if attempt < max_retries - 1:
            await asyncio.sleep(5)
    return None

async def keep_backends_alive():
    unique_backends = {info["backend"] for info in PLATFORMS.values()}
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                for backend in unique_backends:
                    try:
                        await session.get(backend + "/", timeout=aiohttp.ClientTimeout(total=15))
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"Keep-alive error: {e}")
        await asyncio.sleep(600)

# ══════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Download from TikTok", callback_data="plat_tiktok")],
        [InlineKeyboardButton("Download from Facebook", callback_data="plat_facebook")],
        [InlineKeyboardButton("Download from Pinterest", callback_data="plat_pinterest")],
        [InlineKeyboardButton("Download from Twitter / X", callback_data="plat_twitter")],
        [InlineKeyboardButton("Download from Snapchat", callback_data="plat_snapchat")],
        [InlineKeyboardButton("Invite friends, earn bonuses", callback_data="invite")],
        [InlineKeyboardButton("Go Premium", callback_data="buy_premium")],
    ])


def group_share_kb() -> InlineKeyboardMarkup:
    url = f"https://t.me/{BOT_USERNAME}?startgroup=true"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Add me to a group", url=url)],
    ])


def limit_reached_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Watch a short ad for 2 extra", callback_data="watch_ad")],
        [InlineKeyboardButton("Invite friends for bonuses", callback_data="invite")],
        [InlineKeyboardButton("Upgrade to Premium", callback_data="buy_premium")],
    ])


def premium_plans_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"30 days — {PREMIUM_PRICE_STARS} Stars",
            callback_data="premium_30d")],
        [InlineKeyboardButton(
            f"1 year — {PREMIUM_ANNUAL_PRICE_STARS} Stars (best value)",
            callback_data="premium_365d")],
        [InlineKeyboardButton("Back to menu", callback_data="back_menu")],
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    first_name = first_name_of(update.effective_user)

    track_cmd(update.effective_user)

    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0].replace("ref_", ""))
            if referrer_id != user_id:
                if db_register_referral(user_id, referrer_id):
                    track_referral(referrer_id)
                    try:
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=(
                                f"New referral\n\n"
                                f"{first_name} just joined using your link.\n\n"
                                f"You earned +1 bonus download.\n"
                                f"Total referrals: {db_get_referral_count(referrer_id)}"
                            ),
                        )
                    except Exception:
                        pass
        except (ValueError, IndexError):
            pass

    text = (
        f"Hey {first_name}, welcome to ToolifyX.\n"
        "\n"
        "I download videos and images from TikTok, Facebook, Pinterest, "
        "Twitter/X and Snapchat — send me a link and I'll deliver the file "
        "right here in chat.\n"
        "\n"
        f"Free plan: {DAILY_LIMIT} downloads per day\n"
        f"Earn +1 bonus download per friend you invite\n"
        f"Premium: unlimited downloads\n"
        "\n"
        "Pick a platform below to get started, then paste the link."
    )
    await update.message.reply_text(
        text,
        reply_markup=main_menu_kb(),
    )


async def cmd_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_cmd(update.effective_user)
    await update.message.reply_text(
        "Bring ToolifyX to your group\n"
        "\n"
        "Anyone in the group can paste a TikTok, Facebook, Pinterest, "
        "Twitter/X or Snapchat link and I'll deliver the media right "
        "there — no need to forward it around.\n"
        "\n"
        "How to add me\n"
        "1. Tap the button below\n"
        "2. Pick the group\n"
        "3. Confirm the invite\n"
        "\n"
        "Tip for admins\n"
        "After adding, remove and re-add me once so I can read all links "
        "in the group — otherwise I'll only see messages that mention me.",
        reply_markup=group_share_kb(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_cmd(update.effective_user)
    await update.message.reply_text(
        "How to use ToolifyX\n"
        "\n"
        "1. Tap a platform below\n"
        "2. Paste a public link from that platform\n"
        "3. Get the video or image right here\n"
        "\n"
        "Supported platforms\n"
        "TikTok, Facebook, Pinterest, Twitter/X, Snapchat\n"
        "\n"
        "Daily limit\n"
        f"{DAILY_LIMIT} free downloads per day. Invite friends for +1 bonus "
        "each, or upgrade to Premium for unlimited access.\n"
        "\n"
        "Useful commands\n"
        "/start — main menu\n"
        "/help — this message\n"
        "/group — add me to a group\n"
        "/cancel — cancel current action",
        reply_markup=main_menu_kb(),
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_cmd(update.effective_user)
    await update.message.reply_text("Cancelled. Send a new link anytime.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    track_cmd(query.from_user)

    if query.data == "invite":
        link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
        count = db_get_referral_count(user_id)
        bonus = db_get_bonus(user_id)
        text = (
            "Invite friends, earn bonuses\n"
            "\n"
            f"Your referral link:\n{link}\n"
            "\n"
            f"Friends invited: {count}\n"
            f"Bonus downloads available: {bonus}\n"
            "\n"
            "Each friend who joins using your link gives you +1 bonus "
            "download, which doesn't count against your daily limit."
        )
        share_url = f"https://t.me/share/url?url={link}&text=Download videos for free!"
        keyboard = [
            [InlineKeyboardButton("Share your link", url=share_url)],
            [InlineKeyboardButton("Back to menu", callback_data="back_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data == "back_menu":
        await query.edit_message_text(
            "Pick a platform below, then paste the link.",
            reply_markup=main_menu_kb())
        return

    if query.data == "watch_ad":
        ad_timestamps[user_id] = datetime.datetime.now()
        await query.edit_message_text(
            "Ad opened\n"
            "\n"
            "Watch the full ad, then come back and tap Claim bonus.\n"
            "\n"
            f"If the ad didn't open, tap here: {MONETAG_LINK}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Claim bonus", callback_data="claim_bonus")],
            ]),
            disable_web_page_preview=True,
        )
        return

    if query.data == "claim_bonus":
        clicked_at = ad_timestamps.get(user_id)
        if not clicked_at:
            await query.answer("Please tap Watch ad first.", show_alert=True)
            return
        elapsed = (datetime.datetime.now() - clicked_at).total_seconds()
        if elapsed < 15:
            await query.answer(
                f"Please watch the ad a bit longer ({int(15 - elapsed)}s left).",
                show_alert=True)
            return
        db_decrement_daily(user_id, 2)
        ad_timestamps.pop(user_id, None)
        await query.edit_message_text(
            "Bonus claimed. You now have 2 extra downloads today.\n"
            "\n"
            "Paste your link to continue.",
        )
        return

    # ── Premium flow ────────────────────────────────────────────────────────
    if query.data == "buy_premium":
        await query.edit_message_text(
            "Choose your Premium plan\n"
            "\n"
            "Both plans remove the daily download limit and give you "
            "unlimited access.\n"
            "\n"
            f"30 days — {PREMIUM_PRICE_STARS} Stars\n"
            f"1 year — {PREMIUM_ANNUAL_PRICE_STARS} Stars",
            reply_markup=premium_plans_kb(),
        )
        return

    if query.data == "premium_30d":
        await query.message.reply_invoice(
            title="ToolifyX Premium — 30 days",
            description="Unlimited downloads for 30 days. No daily limit.",
            payload="premium_30d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Premium access (30 days)", PREMIUM_PRICE_STARS)],
            start_parameter="premium"
        )
        return

    if query.data == "premium_365d":
        await query.message.reply_invoice(
            title="ToolifyX Premium — 1 year",
            description="Unlimited downloads for 365 days. Best value.",
            payload="premium_365d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Premium access (1 year)", PREMIUM_ANNUAL_PRICE_STARS)],
            start_parameter="premium"
        )
        return

    platform_key = query.data.split("_")[1]
    info = PLATFORMS.get(platform_key)
    if info:
        await query.edit_message_text(
            f"Ready for {info['name']}\n"
            "\n"
            f"Paste any public {info['name']} link below and I'll fetch "
            f"the media for you.",
        )


async def precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    payment = update.message.successful_payment

    # Determine plan from payload
    payload = payment.invoice_payload or ""
    if "365" in payload:
        days = PREMIUM_ANNUAL_DAYS
        plan_name = "1 year"
        amount = PREMIUM_ANNUAL_PRICE_STARS
    else:
        days = PREMIUM_DAILY_DAYS
        plan_name = "30 days"
        amount = PREMIUM_PRICE_STARS

    charge_id = payment.telegram_payment_charge_id
    logging.info(f"Payment received: user={user_id} plan={plan_name} charge={charge_id}")

    # Grant premium
    db_set_premium(user_id, days)
    track_premium(user_id, True)

    # Log to payment history
    username = update.effective_user.username or ""
    log_payment(user_id, amount, plan_name, charge_id, username)

    # Thank the user
    await update.message.reply_text(
        "Welcome to Premium\n"
        "\n"
        f"Your plan: {plan_name}\n"
        "You now have unlimited downloads.\n"
        "\n"
        "Thank you for supporting ToolifyX!",
    )

    # Notify admin
    try:
        name = update.effective_user.first_name or "Someone"
        uname = f"@{username}" if username else f"id {user_id}"
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"New payment received\n"
                f"\n"
                f"User: {name} ({uname})\n"
                f"Plan: {plan_name}\n"
                f"Amount: {amount} Stars\n"
                f"Charge ID: {charge_id}\n"
                f"\n"
                "Premium has been granted automatically."
            ),
        )
    except Exception as e:
        logging.warning(f"Could not notify admin: {e}")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_cmd(update.effective_user)
    if not is_admin(update):
        await update.message.reply_text("This command is admin-only.")
        return

    s = aggregate_stats()
    lines = [
        "<b>ToolifyX stats</b>",
        "",
        f"Uptime — {_fmt_duration(s['uptime_seconds'])}",
        f"Users — {s['users_total']} (today: {s['users_today']})",
        f"Downloads — {s['jobs_done']} (today: {s['downloads_today']})",
        f"Failures — {s['jobs_failed']}",
        f"Bytes sent — {_fmt_size(s['bytes_out'])}",
        f"Premium users — {s['premium_users']}",
        f"Referrals — {s['referrals_total']}",
        "",
        "<b>Revenue</b>",
        f"Payments — {s['payments_count']}",
        f"Stars earned — {s['payments_stars']}",
        f"Stars today — {s['payments_stars_today']}",
        "",
        "<b>Per-platform</b>",
    ]
    if s["tools"]:
        for k, v in sorted(s["tools"].items(), key=lambda x: -x[1]):
            lines.append(f"• {k}: {v}")
    else:
        lines.append("<i>No downloads yet.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_payments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_cmd(update.effective_user)
    if not is_admin(update):
        await update.message.reply_text("This command is admin-only.")
        return

    if not PAYMENTS:
        await update.message.reply_text("No payments recorded yet.")
        return

    recent = list(reversed(PAYMENTS[-20:]))
    total_stars = sum(p.get("amount", 0) for p in PAYMENTS)
    total_count = len(PAYMENTS)

    lines = [
        "<b>Payment history</b>",
        "",
        f"Total payments: {total_count}",
        f"Total Stars: {total_stars}",
        "",
        "<b>Recent 20</b>",
    ]
    for p in recent:
        when = _fmt_duration(time.time() - p["timestamp"])
        uname = p.get("username") or f"id {p['user_id']}"
        lines.append(
            f"• {p['amount']} Stars — {p['plan']} — "
            f"@{uname} — {when} ago"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    url = update.message.text.strip()
    info = get_platform(url)

    track_seen(update.effective_user)

    if not info:
        await update.message.reply_text(
            "That link isn't from a supported platform.\n"
            "\n"
            "Try TikTok, Facebook, Pinterest, Twitter/X or Snapchat. "
            "Or use our website for other platforms.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Open ToolifyX website", url=TOOLIFYX_LINK)],
            ]),
        )
        return

    platform_key = next((k for k in PLATFORMS if k in url.lower()), "unknown")

    premium = db_is_premium(user_id)
    daily_count = db_get_daily(user_id)
    bonus = db_get_bonus(user_id)

    if not premium and daily_count >= DAILY_LIMIT and bonus == 0:
        await update.message.reply_text(
            f"You've used all {DAILY_LIMIT} free downloads today.\n"
            "\n"
            "Watch a short ad or invite a friend to unlock more, "
            "or go Premium for unlimited access.",
            reply_markup=limit_reached_kb(),
        )
        return

    consumed_bonus = False
    if not premium and daily_count >= DAILY_LIMIT and bonus > 0:
        if db_consume_bonus(user_id):
            consumed_bonus = True

    msg = await update.message.reply_text(f"Fetching your {info['name']} media...")

    filepath = None
    file_size = 0
    try:
        timeout = aiohttp.ClientTimeout(total=200)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data = await try_backend(session, info, url)
            if not data:
                raise Exception("BACKEND_UNAVAILABLE")

            if info["type"] == "video":
                video_id = data.get("videoId") or url
                filename = data.get("filename", "toolifyx_video.mp4")
                download_url = f"{info['backend']}/file?videoId={aiohttp.helpers.quote(video_id)}&mode=download&filename={aiohttp.helpers.quote(filename)}"
                is_video = True
            elif info["type"] == "twitter":
                videos = data.get("videos", [])
                if not videos:
                    raise Exception("BACKEND_UNAVAILABLE")
                videos.sort(key=lambda v: v.get("height", 0), reverse=True)
                best = videos[0]
                video_id = data.get("videoId") or url
                quality = best.get("quality", "")
                download_url = f"{info['backend']}/proxy?videoId={aiohttp.helpers.quote(video_id)}&quality={aiohttp.helpers.quote(quality)}&mode=download"
                is_video = True
            else:
                media_url = data.get("media")
                if not media_url:
                    raise Exception("BACKEND_UNAVAILABLE")
                download_url = f"{info['backend']}/api/stream?url={aiohttp.helpers.quote(media_url, safe='')}"
                is_video = data.get("type") == "video"

            ext = ".mp4" if is_video else ".jpg"
            filepath = f"downloads/{abs(hash(url))}{ext}"

            async with session.get(download_url) as file_resp:
                if file_resp.status != 200:
                    raise Exception("BACKEND_UNAVAILABLE")
                size = 0
                with open(filepath, "wb") as f:
                    async for chunk in file_resp.content.iter_chunked(65536):
                        size += len(chunk)
                        if size > MAX_FILE_SIZE_MB * 1024 * 1024:
                            f.close()
                            os.remove(filepath)
                            filepath = None
                            raise Exception("TOO_LARGE")
                        f.write(chunk)
                file_size = size

            caption = f"Done — {info['name']} media"
            if is_video:
                await msg.edit_text("Uploading to Telegram...")
                with open(filepath, "rb") as f:
                    await update.message.reply_video(
                        video=f, caption=caption, supports_streaming=True)
            else:
                await msg.edit_text("Uploading to Telegram...")
                with open(filepath, "rb") as f:
                    await update.message.reply_photo(photo=f, caption=caption)

            await msg.delete()

            if not premium and not consumed_bonus:
                db_increment_daily(user_id)

            track_download(user_id, platform_key, file_size, ok=True)
            if premium:
                track_premium(user_id, True)

            remaining_bonus = db_get_bonus(user_id)
            bonus_line = (f"\n\nYou have {remaining_bonus} bonus downloads left."
                          if remaining_bonus > 0 else "")

            await update.message.reply_text(
                "Want unlimited downloads with no daily limit? "
                "Visit our website:" + bonus_line,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Open ToolifyX", url=TOOLIFYX_LINK)],
                ]),
            )

            if filepath:
                asyncio.create_task(delete_file(filepath, 600))

    except Exception as e:
        err = str(e)
        logging.error(f"Error: {err}")

        if consumed_bonus:
            db_add_bonus(user_id, 1)

        if filepath and os.path.exists(filepath):
            os.remove(filepath)
            filepath = None

        track_download(user_id, platform_key, 0, ok=False)

        if "TOO_LARGE" in err:
            await msg.edit_text(
                "This video is over 50 MB.\n"
                "\n"
                "Download it directly from our website — no size limit.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Open ToolifyX", url=info["site"])],
                ]),
            )
        elif "BACKEND_UNAVAILABLE" in err:
            await msg.edit_text(
                "This link couldn't be processed right now.\n"
                "\n"
                "Try downloading it on our website — it's instant.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Open ToolifyX", url=info["site"])],
                ]),
            )
        else:
            await msg.edit_text(
                "Something went wrong.\n"
                "\n"
                "Please try a different link, or use our website.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Open ToolifyX", url=info["site"])],
                ]),
            )


# ══════════════════════════════════════════════════════════════════════════════
#  WEB SERVER — health + stats API
# ══════════════════════════════════════════════════════════════════════════════

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


def _token_ok(request) -> bool:
    return request.query.get("token") == ADMIN_TOKEN


async def web_handler(request):
    return _cors(web.Response(text="ToolifyX is alive."))


async def healthz(request):
    return _cors(web.json_response({
        "status": "ok",
        "bot": "toolifyx",
        "uptime_seconds": round(time.time() - STATS["start_time"], 1),
        "downloads_done": STATS["downloads_done"],
        "downloads_failed": STATS["downloads_failed"],
    }))


async def api_stats(request):
    if not _token_ok(request):
        return _cors(web.json_response({"error": "unauthorized"}, status=401))
    return _cors(web.json_response(aggregate_stats()))


async def api_users(request):
    if not _token_ok(request):
        return _cors(web.json_response({"error": "unauthorized"}, status=401))
    users = sorted(USERS.values(),
                   key=lambda u: u.get("last_seen", 0), reverse=True)
    return _cors(web.json_response({"users": users, "count": len(users)}))


async def api_payments(request):
    if not _token_ok(request):
        return _cors(web.json_response({"error": "unauthorized"}, status=401))
    recent = list(reversed(PAYMENTS[-100:]))
    return _cors(web.json_response({
        "payments": recent,
        "count": len(PAYMENTS),
        "total_stars": sum(p.get("amount", 0) for p in PAYMENTS),
    }))


async def options_handler(_request):
    return _cors(web.Response(text=""))


async def main():
    _load_users()
    _load_payments()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("group", cmd_group))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("payments", cmd_payments))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    web_app = web.Application()
    web_app.router.add_get("/", web_handler)
    web_app.router.add_get("/healthz", healthz)
    web_app.router.add_get("/api/stats", api_stats)
    web_app.router.add_get("/api/users", api_users)
    web_app.router.add_get("/api/payments", api_payments)
    web_app.router.add_route("OPTIONS", "/api/{tail:.*}", options_handler)

    runner = web.AppRunner(web_app)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server listening on :{port}")

    asyncio.create_task(keep_backends_alive())
    asyncio.create_task(_flusher_loop())

    logging.info("ToolifyX running.")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
