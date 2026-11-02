#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SNB GOLD LIVE — H2 edge + M5 confirmation + optional M5 FVG Entry 2
نسخة للنشر على Render: الإعدادات السرية تُقرأ من Environment Variables.
هذا كود إشارات فقط وليس مستشاراً مالياً.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from aiohttp import web

# ========================= USER SETTINGS =========================
# تُقرأ من متغيرات البيئة (Render -> Environment). للتشغيل المحلي ضعها في PowerShell.
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@GoldBlackFx")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"

DATA_DIR = Path(os.getenv("SNB_DATA_DIR", str(Path.home() / "snb_live_data")))
STATE_FILE = DATA_DIR / "snb_gold_live_state.json"
LIVE_POLL_SECONDS = 3        # فحص السعر اللحظي للصفقات (كل 3 ثوانٍ)
DETECT_POLL_SECONDS = 5      # البحث عن إشارات جديدة (جلب 18 ساعة كما كان)
LIVE_WINDOW_MINUTES = 15     # نافذة الجلب الخفيف للمتابعة اللحظية
LOOKBACK_HOURS = 18
TIMEZONE = "Asia/Riyadh"
INSTRUMENT = "XAU/USD"

# SNB GOLD rules
MAX_EQUAL_GAP = 0.10
PENDING_OFFSET = 1.00
MIN_STOP = 3.00
RAW_STOP_BUFFER = 0.10
PENDING_EXPIRY_HOURS = 2
SIGNAL_MAX_AGE_MINUTES = 10   # تجاهل الإشارات الأقدم من هذا (يمنع إشارات قديمة بعد إعادة التشغيل)
FVG_DELAY_MINUTES = 10
FVG_STOP_DISTANCE = 1.00

NO_NEW_FIRST_HOUR = True
NO_NEW_LAST_HOUR = True

TP_OFFSETS = [7.0, 10.0, 17.0, 20.0, 35.0]
PROGRESS_START = 3.0
PROGRESS_STEP = 2.0

# ========================= LOGGING =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snb-gold")
STOP_REQUESTED = False


def stop_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.info("Stop requested; active trades will be saved.")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


# ========================= WEB SERVER (keeps Render awake) =========================
async def home(request):
    return web.json_response({"status": "ok"})


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", home)
    app.router.add_get("/healthz", home)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Web server running on port %s", port)


# ========================= STATE =========================
def load_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            log.exception("Could not read state; starting clean state.")
    return {"started_message_sent": False, "signals": {}, "active": {}}


def save_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


# ========================= DATA =========================
def get_duka():
    try:
        import dukascopy_python as duka
        from dukascopy_python.instruments import INSTRUMENT_FX_METALS_XAU_USD
        return duka, INSTRUMENT_FX_METALS_XAU_USD
    except Exception as exc:
        raise SystemExit(f"فشل استيراد dukascopy: {exc!r}") from exc


def fetch_1m(duka, instrument, side: str, hours: float = LOOKBACK_HOURS) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(hours=hours)
    interval = getattr(duka, "INTERVAL_MIN_1")
    offer_side = getattr(duka, f"OFFER_SIDE_{side}")
    got = duka.fetch(
        instrument=instrument,
        interval=interval,
        offer_side=offer_side,
        start=start.to_pydatetime(),
        end=now.to_pydatetime(),
        max_retries=5,
        limit=30_000,
        debug=False,
    )
    if got is None or got.empty:
        raise RuntimeError(f"No {side} data (market probably closed)")
    got = got.copy()
    got.columns = [str(c).lower() for c in got.columns]
    idx = pd.DatetimeIndex(pd.to_datetime(got.index))
    got.index = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    got = got[~got.index.duplicated(keep="last")].sort_index()
    for col in ("open", "high", "low", "close"):
        got[col] = pd.to_numeric(got[col], errors="coerce")
    return got.dropna(subset=["open", "high", "low", "close"])


def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    out = df.resample(f"{minutes}min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()
    now_floor = pd.Timestamp.now(tz="UTC").floor(f"{minutes}min")
    return out[out.index + pd.Timedelta(minutes=minutes) <= now_floor]


def riyadh(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.tz_convert(TIMEZONE)


def allow_new_signal(ts: pd.Timestamp) -> bool:
    local = riyadh(ts)
    if NO_NEW_FIRST_HOUR and local.hour == 0:
        return False
    if NO_NEW_LAST_HOUR and local.hour == 23:
        return False
    return True


# ========================= TELEGRAM =========================
class TelegramSender:
    def __init__(self):
        self.client = None

    async def start(self):
        if not TELEGRAM_ENABLED:
            log.warning("Telegram disabled.")
            return
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_SESSION_STRING:
            raise RuntimeError("Set TELEGRAM_API_ID, TELEGRAM_API_HASH and TELEGRAM_SESSION_STRING")
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        self.client = TelegramClient(
            StringSession(TELEGRAM_SESSION_STRING), TELEGRAM_API_ID, TELEGRAM_API_HASH,
            connection_retries=None, retry_delay=1, auto_reconnect=True,
        )
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STRING is invalid or expired; generate a new one")
        log.info("Telegram connected to %s", TELEGRAM_CHANNEL)

    async def send(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        if not TELEGRAM_ENABLED or self.client is None:
            log.info("TELEGRAM PREVIEW:\n%s", text)
            return None
        msg = await self.client.send_message(TELEGRAM_CHANNEL, text, reply_to=reply_to)
        return int(msg.id)

    async def edit_cancelled(self, message_id: Optional[int], text: str) -> None:
        if not message_id or not TELEGRAM_ENABLED or self.client is None:
            return
        try:
            await self.client.edit_message(TELEGRAM_CHANNEL, message_id, text)
            await self.send("❌ **أُلغي الأمر المعلق** — سنأخذ فرصة أقرب إذا ظهرت.", reply_to=message_id)
        except Exception:
            log.exception("Could not edit cancelled message %s", message_id)

    async def close(self):
        if self.client:
            await self.client.disconnect()


# ========================= FORMATTING =========================
def fmt_price(x: float) -> str:
    return f"{x:.2f}"


def signed_points(delta: float, side: str) -> int:
    favorable = delta if side == "buy" else -delta
    return max(0, int(round(favorable * 10)))


def side_word(side: str) -> str:
    return "BUY" if side == "buy" else "SELL"


def pending_text(tr: dict[str, Any]) -> str:
    arrow = "📈 BUY LIMIT" if tr["side"] == "buy" else "📉 SELL LIMIT"
    tps = "\n".join(f"{i}️⃣ TP{i} ➜ {fmt_price(x)}" for i, x in enumerate(tr["targets"], 1))
    return (
        "🔔 **توصية جديدة | صفقة معلقة** 🔔\n\n"
        f"🥇 **XAUUSD**\n{arrow} ➜ **{fmt_price(tr['entry'])}**\n\n"
        f"🛑 وقف الخسارة: **{fmt_price(tr['stop'])}**\n\n"
        f"🎯 **الأهداف:**\n{tps}\n\n"
        "⚠️ إدارة رأس المال مسؤوليتك\n"
        "💡 لا تخاطر بأكثر من 1-2% ، هذه ليست آخر صفقة والسوق لن يهرب 🌍"
    )


def activated_text(tr: dict[str, Any]) -> str:
    return (
        "✅ **تم تفعيل الصفقة** 🚀\n\n"
        f"🥇 XAUUSD | **{side_word(tr['side'])}**\n"
        f"📍 دخول عند **{fmt_price(tr['entry'])}**\n\n"
        "⏳ نتابع معكم وبالتوفيق للجميع 🤝"
    )


def progress_text(tr: dict[str, Any], points: int) -> str:
    phrases = {30: "🔥 الصفقة تمشي صح ✅", 90: "💪🔥 يا وحش 🔥💪", 110: "👑 يا معلم 👑"}
    phrase = phrases.get(points, "🔥 الصفقة مستمرة ✅")
    return f"{phrase}\n💰 **+{points} نقطة** 📈✅"


def tp_text(tr: dict[str, Any], n: int, points: int) -> str:
    return (
        f"🎯 **تحقيق الهدف {n}** ✅\n"
        f"💰 +{points} نقطة 🔥\n\n"
        f"🥇 XAUUSD {side_word(tr['side'])}\n👏 مبروك للجميع 💚"
    )


def cancelled_text(tr: dict[str, Any]) -> str:
    return (
        "❌ **تم إلغاء الأمر المعلق**\n\n"
        f"🥇 XAUUSD | {side_word(tr['side'])}\n"
        f"📍 المستوى: {fmt_price(tr['entry'])}\n"
        "🧹 لم يلمس السعر نقطة الدخول ضمن مدة الصلاحية."
    )


def all_targets_text(tr: dict[str, Any]) -> str:
    total = signed_points(tr["targets"][-1] - tr["entry"] if tr["side"] == "buy" else tr["entry"] - tr["targets"][-1], "buy")
    return f"🏆🏆🏆 **تحققت جميع الأهداف** 🎯🎯🎯\n🥇 XAUUSD {side_word(tr['side'])}\n💰 إجمالي: **+{total} نقطة** 🔥💎👑"


# ========================= STRATEGY =========================
def signal_key(h2_start: pd.Timestamp, side: str) -> str:
    return f"{h2_start.isoformat()}|{side}"


def build_targets(entry: float, side: str) -> list[float]:
    return [round(entry + x, 3) if side == "buy" else round(entry - x, 3) for x in TP_OFFSETS]


def detect_setups(bid: pd.DataFrame, ask: pd.DataFrame, state: dict[str, Any]) -> list[dict[str, Any]]:
    h2 = resample(bid, 120)
    m5 = resample(bid, 5)
    setups: list[dict[str, Any]] = []
    now_utc = pd.Timestamp.now(tz="UTC")
    if len(h2) < 3 or len(m5) < 5:
        return setups

    # كل شمعة H2 مغلقة (بما فيها الأخيرة): نافذة التأكيد تبدأ بعد إغلاقها مباشرة.
    # نفحص الشموع المتاحة فقط (M5 المغلقة)، فتُكتشف الإشارة لحظة تأكدها دون انتظار نهاية النافذة.
    for i in range(2, len(h2)):
        hs = h2.index[i]
        hb = h2.iloc[i]
        if not allow_new_signal(hs):
            continue
        q = m5[(m5.index >= hs + pd.Timedelta(hours=2)) & (m5.index < hs + pd.Timedelta(hours=4))]
        for j in range(1, len(q)):
            first, second = q.iloc[j - 1], q.iloc[j]
            confirm_time = q.index[j]
            upper = max(float(first.high), float(second.high)) >= float(hb.high)
            lower = min(float(first.low), float(second.low)) <= float(hb.low)
            equal = (float(first.close) + float(second.open)) / 2
            near = abs(float(first.close) - float(second.open)) <= MAX_EQUAL_GAP
            # القاعدة الموسّعة: يكفي أن تلمس إحدى الشمعتين حافة H2؛
            # لا يُشترط أن يغلق إغلاق الشمعة الثانية داخل نطاق H2.
            sell = upper and near and first.close > first.open and second.close < second.open
            buy = lower and near and first.close < first.open and second.close > second.open
            if not (sell or buy):
                continue
            side = "sell" if sell else "buy"
            key = signal_key(hs, side)
            if key in state["signals"]:
                break
            # إشارة قديمة (مثلاً بعد إعادة تشغيل ضاعت فيها الحالة): لا ترسلها
            if now_utc - (confirm_time + pd.Timedelta(minutes=5)) > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
                continue
            raw_stop = max(float(first.high), float(second.high)) + RAW_STOP_BUFFER if sell else min(float(first.low), float(second.low)) - RAW_STOP_BUFFER
            raw_risk = abs(equal - raw_stop)
            if raw_risk < MIN_STOP:
                entry = equal
                stop = equal + MIN_STOP if sell else equal - MIN_STOP
            else:
                entry = equal + PENDING_OFFSET if sell else equal - PENDING_OFFSET
                stop = raw_stop
            setups.append({
                "key": key, "side": side, "leg": "P1", "h2_start": hs.isoformat(),
                "confirm_time": confirm_time.isoformat(), "entry": round(entry, 3),
                "stop": round(stop, 3), "targets": build_targets(entry, side),
                "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "expiry": (confirm_time + pd.Timedelta(minutes=5) + pd.Timedelta(hours=PENDING_EXPIRY_HOURS)).isoformat(),
                "message_id": None, "status": "pending", "last_progress": 0,
                "hit_tps": [], "fvg_checked": False, "fvg_order": None,
            })
            break
    return setups


def make_fvg_order(tr: dict[str, Any], bid: pd.DataFrame, m5: pd.DataFrame) -> Optional[dict[str, Any]]:
    if tr.get("fvg_checked"):
        return tr.get("fvg_order")
    confirm = pd.Timestamp(tr["confirm_time"])
    if confirm.tzinfo is None:
        confirm = confirm.tz_localize("UTC")
    start = confirm + pd.Timedelta(minutes=15)
    expiry = pd.Timestamp(tr["expiry"])
    q = m5[(m5.index >= start) & (m5.index < expiry)]
    for k in range(2, len(m5)):
        ft = m5.index[k]
        if ft not in q.index:
            continue
        c1, c3 = m5.iloc[k - 2], m5.iloc[k]
        bullish = float(c1.high) < float(c3.low)
        bearish = float(c1.low) > float(c3.high)
        if tr["side"] == "buy" and bullish:
            low, high = float(c1.high), float(c3.low)
            entry, stop = high, low - FVG_STOP_DISTANCE
        elif tr["side"] == "sell" and bearish:
            low, high = float(c3.high), float(c1.low)
            entry, stop = low, high + FVG_STOP_DISTANCE
        else:
            continue
        return {"entry": round(entry, 3), "stop": round(stop, 3), "formed": ft.isoformat(), "targets": build_targets(entry, tr["side"])}
    return None


# ========================= LIVE ENGINE =========================
def opposite(side: str) -> str:
    return "buy" if side == "sell" else "sell"


def has_active(state: dict[str, Any], side: str) -> bool:
    return any(t.get("status") == "active" and t["side"] == side for t in state["active"].values())


def merge_tail(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    """يدمج شموع جديدة فوق القديمة (الجديدة تتغلب عند التداخل) ويقص الأقدم من نافذة الجلب."""
    if old is None or old.empty:
        out = new
    else:
        out = pd.concat([old[~old.index.isin(new.index)], new]).sort_index()
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=LOOKBACK_HOURS + 1)
    return out[out.index >= cutoff]


def touch_time(side: str, level: float, trigger: pd.DataFrame, price: Optional[float],
               start: pd.Timestamp, end: pd.Timestamp, now: pd.Timestamp) -> Optional[pd.Timestamp]:
    """متى لمس السعر المستوى: من شموع دقيقة مغلقة، أو من السعر اللحظي الحالي."""
    fl = now.floor("1min")
    q = trigger[(trigger.index >= start) & (trigger.index < end) & (trigger.index + pd.Timedelta(minutes=1) <= fl)]
    hit = q[q.high >= level] if side == "sell" else q[q.low <= level]
    if len(hit):
        return hit.index[0] + pd.Timedelta(minutes=1)
    if price is not None and start <= now < end:
        if (side == "sell" and price >= level) or (side == "buy" and price <= level):
            return now
    return None


async def cancel_opposites(state: dict[str, Any], side: str, tg: "TelegramSender") -> None:
    """بعد تفعيل صفقة: الأوامر المعلقة المعاكسة تُلغى ولا تُتابع."""
    for k, t in state["active"].items():
        if t.get("status") == "pending" and t["side"] == opposite(side):
            t["status"] = "cancelled"
            await tg.edit_cancelled(t.get("message_id"), cancelled_text(t))
            if k in state["signals"]:
                state["signals"][k]["status"] = "cancelled_opposite"


async def activate(state: dict[str, Any], tr: dict[str, Any], source: str, when: pd.Timestamp, tg: "TelegramSender") -> None:
    tr["status"] = "active"
    tr["entry_time"] = when.isoformat()
    tr["entry_source"] = source
    tr["best"] = tr["entry"]
    await tg.send(activated_text(tr), reply_to=tr.get("message_id"))
    await cancel_opposites(state, tr["side"], tg)
    save_state(state)


async def nap(seconds: int) -> None:
    for _ in range(seconds):
        if STOP_REQUESTED:
            return
        await asyncio.sleep(1)


async def register_signal(state: dict[str, Any], tr: dict[str, Any], tg: "TelegramSender") -> None:
    """إشارة جديدة: تُغلق بصمت أي صفقة نشطة معاكسة لها، ثم تُرسل وتُتابع. نفس الاتجاه: الاثنتان تُتابعان."""
    for k, t in state["active"].items():
        if t.get("status") == "active" and t["side"] == opposite(tr["side"]):
            t["status"] = "closed"
            t["close_reason"] = "REVERSED"
            if k in state["signals"]:
                state["signals"][k]["status"] = "closed_reversed"
            log.info("Stopped following %s (opposite signal %s)", k, tr["key"])
    msg_id = await tg.send(pending_text(tr))
    tr["message_id"] = msg_id
    state["signals"][tr["key"]] = {"created": tr["created_at"], "status": "pending"}
    state["active"][tr["key"]] = tr
    save_state(state)
    log.info("New pending %s %s", tr["key"], tr["entry"])


async def live_loop(duka, instrument, state, tg, cache, lock):
    """كل LIVE_POLL_SECONDS: سعر لحظي للصفقات المعلقة والنشطة (نفس مكتبة Dukascopy)."""
    while not STOP_REQUESTED:
        try:
            if any(t.get("status") in ("pending", "active") for t in state["active"].values()):
                sb, sa = await asyncio.gather(
                    asyncio.to_thread(fetch_1m, duka, instrument, "BID", LIVE_WINDOW_MINUTES / 60),
                    asyncio.to_thread(fetch_1m, duka, instrument, "ASK", LIVE_WINDOW_MINUTES / 60),
                )
                async with lock:
                    cache["bid"] = merge_tail(cache.get("bid"), sb)
                    cache["ask"] = merge_tail(cache.get("ask"), sa)
                    prices = {"bid": float(sb.close.iloc[-1]), "ask": float(sa.close.iloc[-1])}
                    m5 = resample(cache["bid"], 5)
                    await manage_trades(state, cache["bid"], cache["ask"], m5, tg, prices)
        except Exception as exc:
            log.warning("Live tick failed (will retry): %r", exc)
        await asyncio.sleep(LIVE_POLL_SECONDS)


async def detect_loop(duka, instrument, state, tg, cache, lock):
    """كل DETECT_POLL_SECONDS: جلب كامل 18 ساعة واكتشاف إشارات جديدة."""
    while not STOP_REQUESTED:
        try:
            bid, ask = await asyncio.gather(
                asyncio.to_thread(fetch_1m, duka, instrument, "BID"),
                asyncio.to_thread(fetch_1m, duka, instrument, "ASK"),
            )
            async with lock:
                cache["bid"] = merge_tail(cache.get("bid"), bid)
                cache["ask"] = merge_tail(cache.get("ask"), ask)
                for tr in detect_setups(cache["bid"], cache["ask"], state):
                    await register_signal(state, tr, tg)
        except Exception as exc:
            log.warning("Detect cycle failed (will retry): %r", exc)
        await nap(DETECT_POLL_SECONDS)


async def main_loop():
    await start_web_server()
    duka, instrument = get_duka()
    tg = TelegramSender()
    await tg.start()
    state = load_state()

    if not state.get("started_message_sent"):
        await tg.send("اللهم صلِّ وسلّم وبارك على نبينا محمد وعلى آله وصحبه أجمعين ❤️")
        state["started_message_sent"] = True
        save_state(state)

    cache: dict[str, pd.DataFrame] = {}
    lock = asyncio.Lock()
    await asyncio.gather(
        live_loop(duka, instrument, state, tg, cache, lock),
        detect_loop(duka, instrument, state, tg, cache, lock),
    )
    save_state(state)
    await tg.close()


async def manage_trades(state: dict[str, Any], bid: pd.DataFrame, ask: pd.DataFrame, m5: pd.DataFrame,
                        tg: TelegramSender, prices: Optional[dict[str, float]] = None):
    now = pd.Timestamp.now(tz="UTC")
    fl = now.floor("1min")
    one_min = pd.Timedelta(minutes=1)
    remove = []
    for key, tr in list(state.get("active", {}).items()):
        if tr.get("status") in ("closed", "cancelled"):
            remove.append(key)
            continue
        side = tr["side"]
        trigger = bid if side == "sell" else ask
        price = (prices or {}).get("bid" if side == "sell" else "ask")
        reply_to = tr.get("message_id")
        confirm = pd.Timestamp(tr["confirm_time"])
        if confirm.tzinfo is None:
            confirm = confirm.tz_localize("UTC")
        active_start = confirm + pd.Timedelta(minutes=5)
        expiry = pd.Timestamp(tr["expiry"])
        if expiry.tzinfo is None:
            expiry = expiry.tz_localize("UTC")

        if tr["status"] == "pending":
            when = touch_time(side, tr["entry"], trigger, price, active_start, expiry, now)
            if when is not None:
                await activate(state, tr, "P1", when, tg)
                continue
            if now >= active_start + pd.Timedelta(minutes=FVG_DELAY_MINUTES) and tr.get("fvg_order") is None and not tr.get("fvg_checked"):
                tr["fvg_order"] = make_fvg_order(tr, bid, m5)
                tr["fvg_checked"] = True
                save_state(state)
            fvg = tr.get("fvg_order")
            if fvg:
                fstart = pd.Timestamp(fvg["formed"]) + pd.Timedelta(minutes=5)
                when2 = touch_time(side, fvg["entry"], trigger, price, fstart, expiry, now)
                if when2 is not None:
                    tr["entry"] = fvg["entry"]
                    tr["stop"] = fvg["stop"]
                    tr["targets"] = fvg["targets"]
                    await activate(state, tr, "P2-FVG", when2, tg)
                    continue
            if now >= expiry:
                tr["status"] = "cancelled"
                await tg.edit_cancelled(tr.get("message_id"), cancelled_text(tr))
                if key in state["signals"]:
                    state["signals"][key]["status"] = "cancelled"
                save_state(state)
                remove.append(key)

        elif tr["status"] == "active":
            entry_dt = pd.Timestamp(tr["entry_time"])
            ent_min = entry_dt.floor("1min")
            cl = trigger[(trigger.index > ent_min) & (trigger.index + one_min <= fl)]
            best = float(tr.get("best", tr["entry"]))
            if side == "sell":
                if len(cl):
                    best = min(best, float(cl.low.min()))
                if price is not None:
                    best = min(best, price)
                stopped = (len(cl) and float(cl.high.max()) >= tr["stop"]) or (price is not None and price >= tr["stop"])
                move = tr["entry"] - best
            else:
                if len(cl):
                    best = max(best, float(cl.high.max()))
                if price is not None:
                    best = max(best, price)
                stopped = (len(cl) and float(cl.low.min()) <= tr["stop"]) or (price is not None and price <= tr["stop"])
                move = best - tr["entry"]
            tr["best"] = best

            if stopped:
                # إغلاق صامت: لا رسالة وقف، فقط توقف المتابعة
                tr["status"] = "closed"
                tr["close_reason"] = "SL"
                if key in state["signals"]:
                    state["signals"][key]["status"] = "closed_sl"
                save_state(state)
                remove.append(key)
                continue

            points = max(0, int(round(move * 10)))
            for n, target in enumerate(tr["targets"], 1):
                touched = best <= target if side == "sell" else best >= target
                if touched and n not in tr["hit_tps"]:
                    tr["hit_tps"].append(n)
                    await tg.send(tp_text(tr, n, signed_points(abs(target - tr["entry"]), "buy")), reply_to=reply_to)
                    save_state(state)
            start_pts = int(round(PROGRESS_START * 10))
            step_pts = int(round(PROGRESS_STEP * 10))
            if points >= start_pts:
                milestone = start_pts + ((points - start_pts) // step_pts) * step_pts
                if milestone > tr.get("last_progress", 0):
                    tr["last_progress"] = milestone
                    await tg.send(progress_text(tr, milestone), reply_to=reply_to)
                    save_state(state)
            if len(tr["hit_tps"]) == len(tr["targets"]):
                await tg.send(all_targets_text(tr), reply_to=reply_to)
                tr["status"] = "closed"
                tr["close_reason"] = "ALL_TP"
                if key in state["signals"]:
                    state["signals"][key]["status"] = "closed_tp"
                save_state(state)
                remove.append(key)
    for key in remove:
        state["active"].pop(key, None)
    save_state(state)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
