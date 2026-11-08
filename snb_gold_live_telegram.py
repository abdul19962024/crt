#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SNB LIVE — XAUUSD + NAS100 (كل رمز مستقل تماماً عن الآخر)
H2 edge + M5 confirmation + Closer re-entry (P2)
الإعدادات السرية تُقرأ من Environment Variables.
هذا كود إشارات فقط وليس مستشاراً مالياً.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from aiohttp import web

# ========================= USER SETTINGS =========================
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@GoldBlackFx")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"

DATA_DIR = Path(os.getenv("SNB_DATA_DIR", str(Path.home() / "snb_live_data")))
STATE_FILE = DATA_DIR / "snb_gold_live_state.json"
LIVE_POLL_SECONDS = 3
DETECT_POLL_SECONDS = 5
LIVE_WINDOW_MINUTES = 15
LOOKBACK_HOURS = 18
TIMEZONE = "Asia/Riyadh"

# الرموز المفعّلة (يمكن تغييرها من Render: SNB_SYMBOLS=XAUUSD,NAS100)
ENABLED_SYMBOLS = [s.strip().upper() for s in os.getenv("SNB_SYMBOLS", "XAUUSD,NAS100").split(",") if s.strip()]

# ---------- إعدادات مشتركة ----------
PENDING_EXPIRY_HOURS = 2
SIGNAL_MAX_AGE_MINUTES = 10
FVG_DELAY_MINUTES = 10
PENDING_CANCEL_MINUTES = 10
USE_FVG = False
NO_NEW_FIRST_HOUR = True
NO_NEW_LAST_HOUR = True

# ---------- إعدادات كل رمز ----------
# pt = مضاعف تحويل حركة السعر إلى "نقاط" في الرسائل (الذهب: 0.1$ = نقطة، الناسداك: 1 = نقطة)
# قيم الناسداك = قيم الذهب × 10 كبداية، عدّلها حسب رغبتك.
CONFIGS: dict[str, dict[str, Any]] = {
    "XAUUSD": {
        "instrument_attr": "INSTRUMENT_FX_METALS_XAU_USD",
        "label": "🥇 XAUUSD",
        "name": "XAUUSD",
        "pt": 10,
        "max_equal_gap": 0.10,
        "pending_offset": 1.00,
        "min_stop": 3.00,
        "raw_stop_buffer": 0.10,
        "closer_offset": 0.50,
        "closer_stop": 3.00,
        "fvg_stop": 1.00,
        "tp_offsets": [7.0, 10.0, 17.0, 20.0, 35.0],
        "progress_start": 3.0,
        "progress_step": 2.0,
    },
    "NAS100": {
        "instrument_attr": "INSTRUMENT_IDX_AMERICA_E_NQ_100",   # E_NQ-100
        "label": "📊 NASDAQ",
        "name": "NAS100",
        "pt": 1,
        "max_equal_gap": 1.0,
        "pending_offset": 10.0,
        "min_stop": 30.0,
        "raw_stop_buffer": 1.0,
        "closer_offset": 5.0,
        "closer_stop": 30.0,
        "fvg_stop": 10.0,
        "tp_offsets": [70.0, 100.0, 170.0, 200.0, 350.0],
        "progress_start": 30.0,
        "progress_step": 20.0,
    },
}

# ========================= LOGGING =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snb")
STOP_REQUESTED = False


def stop_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.info("Stop requested; active trades will be saved.")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


# ========================= WEB SERVER =========================
async def home(request):
    return web.json_response({"status": "ok", "symbols": ENABLED_SYMBOLS})


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


def sym_of(tr: dict[str, Any]) -> str:
    return tr.get("sym", "XAUUSD")   # الصفقات القديمة بدون sym = ذهب


# ========================= DATA =========================
def get_duka():
    try:
        import dukascopy_python as duka
        import dukascopy_python.instruments as ins
    except Exception as exc:
        raise SystemExit(f"فشل استيراد dukascopy: {exc!r}") from exc
    instruments = {}
    for sym in ENABLED_SYMBOLS:
        if sym not in CONFIGS:
            raise SystemExit(f"رمز غير معروف: {sym}")
        attr = CONFIGS[sym]["instrument_attr"]
        if not hasattr(ins, attr):
            raise SystemExit(f"الرمز {sym}: الثابت {attr} غير موجود في نسخة المكتبة المثبتة")
        instruments[sym] = getattr(ins, attr)
    return duka, instruments


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
def cfg_of(tr: dict[str, Any]) -> dict[str, Any]:
    return CONFIGS[sym_of(tr)]


def fmt_price(x: float) -> str:
    return f"{x:.2f}"


def to_points(delta: float, c: dict[str, Any]) -> int:
    return max(0, int(round(delta * c["pt"])))


def side_word(side: str) -> str:
    return "BUY" if side == "buy" else "SELL"


def pending_text(tr: dict[str, Any]) -> str:
    c = cfg_of(tr)
    arrow = "📈 BUY LIMIT" if tr["side"] == "buy" else "📉 SELL LIMIT"
    tps = "\n".join(f"{i}️⃣ TP{i} ➜ {fmt_price(x)}" for i, x in enumerate(tr["targets"], 1))
    return (
        "🔔 **توصية جديدة | صفقة معلقة** 🔔\n\n"
        f"{c['label']}\n{arrow} ➜ **{fmt_price(tr['entry'])}**\n\n"
        f"🛑 وقف الخسارة: **{fmt_price(tr['stop'])}**\n\n"
        f"🎯 **الأهداف:**\n{tps}\n\n"
        "⚠️ إدارة رأس المال مسؤوليتك\n"
        "💡 لا تخاطر بأكثر من 1-2% ، هذه ليست آخر صفقة والسوق لن يهرب 🌍"
    )


def activated_text(tr: dict[str, Any]) -> str:
    c = cfg_of(tr)
    return (
        "✅ **تم تفعيل الصفقة** 🚀\n\n"
        f"{c['label']} | **{side_word(tr['side'])}**\n"
        f"📍 دخول عند **{fmt_price(tr['entry'])}**\n\n"
        "⏳ نتابع معكم وبالتوفيق للجميع 🤝"
    )


def progress_text(tr: dict[str, Any], points: int) -> str:
    phrases = {30: "🔥 الصفقة تمشي صح ✅", 90: "💪🔥 يا وحش 🔥💪", 110: "👑 يا معلم 👑"}
    phrase = phrases.get(points, "🔥 الصفقة مستمرة ✅")
    return f"{phrase}\n💰 **+{points} نقطة** 📈✅"


def tp_text(tr: dict[str, Any], n: int, points: int) -> str:
    c = cfg_of(tr)
    return (
        f"🎯 **تحقيق الهدف {n}** ✅\n"
        f"💰 +{points} نقطة 🔥\n\n"
        f"{c['label']} {side_word(tr['side'])}\n👏 مبروك للجميع 💚"
    )


def cancelled_text(tr: dict[str, Any]) -> str:
    c = cfg_of(tr)
    return (
        "❌ **تم إلغاء الأمر المعلق**\n\n"
        f"{c['label']} | {side_word(tr['side'])}\n"
        f"📍 المستوى: {fmt_price(tr['entry'])}\n"
        "🧹 لم يلمس السعر نقطة الدخول ضمن مدة الصلاحية."
    )


def waiting_text(tr: dict[str, Any]) -> str:
    c = cfg_of(tr)
    return (
        "❌ **تم إلغاء الأمر المعلق**\n\n"
        f"{c['label']} | {side_word(tr['side'])}\n"
        f"📍 المستوى: {fmt_price(tr['entry'])}\n"
        "⏳ **انتظر**، ممكن ناخذها من مكان أقرب 👀"
    )


def all_targets_text(tr: dict[str, Any]) -> str:
    c = cfg_of(tr)
    total = to_points(abs(tr["targets"][-1] - tr["entry"]), c)
    return f"🏆🏆🏆 **تحققت جميع الأهداف** 🎯🎯🎯\n{c['label']} {side_word(tr['side'])}\n💰 إجمالي: **+{total} نقطة** 🔥💎👑"


# ========================= STRATEGY =========================
def signal_key(sym: str, h2_start: pd.Timestamp, side: str) -> str:
    return f"{sym}|{h2_start.isoformat()}|{side}"


def build_targets(entry: float, side: str, c: dict[str, Any]) -> list[float]:
    return [round(entry + x, 3) if side == "buy" else round(entry - x, 3) for x in c["tp_offsets"]]


def detect_setups(sym: str, bid: pd.DataFrame, state: dict[str, Any]) -> list[dict[str, Any]]:
    c = CONFIGS[sym]
    h2 = resample(bid, 120)
    m5 = resample(bid, 5)
    setups: list[dict[str, Any]] = []
    now_utc = pd.Timestamp.now(tz="UTC")
    if len(h2) < 3 or len(m5) < 5:
        return setups

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
            near = abs(float(first.close) - float(second.open)) <= c["max_equal_gap"]
            sell = upper and near and first.close > first.open and second.close < second.open
            buy = lower and near and first.close < first.open and second.close > second.open
            if not (sell or buy):
                continue
            side = "sell" if sell else "buy"
            key = signal_key(sym, hs, side)
            if key in state["signals"]:
                break
            if now_utc - (confirm_time + pd.Timedelta(minutes=5)) > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
                continue
            if sell:
                raw_stop = max(float(first.high), float(second.high)) + c["raw_stop_buffer"]
            else:
                raw_stop = min(float(first.low), float(second.low)) - c["raw_stop_buffer"]
            raw_risk = abs(equal - raw_stop)
            if raw_risk < c["min_stop"]:
                entry = equal
                stop = equal + c["min_stop"] if sell else equal - c["min_stop"]
            else:
                entry = equal + c["pending_offset"] if sell else equal - c["pending_offset"]
                stop = raw_stop
            setups.append({
                "key": key, "sym": sym, "side": side, "leg": "P1", "h2_start": hs.isoformat(),
                "confirm_time": confirm_time.isoformat(), "entry": round(entry, 3),
                "stop": round(stop, 3), "targets": build_targets(entry, side, c),
                "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "expiry": (confirm_time + pd.Timedelta(minutes=5) + pd.Timedelta(hours=PENDING_EXPIRY_HOURS)).isoformat(),
                "message_id": None, "status": "pending", "last_progress": 0,
                "hit_tps": [], "fvg_checked": False, "fvg_order": None,
            })
            break
    return setups


def make_fvg_order(tr: dict[str, Any], bid: pd.DataFrame, m5: pd.DataFrame) -> Optional[dict[str, Any]]:
    c = cfg_of(tr)
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
            entry, stop = high, low - c["fvg_stop"]
        elif tr["side"] == "sell" and bearish:
            low, high = float(c3.high), float(c1.low)
            entry, stop = low, high + c["fvg_stop"]
        else:
            continue
        return {"entry": round(entry, 3), "stop": round(stop, 3), "formed": ft.isoformat(),
                "targets": build_targets(entry, tr["side"], c)}
    return None


def find_closer(tr: dict[str, Any], m5: pd.DataFrame) -> Optional[dict[str, Any]]:
    """
    الأمر الأقرب (P2): شمعتا M5 متتاليتان بنفس لون الاتجاه (خضراوان للشراء / حمراوان للبيع)
    وفارق إغلاق الأولى وافتتاح الثانية <= max_equal_gap.
    الأمر عند نقطة التساوي ± closer_offset، والوقف = closer_stop من الدخول.
    """
    c = cfg_of(tr)
    side = tr["side"]
    since = pd.Timestamp(tr["wait_since"]).ceil("5min")
    expiry = pd.Timestamp(tr["expiry"])
    q = m5[m5.index >= since]
    for j in range(1, len(q)):
        a, b = q.iloc[j - 1], q.iloc[j]
        if q.index[j] - q.index[j - 1] != pd.Timedelta(minutes=5):
            continue
        if q.index[j] + pd.Timedelta(minutes=5) >= expiry:
            break
        if side == "buy":
            same_color = a.close > a.open and b.close > b.open
        else:
            same_color = a.close < a.open and b.close < b.open
        if not same_color or abs(float(a.close) - float(b.open)) > c["max_equal_gap"]:
            continue

        equal = (float(a.close) + float(b.open)) / 2
        if side == "buy":
            entry = equal + c["closer_offset"]
            stop = entry - c["closer_stop"]
        else:
            entry = equal - c["closer_offset"]
            stop = entry + c["closer_stop"]

        confirm_time = q.index[j]
        return {
            "key": f"{tr['key']}|P2", "sym": sym_of(tr), "side": side, "leg": "P2",
            "h2_start": tr["h2_start"], "confirm_time": confirm_time.isoformat(),
            "entry": round(entry, 3), "stop": round(stop, 3),
            "targets": build_targets(entry, side, c),
            "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "expiry": tr["expiry"], "message_id": None, "status": "pending",
            "last_progress": 0, "hit_tps": [], "fvg_checked": True, "fvg_order": None,
        }
    return None


# ========================= LIVE ENGINE =========================
def opposite(side: str) -> str:
    return "buy" if side == "sell" else "sell"


def merge_tail(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    if old is None or old.empty:
        out = new
    else:
        out = pd.concat([old[~old.index.isin(new.index)], new]).sort_index()
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=LOOKBACK_HOURS + 1)
    return out[out.index >= cutoff]


def touch_time(side: str, level: float, trigger: pd.DataFrame, price: Optional[float],
               start: pd.Timestamp, end: pd.Timestamp, now: pd.Timestamp) -> Optional[pd.Timestamp]:
    fl = now.floor("1min")
    q = trigger[(trigger.index >= start) & (trigger.index < end) & (trigger.index + pd.Timedelta(minutes=1) <= fl)]
    hit = q[q.high >= level] if side == "sell" else q[q.low <= level]
    if len(hit):
        return hit.index[0] + pd.Timedelta(minutes=1)
    if price is not None and start <= now < end:
        if (side == "sell" and price >= level) or (side == "buy" and price <= level):
            return now
    return None


async def cancel_opposites(state: dict[str, Any], sym: str, side: str, tg: "TelegramSender") -> None:
    """بعد تفعيل صفقة: الأوامر المعلقة/المنتظِرة المعاكسة لنفس الرمز فقط تُلغى."""
    for k, t in state["active"].items():
        if sym_of(t) != sym or t["side"] != opposite(side):
            continue
        if t.get("status") == "pending":
            t["status"] = "cancelled"
            await tg.edit_cancelled(t.get("message_id"), cancelled_text(t))
            if k in state["signals"]:
                state["signals"][k]["status"] = "cancelled_opposite"
        elif t.get("status") == "waiting":
            t["status"] = "cancelled"
            if k in state["signals"]:
                state["signals"][k]["status"] = "cancelled_opposite"


async def activate(state: dict[str, Any], tr: dict[str, Any], source: str, when: pd.Timestamp, tg: "TelegramSender") -> None:
    tr["status"] = "active"
    tr["entry_time"] = when.isoformat()
    tr["entry_source"] = source
    tr["best"] = tr["entry"]
    await tg.send(activated_text(tr), reply_to=tr.get("message_id"))
    await cancel_opposites(state, sym_of(tr), tr["side"], tg)
    save_state(state)


async def nap(seconds: int) -> None:
    for _ in range(seconds):
        if STOP_REQUESTED:
            return
        await asyncio.sleep(1)


async def register_signal(state: dict[str, Any], tr: dict[str, Any], tg: "TelegramSender") -> None:
    """إشارة جديدة: تُغلق بصمت أي صفقة نشطة/منتظِرة معاكسة لها في نفس الرمز فقط."""
    sym = sym_of(tr)
    for k, t in state["active"].items():
        if sym_of(t) == sym and t.get("status") in ("active", "waiting") and t["side"] == opposite(tr["side"]):
            t["status"] = "closed"
            t["close_reason"] = "REVERSED"
            if k in state["signals"]:
                state["signals"][k]["status"] = "closed_reversed"
            log.info("[%s] Stopped following %s (opposite signal %s)", sym, k, tr["key"])
    msg_id = await tg.send(pending_text(tr))
    tr["message_id"] = msg_id
    state["signals"][tr["key"]] = {"created": tr["created_at"], "status": "pending"}
    state["active"][tr["key"]] = tr
    save_state(state)
    log.info("[%s] New pending %s %s", sym, tr["key"], tr["entry"])


async def live_loop(sym, duka, instrument, state, tg, cache, lock):
    while not STOP_REQUESTED:
        try:
            if any(sym_of(t) == sym and t.get("status") in ("pending", "active", "waiting")
                   for t in state["active"].values()):
                sb, sa = await asyncio.gather(
                    asyncio.to_thread(fetch_1m, duka, instrument, "BID", LIVE_WINDOW_MINUTES / 60),
                    asyncio.to_thread(fetch_1m, duka, instrument, "ASK", LIVE_WINDOW_MINUTES / 60),
                )
                async with lock:
                    cache["bid"] = merge_tail(cache.get("bid"), sb)
                    cache["ask"] = merge_tail(cache.get("ask"), sa)
                    prices = {"bid": float(sb.close.iloc[-1]), "ask": float(sa.close.iloc[-1])}
                    m5 = resample(cache["bid"], 5)
                    await manage_trades(sym, state, cache["bid"], cache["ask"], m5, tg, prices)
        except Exception as exc:
            log.warning("[%s] Live tick failed (will retry): %r", sym, exc)
        await asyncio.sleep(LIVE_POLL_SECONDS)


async def detect_loop(sym, duka, instrument, state, tg, cache, lock):
    while not STOP_REQUESTED:
        try:
            bid, ask = await asyncio.gather(
                asyncio.to_thread(fetch_1m, duka, instrument, "BID"),
                asyncio.to_thread(fetch_1m, duka, instrument, "ASK"),
            )
            async with lock:
                cache["bid"] = merge_tail(cache.get("bid"), bid)
                cache["ask"] = merge_tail(cache.get("ask"), ask)
                for tr in detect_setups(sym, cache["bid"], state):
                    await register_signal(state, tr, tg)
        except Exception as exc:
            log.warning("[%s] Detect cycle failed (will retry): %r", sym, exc)
        await nap(DETECT_POLL_SECONDS)


async def main_loop():
    await start_web_server()
    duka, instruments = get_duka()
    tg = TelegramSender()
    await tg.start()
    state = load_state()

    if not state.get("started_message_sent"):
        await tg.send("اللهم صلِّ وسلّم وبارك على نبينا محمد وعلى آله وصحبه أجمعين ❤️")
        state["started_message_sent"] = True
        save_state(state)

    tasks = []
    for sym in ENABLED_SYMBOLS:
        cache: dict[str, pd.DataFrame] = {}
        lock = asyncio.Lock()
        tasks.append(live_loop(sym, duka, instruments[sym], state, tg, cache, lock))
        tasks.append(detect_loop(sym, duka, instruments[sym], state, tg, cache, lock))
        log.info("Tracking %s", sym)
    await asyncio.gather(*tasks)
    save_state(state)
    await tg.close()


async def manage_trades(sym: str, state: dict[str, Any], bid: pd.DataFrame, ask: pd.DataFrame, m5: pd.DataFrame,
                        tg: TelegramSender, prices: Optional[dict[str, float]] = None):
    c = CONFIGS[sym]
    now = pd.Timestamp.now(tz="UTC")
    fl = now.floor("1min")
    one_min = pd.Timedelta(minutes=1)
    remove = []
    for key, tr in list(state.get("active", {}).items()):
        if sym_of(tr) != sym:
            continue
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
                await activate(state, tr, tr.get("leg", "P1"), when, tg)
                continue

            created = pd.Timestamp(tr["created_at"])
            if created.tzinfo is None:
                created = created.tz_localize("UTC")
            if tr.get("leg", "P1") == "P1" and now >= created + pd.Timedelta(minutes=PENDING_CANCEL_MINUTES):
                tr["status"] = "waiting"
                tr["wait_since"] = now.isoformat()
                await tg.send(waiting_text(tr), reply_to=tr.get("message_id"))
                if key in state["signals"]:
                    state["signals"][key]["status"] = "cancelled_wait"
                save_state(state)
                continue

            if USE_FVG:
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

        elif tr["status"] == "waiting":
            if now >= expiry:
                tr["status"] = "cancelled"
                if key in state["signals"]:
                    state["signals"][key]["status"] = "cancelled"
                save_state(state)
                remove.append(key)
                continue
            new_tr = find_closer(tr, m5)
            if new_tr is not None and new_tr["key"] not in state["signals"]:
                tr["status"] = "closed"
                tr["close_reason"] = "REPLACED"
                remove.append(key)
                await register_signal(state, new_tr, tg)

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
                tr["status"] = "closed"
                tr["close_reason"] = "SL"
                if key in state["signals"]:
                    state["signals"][key]["status"] = "closed_sl"
                save_state(state)
                remove.append(key)
                continue

            points = max(0, int(round(move * c["pt"])))
            for n, target in enumerate(tr["targets"], 1):
                touched = best <= target if side == "sell" else best >= target
                if touched and n not in tr["hit_tps"]:
                    tr["hit_tps"].append(n)
                    await tg.send(tp_text(tr, n, to_points(abs(target - tr["entry"]), c)), reply_to=reply_to)
                    save_state(state)
            start_pts = int(round(c["progress_start"] * c["pt"]))
            step_pts = int(round(c["progress_step"] * c["pt"]))
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
