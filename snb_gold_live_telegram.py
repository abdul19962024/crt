#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SNB MULTI LIVE — XAU/USD + Dow Jones + Nasdaq-100
=================================================
إشارات ومتابعة Telegram فقط؛ لا ينفذ أوامر لدى الوسيط.
المصدر: Dukascopy M1، وإعادة التجميع إلى H2 وM5.

قواعد الدخول:
- H2 مغلقة، ثم نافذة تأكيد بعد إغلاقها.
- SELL: M5 خضراء ثم حمراء، BUY: M5 حمراء ثم خضراء.
- لمس/تجاوز طرف H2 يكفي؛ لا يشترط الإغلاق داخل النطاق.
- نقطة التساوي: close الشمعة الأولى مع open الثانية ضمن tolerance الأصل.
- P1: عند equal + offset للبيع أو equal - offset للشراء؛ ينتهي بعد 10 دقائق إن لم يتفعل.
- بعد انتهاء P1، يُبحث عن أول FVG اتجاهي حتى نهاية نافذة الإشارة، ثم يوضع P2 عند حافته.
- P2 صالح 10 دقائق؛ وقف FVG للذهب ثابت على بعد 3$ من الدخول.
- الإشارة المعاكسة تلغي الأوامر غير المفعلة فقط، ولا تغلق الصفقة المفعلة.

تعريف البيب المستخدم:
- XAUUSD: 1 pip = 0.10$.
- DOW: 1 pip = 1 نقطة سعرية (10$ = 10 pips).
- NASDAQ: 1 pip = 10 نقاط سعرية (100$ = 10 pips).

ضع قيم Telegram في الثوابت أدناه أو في متغيرات البيئة.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# ========================= USER SETTINGS =========================
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "12345678"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "PUT_API_HASH_HERE")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "PUT_STRING_SESSION_HERE")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@YOUR_CHANNEL")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"

DATA_DIR = Path(os.getenv("SNB_DATA_DIR", str(Path.home() / "snb_live_data")))
STATE_FILE = DATA_DIR / "snb_multi_live_state.json"
TIMEZONE = "Asia/Riyadh"
POLL_SECONDS = 5
LOOKBACK_HOURS = 18
SIGNAL_MAX_AGE_MINUTES = 10
PENDING_EXPIRY_MINUTES = 10
SETUP_EXPIRY_HOURS = 2
FVG_DELAY_MINUTES = 10
NO_NEW_FIRST_HOUR = True
NO_NEW_LAST_HOUR = True

# كل offset هنا بالبيب. target_prices تُبنى تلقائياً حسب pip_size.
TARGET_PIPS = [70, 100, 170, 200, 350]
PROGRESS_START_PIPS = 30
PROGRESS_STEP_PIPS = 20

# التعديلات الخاصة بكل سوق.
# XAU: equal 0.10$، minimum stop 3$، offset 1$، FVG SL 3$ من سعر الدخول.
# DOW: equal 1 نقطة، fixed stop 30 pips = 30 نقاط.
# NASDAQ: equal 10 نقاط = 1 pip، fixed stop 30 pips = 300 نقاط.
ASSETS = {
    "XAUUSD": {
        "label": "XAUUSD",
        "instrument_name": "INSTRUMENT_FX_METALS_XAU_USD",
        "pip_size": 0.10,
        "equal_tolerance": 0.10,
        "pending_offset_price": 1.00,
        "min_stop_price": 3.00,
        "fvg_stop_price": 3.00,
        "fixed_stop_pips": None,
        "raw_stop_buffer": 0.10,
        "fvg_stop_pips": 10,       # 1$ = 10 pips
        "decimals": 2,
    },
    "DOW": {
        "label": "DOW JONES",
        "instrument_name": "INSTRUMENT_IDX_AMERICA_E_D_J_IND",
        "pip_size": 1.0,
        "equal_tolerance": 1.0,
        "pending_offset_price": 1.0,
        "min_stop_price": 3.0,
        "fvg_stop_price": None,
        "fixed_stop_pips": 30,
        "raw_stop_buffer": 0.10,
        "fvg_stop_pips": 1,
        "decimals": 2,
    },
    "NASDAQ": {
        "label": "NASDAQ-100",
        "instrument_name": "INSTRUMENT_IDX_AMERICA_E_NQ_100",
        "pip_size": 10.0,
        "equal_tolerance": 10.0,
        "pending_offset_price": 10.0,
        "min_stop_price": 30.0,
        "fvg_stop_price": None,
        "fixed_stop_pips": 30,
        "raw_stop_buffer": 0.10,
        "fvg_stop_pips": 1,
        "decimals": 2,
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snb-multi")
STOP_REQUESTED = False


def stop_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.info("Stop requested; active trades will be saved.")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


class NoMarketData(RuntimeError):
    pass


def load_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            log.exception("Could not read state; starting clean.")
    return {"started_message_sent": False, "signals": {}, "active": {}}


def save_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def normalize_pending_state(state: dict[str, Any], now: Optional[pd.Timestamp] = None) -> None:
    """Apply the new 10-minute order lifetime to pending orders saved by older versions."""
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    changed = False
    for tr in state.setdefault("active", {}).values():
        if tr.get("status") not in ("pending", "waiting_fvg"):
            continue
        confirm = pd.Timestamp(tr["confirm_time"])
        setup_expiry = pd.Timestamp(tr.get("setup_expiry", confirm + pd.Timedelta(minutes=5) + pd.Timedelta(hours=SETUP_EXPIRY_HOURS)))
        if tr.get("setup_expiry") is None:
            tr["setup_expiry"] = setup_expiry.isoformat()
            changed = True
        if tr.get("status") == "pending":
            placed = pd.Timestamp(tr.get("order_placed_at", tr.get("created_at", now.isoformat())))
            new_expiry = placed + pd.Timedelta(minutes=PENDING_EXPIRY_MINUTES)
            previous_expiry = pd.Timestamp(tr["expiry"]) if tr.get("expiry") else new_expiry
            final_expiry = min(previous_expiry, new_expiry)
            if tr.get("order_placed_at") != placed.isoformat() or tr.get("expiry") != final_expiry.isoformat():
                tr["order_placed_at"] = placed.isoformat()
                tr["expiry"] = final_expiry.isoformat()
                changed = True
            if tr.get("leg") not in ("P1", "P2-FVG"):
                tr["leg"] = "P1"
                changed = True
        elif tr.get("leg") != "P1-EXPIRED":
            tr["leg"] = "P1-EXPIRED"
            changed = True
    if changed:
        save_state(state)


def get_duka():
    try:
        import dukascopy_python as duka
        import dukascopy_python.instruments as instruments
        result = {}
        for key, cfg in ASSETS.items():
            result[key] = getattr(instruments, cfg["instrument_name"])
        return duka, result
    except Exception as exc:
        raise SystemExit(f"فشل تحميل أدوات Dukascopy: {exc!r}") from exc


def fetch_1m(duka, instrument, side: str) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(hours=LOOKBACK_HOURS)
    got = duka.fetch(
        instrument=instrument,
        interval=duka.INTERVAL_MIN_1,
        offer_side=getattr(duka, f"OFFER_SIDE_{side}"),
        start=start.to_pydatetime(), end=now.to_pydatetime(),
        max_retries=5, limit=30_000, debug=False,
    )
    if got is None or got.empty:
        raise NoMarketData(f"No recent {side} candles")
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
    floor = pd.Timestamp.now(tz="UTC").floor(f"{minutes}min")
    return out[out.index + pd.Timedelta(minutes=minutes) <= floor]


def riyadh(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.tz_convert(TIMEZONE)


def allow_new_signal(ts: pd.Timestamp) -> bool:
    h = riyadh(ts).hour
    return not ((NO_NEW_FIRST_HOUR and h == 0) or (NO_NEW_LAST_HOUR and h == 23))


class TelegramSender:
    def __init__(self):
        self.client = None

    async def start(self):
        if not TELEGRAM_ENABLED:
            log.warning("Telegram disabled; preview mode.")
            return
        if not TELEGRAM_API_ID or TELEGRAM_API_HASH.startswith("PUT_") or TELEGRAM_SESSION_STRING.startswith("PUT_"):
            raise RuntimeError("ضع TELEGRAM_API_ID/API_HASH/SESSION_STRING أولاً")
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        self.client = TelegramClient(StringSession(TELEGRAM_SESSION_STRING), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STRING غير صالح أو منتهي")
        log.info("Telegram connected to %s", TELEGRAM_CHANNEL)

    async def send(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        if not TELEGRAM_ENABLED or self.client is None:
            log.info("TELEGRAM PREVIEW:\n%s", text)
            return None
        msg = await self.client.send_message(TELEGRAM_CHANNEL, text, reply_to=reply_to)
        return int(msg.id)

    async def edit_cancelled(self, message_id: Optional[int], text: str, send_followup: bool = True) -> None:
        if not message_id or not TELEGRAM_ENABLED or self.client is None:
            return
        try:
            await self.client.edit_message(TELEGRAM_CHANNEL, message_id, text)
            if send_followup:
                await self.send("❌ أُلغي الأمر المعلق — سنأخذ فرصة أقرب إذا ظهرت.", reply_to=message_id)
        except Exception:
            log.exception("Could not edit cancelled message")

    async def close(self):
        if self.client:
            await self.client.disconnect()


def cfg(tr):
    return ASSETS[tr["asset"]]


def fmt_price(x: float, tr: Optional[dict] = None) -> str:
    decimals = cfg(tr)["decimals"] if tr else 2
    return f"{x:.{decimals}f}"


def price_to_pips(delta: float, tr: dict) -> int:
    return max(0, int(round(delta / cfg(tr)["pip_size"])))


def side_word(side):
    return "BUY" if side == "buy" else "SELL"


def build_targets(entry: float, side: str, asset: str) -> list[float]:
    p = ASSETS[asset]["pip_size"]
    return [round(entry + (x * p if side == "buy" else -x * p), 3) for x in TARGET_PIPS]


def signal_key(asset: str, h2_start: pd.Timestamp, side: str) -> str:
    return f"{asset}|{h2_start.isoformat()}|{side}"


def pending_text(tr):
    arrow = "📈 BUY LIMIT" if tr["side"] == "buy" else "📉 SELL LIMIT"
    tps = "\n".join(f"{i}️⃣ TP{i} ➜ {fmt_price(x, tr)}" for i, x in enumerate(tr["targets"], 1))
    leg = "P2 — حافة FVG" if tr.get("leg") == "P2-FVG" else "P1 — نقطة التساوي"
    return (f"🔔 **توصية جديدة | صفقة معلقة** 🔔\n\n🥇 **{tr['label']}**\n"
            f"{arrow} ➜ **{fmt_price(tr['entry'], tr)}**\n"
            f"📍 نوع الدخول: **{leg}**\n\n"
            f"🛑 وقف الخسارة: **{fmt_price(tr['stop'], tr)}**\n\n🎯 **الأهداف:**\n{tps}\n"
            f"⏳ صلاحية الأمر المعلق: **{PENDING_EXPIRY_MINUTES} دقائق**\n\n"
            "⚠️ إدارة رأس المال مسؤوليتك\n💡 لا تخاطر بأكثر من 1-2%")


def activated_text(tr):
    return (f"✅ **تم تفعيل الصفقة** 🚀\n\n🥇 {tr['label']} | **{side_word(tr['side'])}**\n"
            f"📍 دخول عند **{fmt_price(tr['entry'], tr)}**\n\n⏳ نتابع معكم")


def cancelled_text(tr, reason="timeout"):
    reasons = {
        "timeout": f"انتهت مهلة {PENDING_EXPIRY_MINUTES} دقائق دون تفعيل.",
        "p1_timeout": f"انتهت مهلة P1 ({PENDING_EXPIRY_MINUTES} دقائق) دون تفعيل؛ لا يوجد أمر معلق الآن، ويجري انتظار فرصة FVG ضمن مدة الإشارة.",
        "opposite": "أُلغي الأمر المعلق بسبب ظهور إشارة جديدة معاكسة.",
        "setup_expired": "انتهت مدة البحث عن فرصة FVG دون ظهور أمر جديد.",
    }
    return (f"❌ **تم إلغاء الأمر المعلق**\n\n🥇 {tr['label']} | {side_word(tr['side'])}\n"
            f"📍 المستوى: {fmt_price(tr['entry'], tr)}\n📝 السبب: {reasons.get(reason, reasons['timeout'])}")


def progress_text(tr, pips):
    return f"🔥 {tr['label']} تمشي صح ✅\n💰 +{pips} بيب 📈✅"


def tp_text(tr, n, pips):
    return f"🎯 **{tr['label']} — تحقق TP{n}** ✅\n💰 +{pips} بيب 🔥\n🥇 {tr['label']} {side_word(tr['side'])}"


def all_targets_text(tr):
    return f"🏆 تحققت جميع الأهداف 🎯\n🥇 {tr['label']} {side_word(tr['side'])}"


def detect_setups(bid, ask, state, asset):
    c = ASSETS[asset]; h2, m5 = resample(bid, 120), resample(bid, 5); out=[]
    if len(h2) < 3 or len(m5) < 5: return out
    now = pd.Timestamp.now(tz="UTC")
    for i in range(2, len(h2)):
        hs, hb = h2.index[i], h2.iloc[i]
        if not allow_new_signal(hs): continue
        q = m5[(m5.index >= hs + pd.Timedelta(hours=2)) & (m5.index < hs + pd.Timedelta(hours=4))]
        for j in range(1, len(q)):
            first, second, confirm = q.iloc[j-1], q.iloc[j], q.index[j]
            if now - (confirm + pd.Timedelta(minutes=5)) > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES): continue
            upper = max(float(first.high), float(second.high)) >= float(hb.high)
            lower = min(float(first.low), float(second.low)) <= float(hb.low)
            equal = (float(first.close) + float(second.open)) / 2
            near = abs(float(first.close) - float(second.open)) <= c["equal_tolerance"]
            sell = upper and near and first.close > first.open and second.close < second.open
            buy = lower and near and first.close < first.open and second.close > second.open
            if not (sell or buy): continue
            side = "sell" if sell else "buy"; key = signal_key(asset, hs, side)
            if key in state["signals"]: break
            raw_stop = (max(float(first.high), float(second.high)) + c["raw_stop_buffer"] if sell else min(float(first.low), float(second.low)) - c["raw_stop_buffer"])
            if c["fixed_stop_pips"] is not None:
                entry = equal + c["pending_offset_price"] if sell else equal - c["pending_offset_price"]
                stop = entry + c["fixed_stop_pips"]*c["pip_size"] if sell else entry - c["fixed_stop_pips"]*c["pip_size"]
            elif abs(equal-raw_stop) < c["min_stop_price"]:
                entry, stop = equal, (equal+c["min_stop_price"] if sell else equal-c["min_stop_price"])
            else:
                entry, stop = equal+c["pending_offset_price"] if sell else equal-c["pending_offset_price"], raw_stop
            setup_expiry = confirm + pd.Timedelta(minutes=5) + pd.Timedelta(hours=SETUP_EXPIRY_HOURS)
            out.append({"key":key,"asset":asset,"label":c["label"],"side":side,"leg":"P1","h2_start":hs.isoformat(),"confirm_time":confirm.isoformat(),"entry":round(entry,3),"stop":round(stop,3),"targets":build_targets(entry,side,asset),"created_at":now.isoformat(),"order_placed_at":now.isoformat(),"expiry":(now+pd.Timedelta(minutes=PENDING_EXPIRY_MINUTES)).isoformat(),"setup_expiry":setup_expiry.isoformat(),"message_id":None,"status":"pending","last_progress":0,"hit_tps":[],"fvg_order":None})
            break
    return out


def make_fvg_order(tr, trigger, m5, now):
    """Return the first directional FVG edge that has not already been touched."""
    c = cfg(tr)
    confirm = pd.Timestamp(tr["confirm_time"])
    search_start = confirm + pd.Timedelta(minutes=5 + FVG_DELAY_MINUTES)
    search_end = pd.Timestamp(tr.get("setup_expiry", confirm + pd.Timedelta(minutes=5) + pd.Timedelta(hours=SETUP_EXPIRY_HOURS)))

    for k in range(2, len(m5)):
        formed = m5.index[k]
        available_at = formed + pd.Timedelta(minutes=5)
        if formed < search_start or formed >= search_end or available_at > now:
            continue
        c1, c3 = m5.iloc[k - 2], m5.iloc[k]
        bullish = float(c1.high) < float(c3.low)
        bearish = float(c1.low) > float(c3.high)

        if tr["side"] == "buy" and bullish:
            low, high = float(c1.high), float(c3.low)
            entry = high  # edge nearest the current price on a bullish FVG
            stop = entry - c["fvg_stop_price"] if c.get("fvg_stop_price") is not None else low - c["fvg_stop_pips"] * c["pip_size"]
        elif tr["side"] == "sell" and bearish:
            low, high = float(c3.high), float(c1.low)
            entry = low  # edge nearest the current price on a bearish FVG
            stop = entry + c["fvg_stop_price"] if c.get("fvg_stop_price") is not None else high + c["fvg_stop_pips"] * c["pip_size"]
        else:
            continue

        before_placement = trigger[(trigger.index >= available_at) & (trigger.index < now)]
        already_touched = (before_placement.high >= entry).any() if tr["side"] == "sell" else (before_placement.low <= entry).any()
        if already_touched:
            continue
        return {
            "entry": round(entry, 3),
            "stop": round(stop, 3),
            "formed": formed.isoformat(),
            "available_at": available_at.isoformat(),
            "targets": build_targets(entry, tr["side"], tr["asset"]),
        }
    return None


async def cancel_pending_trade(state, tr, tg, reason, send_followup=False):
    tr["status"] = "cancelled"
    tr["close_reason"] = reason.upper()
    tr["cancelled_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    state["signals"].setdefault(tr["key"], {})["status"] = "cancelled"
    await tg.edit_cancelled(tr.get("message_id"), cancelled_text(tr, reason), send_followup=send_followup)
    state["active"].pop(tr["key"], None)
    save_state(state)


async def cancel_opposite_pending(state, new_trade, tg):
    """A fresh opposite setup releases only unfilled orders; active positions remain tracked."""
    for key, old in list(state.get("active", {}).items()):
        if old.get("asset") != new_trade["asset"] or old.get("side") == new_trade["side"]:
            continue
        if old.get("status") in ("pending", "waiting_fvg"):
            log.info("Cancelling opposite pending setup %s for new %s signal", key, new_trade["side"])
            await cancel_pending_trade(state, old, tg, "opposite", send_followup=False)


async def place_fvg_order(tr, trigger, m5, now, state, tg):
    fvg = make_fvg_order(tr, trigger, m5, now)
    if fvg is None:
        return False

    parent_message_id = tr.get("message_id")
    tr.update({
        "entry": fvg["entry"],
        "stop": fvg["stop"],
        "targets": fvg["targets"],
        "leg": "P2-FVG",
        "fvg_order": fvg,
        "status": "pending",
        "created_at": now.isoformat(),
        "order_placed_at": now.isoformat(),
        "expiry": (now + pd.Timedelta(minutes=PENDING_EXPIRY_MINUTES)).isoformat(),
        "last_progress": 0,
        "hit_tps": [],
    })
    tr["message_id"] = await tg.send(pending_text(tr), reply_to=parent_message_id)
    state["signals"].setdefault(tr["key"], {})["status"] = "pending_fvg"
    save_state(state)
    log.info("Placed 10-minute FVG-edge pending signal asset=%s side=%s entry=%s stop=%s", tr["asset"], tr["side"], tr["entry"], tr["stop"])
    return True


async def manage_trades(state, bid, ask, m5, tg, asset, now=None):
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    remove = []
    for key, tr in list(state["active"].items()):
        if tr.get("asset") != asset:
            continue
        if tr.get("status") in ("closed", "cancelled"):
            remove.append(key)
            continue

        side = tr["side"]
        trigger = bid if side == "sell" else ask
        status = tr.get("status")

        if status == "waiting_fvg":
            setup_expiry = pd.Timestamp(tr.get("setup_expiry", now + pd.Timedelta(hours=SETUP_EXPIRY_HOURS)))
            if now >= setup_expiry:
                await cancel_pending_trade(state, tr, tg, "setup_expired", send_followup=False)
            else:
                await place_fvg_order(tr, trigger, m5, now, state, tg)
            continue

        if status == "pending":
            order_placed_at = pd.Timestamp(tr.get("order_placed_at", tr.get("created_at", now.isoformat())))
            expiry = pd.Timestamp(tr["expiry"])
            q = trigger[(trigger.index >= order_placed_at) & (trigger.index < expiry)]
            hit = q[q.high >= tr["entry"]] if side == "sell" else q[q.low <= tr["entry"]]
            if len(hit):
                tr["status"] = "active"
                tr["entry_time"] = hit.index[0].isoformat()
                tr["entry_source"] = tr.get("leg", "P1")
                state["signals"].setdefault(key, {})["status"] = "active"
                await tg.send(activated_text(tr))
                save_state(state)
                continue

            if now >= expiry:
                if tr.get("leg", "P1") == "P1":
                    tr["status"] = "waiting_fvg"
                    tr["leg"] = "P1-EXPIRED"
                    tr["p1_expired_at"] = now.isoformat()
                    state["signals"].setdefault(key, {})["status"] = "waiting_fvg"
                    await tg.edit_cancelled(tr.get("message_id"), cancelled_text(tr, "p1_timeout"), send_followup=False)
                    save_state(state)
                    setup_expiry = pd.Timestamp(tr.get("setup_expiry", now + pd.Timedelta(hours=SETUP_EXPIRY_HOURS)))
                    if now >= setup_expiry:
                        await cancel_pending_trade(state, tr, tg, "setup_expired", send_followup=False)
                    else:
                        await place_fvg_order(tr, trigger, m5, now, state, tg)
                else:
                    await cancel_pending_trade(state, tr, tg, "timeout", send_followup=True)
            continue

        if status == "active":
            entry_time = pd.Timestamp(tr["entry_time"])
            q = trigger[trigger.index >= entry_time]
            if q.empty:
                continue
            stopped = False
            for _, bar in q.iterrows():
                if (side == "sell" and float(bar.high) >= tr["stop"]) or (side == "buy" and float(bar.low) <= tr["stop"]):
                    stopped = True
                    break
            if stopped:
                tr["status"] = "closed"
                tr["close_reason"] = "SL"
                await tg.send(f"🛑 {tr['label']} أغلقت على الوقف\n🥇 {tr['label']} {side_word(side)}\n📍 الوقف: {fmt_price(tr['stop'],tr)}")
                save_state(state)
                remove.append(key)
                continue
            favorable = float(q.low.min()) if side == "sell" else float(q.high.max())
            move = (tr["entry"] - favorable) if side == "sell" else (favorable - tr["entry"])
            pips = price_to_pips(move, tr)
            for n, target in enumerate(tr["targets"], 1):
                touched = favorable <= target if side == "sell" else favorable >= target
                if touched and n not in tr["hit_tps"]:
                    tr["hit_tps"].append(n)
                    await tg.send(tp_text(tr, n, price_to_pips(abs(target - tr["entry"]), tr)))
                    save_state(state)
            if pips >= PROGRESS_START_PIPS:
                milestone = (pips // PROGRESS_STEP_PIPS) * PROGRESS_STEP_PIPS
                if milestone > tr.get("last_progress", 0):
                    tr["last_progress"] = milestone
                    await tg.send(progress_text(tr, milestone))
                    save_state(state)
            if len(tr["hit_tps"]) == len(tr["targets"]):
                await tg.send(all_targets_text(tr))
                tr["status"] = "closed"
                remove.append(key)
                save_state(state)

    for key in remove:
        state["active"].pop(key, None)
    save_state(state)


async def main_loop():
    duka,instruments=get_duka();tg=TelegramSender();await tg.start();state=load_state();normalize_pending_state(state)
    if not state.get("started_message_sent"):
        await tg.send("✅ **SNB MULTI LIVE اشتغل**\n\n📡 المتابعة: XAUUSD + DOW JONES + NASDAQ-100\n🕯️ المصدر: Dukascopy M1\n⏱️ لا توجد إشارات جديدة في أول وآخر ساعة من يوم الرياض.");state["started_message_sent"]=True;save_state(state)
    while not STOP_REQUESTED:
        for asset,instrument in instruments.items():
            try:
                bid,ask=await asyncio.gather(asyncio.to_thread(fetch_1m,duka,instrument,"BID"),asyncio.to_thread(fetch_1m,duka,instrument,"ASK"))
                m5=resample(bid,5)
                setups = sorted(detect_setups(bid, ask, state, asset), key=lambda tr: tr["confirm_time"])
                for tr in setups:
                    await cancel_opposite_pending(state, tr, tg)
                    tr["message_id"]=await tg.send(pending_text(tr));state["signals"][tr["key"]]={"created":tr["created_at"],"status":"pending"};state["active"][tr["key"]]=tr;save_state(state);log.info("New %s %s %s",asset,tr["side"],tr["entry"])
                await manage_trades(state,bid,ask,m5,tg,asset)
            except NoMarketData as exc: log.info("%s: السوق مغلق/لا توجد بيانات حديثة: %s",asset,exc)
            except Exception: log.exception("%s polling failed",asset)
        for _ in range(POLL_SECONDS):
            if STOP_REQUESTED: break
            await asyncio.sleep(1)
        save_state(state);await tg.close()


async def self_test_order_flow():
    """Fast offline test of pending expiry, FVG fallback, opposite cancellation, and migration."""
    class FakeTelegram:
        def __init__(self):
            self.sent = []
            self.edited = []
            self.next_id = 100

        async def send(self, text, reply_to=None):
            self.next_id += 1
            self.sent.append((text, reply_to))
            return self.next_id

        async def edit_cancelled(self, message_id, text, send_followup=True):
            self.edited.append((message_id, text, send_followup))

    old_data_dir, old_state_file = DATA_DIR, STATE_FILE
    try:
        with tempfile.TemporaryDirectory() as tmp:
            globals()["DATA_DIR"] = Path(tmp)
            globals()["STATE_FILE"] = Path(tmp) / "self_test_state.json"
            now = pd.Timestamp("2026-01-01T12:30:00Z")
            confirm = now - pd.Timedelta(minutes=30)
            m5_index = pd.date_range(confirm, now - pd.Timedelta(minutes=5), freq="5min")
            m5 = pd.DataFrame({"open": 102.0, "high": 102.5, "low": 101.5, "close": 102.0}, index=m5_index)
            fvg_index = list(m5_index).index(confirm + pd.Timedelta(minutes=15))
            m5.iloc[fvg_index - 2, m5.columns.get_loc("high")] = 101.0
            m5.iloc[fvg_index, m5.columns.get_loc("low")] = 103.0

            m1_index = pd.date_range(now - pd.Timedelta(minutes=35), now - pd.Timedelta(minutes=1), freq="1min")
            ask = pd.DataFrame({"open": 103.5, "high": 104.0, "low": 103.5, "close": 103.7}, index=m1_index)
            bid = pd.DataFrame({"open": 103.4, "high": 103.9, "low": 103.4, "close": 103.6}, index=m1_index)
            key = "XAUUSD|self-test|buy"
            trade = {
                "key": key, "asset": "XAUUSD", "label": "XAUUSD", "side": "buy", "leg": "P1",
                "confirm_time": confirm.isoformat(), "created_at": (now - pd.Timedelta(minutes=11)).isoformat(),
                "order_placed_at": (now - pd.Timedelta(minutes=11)).isoformat(),
                "expiry": (now - pd.Timedelta(minutes=1)).isoformat(),
                "setup_expiry": (now + pd.Timedelta(hours=1)).isoformat(),
                "entry": 102.0, "stop": 99.0, "targets": build_targets(102.0, "buy", "XAUUSD"),
                "status": "pending", "message_id": 99, "last_progress": 0, "hit_tps": [],
            }
            state = {"started_message_sent": True, "signals": {key: {"status": "pending"}}, "active": {key: trade}}
            tg = FakeTelegram()
            await manage_trades(state, bid, ask, m5, tg, "XAUUSD", now=now)
            assert trade["status"] == "pending" and trade["leg"] == "P2-FVG"
            assert trade["entry"] == 103.0 and trade["stop"] == 100.0
            assert pd.Timestamp(trade["expiry"]) == now + pd.Timedelta(minutes=10)

            later = now + pd.Timedelta(minutes=11)
            later_index = pd.date_range(now, later, freq="1min")
            ask_later = pd.DataFrame({"open": 103.5, "high": 104.0, "low": 103.5, "close": 103.7}, index=later_index)
            bid_later = pd.DataFrame({"open": 103.4, "high": 103.9, "low": 103.4, "close": 103.6}, index=later_index)
            await manage_trades(state, bid_later, ask_later, m5, tg, "XAUUSD", now=later)
            assert key not in state["active"] and state["signals"][key]["status"] == "cancelled"

            old_pending = {"key": "old-buy", "asset": "XAUUSD", "side": "buy", "status": "pending", "entry": 100, "stop": 97, "label": "XAUUSD", "message_id": 50}
            active_buy = {"key": "active-buy", "asset": "XAUUSD", "side": "buy", "status": "active", "entry": 100, "stop": 97, "label": "XAUUSD", "message_id": 51}
            same_side = {"key": "same-sell", "asset": "XAUUSD", "side": "sell", "status": "pending", "entry": 110, "stop": 113, "label": "XAUUSD", "message_id": 52}
            opposite_state = {"signals": {x["key"]: {"status": x["status"]} for x in (old_pending, active_buy, same_side)}, "active": {x["key"]: x for x in (old_pending, active_buy, same_side)}}
            await cancel_opposite_pending(opposite_state, {"asset": "XAUUSD", "side": "sell"}, tg)
            assert "old-buy" not in opposite_state["active"]
            assert active_buy["status"] == "active" and same_side["status"] == "pending"

            old_saved = {"active": {"old": {"status": "pending", "confirm_time": confirm.isoformat(), "created_at": (now - pd.Timedelta(minutes=12)).isoformat(), "expiry": (now + pd.Timedelta(hours=2)).isoformat(), "leg": "P1"}}, "signals": {}}
            normalize_pending_state(old_saved, now)
            assert pd.Timestamp(old_saved["active"]["old"]["expiry"]) == now - pd.Timedelta(minutes=2)

        print("SELF-TEST PASSED: P1/P2 10-minute expiry, FVG edge with XAU stop 3.00$, opposite pending cancellation, active-trade retention, and old-state normalization.")
    finally:
        globals()["DATA_DIR"] = old_data_dir
        globals()["STATE_FILE"] = old_state_file


if __name__=="__main__":
    if "--self-test" in sys.argv:
        asyncio.run(self_test_order_flow())
    else:
        try: asyncio.run(main_loop())
        except KeyboardInterrupt: pass
