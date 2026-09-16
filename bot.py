import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
SIGNAL_THRESHOLD = int(os.getenv("SIGNAL_THRESHOLD", "75"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "180"))
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "60"))
DEPTH = int(os.getenv("DEPTH", "50"))
WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
TOP_N = int(os.getenv("TOP_N", "10"))
SUB_BATCH_SYMBOLS = max(1, int(os.getenv("SUB_BATCH_SYMBOLS", "5")))

# Important: Bybit's linear perpetual is 1000PEPEUSDT, not PEPEUSDT.
# The previous version subscribed to PEPEUSDT and Bybit correctly returned
# "handler not found" for that invalid linear topic.
DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,BNBUSDT,ADAUSDT,AVAXUSDT,"
    "LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,SUIUSDT,APTUSDT,NEARUSDT,ARBUSDT,"
    "OPUSDT,1000PEPEUSDT,WIFUSDT,UNIUSDT,AAVEUSDT,INJUSDT,FILUSDT,ETCUSDT,"
    "TRXUSDT,ATOMUSDT,SEIUSDT,TAOUSDT,TONUSDT,ENAUSDT"
)
SYMBOLS = list(dict.fromkeys(
    s.strip().upper() for s in os.getenv("SYMBOLS", DEFAULT_SYMBOLS).split(",") if s.strip()
))

if DEPTH not in (1, 50, 200, 1000):
    DEPTH = 50
if not SYMBOLS:
    SYMBOLS = ["BTCUSDT", "ETHUSDT"]


@dataclass
class SymbolState:
    symbol: str
    trades: deque = field(default_factory=deque)  # (time, signed_qty, price)
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    last_price: float = 0.0
    trade_count: int = 0
    depth_count: int = 0
    last_trade: float = 0.0
    last_depth: float = 0.0
    last_signal: str = "WAIT"
    last_alert: float = 0.0
    book_update_id: Optional[int] = None
    book_ready: bool = False
    trade_stream_seen: bool = False
    depth_stream_seen: bool = False


states = {s: SymbolState(s) for s in SYMBOLS}
app: Optional[Application] = None
subscribers = set()

ws_connected = False
last_ws_message = 0.0
last_ws_error = ""
last_topic = ""
last_subscribe_reply = ""
ws_data_messages = 0

requested_topics = set()
subscription_ok_topics = set()
subscription_failed_topics = {}
pending_request_topics = {}


def now() -> float:
    return time.time()


def trim(st: SymbolState) -> None:
    cutoff = now() - WINDOW_SEC
    while st.trades and st.trades[0][0] < cutoff:
        st.trades.popleft()


def metrics(st: SymbolState) -> dict:
    trim(st)
    buy = sum(x[1] for x in st.trades if x[1] > 0)
    sell = sum(-x[1] for x in st.trades if x[1] < 0)
    total = buy + sell
    delta = buy - sell

    bid = sum(st.bids.values())
    ask = sum(st.asks.values())
    imbalance = (bid - ask) / (bid + ask) if bid + ask else 0.0

    impulse = 0.0
    if len(st.trades) >= 2 and st.last_price:
        first = st.trades[0][2]
        if first:
            impulse = (st.last_price - first) / first * 100.0

    flow = delta / total if total else 0.0

    score = 50.0
    score += max(-18.0, min(18.0, flow * 180.0))
    score += max(-16.0, min(16.0, imbalance * 160.0))
    score += max(-12.0, min(12.0, impulse * 120.0))
    if total > 0:
        score += 4.0
    score = int(round(max(0.0, min(100.0, score))))

    signal = "LONG" if score >= SIGNAL_THRESHOLD else "SHORT" if score <= 100 - SIGNAL_THRESHOLD else "WAIT"
    return {
        "score": score,
        "signal": signal,
        "buy": buy,
        "sell": sell,
        "delta": delta,
        "imbalance": imbalance,
        "impulse": impulse,
        "flow": flow,
        "total": total,
    }


def _price_decimals(price: float) -> int:
    if price >= 1000:
        return 2
    if price >= 100:
        return 3
    if price >= 1:
        return 4
    if price >= 0.1:
        return 5
    if price >= 0.01:
        return 6
    return 8


def round_price(x: float, reference: float) -> float:
    if x <= 0:
        return 0.0
    return round(x, _price_decimals(reference))


def trade_levels(st: SymbolState, m: dict) -> Optional[dict]:
    price = st.last_price
    if not price or m["signal"] == "WAIT":
        return None

    prices = [x[2] for x in st.trades if x[2] > 0]
    if len(prices) < 10:
        return None

    recent = prices[-min(len(prices), 120):]
    recent_range = max(recent) - min(recent)
    moves = [abs(recent[i] - recent[i - 1]) for i in range(1, len(recent))]
    avg_move = sum(moves) / len(moves) if moves else 0.0

    risk = max(avg_move * 8.0, recent_range * 0.22, price * 0.0012)
    risk = min(risk, price * 0.012)
    zone = max(risk * 0.20, price * 0.0004)

    if m["signal"] == "LONG":
        entry_low, entry_high = price - zone, price + zone * 0.15
        sl = price - risk
        tp1, tp2, tp3 = price + risk * 1.5, price + risk * 2.5, price + risk * 3.5
    else:
        entry_low, entry_high = price - zone * 0.15, price + zone
        sl = price + risk
        tp1, tp2, tp3 = price - risk * 1.5, price - risk * 2.5, price - risk * 3.5

    return {
        "entry_low": round_price(max(entry_low, 0), price),
        "entry_high": round_price(max(entry_high, 0), price),
        "sl": round_price(max(sl, 0), price),
        "tp1": round_price(max(tp1, 0), price),
        "tp2": round_price(max(tp2, 0), price),
        "tp3": round_price(max(tp3, 0), price),
        "risk": risk,
    }


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.1:
        return f"{x:.5f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8g}"


def _symbol_from_topic(topic: str) -> Optional[str]:
    if topic.startswith("publicTrade."):
        return topic.split(".", 1)[1]
    if topic.startswith("orderbook."):
        parts = topic.split(".")
        return parts[2] if len(parts) >= 3 else None
    return None


def handle_trade(data, topic: str = "") -> None:
    global last_topic
    if not isinstance(data, list):
        return
    received = now()
    topic_symbol = _symbol_from_topic(topic) if topic else None
    for t in data:
        if not isinstance(t, dict):
            continue
        symbol = str(t.get("s") or topic_symbol or "").upper()
        if symbol not in states:
            continue
        try:
            price = float(t["p"])
            qty = float(t["v"])
        except (KeyError, TypeError, ValueError):
            continue
        if price <= 0 or qty <= 0:
            continue
        side = t.get("S")
        signed = qty if side == "Buy" else -qty if side == "Sell" else 0.0
        if signed == 0:
            continue
        st = states[symbol]
        st.trades.append((received, signed, price))
        st.last_price = price
        st.last_trade = received
        st.trade_count += 1
        st.trade_stream_seen = True
        last_topic = topic or f"publicTrade.{symbol}"


def handle_orderbook(msg: dict) -> None:
    global last_topic
    data = msg.get("data")
    if not isinstance(data, dict):
        return
    symbol = str(data.get("s") or _symbol_from_topic(msg.get("topic", "")) or "").upper()
    if symbol not in states:
        return
    st = states[symbol]

    msg_type = msg.get("type")
    try:
        update_id = int(data.get("u")) if data.get("u") is not None else None
    except (TypeError, ValueError):
        update_id = None

    if msg_type == "snapshot":
        st.bids.clear()
        st.asks.clear()
        st.book_ready = True
        st.book_update_id = update_id
    elif msg_type == "delta":
        if not st.book_ready:
            return
        if update_id is not None and st.book_update_id is not None and update_id <= st.book_update_id:
            return
        st.book_update_id = update_id or st.book_update_id
    else:
        return

    for p, q in data.get("b", []):
        try:
            price, qty = float(p), float(q)
        except (TypeError, ValueError):
            continue
        if qty == 0:
            st.bids.pop(price, None)
        else:
            st.bids[price] = qty

    for p, q in data.get("a", []):
        try:
            price, qty = float(p), float(q)
        except (TypeError, ValueError):
            continue
        if qty == 0:
            st.asks.pop(price, None)
        else:
            st.asks[price] = qty

    st.bids = dict(sorted(st.bids.items(), reverse=True)[:DEPTH])
    st.asks = dict(sorted(st.asks.items())[:DEPTH])
    st.depth_count += 1
    st.last_depth = now()
    st.depth_stream_seen = True
    last_topic = msg.get("topic", f"orderbook.{DEPTH}.{symbol}")


def _record_subscribe_failure(msg: dict) -> None:
    global last_ws_error, last_subscribe_reply
    ret = str(msg.get("ret_msg") or msg.get("retMsg") or "subscription failed")
    last_ws_error = ret
    last_subscribe_reply = "ERROR: " + ret

    # Bybit can return a generic ret_msg without failTopics. Extract the topic from
    # the documented error text when possible.
    marker = "topic:"
    if marker in ret:
        topic = ret.split(marker, 1)[1].strip()
        if topic:
            subscription_failed_topics[topic] = ret
            requested_topics.discard(topic)


def handle_message(msg: dict) -> None:
    global last_ws_error, last_topic, last_subscribe_reply, ws_data_messages
    if not isinstance(msg, dict):
        return

    if msg.get("op") == "subscribe":
        data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
        success_topics = data.get("successTopics") or []
        fail_topics = data.get("failTopics") or []
        req_id = msg.get("req_id") or ""
        req_topics = pending_request_topics.pop(req_id, []) if req_id else []

        for topic in success_topics:
            subscription_ok_topics.add(topic)
            requested_topics.add(topic)
        for topic in fail_topics:
            subscription_failed_topics[topic] = str(msg.get("ret_msg") or msg.get("retMsg") or "subscription failed")
            requested_topics.discard(topic)

        success = msg.get("success") is not False and msg.get("retCode") in (None, 0)
        if not success:
            _record_subscribe_failure(msg)
            return

        # Current linear WS commonly returns a generic successful subscribe response.
        # When it does, associate it with the request ID we sent.
        if not success_topics and not fail_topics and req_topics:
            subscription_ok_topics.update(req_topics)
        last_subscribe_reply = "OK"
        return

    if msg.get("op") in ("ping", "pong"):
        return

    if msg.get("success") is False or (msg.get("retCode") not in (None, 0)):
        last_ws_error = str(msg.get("ret_msg") or msg.get("retMsg") or msg)
        return

    topic = str(msg.get("topic") or "")
    data = msg.get("data")
    if not topic or data is None:
        return

    ws_data_messages += 1
    if topic.startswith("publicTrade."):
        handle_trade(data, topic)
    elif topic.startswith("orderbook."):
        handle_orderbook(msg)
    else:
        # Unknown topics are ignored safely.
        last_topic = topic


async def websocket_loop() -> None:
    global ws_connected, last_ws_message, last_ws_error, last_subscribe_reply
    backoff = 2
    while True:
        try:
            print(f"Connecting {WS_URL}")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=15,
                max_size=16_000_000,
            ) as ws:
                ws_connected = True
                last_ws_error = ""
                last_subscribe_reply = ""
                requested_topics.clear()
                subscription_ok_topics.clear()
                subscription_failed_topics.clear()
                pending_request_topics.clear()

                # Only the two market-data streams the scanner actually uses.
                for start in range(0, len(SYMBOLS), SUB_BATCH_SYMBOLS):
                    batch_symbols = SYMBOLS[start:start + SUB_BATCH_SYMBOLS]
                    batch_topics = []
                    for symbol in batch_symbols:
                        batch_topics.extend([
                            f"publicTrade.{symbol}",
                            f"orderbook.{DEPTH}.{symbol}",
                        ])
                    req_id = f"scan-{int(time.time() * 1000)}-{start}"
                    pending_request_topics[req_id] = list(batch_topics)
                    requested_topics.update(batch_topics)
                    await ws.send(json.dumps({
                        "req_id": req_id,
                        "op": "subscribe",
                        "args": batch_topics,
                    }))
                    await asyncio.sleep(0.25)

                async def heartbeat() -> None:
                    while True:
                        await asyncio.sleep(20)
                        await ws.send(json.dumps({"op": "ping"}))

                async def watchdog() -> None:
                    while True:
                        await asyncio.sleep(10)
                        if last_ws_message and now() - last_ws_message > 60:
                            await ws.close(code=1011, reason="No WebSocket data for 60s")
                            return

                hb = asyncio.create_task(heartbeat())
                wd = asyncio.create_task(watchdog())
                try:
                    async for raw in ws:
                        last_ws_message = now()
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        handle_message(msg)
                finally:
                    hb.cancel()
                    wd.cancel()
                    await asyncio.gather(hb, wd, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_ws_error = repr(exc)
            print("Bybit WebSocket error:", repr(exc))
        finally:
            ws_connected = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


def signal_text(st: SymbolState, m: dict) -> str:
    levels = trade_levels(st, m)
    direction = "🟢 LONG" if m["signal"] == "LONG" else "🔴 SHORT" if m["signal"] == "SHORT" else "⚪ WAIT"
    base = (
        f"*{st.symbol} — {direction} {m['score']}/100*\n\n"
        f"Цена: `{fmt_price(st.last_price)}`\n"
        f"Delta {WINDOW_SEC}s: `{m['delta']:.5g}`\n"
        f"Order Book: `{m['imbalance'] * 100:+.1f}%`\n"
        f"Импульс: `{m['impulse']:+.3f}%`\n"
        f"Flow: `{m['flow'] * 100:+.1f}%`"
    )
    if m["signal"] == "WAIT":
        return base + f"\n\nПорог сигнала: `{SIGNAL_THRESHOLD}/100`"
    if not levels:
        return base + "\n\n⏳ *Недостаточно истории для расчёта уровней*"

    risk = abs(st.last_price - levels["sl"])
    rr = []
    for tp in (levels["tp1"], levels["tp2"], levels["tp3"]):
        reward = abs(tp - st.last_price)
        rr.append(reward / risk if risk else 0)
    return base + (
        "\n\n🎯 *РАСЧЁТНЫЕ УРОВНИ*"
        f"\nВход: `{fmt_price(levels['entry_low'])} — {fmt_price(levels['entry_high'])}`"
        f"\nSL: `{fmt_price(levels['sl'])}`"
        f"\nTP1: `{fmt_price(levels['tp1'])}` · R:R `1:{rr[0]:.1f}`"
        f"\nTP2: `{fmt_price(levels['tp2'])}` · R:R `1:{rr[1]:.1f}`"
        f"\nTP3: `{fmt_price(levels['tp3'])}` · R:R `1:{rr[2]:.1f}`"
        "\n\n⚠️ Расчётные уровни для анализа, не гарантия движения цены."
    )


async def alerts_loop() -> None:
    while True:
        try:
            if app is not None:
                for st in states.values():
                    if not st.last_price or len(st.trades) < 10:
                        continue
                    m = metrics(st)
                    old = st.last_signal
                    st.last_signal = m["signal"]
                    if m["signal"] == "WAIT" or m["signal"] == old:
                        continue
                    if now() - st.last_alert < ALERT_COOLDOWN_SEC:
                        continue
                    levels = trade_levels(st, m)
                    if not levels:
                        continue
                    st.last_alert = now()
                    text = "🚨 *SCALPING SIGNAL*\n\n" + signal_text(st, m)
                    for chat_id in list(subscribers):
                        try:
                            await app.bot.send_message(chat_id, text, parse_mode="Markdown")
                        except Exception as exc:
                            print("Telegram alert error", repr(exc))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print("Alert loop error", repr(exc))
        await asyncio.sleep(2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat:
        subscribers.add(update.effective_chat.id)
    await update.message.reply_text(
        "🤖 Bybit Scanner V8\n\n"
        "/top — только сигналы ≥ порога\n"
        "/coin BTCUSDT — полный разбор\n"
        "/status — диагностика\n"
        "/settings — настройки"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("/top\n/coin BTCUSDT\n/status\n/settings")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    current = now()
    active_trade = sum(1 for st in states.values() if st.trade_stream_seen)
    active_depth = sum(1 for st in states.values() if st.depth_stream_seen)
    trades = sum(st.trade_count for st in states.values())
    depth = sum(st.depth_count for st in states.values())
    priced = sum(1 for st in states.values() if st.last_price)
    books = sum(1 for st in states.values() if st.book_ready)
    age = f"{int(current - last_ws_message)}s ago" if last_ws_message else "нет сообщений"
    failed = len(subscription_failed_topics)
    ok = len(subscription_ok_topics)
    requested = len(requested_topics)
    total_expected = len(SYMBOLS) * 2
    await update.message.reply_text(
        "🟢 *BYBIT SCANNER V8*\n\n"
        f"WebSocket: `{'CONNECTED' if ws_connected else 'RECONNECTING'}`\n"
        f"Последнее WS: `{age}`\n"
        f"Последняя ошибка: `{last_ws_error[:350] if last_ws_error else 'нет'}`\n\n"
        f"Символов: `{len(SYMBOLS)}`\n"
        f"Trade data: `{active_trade}/{len(SYMBOLS)}`\n"
        f"Depth data: `{active_depth}/{len(SYMBOLS)}`\n"
        f"Orderbooks ready: `{books}/{len(SYMBOLS)}`\n\n"
        f"Всего trades: `{trades}`\n"
        f"Всего depth: `{depth}`\n"
        f"Монет с ценой: `{priced}`\n"
        f"WS data messages: `{ws_data_messages}`\n\n"
        f"Подписок подтверждено: `{ok}/{total_expected}`\n"
        f"В ожидании: `{requested}`\n"
        f"Ошибок подписки: `{failed}`\n"
        f"Последний topic: `{last_topic or 'нет'}`\n"
        f"Последний subscribe: `{last_subscribe_reply or 'нет'}`\n"
        f"Порог: `{SIGNAL_THRESHOLD}/100`\n"
        "REST: `DISABLED`\n"
        "Bybit API keys: `NOT REQUIRED`\n"
        "Ticker stream: `DISABLED`",
        parse_mode="Markdown",
    )


async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = []
    for st in states.values():
        if not st.last_price or len(st.trades) < 10:
            continue
        m = metrics(st)
        if m["signal"] not in ("LONG", "SHORT") or m["score"] < SIGNAL_THRESHOLD:
            continue
        levels = trade_levels(st, m)
        if not levels:
            continue
        risk = abs(st.last_price - levels["sl"])
        rr = [abs(tp - st.last_price) / risk if risk else 0 for tp in (levels["tp1"], levels["tp2"], levels["tp3"])]
        rows.append((m["score"], st, m, levels, rr))

    rows.sort(key=lambda row: row[0], reverse=True)
    if not rows:
        await update.message.reply_text(
            f"⏳ Нет подтверждённых LONG/SHORT ≥ {SIGNAL_THRESHOLD}/100.\nПроверь /status"
        )
        return

    parts = [f"📊 *TOP SIGNALS — только ≥ {SIGNAL_THRESHOLD}/100*\n"]
    for score, st, m, levels, rr in rows[:TOP_N]:
        direction = "🟢 LONG" if m["signal"] == "LONG" else "🔴 SHORT"
        parts.append(
            f"*{st.symbol}* — {direction} `{score}/100`\n"
            f"Цена `{fmt_price(st.last_price)}` | Δ `{m['delta']:.4g}` | OB `{m['imbalance'] * 100:+.1f}%`\n"
            f"🎯 Вход `{fmt_price(levels['entry_low'])} — {fmt_price(levels['entry_high'])}`\n"
            f"🛑 SL `{fmt_price(levels['sl'])}`\n"
            f"💰 TP1 `{fmt_price(levels['tp1'])}` · R:R `1:{rr[0]:.1f}`\n"
            f"💰 TP2 `{fmt_price(levels['tp2'])}` · R:R `1:{rr[1]:.1f}`\n"
            f"💰 TP3 `{fmt_price(levels['tp3'])}` · R:R `1:{rr[2]:.1f}`\n"
        )
    await update.message.reply_text("\n".join(parts), parse_mode="Markdown")


async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Пример: /coin BTCUSDT")
        return
    symbol = context.args[0].upper()
    st = states.get(symbol)
    if not st or not st.last_price:
        await update.message.reply_text("Монета не найдена или данные ещё не поступили.")
        return
    await update.message.reply_text(signal_text(st, metrics(st)), parse_mode="Markdown")


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"⚙️ V8\nSymbols: {len(SYMBOLS)}\nDepth: {DEPTH}\nWindow: {WINDOW_SEC}s\n"
        f"Threshold: {SIGNAL_THRESHOLD}\nREST: DISABLED\nAPI keys: NOT REQUIRED\nTicker stream: DISABLED\n"
        "PEPE linear symbol: 1000PEPEUSDT"
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    print("Telegram handler error:", repr(context.error))


async def main() -> None:
    global app
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

    app = Application.builder().token(TOKEN).build()
    handlers = [
        ("start", start),
        ("help", help_cmd),
        ("top", top_cmd),
        ("coin", coin_cmd),
        ("status", status_cmd),
        ("settings", settings_cmd),
    ]
    for command, handler in handlers:
        app.add_handler(CommandHandler(command, handler))
    app.add_error_handler(error_handler)

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message"])

    tasks = [asyncio.create_task(websocket_loop()), asyncio.create_task(alerts_loop())]
    try:
        await asyncio.Event().wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
