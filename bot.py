import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field

import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TOP_N = int(os.getenv("TOP_N", "20"))
SIGNAL_THRESHOLD = int(os.getenv("SIGNAL_THRESHOLD", "75"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "180"))
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "60"))
DEPTH = int(os.getenv("DEPTH", "50"))

WS = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")

# We deliberately do NOT call Bybit REST APIs.
# EEA accounts can receive HTTP restrictions on api.bybit.eu for this use case.
# Instead, the bot uses Bybit's public WebSocket only.
DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,BNBUSDT,ADAUSDT,AVAXUSDT,"
    "LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,SUIUSDT,APTUSDT,NEARUSDT,ARBUSDT,"
    "OPUSDT,PEPEUSDT,WIFUSDT,UNIUSDT,AAVEUSDT,INJUSDT,FILUSDT,ETCUSDT,"
    "TRXUSDT,ATOMUSDT,SEIUSDT,TAOUSDT,TONUSDT,ENAUSDT"
)
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("SYMBOLS", DEFAULT_SYMBOLS).split(",")
    if s.strip()
]

app = None
subscribers = set()
states = {}
ws_connected = False
last_ws_message = 0.0
last_ws_error = ""
subscriptions_ok = 0


@dataclass
class SymbolState:
    symbol: str
    trades: deque = field(default_factory=deque)
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    last_price: float = 0.0
    volume24h: float = 0.0
    turnover24h: float = 0.0
    trade_count: int = 0
    depth_count: int = 0
    ticker_count: int = 0
    last_trade: float = 0.0
    last_depth: float = 0.0
    last_ticker: float = 0.0
    last_signal: str = "WAIT"
    last_alert: float = 0.0


for _symbol in SYMBOLS:
    states[_symbol] = SymbolState(_symbol)


def trim(state: SymbolState):
    cutoff = time.time() - WINDOW_SEC
    while state.trades and state.trades[0][0] < cutoff:
        state.trades.popleft()


def metrics(state: SymbolState):
    trim(state)

    buy = sum(x[2] for x in state.trades if x[2] > 0)
    sell = sum(-x[2] for x in state.trades if x[2] < 0)
    total = buy + sell
    delta = buy - sell

    bid = sum(state.bids.values())
    ask = sum(state.asks.values())
    imbalance = (bid - ask) / (bid + ask) if bid + ask else 0.0

    impulse = 0.0
    if state.trades and state.last_price:
        first = state.trades[0][3]
        if first:
            impulse = (state.last_price - first) / first * 100

    flow = delta / total if total else 0.0

    score = 50
    if flow > 0.10:
        score += 18
    elif flow < -0.10:
        score -= 18

    if imbalance > 0.10:
        score += 16
    elif imbalance < -0.10:
        score -= 16

    if impulse > 0.08:
        score += 12
    elif impulse < -0.08:
        score -= 12

    if total:
        score += 4

    score = max(0, min(100, int(score)))

    if score >= SIGNAL_THRESHOLD:
        signal = "LONG"
    elif score <= 100 - SIGNAL_THRESHOLD:
        signal = "SHORT"
    else:
        signal = "WAIT"

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


def apply_book(state: SymbolState, data: dict, snapshot: bool):
    if snapshot:
        state.bids.clear()
        state.asks.clear()

    for price, qty in data.get("b", []):
        try:
            p, q = float(price), float(qty)
        except (TypeError, ValueError):
            continue
        if q == 0:
            state.bids.pop(p, None)
        else:
            state.bids[p] = q

    for price, qty in data.get("a", []):
        try:
            p, q = float(price), float(qty)
        except (TypeError, ValueError):
            continue
        if q == 0:
            state.asks.pop(p, None)
        else:
            state.asks[p] = q

    # Keep only the requested best levels in memory.
    if len(state.bids) > DEPTH:
        state.bids = dict(sorted(state.bids.items(), reverse=True)[:DEPTH])
    if len(state.asks) > DEPTH:
        state.asks = dict(sorted(state.asks.items())[:DEPTH])


def handle_ticker(data):
    global last_ws_message
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = [data]
    else:
        return

    now = time.time()
    for item in items:
        symbol = item.get("symbol")
        if symbol not in states:
            continue
        st = states[symbol]
        try:
            if item.get("lastPrice"):
                st.last_price = float(item["lastPrice"])
            if item.get("volume24h"):
                st.volume24h = float(item["volume24h"])
            if item.get("turnover24h"):
                st.turnover24h = float(item["turnover24h"])
        except (TypeError, ValueError):
            pass
        st.ticker_count += 1
        st.last_ticker = now


def handle_trade(data):
    if not isinstance(data, list):
        return
    now = time.time()
    for trade in data:
        symbol = trade.get("s")
        if symbol not in states:
            continue
        try:
            price = float(trade["p"])
            qty = float(trade["v"])
        except (KeyError, TypeError, ValueError):
            continue

        side = trade.get("S")
        signed = qty if side == "Buy" else -qty
        st = states[symbol]
        st.trades.append((now, qty, signed, price))
        st.last_price = price
        st.last_trade = now
        st.trade_count += 1


def handle_orderbook(msg):
    data = msg.get("data")
    if not isinstance(data, dict):
        return
    symbol = data.get("s")
    if symbol not in states:
        return
    st = states[symbol]
    apply_book(st, data, msg.get("type") == "snapshot")
    st.last_depth = time.time()
    st.depth_count += 1


async def websocket_loop():
    global ws_connected, last_ws_message, last_ws_error, subscriptions_ok

    args = []
    for symbol in SYMBOLS:
        args.append(f"tickers.{symbol}")
        args.append(f"publicTrade.{symbol}")
        args.append(f"orderbook.{DEPTH}.{symbol}")

    while True:
        try:
            print(f"Connecting to Bybit public WebSocket: {WS}")
            async with websockets.connect(
                WS,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_size=16_000_000,
            ) as ws:
                ws_connected = True
                last_ws_error = ""

                # Subscribe in chunks to keep the request comfortably sized.
                for start in range(0, len(args), 100):
                    chunk = args[start:start + 100]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))
                    await asyncio.sleep(0.1)

                subscriptions_ok = len(args)
                print(f"Subscribed to {len(args)} Bybit topics for {len(SYMBOLS)} symbols")

                async def heartbeat():
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            return

                heartbeat_task = asyncio.create_task(heartbeat())

                try:
                    async for raw in ws:
                        last_ws_message = time.time()
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if msg.get("op") in ("pong", "subscribe"):
                            continue

                        # Bybit can return operation errors such as invalid topic.
                        if msg.get("success") is False or msg.get("retCode", 0) not in (0, None):
                            last_ws_error = msg.get("retMsg") or msg.get("ret_msg") or str(msg)
                            print("Bybit WS response error:", last_ws_error)
                            continue

                        topic = msg.get("topic", "")
                        data = msg.get("data")
                        if not topic or data is None:
                            continue

                        if topic.startswith("tickers."):
                            handle_ticker(data)
                        elif topic.startswith("publicTrade."):
                            handle_trade(data)
                        elif topic.startswith("orderbook."):
                            handle_orderbook(msg)

                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            last_ws_error = repr(e)
            print("Bybit WebSocket error:", repr(e))
        finally:
            ws_connected = False
            subscriptions_ok = 0

        await asyncio.sleep(5)


def signal_text(state: SymbolState, m):
    return (
        f"*{state.symbol} — {m['signal']} {m['score']}/100*\n\n"
        f"Цена: `{state.last_price:.8g}`\n"
        f"Delta {WINDOW_SEC}s: `{m['delta']:.5g}`\n"
        f"Imbalance: `{m['imbalance']*100:+.1f}%`\n"
        f"Импульс: `{m['impulse']:+.3f}%`\n"
        f"Flow: `{m['flow']*100:+.1f}%`\n"
        f"Trades: `{state.trade_count}`\n"
        f"Depth: `{state.depth_count}`"
    )


async def alerts_loop():
    while True:
        try:
            for st in list(states.values()):
                if not st.last_price or not st.trades:
                    continue

                m = metrics(st)
                old = st.last_signal
                st.last_signal = m["signal"]

                if m["signal"] == "WAIT" or m["signal"] == old:
                    continue
                if time.time() - st.last_alert < ALERT_COOLDOWN_SEC:
                    continue

                st.last_alert = time.time()
                text = "🚨 *SCALPING SIGNAL*\n\n" + signal_text(st, m)
                for chat_id in list(subscribers):
                    try:
                        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
                    except Exception as e:
                        print("Telegram alert error:", repr(e))
        except Exception as e:
            print("Alert loop error:", repr(e))

        await asyncio.sleep(2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribers.add(update.effective_chat.id)
    await update.message.reply_text(
        "🤖 Bybit Crypto Scalping Scanner\n\n"
        "/top — топ сигналов\n"
        "/coin BTCUSDT — анализ монеты\n"
        "/status — состояние подключения\n"
        "/settings — настройки\n"
        "/help — помощь"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/top — топ сигналов\n"
        "/coin BTCUSDT — конкретная монета\n"
        "/status — диагностика\n"
        "/settings — настройки"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = time.time()
    recent_trades = sum(1 for s in states.values() if now - s.last_trade < 10)
    recent_depth = sum(1 for s in states.values() if now - s.last_depth < 10)
    recent_tickers = sum(1 for s in states.values() if now - s.last_ticker < 10)
    total_trades = sum(s.trade_count for s in states.values())
    total_depth = sum(s.depth_count for s in states.values())
    total_tickers = sum(s.ticker_count for s in states.values())
    priced = sum(1 for s in states.values() if s.last_price)

    ws_age = f"{int(now - last_ws_message)}s ago" if last_ws_message else "нет сообщений"
    error_line = last_ws_error[:350] if last_ws_error else "нет"

    await update.message.reply_text(
        "🟢 *BYBIT SCANNER V3*\n\n"
        f"WebSocket: `{'CONNECTED' if ws_connected else 'RECONNECTING'}`\n"
        f"Последнее WS сообщение: `{ws_age}`\n"
        f"Последняя WS ошибка: `{error_line}`\n\n"
        f"Символов в списке: `{len(SYMBOLS)}`\n"
        f"Tickers active: `{recent_tickers}`\n"
        f"Trade streams active: `{recent_trades}`\n"
        f"Depth streams active: `{recent_depth}`\n\n"
        f"Всего tickers: `{total_tickers}`\n"
        f"Всего trades: `{total_trades}`\n"
        f"Всего depth updates: `{total_depth}`\n"
        f"Монет с ценой: `{priced}`\n\n"
        f"Подписок: `{subscriptions_ok}`\n"
        f"Порог: `{SIGNAL_THRESHOLD}/100`",
        parse_mode="Markdown",
    )


async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = []
    for st in states.values():
        if not st.last_price or not st.trades:
            continue
        m = metrics(st)
        rows.append((abs(m["score"] - 50), st, m))

    rows.sort(key=lambda x: x[0], reverse=True)

    if not rows:
        await update.message.reply_text(
            "⏳ Пока нет торговых данных.\nИспользуй /status — там видно, получает ли бот данные Bybit."
        )
        return

    text = "📊 *TOP SIGNALS*\n\n"
    for _, st, m in rows[:10]:
        text += (
            f"*{st.symbol}* — {m['signal']} `{m['score']}/100`\n"
            f"Цена `{st.last_price:.8g}` | Δ `{m['delta']:.4g}` | OB `{m['imbalance']*100:+.1f}%`\n\n"
        )
    await update.message.reply_text(text, parse_mode="Markdown")


async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /coin BTCUSDT")
        return

    symbol = context.args[0].upper()
    st = states.get(symbol)
    if not st or not st.last_price:
        await update.message.reply_text("Монета не найдена или данные ещё не поступили.")
        return

    await update.message.reply_text(signal_text(st, metrics(st)), parse_mode="Markdown")


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ *Settings V3*\n\n"
        f"Symbols: `{len(SYMBOLS)}`\n"
        f"TOP N: `{TOP_N}`\n"
        f"Threshold: `{SIGNAL_THRESHOLD}`\n"
        f"Cooldown: `{ALERT_COOLDOWN_SEC}s`\n"
        f"Window: `{WINDOW_SEC}s`\n"
        f"Orderbook depth: `{DEPTH}`\n"
        "Bybit REST: `DISABLED`\n"
        "Bybit API keys: `NOT REQUIRED`",
        parse_mode="Markdown",
    )


async def main():
    global app
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("coin", coin_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    tasks = [
        asyncio.create_task(websocket_loop()),
        asyncio.create_task(alerts_loop()),
    ]

    try:
        await asyncio.Event().wait()
    finally:
        for task in tasks:
            task.cancel()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
