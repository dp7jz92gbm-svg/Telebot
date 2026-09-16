import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field

import aiohttp
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

REST = "https://api.bybit.eu"
WS = "wss://stream.bybit.com/v5/public/linear"

session = None
app = None
subscribers = set()
states = {}
ws_connected = False
last_ws_message = 0.0
last_rest_ok = 0.0


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
    last_trade: float = 0.0
    last_depth: float = 0.0
    last_signal: str = "WAIT"
    last_alert: float = 0.0


def trim(state):
    cutoff = time.time() - WINDOW_SEC
    while state.trades and state.trades[0][0] < cutoff:
        state.trades.popleft()


def metrics(state):
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


async def bybit_get(path, params):
    global last_rest_ok
    async with session.get(REST + path, params=params, timeout=15) as r:
        r.raise_for_status()
        data = await r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", "Bybit API error"))
        last_rest_ok = time.time()
        return data["result"]


async def discover_symbols():
    result = await bybit_get(
        "/v5/market/tickers",
        {"category": "linear"},
    )

    rows = []
    for x in result.get("list", []):
        symbol = x.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        if x.get("status") not in (None, "Trading"):
            continue
        try:
            turnover = float(x.get("turnover24h", 0))
        except Exception:
            turnover = 0
        rows.append((turnover, x))

    rows.sort(key=lambda x: x[0], reverse=True)
    selected = [x[1] for x in rows[:TOP_N]]

    for x in selected:
        s = x["symbol"]
        state = states.setdefault(s, SymbolState(s))
        state.volume24h = float(x.get("volume24h", 0))
        state.turnover24h = float(x.get("turnover24h", 0))
        if x.get("lastPrice"):
            state.last_price = float(x["lastPrice"])

    return [x["symbol"] for x in selected]


def apply_book(state, data, snapshot=False):
    if snapshot:
        state.bids.clear()
        state.asks.clear()

    for price, qty in data.get("b", []):
        p, q = float(price), float(qty)
        if q == 0:
            state.bids.pop(p, None)
        else:
            state.bids[p] = q

    for price, qty in data.get("a", []):
        p, q = float(price), float(qty)
        if q == 0:
            state.asks.pop(p, None)
        else:
            state.asks[p] = q

    # Keep memory bounded to the requested depth.
    if len(state.bids) > DEPTH * 2:
        state.bids = dict(sorted(state.bids.items(), reverse=True)[:DEPTH])
    if len(state.asks) > DEPTH * 2:
        state.asks = dict(sorted(state.asks.items())[:DEPTH])


async def websocket_loop():
    global ws_connected, last_ws_message

    while True:
        try:
            symbols = await discover_symbols()
            if not symbols:
                raise RuntimeError("Bybit returned no USDT perpetuals")

            args = []
            for s in symbols:
                args.append(f"publicTrade.{s}")
                args.append(f"orderbook.{DEPTH}.{s}")

            async with websockets.connect(
                WS,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_size=16_000_000,
            ) as ws:
                ws_connected = True

                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": args,
                }))

                async def heartbeat():
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send(json.dumps({
                                "op": "ping"
                            }))
                        except Exception:
                            return

                heartbeat_task = asyncio.create_task(heartbeat())

                try:
                    async for raw in ws:
                        last_ws_message = time.time()

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("op") in ("pong", "subscribe"):
                            continue

                        topic = msg.get("topic", "")
                        data = msg.get("data")

                        if not topic or data is None:
                            continue

                        if topic.startswith("publicTrade."):
                            if not isinstance(data, list):
                                continue

                            for trade in data:
                                symbol = trade.get("s")
                                if symbol not in states:
                                    continue

                                try:
                                    price = float(trade["p"])
                                    qty = float(trade["v"])
                                except Exception:
                                    continue

                                side = trade.get("S")
                                signed = qty if side == "Buy" else -qty

                                st = states[symbol]
                                now = time.time()
                                st.trades.append((now, qty, signed, price))
                                st.last_price = price
                                st.last_trade = now
                                st.trade_count += 1

                        elif topic.startswith("orderbook."):
                            symbol = data.get("s")
                            if symbol not in states:
                                continue

                            st = states[symbol]
                            apply_book(st, data, msg.get("type") == "snapshot")
                            st.last_depth = time.time()
                            st.depth_count += 1

                finally:
                    heartbeat_task.cancel()

        except Exception as e:
            print("Bybit WebSocket error:", repr(e))
        finally:
            ws_connected = False

        await asyncio.sleep(5)


def signal_text(state, m):
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
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="Markdown",
                        )
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

    recent_trades = sum(
        1 for s in states.values()
        if now - s.last_trade < 10
    )
    recent_depth = sum(
        1 for s in states.values()
        if now - s.last_depth < 10
    )

    total_trades = sum(s.trade_count for s in states.values())
    total_depth = sum(s.depth_count for s in states.values())
    priced = sum(1 for s in states.values() if s.last_price)

    ws_age = (
        f"{int(now - last_ws_message)}s ago"
        if last_ws_message else "нет сообщений"
    )

    await update.message.reply_text(
        "🟢 *BYBIT SCANNER*\n\n"
        f"WebSocket: `{'CONNECTED' if ws_connected else 'RECONNECTING'}`\n"
        f"Последнее WS сообщение: `{ws_age}`\n\n"
        f"Монет: `{len(states)}`\n"
        f"Trade streams active: `{recent_trades}`\n"
        f"Depth streams active: `{recent_depth}`\n\n"
        f"Всего trades: `{total_trades}`\n"
        f"Всего depth updates: `{total_depth}`\n"
        f"Монет с ценой: `{priced}`\n\n"
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
            "⏳ Пока нет торговых данных.\n"
            "Используй /status — там видно, получает ли бот trades и depth."
        )
        return

    text = "📊 *TOP SIGNALS*\n\n"

    for _, st, m in rows[:10]:
        text += (
            f"*{st.symbol}* — {m['signal']} `{m['score']}/100`\n"
            f"Цена `{st.last_price:.8g}` | "
            f"Δ `{m['delta']:.4g}` | "
            f"OB `{m['imbalance']*100:+.1f}%`\n\n"
        )

    await update.message.reply_text(text, parse_mode="Markdown")


async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /coin BTCUSDT")
        return

    symbol = context.args[0].upper()
    st = states.get(symbol)

    if not st or not st.last_price:
        await update.message.reply_text(
            "Монета не найдена или данные ещё не поступили."
        )
        return

    await update.message.reply_text(
        signal_text(st, metrics(st)),
        parse_mode="Markdown",
    )


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ *Settings*\n\n"
        f"TOP N: `{TOP_N}`\n"
        f"Threshold: `{SIGNAL_THRESHOLD}`\n"
        f"Cooldown: `{ALERT_COOLDOWN_SEC}s`\n"
        f"Window: `{WINDOW_SEC}s`\n"
        f"Orderbook depth: `{DEPTH}`",
        parse_mode="Markdown",
    )


async def main():
    global session, app

    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

    session = aiohttp.ClientSession()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("coin", coin_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

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

        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
