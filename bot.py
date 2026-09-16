import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field

import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
SIGNAL_THRESHOLD = int(os.getenv("SIGNAL_THRESHOLD", "75"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "180"))
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "60"))
DEPTH = int(os.getenv("DEPTH", "50"))
WS = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")

DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,BNBUSDT,ADAUSDT,AVAXUSDT,"
    "LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,SUIUSDT,APTUSDT,NEARUSDT,ARBUSDT,"
    "OPUSDT,PEPEUSDT,WIFUSDT,UNIUSDT,AAVEUSDT,INJUSDT,FILUSDT,ETCUSDT,"
    "TRXUSDT,ATOMUSDT,SEIUSDT,TAOUSDT,TONUSDT,ENAUSDT"
)
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", DEFAULT_SYMBOLS).split(",") if s.strip()]

@dataclass
class SymbolState:
    symbol: str
    trades: deque = field(default_factory=deque)
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    last_price: float = 0.0
    trade_count: int = 0
    depth_count: int = 0
    ticker_count: int = 0
    last_trade: float = 0.0
    last_depth: float = 0.0
    last_ticker: float = 0.0
    last_signal: str = "WAIT"
    last_alert: float = 0.0

states = {s: SymbolState(s) for s in SYMBOLS}
app = None
subscribers = set()
ws_connected = False
last_ws_message = 0.0
last_ws_error = ""
last_topic = ""
last_subscribe_reply = ""
subscription_ok = 0
subscription_errors = 0
ws_data_messages = 0


def trim(st):
    cutoff = time.time() - WINDOW_SEC
    while st.trades and st.trades[0][0] < cutoff:
        st.trades.popleft()


def metrics(st):
    trim(st)
    buy = sum(x[1] for x in st.trades if x[1] > 0)
    sell = sum(-x[1] for x in st.trades if x[1] < 0)
    total = buy + sell
    delta = buy - sell
    bid = sum(st.bids.values())
    ask = sum(st.asks.values())
    imbalance = (bid - ask) / (bid + ask) if bid + ask else 0.0
    impulse = 0.0
    if st.trades and st.last_price:
        first = st.trades[0][2]
        if first:
            impulse = (st.last_price - first) / first * 100
    flow = delta / total if total else 0.0
    score = 50
    if flow > .10: score += 18
    elif flow < -.10: score -= 18
    if imbalance > .10: score += 16
    elif imbalance < -.10: score -= 16
    if impulse > .08: score += 12
    elif impulse < -.08: score -= 12
    if total: score += 4
    score = max(0, min(100, int(score)))
    signal = "LONG" if score >= SIGNAL_THRESHOLD else "SHORT" if score <= 100-SIGNAL_THRESHOLD else "WAIT"
    return {"score":score,"signal":signal,"buy":buy,"sell":sell,"delta":delta,"imbalance":imbalance,"impulse":impulse,"flow":flow,"total":total}


def handle_ticker(data):
    global last_topic
    item = data if isinstance(data, dict) else None
    if not item: return
    s = item.get("symbol")
    if s not in states: return
    st = states[s]
    try:
        if item.get("lastPrice"): st.last_price = float(item["lastPrice"])
    except (TypeError, ValueError): pass
    st.ticker_count += 1
    st.last_ticker = time.time()
    last_topic = f"tickers.{s}"


def handle_trade(data):
    global last_topic
    if not isinstance(data, list): return
    now = time.time()
    for t in data:
        s = t.get("s")
        if s not in states: continue
        try:
            price = float(t["p"]); qty = float(t["v"])
        except (KeyError, TypeError, ValueError): continue
        signed = qty if t.get("S") == "Buy" else -qty
        st = states[s]
        st.trades.append((now, signed, price))
        st.last_price = price
        st.last_trade = now
        st.trade_count += 1
        last_topic = f"publicTrade.{s}"


def handle_orderbook(msg):
    global last_topic
    data = msg.get("data")
    if not isinstance(data, dict): return
    s = data.get("s")
    if s not in states: return
    st = states[s]
    if msg.get("type") == "snapshot":
        st.bids.clear(); st.asks.clear()
    for p,q in data.get("b", []):
        try: p=float(p); q=float(q)
        except (TypeError,ValueError): continue
        if q == 0: st.bids.pop(p,None)
        else: st.bids[p]=q
    for p,q in data.get("a", []):
        try: p=float(p); q=float(q)
        except (TypeError,ValueError): continue
        if q == 0: st.asks.pop(p,None)
        else: st.asks[p]=q
    st.bids = dict(sorted(st.bids.items(), reverse=True)[:DEPTH])
    st.asks = dict(sorted(st.asks.items())[:DEPTH])
    st.depth_count += 1
    st.last_depth = time.time()
    last_topic = f"orderbook.{DEPTH}.{s}"


async def websocket_loop():
    global ws_connected,last_ws_message,last_ws_error,last_subscribe_reply,subscription_ok,subscription_errors,ws_data_messages
    while True:
        try:
            print(f"Connecting {WS}")
            async with websockets.connect(WS, ping_interval=None, ping_timeout=None, close_timeout=5, max_size=16_000_000) as ws:
                ws_connected=True
                last_ws_error=""
                # Send small subscription batches. This makes failures visible and avoids any edge-case with a huge request.
                topics=[]
                for s in SYMBOLS:
                    topics += [f"tickers.{s}", f"publicTrade.{s}", f"orderbook.{DEPTH}.{s}"]
                subscription_ok=0; subscription_errors=0
                for start in range(0,len(topics),30):
                    chunk=topics[start:start+30]
                    req_id=f"scan-{int(time.time()*1000)}-{start}"
                    await ws.send(json.dumps({"req_id":req_id,"op":"subscribe","args":chunk}))
                    await asyncio.sleep(.2)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(20)
                        try: await ws.send(json.dumps({"op":"ping"}))
                        except Exception: return
                hb=asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        last_ws_message=time.time()
                        try: msg=json.loads(raw)
                        except json.JSONDecodeError: continue
                        op=msg.get("op")
                        if op=="subscribe":
                            if msg.get("success") is True:
                                subscription_ok += len(msg.get("args") or []) or 30
                                last_subscribe_reply="OK"
                            else:
                                subscription_errors += 1
                                last_subscribe_reply=str(msg.get("ret_msg") or msg.get("retMsg") or msg)
                                last_ws_error=last_subscribe_reply
                            continue
                        if op=="ping": continue
                        if op=="pong": continue
                        if msg.get("success") is False or (msg.get("retCode") not in (None,0)):
                            subscription_errors += 1
                            last_ws_error=str(msg.get("ret_msg") or msg.get("retMsg") or msg)
                            continue
                        topic=msg.get("topic","")
                        data=msg.get("data")
                        if not topic or data is None: continue
                        ws_data_messages += 1
                        if topic.startswith("tickers."): handle_ticker(data)
                        elif topic.startswith("publicTrade."): handle_trade(data)
                        elif topic.startswith("orderbook."): handle_orderbook(msg)
                finally:
                    hb.cancel()
                    try: await hb
                    except asyncio.CancelledError: pass
        except Exception as e:
            last_ws_error=repr(e)
            print("Bybit WebSocket error:",repr(e))
        finally:
            ws_connected=False
        await asyncio.sleep(5)


def signal_text(st,m):
    return (f"*{st.symbol} — {m['signal']} {m['score']}/100*\n\n"
            f"Цена: `{st.last_price:.8g}`\nDelta {WINDOW_SEC}s: `{m['delta']:.5g}`\n"
            f"Imbalance: `{m['imbalance']*100:+.1f}%`\nИмпульс: `{m['impulse']:+.3f}%`\n"
            f"Flow: `{m['flow']*100:+.1f}%`\nTrades: `{st.trade_count}`\nDepth: `{st.depth_count}`")

async def alerts_loop():
    while True:
        try:
            for st in states.values():
                if not st.last_price or not st.trades: continue
                m=metrics(st); old=st.last_signal; st.last_signal=m["signal"]
                if m["signal"]=="WAIT" or m["signal"]==old: continue
                if time.time()-st.last_alert<ALERT_COOLDOWN_SEC: continue
                st.last_alert=time.time()
                for cid in list(subscribers):
                    try: await app.bot.send_message(cid,"🚨 *SCALPING SIGNAL*\n\n"+signal_text(st,m),parse_mode="Markdown")
                    except Exception as e: print("Telegram alert error",repr(e))
        except Exception as e: print("Alert loop error",repr(e))
        await asyncio.sleep(2)

async def start(update:Update,context):
    subscribers.add(update.effective_chat.id)
    await update.message.reply_text("🤖 Bybit Scanner V4\n\n/top — топ\n/coin BTCUSDT — монета\n/status — диагностика\n/settings — настройки")
async def help_cmd(update,context): await update.message.reply_text("/top\n/coin BTCUSDT\n/status\n/settings")
async def status_cmd(update,context):
    now=time.time(); rt=sum(1 for s in states.values() if now-s.last_trade<10); rd=sum(1 for s in states.values() if now-s.last_depth<10); rk=sum(1 for s in states.values() if now-s.last_ticker<10)
    trades=sum(s.trade_count for s in states.values()); depth=sum(s.depth_count for s in states.values()); tick=sum(s.ticker_count for s in states.values()); priced=sum(1 for s in states.values() if s.last_price)
    age=f"{int(now-last_ws_message)}s ago" if last_ws_message else "нет сообщений"
    err=last_ws_error[:300] if last_ws_error else "нет"
    await update.message.reply_text("🟢 *BYBIT SCANNER V4*\n\n"+f"WebSocket: `{'CONNECTED' if ws_connected else 'RECONNECTING'}`\nПоследнее WS: `{age}`\nПоследняя ошибка: `{err}`\n\n"+f"Символов: `{len(SYMBOLS)}`\nTickers active: `{rk}`\nTrade active: `{rt}`\nDepth active: `{rd}`\n\nВсего tickers: `{tick}`\nВсего trades: `{trades}`\nВсего depth: `{depth}`\nМонет с ценой: `{priced}`\n\nWS data messages: `{ws_data_messages}`\nПодписок OK: `{subscription_ok}`\nОшибок подписки: `{subscription_errors}`\nПоследний topic: `{last_topic}`\nПоследний subscribe: `{last_subscribe_reply}`\nПорог: `{SIGNAL_THRESHOLD}/100`",parse_mode="Markdown")
async def top_cmd(update,context):
    rows=[]
    for st in states.values():
        if not st.last_price or not st.trades: continue
        m=metrics(st); rows.append((abs(m['score']-50),st,m))
    rows.sort(key=lambda x:x[0],reverse=True)
    if not rows: return await update.message.reply_text("⏳ Пока нет торговых данных. Проверь /status")
    text="📊 *TOP SIGNALS*\n\n"+"".join(f"*{st.symbol}* — {m['signal']} `{m['score']}/100`\nЦена `{st.last_price:.8g}` | Δ `{m['delta']:.4g}` | OB `{m['imbalance']*100:+.1f}%`\n\n" for _,st,m in rows[:10])
    await update.message.reply_text(text,parse_mode="Markdown")
async def coin_cmd(update,context):
    if not context.args: return await update.message.reply_text("Пример: /coin BTCUSDT")
    st=states.get(context.args[0].upper())
    if not st or not st.last_price: return await update.message.reply_text("Монета не найдена или данные ещё не поступили.")
    await update.message.reply_text(signal_text(st,metrics(st)),parse_mode="Markdown")
async def settings_cmd(update,context): await update.message.reply_text(f"⚙️ V4\nSymbols: {len(SYMBOLS)}\nDepth: {DEPTH}\nWindow: {WINDOW_SEC}s\nThreshold: {SIGNAL_THRESHOLD}\nREST: DISABLED\nAPI keys: NOT REQUIRED")

async def main():
    global app
    if not TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    app=Application.builder().token(TOKEN).build()
    for cmd,fn in [("start",start),("help",help_cmd),("top",top_cmd),("coin",coin_cmd),("status",status_cmd),("settings",settings_cmd)]: app.add_handler(CommandHandler(cmd,fn))
    await app.initialize(); await app.start(); await app.updater.start_polling(drop_pending_updates=True)
    tasks=[asyncio.create_task(websocket_loop()),asyncio.create_task(alerts_loop())]
    try: await asyncio.Event().wait()
    finally:
        for t in tasks: t.cancel()
        await app.updater.stop(); await app.stop(); await app.shutdown()

if __name__=="__main__": asyncio.run(main())
