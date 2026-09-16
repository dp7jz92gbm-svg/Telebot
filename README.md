# Bybit Crypto Scalping Telegram Bot V7

Analysis-only Telegram scanner for Bybit USDT/USDC linear public market data. It does **not** place trades and does not need Bybit API keys.

## What V7 fixes
- Removed the unnecessary `tickers.*` subscription that produced the observed `handler not found, topic:tickers.PEPEUSDT` error.
- Price is taken directly from `publicTrade.*`.
- Uses `publicTrade.{symbol}` and `orderbook.{depth}.{symbol}` only.
- Subscription acknowledgements are tracked per exact topic, including `successTopics` / `failTopics` when Bybit returns them.
- Unknown topics are ignored safely instead of becoming fatal errors.
- Automatic reconnect with backoff.
- 20-second Bybit heartbeat.
- Watchdog reconnects if the connection stops delivering data for 60 seconds.
- Order book snapshot/delta handling.
- `/top` shows only signals at or above the configured threshold, with Entry, SL, TP1/TP2/TP3 and calculated R:R.
- `/status` shows WebSocket health, active streams, orderbook readiness, exact subscription counts and errors.

## Railway
1. Replace `bot.py` and `requirements.txt` in the GitHub repo.
2. Keep `TELEGRAM_BOT_TOKEN` in Railway Variables.
3. Deploy.
4. Make sure this Telegram bot is running in only one process/service. Telegram will return `Conflict: terminated by other getUpdates request` if two instances poll the same bot token.
5. In Telegram run `/status`, then `/top`.

## Environment
Only `TELEGRAM_BOT_TOKEN` is required. Bybit API key/secret are not required.

Optional: `SIGNAL_THRESHOLD`, `ALERT_COOLDOWN_SEC`, `WINDOW_SEC`, `DEPTH`, `TOP_N`, `SUB_BATCH_SYMBOLS`, `BYBIT_WS_URL`, `SYMBOLS`.

## Commands
- `/start`
- `/top`
- `/coin BTCUSDT`
- `/status`
- `/settings`

## Important
Entry/SL/TP are heuristic analysis levels based on recent trade flow, volatility and order-book data. They are not guaranteed execution levels or profit forecasts.
