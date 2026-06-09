"""
polygon_feed.py
───────────────
Real-time futures price feed for AlphaGrid bots using Polygon.io WebSocket.
Replaces yfinance with live tick data for ES, NQ, MES, MNQ.

Sign up free at: https://polygon.io
Copy your API key from: Dashboard → API Keys

Usage:
    from polygon_feed import PolygonFeed, get_price

    feed = PolygonFeed(api_key="YOUR_POLYGON_API_KEY")
    asyncio.create_task(feed.start())   # call once on startup

    price = get_price("MES")            # call anywhere in your bot
"""

import asyncio
import json
import logging
import websockets
from typing import Optional

logger = logging.getLogger(__name__)

# ─── POLYGON FUTURES WEBSOCKET ────────────────────────────────────────────────
# Futures use the "futures" cluster on Polygon
POLYGON_WS_URL = "wss://socket.polygon.io/futures"

# ─── SYMBOL MAP — Polygon.io futures ticker format ───────────────────────────
# Format: "ES:MONTH:YEAR" — update contract month each expiration
# Month codes: H=March, M=June, U=Sept, Z=Dec
# Current: June 2026 = M26
SYMBOLS = {
    "ES":  "ES:M26",
    "NQ":  "NQ:M26",
    "MES": "MES:M26",
    "MNQ": "MNQ:M26",
}

# ─── LIVE PRICE STORE ─────────────────────────────────────────────────────────
_prices: dict[str, Optional[float]] = {
    "ES":  None,
    "NQ":  None,
    "MES": None,
    "MNQ": None,
}

def get_price(symbol: str) -> Optional[float]:
    """
    Get the latest live price for a symbol.
    Returns None if feed hasn't received data yet.
    
    Example: get_price("MES") → 5342.75
    """
    return _prices.get(symbol.upper())

def get_all_prices() -> dict:
    """Return all current prices as a dict."""
    return dict(_prices)

def is_feed_live() -> bool:
    """Returns True if at least one price has been received."""
    return any(v is not None for v in _prices.values())


# ─── MAIN FEED CLASS ──────────────────────────────────────────────────────────

class PolygonFeed:
    """
    Connects to Polygon.io futures WebSocket and streams
    real-time quotes for ES, NQ, MES, MNQ into the price store.
    
    Auto-reconnects on disconnect.
    """

    def __init__(self, api_key: str):
        self.api_key  = api_key
        self._running = False

    async def _connect_and_stream(self):
        """Open WebSocket, authenticate, subscribe, and stream prices."""
        async with websockets.connect(POLYGON_WS_URL) as ws:

            # ── Step 1: Wait for connected confirmation ───────────────────
            msg = await ws.recv()
            data = json.loads(msg)
            if data[0].get("status") == "connected":
                logger.info("Polygon.io WebSocket connected")

            # ── Step 2: Authenticate ──────────────────────────────────────
            await ws.send(json.dumps({
                "action": "auth",
                "params": self.api_key
            }))

            msg = await ws.recv()
            data = json.loads(msg)
            status = data[0].get("status", "")

            if status == "auth_success":
                logger.info("Polygon.io auth successful")
            elif status == "auth_failed":
                raise RuntimeError(f"Polygon.io auth failed — check your API key: {data}")

            # ── Step 3: Subscribe to quotes for all symbols ───────────────
            # "Q." prefix = quotes (bid/ask), "T." prefix = trades (last price)
            # We subscribe to both for maximum coverage
            tickers_quotes = ",".join(f"Q.{sym}" for sym in SYMBOLS.values())
            tickers_trades = ",".join(f"T.{sym}" for sym in SYMBOLS.values())

            await ws.send(json.dumps({
                "action": "subscribe",
                "params": f"{tickers_quotes},{tickers_trades}"
            }))

            logger.info(f"Subscribed to: {list(SYMBOLS.values())}")

            # ── Step 4: Stream messages ───────────────────────────────────
            async for raw in ws:
                await self._handle_message(raw)

    async def _handle_message(self, raw: str):
        """Parse Polygon.io message and update price store."""
        try:
            events = json.loads(raw)
            for event in events:
                ev_type = event.get("ev")
                ticker  = event.get("sym", "")  # e.g. "ES:M26"

                # Match ticker back to short symbol
                short = None
                for s, full in SYMBOLS.items():
                    if ticker == full or ticker.startswith(s + ":"):
                        short = s
                        break

                if not short:
                    continue

                # "Q" = quote event (bid/ask)
                if ev_type == "Q":
                    ask = event.get("ap")  # ask price
                    bid = event.get("bp")  # bid price
                    if ask and bid:
                        _prices[short] = round((ask + bid) / 2, 2)
                        logger.debug(f"{short} quote: {_prices[short]}")

                # "T" = trade event (actual last traded price)
                elif ev_type == "T":
                    price = event.get("p")  # last trade price
                    if price:
                        _prices[short] = round(price, 2)
                        logger.debug(f"{short} trade: {_prices[short]}")

        except Exception as e:
            logger.warning(f"Error parsing Polygon message: {e} | raw: {raw[:100]}")

    # ── Public: Start the feed ────────────────────────────────────────────────

    async def start(self):
        """
        Start the live feed. Runs forever with auto-reconnect.
        
        Call once at bot startup:
            asyncio.create_task(feed.start())
        """
        self._running = True
        logger.info("Starting Polygon.io futures feed...")
        while self._running:
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.error(f"Feed error: {e} — reconnecting in 5s")
                await asyncio.sleep(5)

    def stop(self):
        self._running = False


# ─── CONTRACT MONTH HELPER ────────────────────────────────────────────────────

def update_contract_month(month_code: str, year: str = "26"):
    """
    Update contract month for all symbols.
    Call this each time contracts roll over.
    
    month_code: H=March, M=June, U=Sept, Z=Dec
    year: last 2 digits of year (e.g. "26" for 2026)
    
    Example: update_contract_month("U", "26")  → September 2026
    """
    for short in list(SYMBOLS.keys()):
        SYMBOLS[short] = f"{short}:{month_code}{year}"
    logger.info(f"Contracts rolled to {month_code}{year}: {SYMBOLS}")


# ─── QUICK TEST ───────────────────────────────────────────────────────────────
# Run this file directly to test your API key:
# python polygon_feed.py

if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO)

    API_KEY = os.getenv("POLYGON_API_KEY") or input("Enter your Polygon API key: ")

    feed = PolygonFeed(api_key=API_KEY)

    async def test():
        task = asyncio.create_task(feed.start())
        print("Waiting for prices...")
        for i in range(10):
            await asyncio.sleep(2)
            prices = get_all_prices()
            print(f"[{i*2}s] Prices: {prices}")
            if is_feed_live():
                print("✅ Live feed working!")
                break
        task.cancel()

    asyncio.run(test())
