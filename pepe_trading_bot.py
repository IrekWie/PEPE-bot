import os
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA HYBRYDOWEGO BOTA MULTI-STRATEGY (BYBIT V5 UTA)
# ==============================================================================
BYBIT_API_KEY = "reD4jltbDxVY9UI2Wb"
BYBIT_API_SECRET = "ImYwIW5B59XBf59JePHtn8agsXSaoVm7g6Uu"
TESTNET = False

CATEGORY = "linear"
LEVERAGE = 3

# --- 1. USTAWIENIA STRATEGII EMA TREND (Świece 1H) ---
ASSETS_EMA = {
    "1000PEPEUSDT": {"risk_pct": 0.25, "scale": 1000, "qty_decimals": 0}, # 25% salda
    "ETHUSDT":      {"risk_pct": 0.12, "scale": 1,    "qty_decimals": 3}, # 12% salda
    "SOLUSDT":      {"risk_pct": 0.13, "scale": 1,    "qty_decimals": 1}  # 13% salda
}
SL_PCT_EMA = 0.02  # Stop Loss 2%
TP_PCT_EMA = 0.04  # Take Profit 4%

# --- 2. USTAWIENIA STRATEGII TOP DIP HUNTER (Świece 15M + Trailing Stop) ---
RISK_PCT_DIP = 0.10            # 10% salda na pozycję
TOP_DIPS_COUNT = 4             # 4 monety z największym spadkiem 24h
SL_PCT_DIP = 0.025             # Początkowy Stop Loss 2.5%
TRAILING_ACT_PCT_DIP = 0.030   # Aktywacja Trailing Stopa od +3.0% zysku
TRAILING_DIST_PCT_DIP = 0.015  # Dystans śledzenia (Callback) = 1.5%

APP_URL = "https://pepe-trading-bot-ujqx.onrender.com"

LOG_FILE = "trade_history.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

session = HTTP(
    testnet=TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET
)

def get_available_balance():
    """Pobiera dostępne wolne saldo w USDT / USDC z konta UTA."""
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        coins = res.get("result", {}).get("list", [{}])[0].get("coin", [])
        for coin in coins:
            if coin.get("coin") in ["USDT", "USDC"]:
                equity = float(coin.get("equity", 0.0))
                if equity > 0:
                    return equity
    except Exception as e:
        logging.error(f"Błąd pobierania salda z Bybit: {e}")
    return 100.0

def get_market_data(symbol, interval, limit=100):
    """Pobiera świece i przelicza wskaźniki techniczne."""
    response = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
    candles = response.get("result", {}).get("list", [])
    if not candles:
        return None

    df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
    df = df.iloc[::-1].reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
    return df

# ==============================================================================
# STRATEGIA 1: EMA TREND (PEPE / ETH / SOL - 1H)
# ==============================================================================
def run_ema_bot():
    logging.info("=== [STRATEGIA 1] ANALIZA EMA TREND (PEPE / ETH / SOL 1H) ===")
    for symbol, config in ASSETS_EMA.items():
        try:
            df = get_market_data(symbol, interval="60")
            if df is None or len(df) < 5: continue
            
            candle, prev_candle = df.iloc[-2], df.iloc[-3]
            c_open, c_close = candle['open'], candle['close']
            c_high, c_low = candle['high'], candle['low']
            ema21, ema89 = candle['ema21'], candle['ema89']

            bullish_trend = ema21 > ema89
            in_value_zone = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
            
            bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
            bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])

            if bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing):
                available_balance = get_available_balance()
                margin = available_balance * config["risk_pct"]
                raw_qty = (margin * LEVERAGE) / (c_close * config["scale"])
                
                decimals = config["qty_decimals"]
                qty_str = str(int(raw_qty)) if decimals == 0 else f"{round(raw_qty, decimals):.{decimals}f}"

                if float(qty_str) <= 0:
                    continue

                sl_price = round(c_close * (1 - SL_PCT_EMA), 6)
                tp_price = round(c_close * (1 + TP_PCT_EMA), 6)

                session.place_order(
                    category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str,
                    stopLoss=str(sl_price), takeProfit=str(tp_price),
                    tpslMode="Full", slOrderType="Market", tpOrderType="Market"
                )
                logging.info(f"🔥 [EMA] ZŁOŻONO ZLECENIE DLA {symbol} | Ilość: {qty_str} | Margin: {margin:.2f} USD")
            else:
                logging.info(f"[EMA - {symbol}] Brak sygnału.")
        except Exception as e:
            logging.error(f"[EMA - {symbol}] Błąd: {e}")

# ==============================================================================
# STRATEGIA 2: TOP DIP HUNTER + TRAILING STOP (15M)
# ==============================================================================
def run_dip_bot():
    logging.info("=== [STRATEGIA 2] SKANOWANIE 4 NAJWIĘKSZYCH SPADKÓW 24H (15M) ===")
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("price24hPcnt", 0.0)))
        top_losers = [t["symbol"] for t in sorted_tickers[:TOP_DIPS_COUNT]]

        for symbol in top_losers:
            df = get_market_data(symbol, interval="15")
            if df is None or len(df) < 5: continue
            
            candle, prev_candle = df.iloc[-2], df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            
            body = abs(c_close - c_open)
            lower_wick = min(c_open, c_close) - c_low
            is_pinbar = lower_wick > (body * 2.0) and (c_close > c_open)
            is_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])
            is_above_ema = (c_close > candle['ema9']) and (prev_candle['close'] <= prev_candle['ema9'])

            if is_pinbar or is_engulfing or is_above_ema:
                balance = get_available_balance()
                margin = balance * RISK_PCT_DIP
                scale = 1000 if symbol.startswith("1000") else 1
                raw_qty = (margin * LEVERAGE) / (c_close * scale)
                
                if scale == 1000 or "SHIB" in symbol or "PEPE" in symbol:
                    qty_str = str(int(raw_qty))
                else:
                    qty_str = f"{round(raw_qty, 2):.2f}"

                if float(qty_str) <= 0:
                    continue

                sl_price = round(c_close * (1 - SL_PCT_DIP), 6)
                act_price = round(c_close * (1 + TRAILING_ACT_PCT_DIP), 6)
                dist_val = round(c_close * TRAILING_DIST_PCT_DIP, 6)

                session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str, stopLoss=str(sl_price), tpslMode="Full", slOrderType="Market")
                time.sleep(1)
                session.set_trading_stop(category=CATEGORY, symbol=symbol, trailingStop=str(dist_val), activePrice=str(act_price), positionIdx=0)
                logging.info(f"🔥 [DIP HUNTER] ZŁOŻONO ZLECENIE DLA {symbol} z Trailing Stopem | Margin: {margin:.2f} USD")
            else:
                logging.info(f"[DIP - {symbol}] Brak sygnału odbicia.")
    except Exception as e:
        logging.error(f"[DIP HUNTER] Błąd: {e}")

# ==============================================================================
# PĘTLA GŁÓWNA I SERWER HEALTH-CHECK
# ==============================================================================
def bot_loop():
    last_ema_check = 0
    while True:
        try:
            # Strategia 2 (Dip Hunter) wywołuje się co 15 minut
            run_dip_bot()
            
            # Strategia 1 (EMA Trend) wywołuje się raz na godzinę
            current_time = time.time()
            if current_time - last_ema_check >= 3600:
                run_ema_bot()
                last_ema_check = current_time

        except Exception as e:
            logging.error(f"Błąd głównej pętli bota: {e}")
            
        time.sleep(15 * 60) # Co 15 minut

def self_ping_loop():
    while True:
        time.sleep(300)
        try:
            requests.get(APP_URL, timeout=10)
            logging.info("PING: Serwer aktywny (Render Keep-Alive OK)")
        except Exception:
            pass

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Multi-Strategy Trading Bot is Running 24/7!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    t_bot = threading.Thread(target=bot_loop, daemon=True)
    t_bot.start()

    t_ping = threading.Thread(target=self_ping_loop, daemon=True)
    t_ping.start()

    run_health_server()
