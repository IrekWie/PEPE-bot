import os
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA BOTA MULTI-ASSET (1H - PEPE, ETH, SOL)
# ==============================================================================
BYBIT_API_KEY = "reD4jltbDxVY9UI2Wb"
BYBIT_API_SECRET = "ImYwIW5B59XBf59JePHtn8agsXSaoVm7g6Uu"
TESTNET = False

CATEGORY = "linear"  
INTERVAL = "60"       # Świeca 1H
LEVERAGE = 3         # Dźwignia 3x

ASSETS_CONFIG = {
    "1000PEPEUSDT": {"risk_pct": 0.50, "scale": 1000, "qty_decimals": 0},
    "ETHUSDT":      {"risk_pct": 0.25, "scale": 1,    "qty_decimals": 3},
    "SOLUSDT":      {"risk_pct": 0.25, "scale": 1,    "qty_decimals": 1}
}

LIMIT_CANDLES = 200  
SL_PERCENT = 0.02    # Stop Loss = 2%
TP_PERCENT = 0.04    # Take Profit = 4%

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
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        coins = res.get("result", {}).get("list", [{}])[0].get("coin", [])
        for coin in coins:
            coin_name = coin.get("coin")
            if coin_name in ["USDC", "USDT"]:
                wallet_equity = float(coin.get("equity", 0.0))
                if wallet_equity > 0:
                    return wallet_equity
    except Exception as e:
        logging.error(f"Błąd pobierania salda z Bybit: {e}")
    
    return 50.0

def get_market_data(symbol):
    response = session.get_kline(
        category=CATEGORY,
        symbol=symbol,
        interval=INTERVAL,
        limit=LIMIT_CANDLES
    )
    candles = response.get("result", {}).get("list", [])
    if not candles:
        raise Exception(f"Brak danych kline dla {symbol} z Bybit.")

    df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
    df = df.iloc[::-1].reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
    return df

def execute_trade(symbol, side, close_price):
    config = ASSETS_CONFIG[symbol]
    available_balance = get_available_balance()
    position_margin = available_balance * config["risk_pct"]
    
    raw_qty = (position_margin * LEVERAGE) / (close_price * config["scale"])
    
    decimals = config["qty_decimals"]
    if decimals == 0:
        qty_str = str(int(raw_qty))
        qty_num = float(qty_str)
    else:
        qty_num = round(raw_qty, decimals)
        qty_str = f"{qty_num:.{decimals}f}"

    if qty_num <= 0:
        logging.warning(f"[{symbol}] Wyliczona ilość ({qty_str}) jest za mała na otwarcie zlecenia.")
        return

    if side == "Buy":
        sl_price = round(close_price * (1 - SL_PERCENT), 8)
        tp_price = round(close_price * (1 + TP_PERCENT), 8)
    else:
        sl_price = round(close_price * (1 + SL_PERCENT), 8)
        tp_price = round(close_price * (1 - TP_PERCENT), 8)

    try:
        session.place_order(
            category=CATEGORY,
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty_str,
            stopLoss=str(sl_price),
            takeProfit=str(tp_price),
            tpslMode="Full",
            slOrderType="Market",
            tpOrderType="Market"
        )
        logging.info(f"SUKCES: Złożono zlecenie {side} dla {symbol} | Ilość: {qty_str} | Depozyt: {position_margin:.2f} USD")
    except Exception as e:
        logging.error(f"[{symbol}] Błąd zlecenia: {e}")

def run_bot_for_symbol(symbol):
    logging.info(f"=== ANALIZA ŚWIECY ({symbol} {INTERVAL}m) ===")

    df = get_market_data(symbol)
    candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    c_open, c_close = candle['open'], candle['close']
    c_high, c_low = candle['high'], candle['low']
    ema21, ema89 = candle['ema21'], candle['ema89']

    bullish_trend = ema21 > ema89
    bearish_trend = ema21 < ema89

    in_value_zone_long = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
    in_value_zone_short = (c_high >= min(ema21, ema89)) and (c_high <= max(ema21, ema89))

    bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
    bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])
    bullish_candle = bullish_pinbar or bullish_engulfing

    bearish_pinbar = (c_close < c_open) and ((c_high - c_open) > (c_open - c_close) * 1.5)
    bearish_engulfing = (c_close < c_open) and (prev_candle['close'] > prev_candle['open']) and (c_close < prev_candle['open'])
    bearish_candle = bearish_pinbar or bearish_engulfing

    if bullish_trend and in_value_zone_long and bullish_candle:
        execute_trade(symbol, "Buy", c_close)
    elif bearish_trend and in_value_zone_short and bearish_candle:
        execute_trade(symbol, "Sell", c_close)
    else:
        logging.info(f"[{symbol}] Brak sygnału. Czekam na kolejną świecę.")

def bot_loop():
    sleep_time = int(INTERVAL) * 60
    while True:
        try:
            for symbol in ASSETS_CONFIG.keys():
                run_bot_for_symbol(symbol)
                time.sleep(2)
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(sleep_time)

def self_ping_loop():
    while True:
        time.sleep(300)  # Co 5 minut
        try:
            requests.get(APP_URL, timeout=10)
            logging.info("PING: Serwer aktywny (Render Keep-Alive OK)")
        except Exception as e:
            logging.error(f"PING BŁĄD: {e}")

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Multi-Asset Trading Bot is Running 24/7!")

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
