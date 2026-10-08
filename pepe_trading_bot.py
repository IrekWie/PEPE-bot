import os
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA BOTA HANDLOWEGO (1H - 1000PEPEUSDT)
# ==============================================================================
BYBIT_API_KEY = "reD4jltbDxVY9UI2Wb"
BYBIT_API_SECRET = "ImYwIW5B59XBf59JePHtn8agsXSaoVm7g6Uu"
TESTNET = False

SYMBOL = "1000PEPEUSDT"
CATEGORY = "linear"  
INTERVAL = "60"       # 1H
LEVERAGE = 3         
POSITION_SIZE_USDT = 50.0  

LIMIT_CANDLES = 200  
SL_PERCENT = 0.02    # Stop Loss = 2%
TP_PERCENT = 0.04    # Take Profit = 4%

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

def set_leverage():
    try:
        session.set_leverage(
            category=CATEGORY,
            symbol=SYMBOL,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE)
        )
    except Exception as e:
        if "110043" not in str(e):
            logging.warning(f"Informacja o dźwigni: {e}")

def has_active_position():
    try:
        response = session.get_positions(category=CATEGORY, symbol=SYMBOL)
        positions = response.get("result", {}).get("list", [])
        for pos in positions:
            if float(pos.get("size", 0)) > 0:
                logging.info(f"Wykryto aktywną pozycję na {SYMBOL}. Pomijam.")
                return True
        return False
    except Exception as e:
        logging.error(f"Błąd sprawdzania pozycji: {e}")
        return False

def get_market_data():
    response = session.get_kline(
        category=CATEGORY,
        symbol=SYMBOL,
        interval=INTERVAL,
        limit=LIMIT_CANDLES
    )
    candles = response.get("result", {}).get("list", [])
    if not candles:
        raise Exception("Brak danych kline z Bybit.")

    df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
    df = df.iloc[::-1].reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
    return df

def execute_trade(side, close_price):
    raw_qty = (POSITION_SIZE_USDT * LEVERAGE) / (close_price * 1000)
    qty = int(raw_qty)

    if qty <= 0:
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
            symbol=SYMBOL,
            side=side,
            orderType="Market",
            qty=str(qty),
            stopLoss=str(sl_price),
            takeProfit=str(tp_price),
            tpslMode="Full",
            slOrderType="Market",
            tpOrderType="Market"
        )
        logging.info(f"SUKCES: Złożono zlecenie {side} dla {SYMBOL}!")
    except Exception as e:
        logging.error(f"Błąd zlecenia: {e}")

def run_bot():
    logging.info("=== ANALIZA ŚWIECY GODZINOWEJ (1000PEPE 1H) ===")
    set_leverage()

    if has_active_position():
        return

    df = get_market_data()
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
        execute_trade("Buy", c_close)
    elif bearish_trend and in_value_zone_short and bearish_candle:
        execute_trade("Sell", c_close)
    else:
        logging.info("Brak sygnału. Czekam na kolejną świecę 1H.")

def bot_loop():
    while True:
        try:
            run_bot()
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(3600)

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Pepe Trading Bot is Running 24/7!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()
    run_health_server()
