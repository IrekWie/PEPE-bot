import os
import time
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA BOTA HANDLOWEGO (INTERWAŁ 1-GODZINNY: 1H)
# ==============================================================================

# 1. Klucze Bybit API (Wklej swoje prawdziwe klucze między cudzysłowami)
BYBIT_API_KEY = "qFUYjYvjxptqpI5CBU"
BYBIT_API_SECRET = "6ylxNJoW3qaB71BneLorAXfACLGWRixtcKmG"
TESTNET = False  # Ustaw True tylko dla środowiska testowego (testnet.bybit.com)

# 2. Parametry handlowe dla PEPEUSDT
SYMBOL = "PEPEUSDT"
CATEGORY = "linear"  # Kontrakty USDT Perpetual (UTA)
INTERVAL = "60"      # Interwał 1H: na Bybit V5 "60" oznacza 60 minut
LEVERAGE = 3         # Dźwignia 3x
POSITION_SIZE_USDT = 50.0  # Kwota w USDT przeznaczona na pojedynczą pozycję

# 3. Parametry strategii EMA dla interwału 1H
LIMIT_CANDLES = 200  # Liczba świec do obliczenia EMA 89
SL_PERCENT = 0.02    # Stop Loss = 2%
TP_PERCENT = 0.04    # Take Profit = 4% (R:R = 1:2)

# ==============================================================================
# DZIENNIK ZDARZEŃ (LOGI)
# ==============================================================================
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
    """Ustawia poziom dźwigni dla pary PEPEUSDT na koncie Bybit."""
    try:
        session.set_leverage(
            category=CATEGORY,
            symbol=SYMBOL,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE)
        )
        logging.info(f"Dźwignia dla {SYMBOL} potwierdzona na {LEVERAGE}x.")
    except Exception as e:
        if "110043" in str(e):
            pass  # Dźwignia jest już ustawiona
        else:
            logging.warning(f"Dźwignia: {e}")

def has_active_position():
    """Sprawdza, czy na koncie Bybit znajduje się już otwarta pozycja na PEPEUSDT."""
    try:
        response = session.get_positions(category=CATEGORY, symbol=SYMBOL)
        positions = response.get("result", {}).get("list", [])
        for pos in positions:
            if float(pos.get("size", 0)) > 0:
                logging.info(f"Aktywna pozycja na {SYMBOL} (Wielkość: {pos['size']}). Czekam na zamknięcie.")
                return True
        return False
    except Exception as e:
        logging.error(f"Błąd sprawdzania pozycji: {e}")
        return True

def get_market_data():
    """Pobiera historyczne świece 1H z Bybit i oblicza EMA 21 oraz EMA 89."""
    response = session.get_kline(
        category=CATEGORY,
        symbol=SYMBOL,
        interval=INTERVAL,
        limit=LIMIT_CANDLES
    )
    candles = response.get("result", {}).get("list", [])
    if not candles:
        raise Exception("Nie udało się pobrać danych rynkowych 1H z Bybit.")

    df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
    df = df.iloc[::-1].reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
    return df

def execute_trade(side, close_price):
    """Kalkuluje wielkość pozycji, SL, TP i składa zlecenie na Bybit."""
    raw_qty = (POSITION_SIZE_USDT * LEVERAGE) / close_price
    qty = int(raw_qty // 100) * 100  # Zaokrąglenie do setek PEPE

    if qty <= 0:
        logging.error("Wyliczona wielkość pozycji wynosi 0.")
        return

    if side == "Buy":
        sl_price = round(close_price * (1 - SL_PERCENT), 8)
        tp_price = round(close_price * (1 + TP_PERCENT), 8)
    else:
        sl_price = round(close_price * (1 + SL_PERCENT), 8)
        tp_price = round(close_price * (1 - TP_PERCENT), 8)

    logging.info(f"Otwieranie pozycji {side} (1H) | Wielkość: {qty} PEPE | Cena: {close_price} | SL: {sl_price} | TP: {tp_price}")

    try:
        order = session.place_order(
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
        logging.info(f"SUCCESS: Zlecenie złożone pomyślnie! ID: {order['result']['orderId']}")
    except Exception as e:
        logging.error(f"Błąd składania zlecenia: {e}")

def run_bot():
    """Główny cykl analityczny świecy 1H."""
    logging.info("=== ANALIZA ŚWIECY GODZINOWEJ (PEPE 1H) ===")
    set_leverage()

    if has_active_position():
        return

    df = get_market_data()
    candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    c_open, c_close = candle['open'], candle['close']
    c_high, c_low = candle['high'], candle['low']
    ema21, ema89 = candle['ema21'], candle['ema89']

    logging.info(f"Świeca 1H -> Close: {c_close:.8f} | EMA21: {ema21:.8f} | EMA89: {ema89:.8f}")

    # 1. Kierunek trendu
    bullish_trend = ema21 > ema89
    bearish_trend = ema21 < ema89

    # 2. Test strefy wartości
    in_value_zone_long = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
    in_value_zone_short = (c_high >= min(ema21, ema89)) and (c_high <= max(ema21, ema89))

    # 3. Formacje reakcyjne świecy 1H
    bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
    bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])
    bullish_candle = bullish_pinbar or bullish_engulfing

    bearish_pinbar = (c_close < c_open) and ((c_high - c_open) > (c_open - c_close) * 1.5)
    bearish_engulfing = (c_close < c_open) and (prev_candle['close'] > prev_candle['open']) and (c_close < prev_candle['open'])
    bearish_candle = bearish_pinbar or bearish_engulfing

    # 4. Egzekucja
    if bullish_trend and in_value_zone_long and bullish_candle:
        logging.info("Sygnał KUPNA (LONG na 1H)! Składanie zlecenia...")
        execute_trade("Buy", c_close)
    elif bearish_trend and in_value_zone_short and bearish_candle:
        logging.info("Sygnał SPRZEDAŻY (SHORT na 1H)! Składanie zlecenia...")
        execute_trade("Sell", c_close)
    else:
        logging.info("Brak sygnału. Oczekiwanie na kolejną świecę 1H.")

# ==============================================================================
# PĘTLA 24/7 ORAZ SERWER HEALTH-CHECK DLA RENDER.COM
# ==============================================================================
def bot_loop():
    """Pętla wykonująca analizę natychmiast po starcie, a potem równo co 60 minut."""
    while True:
        try:
            run_bot()
        except Exception as e:
            logging.error(f"Błąd podczas analizy: {e}")
        
        logging.info("Następna analiza rynku za 60 minut...")
        time.sleep(3600)  # Czeka 3600 sekund (1 godzinę)

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Pepe Trading Bot is Running 24/7!")

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    # Uruchomienie bota w osobnym wątku roboczym
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()

    # Serwer HTTP trzyma proces przy życiu na Render.com
    run_health_server()
