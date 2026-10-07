import os
import logging
from datetime import datetime
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA BOTAHANDLOWEGO
# ==============================================================================

# 1. Klucze Bybit API (Uzyskane w panelu Bybit -> Profile -> API)
BYBIT_API_KEY = "qFUYjYvjxptqpI5CBU"
BYBIT_API_SECRET = "6ylxNJoW3qaB71BneLorAXfACLGWRixtcKmG"
TESTNET = False  # Ustaw na True, jeśli testujesz na koncie demo/testnet Bybit

# 2. Parametry handlowe dla PEPEUSDT
SYMBOL = "PEPEUSDT"
CATEGORY = "linear"  # Kontrakty USDT Perpetual
INTERVAL = "D"       # Interwał 1D (Świece dzienne)
LEVERAGE = 3         # Dźwignia (np. 3x)
POSITION_SIZE_USDT = 50.0  # Kwota w USDT przeznaczona na pojedynczą pozycję

# 3. Parametry strategii EMA
LIMIT_CANDLES = 200  # Liczba świec potrzebna do precyzyjnego wyliczenia EMA 89
SL_PERCENT = 0.05    # Stop Loss = 5% od ceny wejścia
TP_PERCENT = 0.10    # Take Profit = 10% od ceny wejścia

# ==============================================================================
# KONFIGURACJA DZIENNIKA ZDARZEŃ (LOGÓW)
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

# Inicjalizacja klienta Bybit V5 API
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
        logging.info(f"Dźwignia dla {SYMBOL} została ustawiona na {LEVERAGE}x.")
    except Exception as e:
        # Kod 110043 oznacza, że dźwignia jest już ustawiona na ten poziom
        if "110043" in str(e):
            logging.info(f"Dźwignia {LEVERAGE}x jest już skonfigurowana.")
        else:
            logging.warning(f"Błąd podczas ustawiania dźwigni: {e}")

def has_active_position():
    """Sprawdza, czy na koncie Bybit znajduje się już otwarta pozycja na PEPEUSDT."""
    try:
        response = session.get_positions(category=CATEGORY, symbol=SYMBOL)
        positions = response.get("result", {}).get("list", [])
        for pos in positions:
            if float(pos.get("size", 0)) > 0:
                logging.info(f"Wykryto aktywną pozycję na {SYMBOL} (Wielkość: {pos['size']}). Bot pomija nowe wejście.")
                return True
        return False
    except Exception as e:
        logging.error(f"Błąd podczas sprawdzania otwartych pozycji: {e}")
        return True  # Bezpiecznik: w przypadku błędu zakładamy, że pozycja może istnieć

def get_market_data():
    """Pobiera historyczne świece z Bybit i oblicza wskaźniki EMA 21 oraz EMA 89."""
    response = session.get_kline(
        category=CATEGORY,
        symbol=SYMBOL,
        interval=INTERVAL,
        limit=LIMIT_CANDLES
    )
    candles = response.get("result", {}).get("list", [])
    if not candles:
        raise Exception("Nie udało się pobrać danych rynkowych z Bybit.")

    # Bybit zwraca świece od najnowszej do najstarszej — odwracamy kolejność
    df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
    df = df.iloc[::-1].reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    # Wskaźniki EMA
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()

    return df

def execute_trade(side, close_price):
    """
    Wylicza wielkość pozycji, Stop Loss, Take Profit oraz wysyła zlecenie na giełdę Bybit.
    """
    # Sprawdzenie kroku wielkości pozycji (qty step) dla PEPEUSDT
    # PEPE na Bybit ma zazwyczaj krok 100 lub 1000 sztuk
    raw_qty = (POSITION_SIZE_USDT * LEVERAGE) / close_price
    qty = int(raw_qty // 100) * 100  # Zaokrąglenie w dół do pełnych setek

    if qty <= 0:
        logging.error("Wyliczona wielkość pozycji wynosi 0. Zwiększ POSITION_SIZE_USDT.")
        return

    # Kalkulacja Poziomów SL i TP
    if side == "Buy":  # LONG
        sl_price = round(close_price * (1 - SL_PERCENT), 8)
        tp_price = round(close_price * (1 + TP_PERCENT), 8)
    else:            # SHORT
        sl_price = round(close_price * (1 + SL_PERCENT), 8)
        tp_price = round(close_price * (1 - TP_PERCENT), 8)

    logging.info(f"Otwieranie pozycji {side} | Zlecenie: {qty} PEPE | Cena: {close_price} | SL: {sl_price} | TP: {tp_price}")

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

        logging.info(f"SUCCESS: Zlecenie złożone pomyślnie! ID Zlecenia: {order['result']['orderId']}")
        logging.info("Aplikacja Bybit na Twoim telefonie wyśle Ci natychmiastowe powiadomienie Push.")

    except Exception as e:
        logging.error(f"Błąd składania zlecenia na Bybit: {e}")

def run_bot():
    """Główna logika analizy rynkowej i egzekucji strategii."""
    logging.info("=== ROZPOCZĘCIE ANALIZY DZIENNEJ (PEPE 1D) ===")

    set_leverage()

    if has_active_position():
        logging.info("Zakończono: Pozycja jest już otwarta na koncie.")
        return

    df = get_market_data()

    # Ostatnia ZAMKNIĘTA świeca dzienna (iloc[-2]), iloc[-1] to świeca w trakcie formowania
    candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    c_open, c_close = candle['open'], candle['close']
    c_high, c_low = candle['high'], candle['low']
    ema21, ema89 = candle['ema21'], candle['ema89']

    logging.info(f"Ostatnia zamknięta świeca D1 -> Cena: {c_close:.8f} | EMA21: {ema21:.8f} | EMA89: {ema89:.8f}")

    # 1. Określenie Trendu
    bullish_trend = ema21 > ema89
    bearish_trend = ema21 < ema89

    # 2. Test Strefy Wartości (Pullback)
    in_value_zone_long = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
    in_value_zone_short = (c_high >= min(ema21, ema89)) and (c_high <= max(ema21, ema89))

    # 3. Formacje Świecowe
    bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
    bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])
    bullish_candle = bullish_pinbar or bullish_engulfing

    bearish_pinbar = (c_close < c_open) and ((c_high - c_open) > (c_open - c_close) * 1.5)
    bearish_engulfing = (c_close < c_open) and (prev_candle['close'] > prev_candle['open']) and (c_close < prev_candle['open'])
    bearish_candle = bearish_pinbar or bearish_engulfing

    # 4. Decyzja o otwarciu transakcji
    if bullish_trend and in_value_zone_long and bullish_candle:
        logging.info("Sygnał KUPNA (LONG) spełniony! Wykonuję zlecenie...")
        execute_trade("Buy", c_close)

    elif bearish_trend and in_value_zone_short and bearish_candle:
        logging.info("Sygnał SPRZEDAŻY (SHORT) spełniony! Wykonuję zlecenie...")
        execute_trade("Sell", c_close)

    else:
        logging.info("Brak spełnionych warunków wejścia w pozycję. Czekamy na kolejną świecę dzienną.")

if __name__ == "__main__":
    run_bot()
