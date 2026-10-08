import os
import time
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# KONFIGURACJA BOTA SPOT (EMA TREND + TOP DIP HUNTER TRAILING UP)
# ==============================================================================
FALLBACK_KEY = "1DUKhuFk2sZbu4jpqQ"
FALLBACK_SECRET = "PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl"

RAW_KEY = os.environ.get("1DUKhuFk2sZbu4jpqQ", FALLBACK_KEY)
RAW_SECRET = os.environ.get("PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl", FALLBACK_SECRET)

BYBIT_API_KEY = RAW_KEY.strip()
BYBIT_API_SECRET = RAW_SECRET.strip()

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (zgodny z Bybit EU / MiCA)

# --- 1. USTAWIENIA EMA TREND (1H) - DOKŁADNE PROCENTY ---
ASSETS_EMA = {
    "PEPEUSDT": {"risk_pct": 0.25, "qty_decimals": 0},  # 25% salda
    "ETHUSDT":  {"risk_pct": 0.12, "qty_decimals": 4},  # 12% salda
    "SOLUSDT":  {"risk_pct": 0.13, "qty_decimals": 2}   # 13% salda
}

# --- 2. USTAWIENIA TOP DIP HUNTER SPOT (15M) ---
TOP_DIPS_COUNT = 4           # Maksymalnie 4 monety o największej stracie 24h
RISK_PCT_DIP = 0.10          # Dokładnie 10% salda na każdą monete w spadek (łącznie do 40%)
TRAILING_DROP_PCT = 0.015    # Sprzedaż po spadku o 1.5% od najwyższego szczytu (Trailing Stop)
HARD_STOP_LOSS_PCT = 0.02    # Sztywny Stop Loss -2% od ceny wejścia

APP_URL = "https://pepe-trading-bot-ujqx.onrender.com"
POSITIONS_FILE = "active_spot_positions.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

session = HTTP(
    testnet=TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=20000
)

# ==============================================================================
# ZARZĄDZANIE PAMIĘCIĄ POZYCJI SPOT (JSON)
# ==============================================================================
def load_positions():
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_positions(positions):
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu pozycji: {e}")

active_positions = load_positions()

# ==============================================================================
# FUNKCJE POMOCNICZE
# ==============================================================================
def get_available_balance():
    """Pobiera wolne saldo USDT na koncie SPOT / UNIFIED."""
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        coins = res.get("result", {}).get("list", [{}])[0].get("coin", [])
        for coin in coins:
            if coin.get("coin") == "USDT":
                return float(coin.get("equity", 0.0))
    except Exception:
        pass
    return 0.0

def get_market_data(symbol, interval="15", limit=100):
    """Pobiera świece i wylicza EMA."""
    try:
        time.sleep(0.2)
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None

        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)

        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
        return df
    except Exception as e:
        logging.error(f"Błąd danych {symbol}: {e}")
        return None

# ==============================================================================
# STRATEGIA 1: EMA TREND (1H) - DLA PEPE (25%), ETH (12%), SOL (13%)
# ==============================================================================
def run_ema_strategy():
    global active_positions
    logging.info("=== [STRATEGIA 1] ANALIZA EMA TREND SPOT (PEPE 25%, ETH 12%, SOL 13%) ===")

    for symbol, config in ASSETS_EMA.items():
        try:
            df = get_market_data(symbol, interval="60")
            if df is None or len(df) < 5: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            ema21, ema89 = candle['ema21'], candle['ema89']

            bullish_trend = ema21 > ema89
            in_value_zone = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
            bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
            bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])

            buy_signal = bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing)

            # KUPNO EMA
            if symbol not in active_positions and buy_signal:
                balance = get_available_balance()
                if balance < 5.0: continue

                order_val = balance * config["risk_pct"]
                raw_qty = order_val / c_close
                decimals = config["qty_decimals"]
                qty_str = str(int(raw_qty)) if decimals == 0 else f"{round(raw_qty, decimals):.{decimals}f}"

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "EMA",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [EMA KUPNO - {symbol}] Kupiono za {config['risk_pct']*100}% salda | Ilość: {qty_str} po {c_close}")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")
            else:
                logging.info(f"[EMA - {symbol}] Brak sygnału kupna.")

        except Exception as e:
            logging.error(f"[EMA - {symbol}] Błąd: {e}")

# ==============================================================================
# STRATEGIA 2: TOP DIP HUNTER SPOT (4 TOP SPADKI - KAŻDY PO 10% SALDA)
# ==============================================================================
def run_dip_hunter_strategy():
    global active_positions
    logging.info("=== [STRATEGIA 2] TOP DIP HUNTER SPOT (4 TOP SPADKI - 10% SALDA / POZYCJA) ===")

    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("price24hPcnt", 0.0)))
        top_losers = [t["symbol"] for t in sorted_tickers[:TOP_DIPS_COUNT]]

        for symbol in top_losers:
            if symbol in active_positions: continue

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
                if balance < 5.0: continue

                order_val = balance * RISK_PCT_DIP  # Exactly 10% per dip position
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "DIP_TRAILING",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [DIP HUNTER KUPNO - {symbol}] Kupiono za 10% salda | Ilość: {qty_str} po {c_close}")
            else:
                logging.info(f"[DIP - {symbol}] Brak sygnału odbicia.")

    except Exception as e:
        logging.error(f"[DIP HUNTER] Błąd: {e}")

# ==============================================================================
# MONITOROWANIE I SPRZEDAŻ (TRAILING UP LOGIC)
# ==============================================================================
def monitor_and_close_positions():
    global active_positions
    if not active_positions: return

    logging.info("=== [MONITOROWANIE POZYCJI & TRAILING UP] ===")
    symbols_to_delete = []

    for symbol, pos in active_positions.items():
        try:
            df = get_market_data(symbol, interval="15", limit=5)
            if df is None: continue

            current_price = df.iloc[-1]['close']
            buy_price = pos["buy_price"]
            peak_price = pos.get("peak_price", buy_price)
            qty_to_sell = pos["qty"]

            # 1. Aktualizacja najwyższego szczytu pozycji (Trailing Up)
            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt pozycji: {current_price} USDT")

            # 2. Wyliczenie odchyleń cenowych
            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price

            # 3. Warunki sprzedaży SPOT
            trailing_sell = drop_from_peak >= TRAILING_DROP_PCT and current_price > buy_price
            stop_loss_sell = total_pnl <= -HARD_STOP_LOSS_PCT

            if trailing_sell or stop_loss_sell:
                reason = "Trailing Up Profit Take" if trailing_sell else "Hard Stop Loss"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)

                if res.get("retCode") == 0:
                    logging.info(f"💰 [SPOT SPRZEDAŻ - {symbol}] Sprzedano | Powód: {reason} | Wynik PnL: {total_pnl*100:.2f}%")
                    symbols_to_delete.append(symbol)
                else:
                    logging.error(f"❌ Błąd sprzedaży {symbol}: {res}")
            else:
                logging.info(f"ℹ️ [{symbol}] PnL: {total_pnl*100:.2f}% | Szczyt: {pos['peak_price']} | Aktualna cena: {current_price}")

        except Exception as e:
            logging.error(f"Błąd monitorowania {symbol}: {e}")

    for s in symbols_to_delete:
        del active_positions[s]
    if symbols_to_delete:
        save_positions(active_positions)

# ==============================================================================
# PĘTLA GŁÓWNA I SERWER
# ==============================================================================
def bot_loop():
    while True:
        try:
            run_dip_hunter_strategy()
            run_ema_strategy()
            monitor_and_close_positions()
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(5 * 60)

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
        self.wfile.write(b"Bybit SPOT Multi-Strategy Bot + Trailing Up is Active 24/7!")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    httpd = HTTPServer(('', port), SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    t_bot = threading.Thread(target=bot_loop, daemon=True)
    t_bot.start()

    t_ping = threading.Thread(target=self_ping_loop, daemon=True)
    t_ping.start()

    run_health_server()
