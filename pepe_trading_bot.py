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
# KONFIGURACJA BOTA SPOT (EMA TREND - TOP CAPITALIZATION SCANNER)
# ==============================================================================
FALLBACK_KEY = "1DUKhuFk2sZbu4jpqQ"
FALLBACK_SECRET = "PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl"

RAW_KEY = os.environ.get("1DUKhuFk2sZbu4jpqQ", FALLBACK_KEY)
RAW_SECRET = os.environ.get("PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl", FALLBACK_SECRET)

BYBIT_API_KEY = RAW_KEY.strip()
BYBIT_API_SECRET = RAW_SECRET.strip()

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (Zgodny z Bybit EU)

# --- USTAWIENIA SKANERA RYNKU TOP VOLUME/CAP ---
TOP_MARKETS_COUNT = 100       # Skanuj TOP 1000 par o największym wolumenie/kapitalizacji
MAX_ACTIVE_POSITIONS = 5     # Maksymalnie 5 otwartych pozycji jednocześnie
RISK_PCT_PER_TRADE = 0.10    # 10% wolnego salda na każdą nową pozycję
TRAILING_DROP_PCT = 0.015    # Sprzedaż po spadku o 1.5% od szczytu (Trailing Up)
HARD_STOP_LOSS_PCT = 0.02    # Sztywny Stop Loss -2% od ceny zakupu

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
# FUNKCJE POMOCNICZE I SELEKCJA TOP KAPITALIZACJI
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

def get_top_volume_spot_usdt_symbols(limit_count=TOP_MARKETS_COUNT):
    """Pobiera pary USDT z rynku SPOT posortowane po największym 24-godzinnym wolumenie obrotu."""
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        
        # Sortowanie wg obrotu 24h (turnover24h = cena * wolumen) malejąco
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("turnover24h", 0.0)), reverse=True)
        top_symbols = [t["symbol"] for t in sorted_tickers[:limit_count]]
        return top_symbols
    except Exception as e:
        logging.error(f"Błąd pobierania top symboli: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PEPEUSDT", "XRPUSDT"]

def get_market_data(symbol, interval="15", limit=100):
    """Pobiera świece i wylicza wskaźniki EMA."""
    try:
        time.sleep(0.15)  # Pauza chroniąca przed API Rate Limit
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None

        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)

        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
        return df
    except Exception as e:
        logging.error(f"Błąd pobierania danych dla {symbol}: {e}")
        return None

# ==============================================================================
# SKANER TOP KAPITALIZACJI POD KĄTEM SYGNAŁU EMA (15M)
# ==============================================================================
def scan_top_market_for_ema_signals():
    global active_positions

    if len(active_positions) >= MAX_ACTIVE_POSITIONS:
        logging.info(f"=== [EMA TOP SCANNER] Osiągnięto limit {MAX_ACTIVE_POSITIONS} aktywnych pozycji. Pomijam skanowanie. ===")
        return

    top_symbols = get_top_volume_spot_usdt_symbols()
    logging.info(f"=== [EMA TOP SCANNER] Skanowanie TOP {len(top_symbols)} największych par SPOT (15M) ===")

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_ACTIVE_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            if df is None or len(df) < 5: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            ema21, ema89 = candle['ema21'], candle['ema89']

            # Warunki sygnału EMA Trend
            bullish_trend = ema21 > ema89
            in_value_zone = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
            bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
            bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])

            buy_signal = bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing)

            if buy_signal:
                balance = get_available_balance()
                if balance < 5.0:
                    logging.warning("Brak wystarczającego salda USDT na otwarcie pozycji.")
                    break

                order_val = balance * RISK_PCT_PER_TRADE
                raw_qty = order_val / c_close

                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [EMA TOP KUPNO] Wykryto sygnał na {symbol}! Kupiono za 10% salda | Ilość: {qty_str} po cenie {c_close}")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

# ==============================================================================
# MONITOROWANIE I SPRZEDAŻ (TRAILING UP)
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

            # Aktualizacja szczytu cenowego
            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt pozycji: {current_price} USDT")

            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price

            # Warunki sprzedaży SPOT
            trailing_sell = drop_from_peak >= TRAILING_DROP_PCT and current_price > buy_price
            stop_loss_sell = total_pnl <= -HARD_STOP_LOSS_PCT

            if trailing_sell or stop_loss_sell:
                reason = "Trailing Up Profit Take" if trailing_sell else "Hard Stop Loss"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)

                if res.get("retCode") == 0:
                    logging.info(f"💰 [SPOT SPRZEDAŻ - {symbol}] Sprzedano | Powód: {reason} | PnL: {total_pnl*100:.2f}%")
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
            scan_top_market_for_ema_signals()
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
        self.wfile.write(b"Bybit SPOT Top Volume EMA Scanner Bot is Running 24/7!")
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
