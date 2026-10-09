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
# KONFIGURACJA BOTA SPOT (EMA TOP 100 + DIP HUNTER + PUMP HUNTER)
# ==============================================================================
FALLBACK_KEY = "1DUKhuFk2sZbu4jpqQ"
FALLBACK_SECRET = "PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl"

RAW_KEY = os.environ.get("1DUKhuFk2sZbu4jpqQ", FALLBACK_KEY)
RAW_SECRET = os.environ.get("PrhWFT0KFql7RHQWtAhN5kBOFfyjRyXJb8Yl", FALLBACK_SECRET)

BYBIT_API_KEY = RAW_KEY.strip()
BYBIT_API_SECRET = RAW_SECRET.strip()

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (zgodny z Bybit EU / MiCA)

# --- USTAWIENIA SKANOWANIA ---
TOP_MARKETS_COUNT = 100      # Skanuj TOP 100 par o największej kapitalizacji/wolumenie
TOP_DIPS_COUNT = 4           # Skanuj 4 monety o największym spadku 24h
MAX_PUMP_POSITIONS = 4       # Maksymalnie 4 aktywne pozycje ze strategii PUMP
MAX_TOTAL_POSITIONS = 8      # Łączny limit otwartych pozycji bota jednocześnie
RISK_PCT_PER_TRADE = 0.10    # 10% wolnego salda na każdą pozycję

# Zarządzanie pozycją (Trailing Up SPOT)
TRAILING_DROP_PCT = 0.015    # Sprzedaż po spadku o 1.5% od najwyższego szczytu
HARD_STOP_LOSS_PCT = 0.02    # Sztywny Stop Loss -2% od ceny zakupu

# Parametry wykrywania wybić (PUMP HUNTER STRATEGY)
PUMP_VOL_MULTIPLIER = 3.0    # Wolumen 3x wyższy od średniej z 20 świec
PUMP_PRICE_CHANGE_PCT = 0.03 # Wzrost ceny o min. +3% w jednej świecy 15M

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
# FUNKCJE POMOCNICZE I SELEKCJA RYNKÓW
# ==============================================================================
def get_available_balance():
    """Pobiera wolne saldo USDT z konta SPOT / UNIFIED."""
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        coins = res.get("result", {}).get("list", [{}])[0].get("coin", [])
        for coin in coins:
            if coin.get("coin") == "USDT":
                return float(coin.get("equity", 0.0))
    except Exception:
        pass
    return 0.0

def get_top_volume_symbols(limit_count=TOP_MARKETS_COUNT):
    """Pobiera rynek SPOT posortowany po największym 24h obrocie (kapitalizacji)."""
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("turnover24h", 0.0)), reverse=True)
        return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania top symboli: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PEPEUSDT", "STRKUSDT"]

def get_top_losers_symbols(limit_count=TOP_DIPS_COUNT):
    """Pobiera 4 pary USDT z rynku SPOT o największym spadku 24h."""
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("price24hPcnt", 0.0)))
        return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania spadkowych symboli: {e}")
        return []

def get_market_data(symbol, interval="15", limit=100):
    """Pobiera świece i wylicza EMA oraz średni wolumen MA20."""
    try:
        time.sleep(0.12)
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None

        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        return df
    except Exception as e:
        logging.error(f"Błąd pobierania danych dla {symbol}: {e}")
        return None

# ==============================================================================
# STRATEGIA 1: MOMENTUM PUMP HUNTER (MAX 4 POZYCJE SKOKOWE JAK STRK)
# ==============================================================================
def scan_for_pumps():
    global active_positions

    # Zliczanie aktualnie otwartych pozycji typu PUMP
    pump_positions_count = sum(1 for p in active_positions.values() if p.get("strategy") == "PUMP_HUNTER")
    if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    symbols = get_top_volume_symbols()
    logging.info(f"=== [STRATEGIA 1] MOMENTUM PUMP HUNTER (Skanowanie pomp - limit max {MAX_PUMP_POSITIONS}) ===")

    for symbol in symbols:
        if symbol in active_positions: continue
        if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            if df is None or len(df) < 25: continue

            candle = df.iloc[-2]  # Ostatnio zamknięta świeca
            c_open, c_close = candle['open'], candle['close']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']

            price_change = (c_close - c_open) / c_open
            volume_spike = c_vol >= (vol_ma * PUMP_VOL_MULTIPLIER)

            # Wystrzał ceny + Wystrzał wolumenu
            if price_change >= PUMP_PRICE_CHANGE_PCT and volume_spike:
                balance = get_available_balance()
                if balance < 5.0: break

                order_val = balance * RISK_PCT_PER_TRADE
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "PUMP_HUNTER",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    pump_positions_count += 1
                    save_positions(active_positions)
                    logging.info(f"🚀 [PUMP HUNTER KUPNO] Wykryto wystrzał na {symbol}! Wzrost: +{price_change*100:.2f}%, Wolumen: {c_vol/vol_ma:.1f}x MA. Kupiono za 10% salda!")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy pompy {symbol}: {e}")

# ==============================================================================
# STRATEGIA 2: TOP DIP HUNTER (4 TOP SPADKI SPOT)
# ==============================================================================
def run_dip_hunter_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    top_losers = get_top_losers_symbols()
    logging.info("=== [STRATEGIA 2] TOP DIP HUNTER (4 Największe spadki SPOT) ===")

    for symbol in top_losers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
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
                if balance < 5.0: break

                order_val = balance * RISK_PCT_PER_TRADE
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "DIP_HUNTER",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [DIP HUNTER KUPNO] Kupiono spadek na {symbol}! Kupiono za 10% salda | Cena: {c_close}")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy dipu {symbol}: {e}")

# ==============================================================================
# STRATEGIA 3: SKANER EMA TOP 100 KAPITALIZACJI (15M)
# ==============================================================================
def scan_top100_for_ema_signals():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    top_symbols = get_top_volume_symbols()
    logging.info(f"=== [STRATEGIA 3] SKANOWANIE TOP {len(top_symbols)} PAR SPOT (EMA TREND - 15M) ===")

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            if df is None or len(df) < 5: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            ema21, ema89 = candle['ema21'], candle['ema89']

            bullish_trend = ema21 > ema89
            in_value_zone = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
            bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
            bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])

            if bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing):
                balance = get_available_balance()
                if balance < 5.0: break

                order_val = balance * RISK_PCT_PER_TRADE
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "EMA_TOP100",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [EMA TOP 100 KUPNO] Wykryto sygnał na {symbol}! Kupiono za 10% salda | Cena: {c_close}")
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

            # 1. Aktualizacja szczytu cenowego pozycji (Trailing Up)
            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt: {current_price} USDT")

            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price

            # 2. Warunki sprzedaży SPOT
            trailing_sell = drop_from_peak >= TRAILING_DROP_PCT and current_price > buy_price
            stop_loss_sell = total_pnl <= -HARD_STOP_LOSS_PCT

            if trailing_sell or stop_loss_sell:
                reason = "Trailing Up Profit" if trailing_sell else "Hard Stop Loss"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)

                if res.get("retCode") == 0:
                    logging.info(f"💰 [SPOT SPRZEDAŻ - {symbol}] Sprzedano | Powód: {reason} | Wynik PnL: {total_pnl*100:.2f}%")
                    symbols_to_delete.append(symbol)
                else:
                    logging.error(f"❌ Błąd sprzedaży {symbol}: {res}")
            else:
                logging.info(f"ℹ️ [{symbol}] PnL: {total_pnl*100:.2f}% | Szczyt: {pos['peak_price']} | Cena: {current_price}")

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
            scan_for_pumps()               # 1. Priorytet: Szukaj pomp (Maksymalnie 4 pozycje)
            run_dip_hunter_strategy()      # 2. Skanuj 4 największe spadki
            scan_top100_for_ema_signals()  # 3. Skanuj TOP 100 pod kątem EMA
            monitor_and_close_positions()  # 4. Prowadź pozycje Trailing Up
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(3 * 60) # Skanowanie co 3 minuty

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
        self.wfile.write(b"Bybit SPOT Triple-Strategy Bot (EMA TOP 100 + DIP HUNTER + PUMP HUNTER) is Running 24/7!")
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
