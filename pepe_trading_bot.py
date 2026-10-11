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
# KONFIGURACJA BOTA SPOT 
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

print(f"🔑 DEBUG KEY: [{BYBIT_API_KEY[:5]}...] (Długość: {len(BYBIT_API_KEY)})", flush=True)
print(f"🔑 DEBUG SEC: [{BYBIT_API_SECRET[:5]}...] (Długość: {len(BYBIT_API_SECRET)})", flush=True)

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (zgodny z Bybit EU / MiCA)

# --- USTAWIENIA SKANOWANIA ---
TOP_MARKETS_COUNT = 150      # Skanuj TOP 150 par pod kątem EMA Trend
TOP_DIPS_COUNT = 4           # Skanuj 4 monety o największym spadku 24h
MAX_PUMP_POSITIONS = 4       # Maksymalnie 4 pozycje typu PUMP
MAX_TOTAL_POSITIONS = 8      # Łączny limit otwartych pozycji

# --- ZARZĄDZANIE KAPITAŁEM ---
RISK_PCT_PER_TRADE = 0.15    # 15% CAŁKOWITEJ wartości konta na każdą pozycję
MIN_ORDER_VALUE = 100.0      # Minimalna wartość zakupu to 100 USD

# Progi Zabezpieczające (Trailing Up SPOT)
TRAILING_DROP_PCT = 0.015    # Sprzedaż po spadku o 1.5% od najwyższego szczytu
HARD_STOP_LOSS_PCT = 0.02    # Sztywny Stop Loss -2% od ceny zakupu

# Progi RSI & VOL
RSI_OVERBOUGHT_SELL = 80     # Sprzedaj, gdy RSI przekroczy 80
RSI_MAX_BUY_EMA = 65         # Maksymalne RSI dla zakupu w trendzie
PUMP_VOL_MULTIPLIER = 2.5    # Wolumen 2.5x wyższy od średniej MA20 dla PUMP
PUMP_PRICE_CHANGE_PCT = 0.025# Wzrost ceny o min. +2.5% w świecy 15M

APP_URL = "https://pepe-trading-bot-ujqx.onrender.com"
POSITIONS_FILE = "active_spot_positions.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

_original_time = time.time

def safe_float(val, default=0.0):
    try:
        if val is None or str(val).strip() == "":
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

# ==============================================================================
# INICJALIZACJA PUNKTU KOŃCOWEGO BYBIT (EU PRIORITY)
# ==============================================================================
endpoints_to_test = [
    ("EU Region / MiCA", "https://api.bybit.eu"),
    ("Global Mainnet", "https://api.bybit.com"),
    ("Bytick Mainnet", "https://api.bytick.com"),
]

session = None

for name, url in endpoints_to_test:
    try:
        test_sess = HTTP(
            testnet=TESTNET,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            recv_window=20000
        )
        test_sess.endpoint = url
        
        res_bal = test_sess.get_wallet_balance(accountType="UNIFIED")
        if res_bal.get("retCode") == 0:
            session = test_sess
            print(f"✅ POŁĄCZONO SUKCESEM PRZEZ: {name} ({url})!", flush=True)
            
            res_time = test_sess.get_server_time()
            bybit_ms = int(res_time["result"]["timeNano"]) // 1000000
            local_ms = int(_original_time() * 1000)
            time_offset_sec = (bybit_ms - local_ms) / 1000.0
            time.time = lambda: _original_time() + time_offset_sec
            break
    except Exception:
        pass

if not session:
    session = HTTP(
        testnet=TESTNET,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        recv_window=20000
    )
    session.endpoint = "https://api.bybit.eu"

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
# FUNKCJE POMOCNICZE, SALDO, RSI & VOL
# ==============================================================================
def get_balances():
    """Zwraca dwuelementową krotkę: (Całkowita wartość konta w USD, Wolne środki USDT)"""
    if not session:
        logging.error("Sesja Bybit nie została poprawnie utworzona.")
        return 0.0, 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        
        ret_code = res.get("retCode")
        ret_msg = res.get("retMsg", "")
        if ret_code != 0:
            logging.error(f"Błąd API Bybit przy pobieraniu salda (Code {ret_code}): {ret_msg}")
            return 0.0, 0.0

        account_list = res.get("result", {}).get("list", [])
        if not account_list:
            return 0.0, 0.0

        account_data = account_list[0]
        total_equity = safe_float(account_data.get("totalEquity"))
        free_usdt = 0.0
        
        for coin in account_data.get("coin", []):
            if coin.get("coin") == "USDT":
                avail = coin.get("availableToWithdraw")
                eq = coin.get("equity")
                free_usdt = safe_float(avail) if (avail is not None and str(avail).strip() != "") else safe_float(eq)
                break
                
        return total_equity, free_usdt
    except Exception as e:
        logging.error(f"Błąd pobierania salda: {e}")
        return 0.0, 0.0

def get_all_spot_usdt_symbols():
    symbols = []
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        symbols = [t["symbol"] for t in tickers if t.get("symbol", "").endswith("USDT")]
    except Exception as e:
        logging.warning(f"Nie udało się pobrać z get_tickers: {e}")

    if not symbols:
        try:
            instruments = session.get_instruments_info(category=CATEGORY).get("result", {}).get("list", [])
            symbols = [i["symbol"] for i in instruments if i.get("symbol", "").endswith("USDT") and i.get("status") == "Trading"]
        except Exception as e:
            logging.warning(f"Nie udało się pobrać z get_instruments_info: {e}")

    if not symbols:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PEPEUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

    return symbols

def get_top_volume_symbols(limit_count=TOP_MARKETS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        if usdt_tickers:
            sorted_tickers = sorted(usdt_tickers, key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania top symboli: {e}")
    
    return get_all_spot_usdt_symbols()[:limit_count]

def get_top_losers_symbols(limit_count=TOP_DIPS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        if usdt_tickers:
            sorted_tickers = sorted(usdt_tickers, key=lambda x: safe_float(x.get("price24hPcnt")))
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania spadkowych symboli: {e}")

    return get_all_spot_usdt_symbols()[:limit_count]

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_market_data(symbol, interval="15", limit=100):
    try:
        time.sleep(0.05)
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
        df['rsi'] = calculate_rsi(df['close'], period=14)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()

        return df
    except Exception as e:
        logging.error(f"Błąd pobierania danych dla {symbol}: {e}")
        return None

# ==============================================================================
# STRATEGIA 1: MOMENTUM PUMP HUNTER (FULL MARKET SCAN)
# ==============================================================================
def scan_for_pumps():
    global active_positions

    pump_positions_count = sum(1 for p in active_positions.values() if p.get("strategy") == "PUMP_HUNTER")
    if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        logging.info("⏸️ Osiągnięto limit pozycji dla Strategii PUMP HUNTER.")
        return

    total_equity, free_usdt = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    all_symbols = get_all_spot_usdt_symbols()
    logging.info(f"=== [STRATEGIA 1] MOMENTUM PUMP HUNTER (PAR DO SKANOWANIA: {len(all_symbols)}) ===")
    logging.info(f"💰 Saldo UTA: {free_usdt:.2f} USDT | Wartość zakupu 1 pozycji: {order_val:.2f} USDT")

    if free_usdt < order_val:
        logging.info(f"⏭️ Pomijam skanowanie. Brak wymaganych wolnych środków {order_val:.2f} USDT.")
        return

    scanned_count = 0
    for symbol in all_symbols:
        if symbol in active_positions: continue
        if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            scanned_count += 1
            if df is None or len(df) < 25: continue

            candle = df.iloc[-2]
            c_open, c_close = candle['open'], candle['close']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']

            price_change = (c_close - c_open) / c_open
            volume_spike = c_vol >= (vol_ma * PUMP_VOL_MULTIPLIER)
            rsi_valid = 45 <= c_rsi <= 75

            if price_change >= PUMP_PRICE_CHANGE_PCT and volume_spike and rsi_valid:
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if safe_float(qty_str) <= 0: continue

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
                    logging.info(f"🚀 [PUMP KUPNO OK] {symbol}! Wzrost: +{price_change*100:.2f}%, Vol: {c_vol/vol_ma:.1f}x, RSI: {c_rsi:.1f}")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy pompy {symbol}: {e}")

    logging.info(f"✅ Strategia 1: Przeanalizowano {scanned_count} par. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 2: TOP DIP HUNTER (Z RSI & VOL)
# ==============================================================================
def run_dip_hunter_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    total_equity, free_usdt = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    top_losers = get_top_losers_symbols()
    logging.info(f"=== [STRATEGIA 2] TOP DIP HUNTER (PAR DO SKANOWANIA: {len(top_losers)}) ===")

    if free_usdt < order_val:
        logging.info("⏭️ Pomijam skanowanie DIP HUNTER. Brak wolnych środków.")
        return

    scanned_count = 0
    for symbol in top_losers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            scanned_count += 1
            if df is None or len(df) < 25: continue

            candle, prev_candle = df.iloc[-2], df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']

            body = abs(c_close - c_open)
            lower_wick = min(c_open, c_close) - c_low
            is_pinbar = lower_wick > (body * 2.0) and (c_close > c_open)
            is_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])
            is_above_ema = (c_close > candle['ema9']) and (prev_candle['close'] <= prev_candle['ema9'])

            vol_confirmed = c_vol > vol_ma
            rsi_confirmed = c_rsi <= 55

            if (is_pinbar or is_engulfing or is_above_ema) and vol_confirmed and rsi_confirmed:
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if safe_float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "DIP_HUNTER",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [DIP KUPNO OK] {symbol}! RSI: {c_rsi:.1f}, Vol Ratio: {c_vol/vol_ma:.1f}x")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy dipu {symbol}: {e}")

    logging.info(f"✅ Strategia 2: Przeanalizowano {scanned_count} par. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 3: EMA TREND SCANNER (TOP 150)
# ==============================================================================
def scan_top150_for_ema_signals():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    total_equity, free_usdt = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    top_symbols = get_top_volume_symbols()
    logging.info(f"=== [STRATEGIA 3] EMA TREND SCANNER (PAR DO SKANOWANIA: {len(top_symbols)}) ===")

    if free_usdt < order_val:
        logging.info("⏭️ Pomijam skanowanie EMA TREND. Brak wolnych środków.")
        return

    scanned_count = 0
    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
            scanned_count += 1
            if df is None or len(df) < 25: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']
            ema21, ema89 = candle['ema21'], candle['ema89']

            bullish_trend = ema21 > ema89
            in_value_zone = (c_low <= max(ema21, ema89)) and (c_low >= min(ema21, ema89))
            bullish_pinbar = (c_close > c_open) and ((c_open - c_low) > (c_close - c_open) * 1.5)
            bullish_engulfing = (c_close > c_open) and (prev_candle['close'] < prev_candle['open']) and (c_close > prev_candle['open'])

            rsi_ok = 40 <= c_rsi <= RSI_MAX_BUY_EMA
            vol_ok = c_vol >= vol_ma

            if bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing) and rsi_ok and vol_ok:
                raw_qty = order_val / c_close
                qty_str = f"{round(raw_qty, 2):.2f}" if not (symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol) else str(int(raw_qty))

                if safe_float(qty_str) <= 0: continue

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=qty_str)
                if res.get("retCode") == 0:
                    active_positions[symbol] = {
                        "strategy": "EMA_TOP150",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [EMA KUPNO OK] {symbol}! RSI: {c_rsi:.1f}, Vol: OK | Cena: {c_close}")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    logging.info(f"✅ Strategia 3: Przeanalizowano {scanned_count} par. Brak nowych sygnałów zakupu.")

# ==============================================================================
# MONITOROWANIE I SPRZEDAŻ (TRAILING UP + RSI OVERBOUGHT + VOL DUMP)
# ==============================================================================
def monitor_and_close_positions():
    global active_positions
    if not active_positions:
        logging.info("ℹ️ Brak otwartych pozycji SPOT do monitorowania.")
        return

    logging.info(f"=== [MONITOROWANIE POZYCJI: {len(active_positions)} AKTYWNYCH] ===")
    symbols_to_delete = []

    for symbol, pos in active_positions.items():
        try:
            df = get_market_data(symbol, interval="15", limit=25)
            if df is None: continue

            candle = df.iloc[-1]
            current_price = candle['close']
            current_rsi = candle['rsi']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']

            buy_price = pos["buy_price"]
            peak_price = pos.get("peak_price", buy_price)
            qty_to_sell = pos["qty"]

            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt: {current_price} USDT")

            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price

            trailing_sell = drop_from_peak >= TRAILING_DROP_PCT and current_price > buy_price
            stop_loss_sell = total_pnl <= -HARD_STOP_LOSS_PCT
            rsi_overbought_sell = current_rsi >= RSI_OVERBOUGHT_SELL and total_pnl > 0
            vol_dump_sell = (c_vol > vol_ma * 2.0) and (candle['close'] < candle['open']) and (total_pnl < -0.01)

            if trailing_sell or stop_loss_sell or rsi_overbought_sell or vol_dump_sell:
                reason = "Trailing Up" if trailing_sell else ("Stop Loss" if stop_loss_sell else ("RSI Overbought (>80)" if rsi_overbought_sell else "Volume Dump Exit"))
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)

                if res.get("retCode") == 0:
                    logging.info(f"💰 [SPOT SPRZEDAŻ - {symbol}] Powód: {reason} | PnL: {total_pnl*100:.2f}% | RSI: {current_rsi:.1f}")
                    symbols_to_delete.append(symbol)
                else:
                    logging.error(f"❌ Błąd sprzedaży {symbol}: {res}")
            else:
                logging.info(f"ℹ️ [{symbol}] PnL: {total_pnl*100:.2f}% | RSI: {current_rsi:.1f} | Szczyt: {pos['peak_price']} | Cena: {current_price}")

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
            logging.info("--------------------------------------------------------------------------------")
            logging.info("🚀 ROZPOCZYNAM NOWY CYKL SKANOWANIA RYNKU SPOT...")
            scan_for_pumps()               
            run_dip_hunter_strategy()      
            scan_top150_for_ema_signals()  
            monitor_and_close_positions()  
            logging.info("💤 CYKL ZAKOŃCZONY. Następne skanowanie za 3 minuty...")
            logging.info("--------------------------------------------------------------------------------")
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(3 * 60)

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
        self.wfile.write(b"Bybit SPOT Bot (Equity Sizing 15% / Min 100$) is Running 24/7!")
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
