import os
import time
import json
import logging
import threading
import socket
from decimal import Decimal
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# OCHRONA PRZED ZAWIESZENIEM POŁĄCZEŃ SIECIOWYCH (SOCKET TIMEOUT)
# ==============================================================================
socket.setdefaulttimeout(15)  # Maksymalnie 15 sekund na odpowiedź sieciową

# ==============================================================================
# KONFIGURACJA BOTA SPOT (RYNEK USDC - MICA COMPLIANT)
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

print(f"🔑 DEBUG KEY: [{BYBIT_API_KEY[:5]}...] (Długość: {len(BYBIT_API_KEY)})", flush=True)
print(f"🔑 DEBUG SEC: [{BYBIT_API_SECRET[:5]}...] (Długość: {len(BYBIT_API_SECRET)})", flush=True)

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (zgodny z Bybit EU / MiCA)
QUOTE_COIN = "USDC" # Waluta bazowa do handlu

# --- USTAWIENIA SKANOWANIA ---
TOP_MARKETS_COUNT = 150      # Skanuj TOP 150 par USDC
TOP_DIPS_COUNT = 4           # Skanuj 4 monety o największym spadku 24h
TOP_GAINERS_COUNT = 4        # Skanuj 4 monety o największym wzroście 24h
MAX_EMA_CROSS_POSITIONS = 5  # Maksymalnie 5 pozycji dla czystego przecięcia EMA 21/89
MAX_PUMP_POSITIONS = 4       # Maksymalnie 4 pozycje typu PUMP
MAX_TOTAL_POSITIONS = 8      # Łączny limit otwartych pozycji

# --- ZARZĄDZANIE KAPITAŁEM ---
RISK_PCT_PER_TRADE = 0.15    # Standardowo 15% na pozycję
PEPE_RISK_HIGH_PCT = 0.20    # 20% kapitału dla PEPE (gdy jest wystarczająco środków)
PEPE_RISK_LOW_PCT = 0.10     # 10% kapitału dla PEPE (gdy jest mniej środków)
MIN_ORDER_VALUE = 100.0      # Minimalna wartość zakupu to 100 USDC

# --- FILTRY ANTY-SZUMOWE EMA 21/89 ---
EMA_CROSS_MIN_SPREAD_PCT = 0.0020  # Wymagana separacja min. +0.20% ponad EMA 89
EMA_CROSS_VOL_MULTIPLIER = 1.10    # Wolumen na świecy min. 1.1x MA20
EMA_EXIT_HYSTERESIS_PCT  = 0.0020  # Histereza wyjścia: EMA 21 musi spaść min. 0.20% poniżej EMA 89

# --- STRATEGIA 2 (TOP DIP HUNTER) ---
MIN_DIP_24H_PCT = -0.050     # Wymagany SPADEK minimum -5.0% w 24h
DIP_RSI_15M_MAX = 40         # Głębokie wyprzedanie na 15M (RSI <= 40)
DIP_VOL_MULTIPLIER = 1.5     # Wolumen odbicia co najmniej 1.5x większy od MA20

# --- PROGI ZABEZPIECZAJĄCE ---
TRAILING_DROP_PCT = 0.015       # Standardowy Trailing Stop (-1.5% od szczytu)
PEPE_TRAILING_DROP_PCT = 0.028  # Dedykowany Trailing Stop dla PEPE (-2.8% od szczytu)
MEME_TRAILING_DROP_PCT = 0.025  # Trailing Stop dla pozostałych Memecoinów (-2.5%)
HARD_STOP_LOSS_PCT = 0.02       # Sztywny Stop Loss (-2.0% od ceny zakupu)

# --- MECHANIZM BREAKEVEN ---
BREAKEVEN_TRIGGER_PCT = 0.015   # Aktywacja po osiągnięciu szczytu +1.5% PnL
BREAKEVEN_SL_PCT = 0.002        # Przesunięcie Stop Loss na +0.2% PnL (pokrycie opłat)

# --- PROGI RSI & VOL ---
RSI_OVERBOUGHT_SELL = 80     # Sprzedaj, gdy RSI przekroczy 80
RSI_MAX_BUY_EMA = 65         # Maksymalne RSI dla zakupu w trendzie
PUMP_VOL_MULTIPLIER = 2.5    # Wolumen 2.5x wyższy od średniej MA20 dla PUMP
PUMP_PRICE_CHANGE_PCT = 0.025# Wzrost ceny o min. +2.5% w świecy 15M

APP_URL = "https://bybit-bot-i1lw.onrender.com"
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
# PRECYZYJNY SILNIK FORMOWANIA ILOŚCI (DECIMAL QUANTIZATION)
# ==============================================================================
instrument_filters = {}

def get_symbol_lot_filter(symbol):
    if symbol in instrument_filters:
        return instrument_filters[symbol]
    
    try:
        if session:
            res = session.get_instruments_info(category=CATEGORY, symbol=symbol)
            if res.get("retCode") == 0:
                list_data = res.get("result", {}).get("list", [])
                if list_data:
                    lot_filter = list_data[0].get("lotSizeFilter", {})
                    qty_step = lot_filter.get("qtyStep", "1")
                    min_qty = lot_filter.get("minOrderQty", "1")
                    instrument_filters[symbol] = (qty_step, min_qty)
                    return qty_step, min_qty
    except Exception as e:
        logging.warning(f"Nie udało się pobrać filtrów dla {symbol}: {e}")

    if symbol.startswith("1000") or "SHIB" in symbol or "PEPE" in symbol or "BONK" in symbol or "FLOKI" in symbol:
        return "1", "1"
    return "0.1", "0.1"

def format_qty_with_step(raw_qty, qty_step_str, min_qty_str):
    try:
        qty_dec = Decimal(str(raw_qty))
        step_dec = Decimal(str(qty_step_str))
        min_dec = Decimal(str(min_qty_str))
        
        if step_dec <= 0:
            floored_dec = qty_dec
        else:
            floored_dec = (qty_dec // step_dec) * step_dec
        
        if floored_dec < min_dec:
            return None
        
        normalized_step = step_dec.normalize()
        exp = normalized_step.as_tuple().exponent
        
        if exp < 0:
            decimals = abs(exp)
            return f"{floored_dec:.{decimals}f}"
        else:
            return f"{int(floored_dec)}"
    except Exception as e:
        logging.error(f"Błąd precyzyjnego formatowania ilości: {e}")
        return None

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
# FUNKCJE POMOCNICZE, SALDO USDC, RSI, VOL & FILTR BTC
# ==============================================================================
def get_balances():
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
        free_usdc = 0.0
        
        for coin in account_data.get("coin", []):
            if coin.get("coin") == QUOTE_COIN:
                avail = coin.get("availableToWithdraw")
                eq = coin.get("equity")
                free_usdc = safe_float(avail) if (avail is not None and str(avail).strip() != "") else safe_float(eq)
                break
                
        return total_equity, free_usdc
    except Exception as e:
        logging.error(f"Błąd pobierania salda: {e}")
        return 0.0, 0.0

def get_coin_available_balance(coin_name):
    if not session: return 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED", coin=coin_name)
        if res.get("retCode") == 0:
            account_list = res.get("result", {}).get("list", [])
            if account_list:
                for c in account_list[0].get("coin", []):
                    if c.get("coin") == coin_name:
                        avail = c.get("availableToWithdraw")
                        eq = c.get("equity")
                        return safe_float(avail) if (avail is not None and str(avail).strip() != "") else safe_float(eq)
    except Exception as e:
        logging.error(f"Błąd pobierania salda dla {coin_name}: {e}")
    return 0.0

def is_btc_market_healthy(drop_threshold=-0.010):
    try:
        df_btc = get_market_data("BTCUSDC", interval="15", limit=25)
        if df_btc is None or len(df_btc) < 10:
            return True
        last_candle = df_btc.iloc[-2]
        btc_change = (last_candle['close'] - last_candle['open']) / last_candle['open']
        if btc_change <= drop_threshold:
            logging.warning(f"⚠️ [BTC MARKET GUARD] Spadek Bitcoina {btc_change*100:.2f}% w 15M (Próg: {drop_threshold*100:.1f}%). Wstrzymuję zakupy.")
            return False
        return True
    except Exception as e:
        logging.warning(f"Błąd weryfikacji trendu BTC: {e}")
        return True

def check_quarterly_health(symbol):
    try:
        time.sleep(0.05)
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval="D", limit=90)
        candles = res.get("result", {}).get("list", [])
        if not candles or len(candles) < 60:
            return False

        df_q = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df_q = df_q.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df_q[col] = df_q[col].astype(float)

        close_now = df_q.iloc[-1]['close']
        close_90d_ago = df_q.iloc[0]['close']
        q_change = (close_now - close_90d_ago) / close_90d_ago

        if q_change <= -0.75:
            return False

        df_q['rsi_d1'] = calculate_rsi(df_q['close'], period=14)
        rsi_d1_now = df_q.iloc[-1]['rsi_d1']

        if rsi_d1_now > 55:
            return False

        return True
    except Exception as e:
        logging.warning(f"Błąd analizy kwartalnej dla {symbol}: {e}")
        return False

def get_all_spot_usdc_symbols():
    symbols = []
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        symbols = [t["symbol"] for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN) and t.get("symbol") not in ["USDTUSDC", "USDCUSDT"]]
    except Exception as e:
        logging.warning(f"Nie udało się pobrać z get_tickers: {e}")

    if not symbols:
        try:
            instruments = session.get_instruments_info(category=CATEGORY).get("result", {}).get("list", [])
            symbols = [i["symbol"] for i in instruments if i.get("symbol", "").endswith(QUOTE_COIN) and i.get("status") == "Trading"]
        except Exception as e:
            logging.warning(f"Nie udało się pobrać z get_instruments_info: {e}")

    if not symbols:
        symbols = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "PEPEUSDC", "XRPUSDC", "ADAUSDC", "DOGEUSDC", "LINKUSDC", "NEARUSDC"]

    if "PEPEUSDC" not in symbols:
        symbols.append("PEPEUSDC")

    return symbols

def sync_positions_from_bybit():
    global active_positions
    if not session: return
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") != 0: return
        account_list = res.get("result", {}).get("list", [])
        if not account_list: return

        valid_usdc_symbols = get_all_spot_usdc_symbols()
        restored = False

        for coin_info in account_list[0].get("coin", []):
            coin = coin_info.get("coin")
            if coin in ["USDC", "USDT", "USD", "EUR", "PLN", "GBP", "TRY", "UAH", "RUB"]: continue

            avail = coin_info.get("availableToWithdraw")
            eq = coin_info.get("equity")
            bal = safe_float(avail) if (avail is not None and str(avail).strip() != "") else safe_float(eq)

            if bal <= 0: continue

            symbol = f"{coin}USDC"
            if symbol not in valid_usdc_symbols:
                continue

            if symbol not in active_positions:
                df = get_market_data(symbol, interval="15", limit=5)
                if df is not None and len(df) > 0:
                    curr_price = df.iloc[-1]['close']
                    val_in_usdc = bal * curr_price
                    
                    if val_in_usdc >= 10.0:
                        buy_price = curr_price
                        try:
                            exec_res = session.get_executions(category=CATEGORY, symbol=symbol, limit=10)
                            if exec_res.get("retCode") == 0:
                                exec_list = exec_res.get("result", {}).get("list", [])
                                buys = [e for e in exec_list if e.get("side") == "Buy"]
                                if buys:
                                    buy_price = safe_float(buys[0].get("execPrice"), curr_price)
                        except Exception:
                            pass

                        qty_step, min_qty = get_symbol_lot_filter(symbol)
                        qty_str = format_qty_with_step(bal, qty_step, min_qty)
                        if qty_str:
                            active_positions[symbol] = {
                                "strategy": "AUTO_RESTORED",
                                "buy_price": buy_price,
                                "peak_price": max(curr_price, buy_price),
                                "qty": qty_str
                            }
                            restored = True
                            logging.info(f"🔄 [AUTOREKUPERACJA] Przywrócono pozycję {symbol}: {qty_str} monet | Realna cena zakupu: {buy_price} USDC (~{val_in_usdc:.2f} USDC)")

        if restored:
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd automatycznej synchronizacji z Bybit: {e}")

def get_top_volume_symbols(limit_count=TOP_MARKETS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN)]
        if usdc_tickers:
            sorted_tickers = sorted(usdc_tickers, key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
            res_symbols = [t["symbol"] for t in sorted_tickers[:limit_count]]
            if "PEPEUSDC" not in res_symbols:
                res_symbols.append("PEPEUSDC")
            return res_symbols
    except Exception as e:
        logging.error(f"Błąd pobierania top symboli: {e}")
    
    return get_all_spot_usdc_symbols()[:limit_count]

def get_top_losers_symbols(limit_count=TOP_DIPS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN)]
        if usdc_tickers:
            valid_losers = [t for t in usdc_tickers if safe_float(t.get("price24hPcnt")) <= MIN_DIP_24H_PCT]
            sorted_tickers = sorted(valid_losers, key=lambda x: safe_float(x.get("price24hPcnt")))
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania spadkowych symboli: {e}")
    return []

def get_top_gainers_symbols(limit_count=TOP_GAINERS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN) and t.get("symbol") not in ["USDTUSDC", "USDCUSDT"]]
        if usdc_tickers:
            valid_gainers = [t for t in usdc_tickers if safe_float(t.get("price24hPcnt")) >= 0.02]
            sorted_tickers = sorted(valid_gainers, key=lambda x: safe_float(x.get("price24hPcnt")), reverse=True)
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception as e:
        logging.error(f"Błąd pobierania top wzrostowych symboli: {e}")
    return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_market_data(symbol, interval="15", limit=250):
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
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = calculate_rsi(df['close'], period=14)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()

        return df
    except Exception as e:
        logging.error(f"Błąd pobierania danych dla {symbol}: {e}")
        return None

# ==============================================================================
# STRATEGIA 1: MOMENTUM PUMP HUNTER
# ==============================================================================
def scan_for_pumps():
    global active_positions

    pump_positions_count = sum(1 for p in active_positions.values() if p.get("strategy") == "PUMP_HUNTER")
    if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        logging.info("⏸️ Osiągnięto limit pozycji dla Strategii PUMP HUNTER.")
        return

    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    all_symbols = get_all_spot_usdc_symbols()
    logging.info(f"=== [STRATEGIA 1] MOMENTUM PUMP HUNTER (PAR USDC: {len(all_symbols)}) ===")
    logging.info(f"💰 Saldo UTA: {free_usdc:.2f} USDC | Wartość zakupu 1 pozycji: {order_val:.2f} USDC")

    if free_usdc < order_val:
        logging.info(f"⏭️ Pomijam skanowanie. Brak wymaganych wolnych środków {order_val:.2f} USDC.")
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
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
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

    logging.info(f"✅ Strategia 1: Przeanalizowano {scanned_count} par USDC. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 2: TOP DIP HUNTER
# ==============================================================================
def run_dip_hunter_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    top_losers = get_top_losers_symbols()
    logging.info(f"=== [STRATEGIA 2] TOP DIP HUNTER (SPADKI > -5%: {len(top_losers)} MONET) ===")

    if not top_losers:
        logging.info("⏭️ Brak monet spełniających warunek spadku minimum -5.0% w 24h. Pomijam DIP HUNTER.")
        return

    if free_usdc < order_val:
        logging.info("⏭️ Pomijam skanowanie DIP HUNTER. Brak wolnych środków.")
        return

    scanned_count = 0
    for symbol in top_losers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            if not check_quarterly_health(symbol):
                logging.info(f"⏭️ Odrzucono {symbol}: Słaby profil kwartalny.")
                continue

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

            vol_confirmed = c_vol >= (vol_ma * DIP_VOL_MULTIPLIER)
            rsi_confirmed = c_rsi <= DIP_RSI_15M_MAX

            if (is_pinbar or is_engulfing or is_above_ema) and vol_confirmed and rsi_confirmed:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {
                        "strategy": "DIP_HUNTER",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🔥 [DIP KUPNO OK] {symbol}! RSI: {c_rsi:.1f}, Vol: {c_vol/vol_ma:.1f}x")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy dipu {symbol}: {e}")

    logging.info(f"✅ Strategia 2: Przeanalizowano {scanned_count} kwalifikujących się spadków. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 3: EMA TREND SCANNER
# ==============================================================================
def scan_top150_for_ema_signals():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    top_symbols = get_top_volume_symbols()
    logging.info(f"=== [STRATEGIA 3] EMA TREND SCANNER (PAR USDC: {len(top_symbols)}) ===")

    if free_usdc < order_val:
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
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
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

    logging.info(f"✅ Strategia 3: Przeanalizowano {scanned_count} par USDC. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 4: DEDYKOWANY SKANER PEPE (EMA 21/89/200 + FILTR ANTY-SZUMOWY)
# ==============================================================================
def scan_pepe_specialist():
    global active_positions
    pepe_symbol = "PEPEUSDC"

    if pepe_symbol in active_positions or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    if not is_btc_market_healthy(drop_threshold=-0.015):
        logging.info("⏭️ [PEPE] Pomijam skanowanie PEPE ze względu na gwałtowny spadek Bitcoina (> -1.5%).")
        return

    total_equity, free_usdc = get_balances()
    target_val_20 = total_equity * PEPE_RISK_HIGH_PCT
    target_val_10 = total_equity * PEPE_RISK_LOW_PCT

    if free_usdc >= target_val_20 and target_val_20 >= MIN_ORDER_VALUE:
        order_val = target_val_20
        risk_label = "20%"
    elif free_usdc >= target_val_10 and target_val_10 >= MIN_ORDER_VALUE:
        order_val = target_val_10
        risk_label = "10%"
    elif free_usdc >= MIN_ORDER_VALUE:
        order_val = free_usdc
        risk_label = "Wolne Środki"
    else:
        logging.info(f"=== [STRATEGIA 4] PEPE EMA SPECIALIST ({pepe_symbol}) ===")
        logging.info(f"⏭️ Pomijam skanowanie PEPE. Wolne środki ({free_usdc:.2f} USDC) poniżej wymaganego minimum.")
        return

    logging.info(f"=== [STRATEGIA 4] PEPE EMA SPECIALIST ({pepe_symbol}) ===")
    logging.info(f"🐸 Saldo UTA: {free_usdc:.2f} USDC | Dedykowany zakup PEPE: {order_val:.2f} USDC ({risk_label} kapitału)")

    try:
        df = get_market_data(pepe_symbol, interval="15", limit=250)
        if df is None or len(df) < 200: return

        candle = df.iloc[-2]
        prev_candle = df.iloc[-3]
        c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
        c_vol, vol_ma = candle['volume'], candle['vol_ma20']
        c_rsi = candle['rsi']
        
        ema21 = candle['ema21']
        ema89 = candle['ema89']
        ema200 = candle['ema200']
        
        prev_ema21 = prev_candle['ema21']
        prev_ema89 = prev_candle['ema89']

        # 1. MAKRO TREND WZROSTOWY
        macro_trend_ok = (c_close > ema200) and (ema21 > ema200)

        # 2. FILTR ANTY-SZUMOWY WEJŚCIA DLA PEPE
        ema_spread = (ema21 - ema89) / ema89
        ema_rising = ema21 > prev_ema21
        
        # Złoty Krzyż z minimalną separacją i rosnącym nachyleniem
        ema_crossover = (prev_ema21 <= prev_ema89) and (ema21 > ema89) and (ema_spread >= 0.0015) and ema_rising
        # Odbicie od wsparcia z zachowaniem przewagi EMA21
        ema_bounce = (ema21 > ema89) and (c_low <= ema21) and (c_close > ema21) and (c_close > c_open) and ema_rising

        rsi_valid = 45 <= c_rsi <= 70
        volume_ok = c_vol >= vol_ma

        if macro_trend_ok and (ema_crossover or ema_bounce) and rsi_valid and volume_ok:
            order_qty_usdc = f"{order_val:.2f}"
            res = session.place_order(category=CATEGORY, symbol=pepe_symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
            if res.get("retCode") == 0:
                qty_step, min_qty = get_symbol_lot_filter(pepe_symbol)
                qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                active_positions[pepe_symbol] = {
                    "strategy": "PEPE_EMA_SPECIALIST",
                    "buy_price": c_close,
                    "peak_price": c_close,
                    "qty": qty_str
                }
                save_positions(active_positions)
                s_type = "Złoty Krzyż EMA21/89 (Clean)" if ema_crossover else "Odbicie w Trendzie EMA21"
                logging.info(f"🐸🔥 [PEPE KUPNO OK] Sygnał: {s_type} | Zakup: {order_val:.2f} USDC ({risk_label})! RSI: {c_rsi:.1f}, Vol: OK")
            else:
                logging.error(f"❌ Błąd zakupu PEPE: {res}")
        else:
            logging.info(f"✅ Strategia 4 (PEPE): Brak potwierdzonego sygnału EMA dla PEPEUSDC w tej 15M świecy.")

    except Exception as e:
        logging.error(f"Błąd analizy PEPE: {e}")

# ==============================================================================
# STRATEGIA 5: TOP 4 GAINERS EMA PULLBACK HUNTER
# ==============================================================================
def run_top_gainers_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    top_gainers = get_top_gainers_symbols()
    logging.info(f"=== [STRATEGIA 5] TOP 4 GAINERS EMA PULLBACK (PAR: {len(top_gainers)}) ===")

    if not top_gainers:
        logging.info("⏭️ Brak silnych liderów wzrostowych (+2% 24h). Pomijam TOP GAINERS.")
        return

    if free_usdc < order_val:
        logging.info("⏭️ Pomijam skanowanie TOP GAINERS. Brak wolnych środków.")
        return

    scanned_count = 0
    for symbol in top_gainers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15", limit=250)
            scanned_count += 1
            if df is None or len(df) < 100: continue

            candle = df.iloc[-2]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']
            
            ema9 = candle['ema9']
            ema21 = candle['ema21']
            ema89 = candle['ema89']

            bullish_structure = (c_close > ema89) and (ema21 > ema89)
            tested_ema_support = (c_low <= max(ema9, ema21)) and (c_close >= ema21)
            bullish_candle_reaction = (c_close > c_open)

            rsi_healthy = 50 <= c_rsi <= 68
            vol_confirmed = c_vol >= vol_ma

            if bullish_structure and tested_ema_support and bullish_candle_reaction and rsi_healthy and vol_confirmed:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {
                        "strategy": "TOP_GAINERS_PULLBACK",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    save_positions(active_positions)
                    logging.info(f"🚀🔥 [TOP GAINERS KUPNO OK] {symbol}! Odbicie od EMA przed wystrzałem! RSI: {c_rsi:.1f}, Vol: OK")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy lidera wzrostów {symbol}: {e}")

    logging.info(f"✅ Strategia 5: Przeanalizowano {scanned_count} liderów rynku. Brak nowych sygnałów zakupu.")

# ==============================================================================
# STRATEGIA 6: CZYSTY CROSS EMA 21 / 89 (POZBAWIONY SZUMÓW / ANTI-WHIPSAW)
# ==============================================================================
def run_ema21_89_pure_crossover_strategy():
    global active_positions

    current_cross_positions = sum(1 for p in active_positions.values() if p.get("strategy") == "EMA21_89_CROSS")
    if current_cross_positions >= MAX_EMA_CROSS_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val = max(total_equity * RISK_PCT_PER_TRADE, MIN_ORDER_VALUE)

    all_symbols = get_all_spot_usdc_symbols()
    logging.info(f"=== [STRATEGIA 6] CZYSTY CROSS EMA 21/89 ANTI-SZUM (AKTYWNYCH: {current_cross_positions}/{MAX_EMA_CROSS_POSITIONS}) ===")

    if free_usdc < order_val:
        logging.info(f"⏭️ Pomijam skanowanie EMA 21/89 Cross. Brak wolnych środków {order_val:.2f} USDC.")
        return

    scanned_count = 0
    for symbol in all_symbols:
        if symbol in active_positions: continue
        if current_cross_positions >= MAX_EMA_CROSS_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15", limit=120)
            scanned_count += 1
            if df is None or len(df) < 95: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close = candle['open'], candle['close']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']

            ema21_now, ema89_now = candle['ema21'], candle['ema89']
            ema21_prev, ema89_prev = prev_candle['ema21'], prev_candle['ema89']

            # 1. ZŁOTY KRZYŻ: ŚWIEŻE PRZECIĘCIE
            basic_crossover = (ema21_prev <= ema89_prev) and (ema21_now > ema89_now)

            # 2. FILTR SEPARACJI (SPREAD): EMA 21 WYBIŁA SIĘ MINIMUM 0.20% PONAD EMA 89
            spread_pct = (ema21_now - ema89_now) / ema89_now
            has_clean_spread = spread_pct >= EMA_CROSS_MIN_SPREAD_PCT

            # 3. FILTR NACHYLENIA (SLOPE): EMA 21 MA WYRAŹNY WEKTOR WZROSTOWY
            ema21_sloping_up = ema21_now > ema21_prev

            # 4. POTWIERDZENIE CENĄ I ŚWIECĄ: CENA PONAD EMA21 ORAZ ZIELONY KORPUS
            price_confirmed = (c_close > ema21_now) and (c_close > c_open)

            # 5. POTWIERDZENIE WOLUMENEM: RUCH POTWIERDZONY NAPŁYWEM KAPITAŁU
            volume_confirmed = c_vol >= (vol_ma * EMA_CROSS_VOL_MULTIPLIER)

            if basic_crossover and has_clean_spread and ema21_sloping_up and price_confirmed and volume_confirmed:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {
                        "strategy": "EMA21_89_CROSS",
                        "buy_price": c_close,
                        "peak_price": c_close,
                        "qty": qty_str
                    }
                    current_cross_positions += 1
                    save_positions(active_positions)
                    logging.info(f"⚡🔥 [EMA CROSS 21/89 KUPNO OK] {symbol}! Potwierdzony Złoty Krzyż (Spread: +{spread_pct*100:.2f}%, Vol: {c_vol/vol_ma:.1f}x) | Zakup: {order_val:.2f} USDC")
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception as e:
            logging.error(f"Błąd analizy EMA Cross {symbol}: {e}")

    logging.info(f"✅ Strategia 6: Przeanalizowano {scanned_count} par. Pozycje Cross: {current_cross_positions}/{MAX_EMA_CROSS_POSITIONS}.")

# ==============================================================================
# MONITOROWANIE I BEZ BŁĘDNA SPRZEDAŻ (Z OBSŁUGĄ HISTEREZY WYJŚCIA)
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
            df = get_market_data(symbol, interval="15", limit=250)
            if df is None: continue

            candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            current_price = candle['close']
            current_rsi = candle['rsi']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']

            buy_price = pos["buy_price"]
            peak_price = pos.get("peak_price", buy_price)
            strat_name = pos.get("strategy", "")

            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt: {current_price} USDC")

            # 1. Trailing Stop
            if symbol == "PEPEUSDC":
                trailing_drop_pct = PEPE_TRAILING_DROP_PCT
            elif any(m in symbol for m in ["SHIB", "DOGE", "BONK", "FLOKI"]):
                trailing_drop_pct = MEME_TRAILING_DROP_PCT
            else:
                trailing_drop_pct = TRAILING_DROP_PCT

            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price
            peak_pnl = (pos["peak_price"] - buy_price) / buy_price

            # 2. Breakeven & Hard Stop Loss
            if peak_pnl >= BREAKEVEN_TRIGGER_PCT:
                breakeven_price = buy_price * (1.0 + BREAKEVEN_SL_PCT)
                stop_loss_sell = current_price <= breakeven_price
                sl_type = "Breakeven SL (+0.2%)"
            else:
                stop_loss_sell = total_pnl <= -HARD_STOP_LOSS_PCT
                sl_type = "Hard Stop Loss (-2.0%)"

            trailing_sell = drop_from_peak >= trailing_drop_pct and current_price > buy_price

            # 3. ZASADY WYJŚCIA DLA STRATEGII 6 (HISTEREZA ANTY-SZUMOWA)
            ema_death_cross_sell = False
            if strat_name == "EMA21_89_CROSS":
                ema21 = candle['ema21']
                ema89 = candle['ema89']

                # Histereza: Realny Krzyż Śmierci (spadek EMA 21 min. 0.20% poniżej EMA 89)
                hysteresis_exit = ema21 < (ema89 * (1.0 - EMA_EXIT_HYSTERESIS_PCT))
                # LUB 2 kolejne świece zamknięte pod EMA 89
                two_candles_below_ema89 = (current_price < ema89) and (prev_candle['close'] < prev_candle['ema89'])

                if hysteresis_exit or two_candles_below_ema89:
                    ema_death_cross_sell = True
                
                rsi_overbought_sell = False
                vol_dump_sell = False
                pepe_ema_break_exit = False
            else:
                rsi_overbought_sell = current_rsi >= RSI_OVERBOUGHT_SELL and total_pnl > 0
                vol_dump_sell = (c_vol > vol_ma * 2.0) and (candle['close'] < candle['open']) and (total_pnl < -0.01)
                pepe_ema_break_exit = False
                if symbol == "PEPEUSDC":
                    ema89 = candle['ema89']
                    ema21 = candle['ema21']
                    if current_price < ema89 or ema21 < ema89:
                        pepe_ema_break_exit = True

            if trailing_sell or stop_loss_sell or rsi_overbought_sell or vol_dump_sell or pepe_ema_break_exit or ema_death_cross_sell:
                if trailing_sell:
                    reason = f"Trailing Up (-{trailing_drop_pct*100:.1f}%)"
                elif stop_loss_sell:
                    reason = sl_type
                elif ema_death_cross_sell:
                    reason = "Potwierdzony Krzyż Śmierci EMA 21/89 (Zanikanie Trendu)"
                elif pepe_ema_break_exit:
                    reason = "PEPE EMA Trend Breakdown"
                elif rsi_overbought_sell:
                    reason = "RSI Overbought (>80)"
                else:
                    reason = "Volume Dump Exit"

                coin_name = symbol.replace(QUOTE_COIN, "")
                real_balance = get_coin_available_balance(coin_name)

                if real_balance > 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_to_sell = format_qty_with_step(real_balance, qty_step, min_qty)

                    if not qty_to_sell:
                        logging.warning(f"⚠️ Saldo {coin_name} ({real_balance}) poniżej wymaganego minimum. Usuwam z pamięci.")
                        symbols_to_delete.append(symbol)
                    else:
                        res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)

                        if res.get("retCode") == 170131:
                            logging.warning(f"⚠️ Wykryto brak ułamka środków (170131) dla {symbol}. Ponawiam próbę z korektą 0.1%...")
                            time.sleep(0.5)
                            real_balance_retry = get_coin_available_balance(coin_name) * 0.999
                            qty_to_sell_retry = format_qty_with_step(real_balance_retry, qty_step, min_qty)
                            if qty_to_sell_retry:
                                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell_retry)

                        if res.get("retCode") == 0:
                            logging.info(f"💰 [SPOT SPRZEDAŻ - {symbol}] Strategia: {strat_name} | Powód: {reason} | PnL: {total_pnl*100:.2f}% | Szczyt: {pos['peak_price']}")
                            symbols_to_delete.append(symbol)
                        else:
                            logging.error(f"❌ Błąd sprzedaży {symbol}: {res}")
                else:
                    logging.warning(f"⚠️ Brak monet {coin_name} na koncie (pozycja mogła zostać zamknięta ręcznie). Usuwam z pamięci.")
                    symbols_to_delete.append(symbol)
            else:
                logging.info(f"ℹ️ [{symbol}] [{strat_name}] PnL: {total_pnl*100:.2f}% | Szczyt: {pos['peak_price']} | Cena: {current_price}")

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
    sync_positions_from_bybit()

    while True:
        try:
            logging.info("--------------------------------------------------------------------------------")
            logging.info("🚀 ROZPOCZYNAM NOWY CYKL SKANOWANIA RYNKU SPOT USDC...")

            sync_positions_from_bybit()

            scan_for_pumps()
            run_dip_hunter_strategy()
            run_top_gainers_strategy()
            run_ema21_89_pure_crossover_strategy()
            scan_top150_for_ema_signals()
            scan_pepe_specialist()
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
        self.wfile.write(b"Bybit USDC SPOT Bot is Running 24/7!")
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
