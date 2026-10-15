import os
import time
import json
import logging
import threading
import socket
from decimal import Decimal
import math
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# OCHRONA PRZED ZAWIESZENIEM POŁĄCZEŃ SIECIOWYCH
# ==============================================================================
socket.setdefaulttimeout(15)

# ==============================================================================
# KONFIGURACJA BOTA SPOT (RYNEK USDC / USDT HYBRID)
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

print(f"🔑 DEBUG KEY: [{BYBIT_API_KEY[:5]}...] (Długość: {len(BYBIT_API_KEY)})", flush=True)
print(f"🔑 DEBUG SEC: [{BYBIT_API_SECRET[:5]}...] (Długość: {len(BYBIT_API_SECRET)})", flush=True)

TESTNET = False
CATEGORY = "spot"
QUOTE_COIN = "USDC"

# --- USTAWIENIA SKANOWANIA ---
TOP_MARKETS_COUNT = 150
TOP_DIPS_COUNT = 6
TOP_GAINERS_COUNT = 4
MAX_PUMP_POSITIONS = 4

# --- LIMITY FINANSOWE ---
STANDARD_MIN_ORDER_VALUE = 100.0  
MIN_SYNC_POSITION_VALUE = 8.0     
MIN_EXCHANGE_NOTIONAL = 5.0       

# ==============================================================================
# ZMIENNE GLOBALNE DYNAMICZNEGO RYZYKA
# ==============================================================================
MAX_TOTAL_POSITIONS = 12
MAX_EMA_CROSS_POSITIONS = 5
SENTIMENT_STATE = "NEUTRAL"

SAFE_RISK_PCT = 0.20
MODERATE_RISK_PCT = 0.15
HIGH_RISK_PCT = 0.10

BTC_FAST_DROP_BUY_BLOCK_PCT = -0.008
BTC_FLASH_CRASH_EXIT_PCT    = -0.010

EMA_CROSS_MIN_SPREAD_PCT = 0.0020  
EMA_CROSS_VOL_MULTIPLIER = 1.10    
EMA_EXIT_HYSTERESIS_PCT  = 0.0020  

DIP_RSI_5M_MAX = 30          
DIP_VOL_MULTIPLIER = 2.5     

# Zabezpieczenia Take Profit & Breakeven
PARTIAL_TP_PCT = 0.030          # Próg realizacji 50% zysków (+3.0%)
BREAKEVEN_TRIGGER_PCT = 0.015   # Próg aktywacji Breakeven (+1.5% szczytu)
BREAKEVEN_SL_PCT = 0.003        # Po TP zostawiamy +0.3% zysku jako dno

RSI_OVERBOUGHT_SELL = 82     
PUMP_VOL_MULTIPLIER = 2.5    
PUMP_PRICE_CHANGE_PCT = 0.025

APP_URL = "https://bybit-bot-i1lw.onrender.com"
POSITIONS_FILE = "active_spot_positions.json"
TRADE_HISTORY_FILE = "trade_history.json"

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
# MODUŁ KSIĘGOWANIA BILANSU I HISTORII TRANSAKCJI
# ==============================================================================
def load_trade_history():
    if os.path.exists(TRADE_HISTORY_FILE):
        try:
            with open(TRADE_HISTORY_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "total_realized_usd": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "daily": {}
    }

def save_trade_history(history):
    try:
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu historii transakcji: {e}")

trade_history = load_trade_history()

def record_closed_trade(symbol, strat, buy_price, sell_price, qty, reason):
    global trade_history
    pnl_pct = (sell_price - buy_price) / buy_price
    pnl_usd = (sell_price - buy_price) * qty
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    trade_history["total_realized_usd"] = round(trade_history.get("total_realized_usd", 0.0) + pnl_usd, 2)
    trade_history["total_trades"] = trade_history.get("total_trades", 0) + 1
    if pnl_usd >= 0:
        trade_history["wins"] = trade_history.get("wins", 0) + 1
    else:
        trade_history["losses"] = trade_history.get("losses", 0) + 1

    if "daily" not in trade_history:
        trade_history["daily"] = {}
    if today_str not in trade_history["daily"]:
        trade_history["daily"][today_str] = {"pnl_usd": 0.0, "trades": 0, "wins": 0, "losses": 0}

    d = trade_history["daily"][today_str]
    d["pnl_usd"] = round(d["pnl_usd"] + pnl_usd, 2)
    d["trades"] += 1
    if pnl_usd >= 0:
        d["wins"] += 1
    else:
        d["losses"] += 1

    save_trade_history(trade_history)
    sign = "+" if pnl_usd >= 0 else ""
    logging.info(f"📊 [BILANS ZAPISANY] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | Ilość: {qty} | Powód: {reason}")

# ==============================================================================
# ELASTYCZNE ZARZĄDZANIE BUDŻETEM ZAKUPU
# ==============================================================================
def get_order_budget(total_equity, free_usdc, target_pct, is_bearish_dip=False):
    target_val = total_equity * target_pct
    effective_target = max(target_val, STANDARD_MIN_ORDER_VALUE)

    if is_bearish_dip:
        effective_target = STANDARD_MIN_ORDER_VALUE

    if free_usdc >= effective_target:
        label = f"{target_pct * 100:.0f}%" if target_val >= STANDARD_MIN_ORDER_VALUE else f"Min. {STANDARD_MIN_ORDER_VALUE:.0f}$"
        if is_bearish_dip: label = f"Obrona w bessie ({STANDARD_MIN_ORDER_VALUE:.0f}$)"
        return round(effective_target, 2), label

    available_spend = math.floor(free_usdc * 0.995 * 100) / 100.0
    if available_spend >= MIN_EXCHANGE_NOTIONAL:
        return available_spend, f"Reszta salda ({available_spend:.2f}$)"

    return 0.0, f"Brak środków (< {MIN_EXCHANGE_NOTIONAL}$)"

# ==============================================================================
# PRECYZYJNY SILNIK FORMOWANIA ILOŚCI
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
                    base_prec = lot_filter.get("basePrecision")
                    qty_step = lot_filter.get("qtyStep")
                    min_qty = lot_filter.get("minOrderQty", "0.000001")
                    
                    eff_step = base_prec if (base_prec and str(base_prec).strip() != "") else (qty_step if qty_step else "0.000001")
                    instrument_filters[symbol] = (eff_step, min_qty)
                    return eff_step, min_qty
    except Exception as e:
        logging.warning(f"Nie udało się pobrać filtrów dla {symbol}: {e}")

    if any(m in symbol for m in ["BTC"]):
        return "0.00001", "0.00001"
    elif any(m in symbol for m in ["ETH", "SOL"]):
        return "0.0001", "0.0001"
    elif any(m in symbol for m in ["LINK", "UNI"]):
        return "0.01", "0.01"
    elif symbol.startswith("1000") or any(m in symbol for m in ["SHIB", "PEPE", "BONK", "FLOKI", "ZEREBRO", "WAL", "PYTH", "STRK"]):
        return "1", "1"
    return "0.0001", "0.0001"

def format_qty_with_step(raw_qty, qty_step_str, min_qty_str):
    try:
        qty_dec = Decimal(str(raw_qty))
        step_dec = Decimal(str(qty_step_str))
        min_dec = Decimal(str(min_qty_str))
        
        if step_dec <= 0:
            floored_dec = qty_dec
        else:
            floored_dec = (qty_dec // step_dec) * step_dec
        
        if floored_dec < min_dec and floored_dec <= 0:
            return None
        
        if floored_dec <= 0 and qty_dec > 0:
            floored_dec = qty_dec

        normalized_step = step_dec.normalize()
        exp = normalized_step.as_tuple().exponent
        
        if exp < 0:
            decimals = abs(exp)
            return f"{floored_dec:.{decimals}f}"
        else:
            return f"{int(floored_dec)}"
    except Exception as e:
        logging.error(f"Błąd formatowania ilości: {e}")
        return str(raw_qty)

# ==============================================================================
# INICJALIZACJA PUNKTU KOŃCOWEGO BYBIT
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
# ZARZĄDZANIE PAMIĘCIĄ POZYCJI
# ==============================================================================
def load_positions():
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, "r") as f:
                pos = json.load(f)
                for p in pos.values():
                    if "sold_half" not in p:
                        p["sold_half"] = False
                return pos
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
# FUNKCJE POMOCNICZE, SALDO I POBIERANIE RYNKÓW
# ==============================================================================
def get_balances():
    if not session:
        return 0.0, 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") != 0: return 0.0, 0.0

        account_list = res.get("result", {}).get("list", [])
        if not account_list: return 0.0, 0.0

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
                        wb = safe_float(c.get("walletBalance"))
                        eq = safe_float(c.get("equity"))
                        avail = safe_float(c.get("availableToWithdraw"))
                        return wb if wb > 0 else (eq if eq > 0 else avail)
    except Exception:
        pass
    return 0.0

def get_all_spot_trading_symbols():
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        return [t["symbol"] for t in tickers if "symbol" in t]
    except Exception:
        pass
    return []

def get_all_spot_usdc_symbols():
    symbols = []
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        symbols = [t["symbol"] for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN) and t.get("symbol") not in ["USDTUSDC", "USDCUSDT"]]
    except Exception:
        pass
    if not symbols:
        symbols = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "PEPEUSDC", "XRPUSDC", "ADAUSDC", "DOGEUSDC", "LINKUSDC", "NEARUSDC"]
    if "PEPEUSDC" not in symbols: symbols.append("PEPEUSDC")
    return symbols

def sync_positions_from_bybit():
    global active_positions
    if not session: return
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") != 0: return
        account_list = res.get("result", {}).get("list", [])
        if not account_list: return

        all_trading_symbols = get_all_spot_trading_symbols()
        restored = False

        for coin_info in account_list[0].get("coin", []):
            try:
                coin = coin_info.get("coin")
                if coin in ["USDC", "USDT", "USD", "EUR", "PLN", "GBP", "TRY", "UAH", "RUB"]: 
                    continue

                wb = safe_float(coin_info.get("walletBalance"))
                eq = safe_float(coin_info.get("equity"))
                bal = wb if wb > 0 else eq

                if bal <= 0: continue

                matched_symbol = None
                if f"{coin}USDC" in all_trading_symbols:
                    matched_symbol = f"{coin}USDC"
                elif f"{coin}USDT" in all_trading_symbols:
                    matched_symbol = f"{coin}USDT"

                if not matched_symbol:
                    continue

                if matched_symbol not in active_positions:
                    df = get_market_data(matched_symbol, interval="15", limit=5)
                    if df is not None and len(df) > 0:
                        curr_price = df.iloc[-1]['close']
                        val_usd = bal * curr_price
                        
                        if val_usd >= MIN_SYNC_POSITION_VALUE:
                            buy_price = curr_price
                            try:
                                exec_res = session.get_executions(category=CATEGORY, symbol=matched_symbol, limit=10)
                                if exec_res.get("retCode") == 0:
                                    exec_list = exec_res.get("result", {}).get("list", [])
                                    buys = [e for e in exec_list if e.get("side") == "Buy"]
                                    if buys: buy_price = safe_float(buys[0].get("execPrice"), curr_price)
                            except Exception:
                                pass

                            qty_step, min_qty = get_symbol_lot_filter(matched_symbol)
                            qty_str = format_qty_with_step(bal, qty_step, min_qty)
                            if qty_str:
                                active_positions[matched_symbol] = {
                                    "strategy": "AUTO_RESTORED",
                                    "buy_price": buy_price,
                                    "peak_price": max(curr_price, buy_price),
                                    "qty": qty_str,
                                    "sold_half": False
                                }
                                restored = True
                                logging.info(f"🔄 [AUTOREKUPERACJA] Przywrócono {matched_symbol}: {qty_str} monet | Wartość: ~{val_usd:.2f} USD")
            except Exception as e:
                logging.warning(f"Błąd synchronizacji monety {coin_info.get('coin')}: {e}")

        if restored: 
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd ogólny synchronizacji z Bybit: {e}")

def update_market_sentiment():
    global MAX_TOTAL_POSITIONS, MAX_EMA_CROSS_POSITIONS, SAFE_RISK_PCT, MODERATE_RISK_PCT, HIGH_RISK_PCT
    global SENTIMENT_STATE
    try:
        df = get_market_data("BTCUSDC", interval="240", limit=100)
        if df is None or len(df) < 50:
            SENTIMENT_STATE = "NEUTRAL"
            return "NEUTRAL (Brak danych)"
        
        candle = df.iloc[-1]
        c_close = candle['close']
        ema21 = candle['ema21']
        ema50 = candle['ema50']
        ema89 = candle['ema89']

        if c_close > ema50 and ema21 > ema50:
            sentiment = "BULLISH 🟢"
            SENTIMENT_STATE = "BULLISH"
            MAX_TOTAL_POSITIONS = 12
            MAX_EMA_CROSS_POSITIONS = 5
            SAFE_RISK_PCT = 0.20
            MODERATE_RISK_PCT = 0.15
            HIGH_RISK_PCT = 0.10
        elif c_close < ema89 or ema21 < ema89:
            sentiment = "BEARISH 🔴 (Defensywa)"
            SENTIMENT_STATE = "BEARISH"
            MAX_TOTAL_POSITIONS = 10
            MAX_EMA_CROSS_POSITIONS = 1
            SAFE_RISK_PCT = 0.15      
            MODERATE_RISK_PCT = 0.0   
            HIGH_RISK_PCT = 0.0       
        else:
            sentiment = "NEUTRAL 🟡"
            SENTIMENT_STATE = "NEUTRAL"
            MAX_TOTAL_POSITIONS = 10
            MAX_EMA_CROSS_POSITIONS = 3
            SAFE_RISK_PCT = 0.18
            MODERATE_RISK_PCT = 0.12
            HIGH_RISK_PCT = 0.0       
        
        logging.info(f"📊 [MAKRO SENTYMENT BTC 4H] {sentiment} | Max Pozycji: {MAX_TOTAL_POSITIONS} | Alokacje: Safe {SAFE_RISK_PCT*100:.0f}% / Mod {MODERATE_RISK_PCT*100:.0f}% / High {HIGH_RISK_PCT*100:.0f}%")
        return sentiment
    except Exception as e:
        logging.error(f"Błąd aktualizacji sentymentu: {e}")
        SENTIMENT_STATE = "NEUTRAL"
        return "UNKNOWN"

def get_btc_realtime_metrics():
    try:
        df_btc = get_market_data("BTCUSDC", interval="5", limit=6)
        if df_btc is None or len(df_btc) < 4:
            return 0.0, 0.0
        
        current_candle = df_btc.iloc[-1]
        curr_price = current_candle['close']
        curr_open = current_candle['open']

        candle_change = (curr_price - curr_open) / curr_open
        recent_15m_high = df_btc['high'].iloc[-3:].max()
        drop_from_recent_high = (curr_price - recent_15m_high) / recent_15m_high

        return candle_change, drop_from_recent_high
    except Exception as e:
        return 0.0, 0.0

def is_btc_market_healthy(drop_threshold=BTC_FAST_DROP_BUY_BLOCK_PCT):
    candle_change, drop_from_high = get_btc_realtime_metrics()
    if candle_change <= drop_threshold or drop_from_high <= drop_threshold:
        logging.warning(f"⚠️ [BTC RAPID GUARD] Spadek BTC! 5M: {candle_change*100:.2f}%, od szczytu 15M: {drop_from_high*100:.2f}%. Wstrzymuję zakupy.")
        return False
    return True

def get_top_volume_symbols(limit_count=TOP_MARKETS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN)]
        if usdc_tickers:
            sorted_tickers = sorted(usdc_tickers, key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
            res_symbols = [t["symbol"] for t in sorted_tickers[:limit_count]]
            if "PEPEUSDC" not in res_symbols: res_symbols.append("PEPEUSDC")
            return res_symbols
    except Exception:
        pass
    return get_all_spot_usdc_symbols()[:limit_count]

def get_top_losers_symbols(limit_count=TOP_DIPS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN)]
        if usdc_tickers:
            valid_losers = [t for t in usdc_tickers if safe_float(t.get("price24hPcnt")) <= -0.03]
            sorted_tickers = sorted(valid_losers, key=lambda x: safe_float(x.get("price24hPcnt")))
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception:
        pass
    return []

def get_top_gainers_symbols(limit_count=TOP_GAINERS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        usdc_tickers = [t for t in tickers if t.get("symbol", "").endswith(QUOTE_COIN) and t.get("symbol") not in ["USDTUSDC", "USDCUSDT"]]
        if usdc_tickers:
            valid_gainers = [t for t in usdc_tickers if safe_float(t.get("price24hPcnt")) >= 0.02]
            sorted_tickers = sorted(valid_gainers, key=lambda x: safe_float(x.get("price24hPcnt")), reverse=True)
            return [t["symbol"] for t in sorted_tickers[:limit_count]]
    except Exception:
        pass
    return []

# ==============================================================================
# WYZNACZANIE WSKAŹNIKÓW (Z DODATKIEM ATR DO ZARZĄDZANIA RYZYKIEM)
# ==============================================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_market_data(symbol, interval="15", limit=250):
    try:
        time.sleep(0.04)
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None

        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = calculate_rsi(df['close'], period=14)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()

        df['bb_mid'] = df['close'].rolling(window=20).mean()
        df['bb_std'] = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_mid'] + (2.0 * df['bb_std'])
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']

        # Average True Range (ATR)
        df['tr0'] = abs(df['high'] - df['low'])
        df['tr1'] = abs(df['high'] - df['close'].shift())
        df['tr2'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_pct'] = (df['atr'] / df['close']).fillna(0.02)

        return df
    except Exception as e:
        logging.error(f"Błąd pobierania danych dla {symbol}: {e}")
        return None

# ==============================================================================
# STRATEGIE 1 - 9
# ==============================================================================
def run_mtf_alignment_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)

    top_symbols = get_top_volume_symbols(80)
    logging.info(f"=== [POZ 1: NAJBEZPIECZNIEJSZA] MULTI-TIMEFRAME ALIGNMENT 4H+15M ===")

    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df_4h = get_market_data(symbol, interval="240", limit=60)
            if df_4h is None or len(df_4h) < 45: continue

            candle_4h = df_4h.iloc[-1]
            macro_uptrend = (candle_4h['close'] > candle_4h['ema50']) and (candle_4h['ema21'] > candle_4h['ema50'])
            if not macro_uptrend: continue

            df_15m = get_market_data(symbol, interval="15", limit=35)
            if df_15m is None or len(df_15m) < 20: continue

            c_15m = df_15m.iloc[-2]
            c_open, c_close, c_low = c_15m['open'], c_15m['close'], c_15m['low']
            rsi_15m = c_15m['rsi']
            
            oversold_on_15m = rsi_15m <= 36
            rebound_reaction = (c_close > c_open) or ((c_open - c_low) > abs(c_close - c_open) * 1.5)

            if oversold_on_15m and rebound_reaction:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "MTF_ALIGNMENT_4H_15M", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"💎🎯 [MTF KUPNO OK] {symbol}! Hossa 4H + Dołek 15M (RSI: {rsi_15m:.1f}) | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
                    if order_val <= 0: break
        except Exception:
            pass

def scan_top150_for_ema_signals():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)

    top_symbols = get_top_volume_symbols()
    logging.info(f"=== [POZ 2: BARDZO BEZPIECZNA] EMA TREND SCANNER ===")

    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
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

            rsi_ok = 40 <= c_rsi <= 65
            vol_ok = c_vol >= vol_ma

            if bullish_trend and in_value_zone and (bullish_pinbar or bullish_engulfing) and rsi_ok and vol_ok:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "EMA_TOP150", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"🔥 [EMA KUPNO OK] {symbol}! RSI: {c_rsi:.1f}, Vol: OK | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
                    if order_val <= 0: break
        except Exception:
            pass

def run_top_gainers_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)

    top_gainers = get_top_gainers_symbols()
    logging.info(f"=== [POZ 3: UMIARKOWANIE BEZPIECZNA] TOP GAINERS PULLBACK ===")

    if not top_gainers or order_val <= 0: return

    for symbol in top_gainers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15", limit=250)
            if df is None or len(df) < 100: continue

            candle = df.iloc[-2]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']
            
            ema9, ema21, ema89 = candle['ema9'], candle['ema21'], candle['ema89']

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
                    active_positions[symbol] = {"strategy": "TOP_GAINERS_PULLBACK", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"🚀🔥 [TOP GAINERS KUPNO OK] {symbol}! Odbicie od EMA! | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
                    if order_val <= 0: break
        except Exception:
            pass

def run_relative_strength_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)

    logging.info("=== [POZ 4: BEZPIECZNA / OBRONNA] RELATIVE STRENGTH VS BTC ===")
    if order_val <= 0: return

    try:
        df_btc = get_market_data("BTCUSDC", interval="15", limit=10)
        if df_btc is None or len(df_btc) < 6: return

        btc_close = df_btc.iloc[-2]['close']
        btc_open_1h = df_btc.iloc[-5]['open']
        btc_1h_change = (btc_close - btc_open_1h) / btc_open_1h

        if not (-0.015 <= btc_1h_change <= -0.003): return

        top_symbols = get_top_volume_symbols(TOP_MARKETS_COUNT)
        for symbol in top_symbols:
            if symbol in active_positions or symbol == "BTCUSDC": continue
            if len(active_positions) >= MAX_TOTAL_POSITIONS: break

            df_alt = get_market_data(symbol, interval="15", limit=20)
            if df_alt is None or len(df_alt) < 10: continue

            c_now = df_alt.iloc[-2]
            alt_close = c_now['close']
            alt_open_1h = df_alt.iloc[-5]['open']
            alt_1h_change = (alt_close - alt_open_1h) / alt_open_1h

            c_vol, vol_ma = c_now['volume'], c_now['vol_ma20']
            c_rsi = c_now['rsi']
            ema21 = c_now['ema21']

            alt_outperforms = (alt_1h_change >= 0.010) and (alt_close > ema21)
            vol_valid = c_vol >= (vol_ma * 1.3)
            rsi_ok = 50 <= c_rsi <= 72

            if alt_outperforms and vol_valid and rsi_ok:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / alt_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "RELATIVE_STRENGTH_BTC", "buy_price": alt_close, "peak_price": alt_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"🛡️🚀 [RS KUPNO OK] {symbol}! Rośnie (+{alt_1h_change*100:.2f}%) mimo spadku BTC! | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
                    if order_val <= 0: break
    except Exception:
        pass

def run_bb_squeeze_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)

    top_symbols = get_top_volume_symbols(TOP_MARKETS_COUNT)
    logging.info(f"=== [POZ 5: ŚREDNIE RYZYKO] BOLLINGER BANDS SQUEEZE ===")

    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15", limit=60)
            if df is None or len(df) < 40: continue

            candle = df.iloc[-2]
            c_open, c_close = candle['open'], candle['close']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']
            bb_upper = candle['bb_upper']

            recent_widths = df['bb_width'].iloc[-20:-2]
            avg_width_50 = df['bb_width'].iloc[-40:-2].mean()
            min_width = recent_widths.min()

            was_squeezed = min_width <= (avg_width_50 * 0.70)
            breakout_up = (c_close > bb_upper) and (c_close > c_open)
            vol_spike = c_vol >= (vol_ma * 2.5)
            rsi_momentum = 50 <= c_rsi <= 72

            if was_squeezed and breakout_up and vol_spike and rsi_momentum:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "BB_SQUEEZE_BREAKOUT", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"💥🚀 [BB SQUEEZE KUPNO OK] {symbol}! Wybicie po kompresji! | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
                    if order_val <= 0: break
        except Exception:
            pass

def run_ema21_89_pure_crossover_strategy():
    global active_positions

    current_cross_positions = sum(1 for p in active_positions.values() if p.get("strategy") == "EMA21_89_CROSS")
    if current_cross_positions >= MAX_EMA_CROSS_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)

    all_symbols = get_all_spot_usdc_symbols()
    logging.info(f"=== [POZ 6: ŚREDNIO-WYŻSZE RYZYKO] CZYSTY CROSS EMA 21/89 ===")

    if order_val <= 0: return

    for symbol in all_symbols:
        if symbol in active_positions: continue
        if current_cross_positions >= MAX_EMA_CROSS_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None or len(df) < 95: continue

            candle = df.iloc[-2]
            prev_candle = df.iloc[-3]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']

            ema21_now, ema89_now = candle['ema21'], candle['ema89']
            ema21_prev, ema89_prev = prev_candle['ema21'], prev_candle['ema89']

            basic_crossover = (ema21_prev <= ema89_prev) and (ema21_now > ema89_now)
            spread_pct = (ema21_now - ema89_now) / ema89_now
            has_clean_spread = spread_pct >= EMA_CROSS_MIN_SPREAD_PCT
            ema21_sloping_up = ema21_now > ema21_prev
            price_confirmed = (c_close > ema21_now) and (c_close > c_open)
            volume_confirmed = c_vol >= (vol_ma * EMA_CROSS_VOL_MULTIPLIER)

            if basic_crossover and has_clean_spread and ema21_sloping_up and price_confirmed and volume_confirmed:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "EMA21_89_CROSS", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    current_cross_positions += 1
                    save_positions(active_positions)
                    logging.info(f"⚡🔥 [EMA CROSS 21/89 KUPNO OK] {symbol}! Złoty Krzyż | Zakup: {order_val:.2f}$ ({risk_label})")

                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
                    if order_val <= 0: break
        except Exception:
            pass

def run_dip_hunter_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return

    is_bearish = SENTIMENT_STATE == "BEARISH"
    if not is_btc_market_healthy() and not is_bearish: return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT, is_bearish_dip=is_bearish)

    top_losers = get_top_losers_symbols(limit_count=6)
    logging.info(f"=== [POZ 7: WYŻSZE RYZYKO] REAL-TIME DIP HUNTER (5M) ===")

    if not top_losers or order_val <= 0: return

    for symbol in top_losers:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="5", limit=30)
            if df is None or len(df) < 25: continue

            candle = df.iloc[-1]
            c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
            c_vol, vol_ma = candle['volume'], candle['vol_ma20']
            c_rsi = candle['rsi']

            body = abs(c_close - c_open)
            lower_wick = min(c_open, c_close) - c_low
            
            strong_defense = lower_wick > (body * 2.0)
            vol_confirmed = c_vol >= (vol_ma * DIP_VOL_MULTIPLIER)
            rsi_confirmed = c_rsi <= DIP_RSI_5M_MAX

            if strong_defense and vol_confirmed and rsi_confirmed:
                order_qty_usdc = f"{order_val:.2f}"
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
                if res.get("retCode") == 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                    active_positions[symbol] = {"strategy": "DIP_HUNTER_5M", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    save_positions(active_positions)
                    logging.info(f"🔥 [DIP KUPNO OK] {symbol}! Szybkie wyłapanie na 5M! RSI: {c_rsi:.1f} | Zakup: {order_val:.2f}$ ({risk_label})")
                    
                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT, is_bearish_dip=is_bearish)
                    if order_val <= 0: break
        except Exception:
            pass

def scan_for_pumps():
    global active_positions

    pump_positions_count = sum(1 for p in active_positions.values() if p.get("strategy") == "PUMP_HUNTER")
    if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    if not is_btc_market_healthy(): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)

    all_symbols = get_all_spot_usdc_symbols()
    logging.info(f"=== [POZ 8: WYSOKIE RYZYKO] MOMENTUM PUMP HUNTER ===")

    if order_val <= 0: return

    for symbol in all_symbols:
        if symbol in active_positions: continue
        if pump_positions_count >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: break

        try:
            df = get_market_data(symbol, interval="15")
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
                    active_positions[symbol] = {"strategy": "PUMP_HUNTER", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                    pump_positions_count += 1
                    save_positions(active_positions)
                    logging.info(f"🚀 [PUMP KUPNO OK] {symbol}! Wzrost: +{price_change*100:.2f}% | Zakup: {order_val:.2f}$ ({risk_label})")
                    
                    total_equity, free_usdc = get_balances()
                    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)
                    if order_val <= 0: break
                else:
                    logging.error(f"❌ Błąd zakupu {symbol}: {res}")

        except Exception:
            pass

def scan_pepe_specialist():
    global active_positions
    pepe_symbol = "PEPEUSDC"

    if pepe_symbol in active_positions or len(active_positions) >= MAX_TOTAL_POSITIONS:
        return

    if not is_btc_market_healthy(drop_threshold=-0.010): return

    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)

    logging.info(f"=== [POZ 9: NAJBARDZIEJ RYZYKOWNA / MEME] PEPE EMA SPECIALIST ===")

    if order_val <= 0: return

    try:
        df = get_market_data(pepe_symbol, interval="15", limit=250)
        if df is None or len(df) < 200: return

        candle = df.iloc[-2]
        prev_candle = df.iloc[-3]
        c_open, c_close, c_low = candle['open'], candle['close'], candle['low']
        c_vol, vol_ma = candle['volume'], candle['vol_ma20']
        c_rsi = candle['rsi']
        
        ema21, ema89, ema200 = candle['ema21'], candle['ema89'], candle['ema200']
        prev_ema21, prev_ema89 = prev_candle['ema21'], prev_candle['ema89']

        macro_trend_ok = (c_close > ema200) and (ema21 > ema200)
        ema_spread = (ema21 - ema89) / ema89
        ema_rising = ema21 > prev_ema21
        
        ema_crossover = (prev_ema21 <= prev_ema89) and (ema21 > ema89) and (ema_spread >= 0.0015) and ema_rising
        ema_bounce = (ema21 > ema89) and (c_low <= ema21) and (c_close > ema21) and (c_close > c_open) and ema_rising

        rsi_valid = 45 <= c_rsi <= 70
        volume_ok = c_vol >= vol_ma

        if macro_trend_ok and (ema_crossover or ema_bounce) and rsi_valid and volume_ok:
            order_qty_usdc = f"{order_val:.2f}"
            res = session.place_order(category=CATEGORY, symbol=pepe_symbol, side="Buy", orderType="Market", qty=order_qty_usdc)
            if res.get("retCode") == 0:
                qty_step, min_qty = get_symbol_lot_filter(pepe_symbol)
                qty_str = format_qty_with_step(order_val / c_close, qty_step, min_qty) or "1"
                active_positions[pepe_symbol] = {"strategy": "PEPE_EMA_SPECIALIST", "buy_price": c_close, "peak_price": c_close, "qty": qty_str, "sold_half": False}
                save_positions(active_positions)
                s_type = "Złoty Krzyż EMA21/89" if ema_crossover else "Odbicie EMA21"
                logging.info(f"🐸🔥 [PEPE KUPNO OK] {s_type} | Zakup: {order_val:.2f}$ ({risk_label})")

    except Exception:
        pass

# ==============================================================================
# MONITOROWANIE I SPRZEDAŻ (Z OCHRONĄ PRZED KRWOTOKIEM BTC I DYNAMICZNYM ATR)
# ==============================================================================
def monitor_and_close_positions():
    global active_positions
    if not active_positions:
        logging.info("ℹ️ Brak otwartych pozycji SPOT do monitorowania.")
        return

    btc_5m_change, btc_drop_15m = get_btc_realtime_metrics()
    btc_flash_crash = (btc_5m_change <= BTC_FLASH_CRASH_EXIT_PCT) or (btc_drop_15m <= BTC_FLASH_CRASH_EXIT_PCT)
    if btc_flash_crash:
        logging.warning(f"🚨 [BTC FLASH CRASH DETECTED] Spadek BTC {min(btc_5m_change, btc_drop_15m)*100:.2f}% (Próg: {BTC_FLASH_CRASH_EXIT_PCT*100:.1f}%)! Awaryjna ewakuacja stratnych altów!")

    logging.info(f"=== [MONITOROWANIE POZYCJI: {len(active_positions)}/{MAX_TOTAL_POSITIONS} AKTYWNYCH] ===")
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
            
            atr_val = safe_float(candle.get('atr_pct'), 0.02)
            atr_pct = atr_val if (atr_val > 0 and not math.isnan(atr_val)) else 0.02

            buy_price = pos["buy_price"]
            peak_price = pos.get("peak_price", buy_price)
            strat_name = pos.get("strategy", "")
            sold_half = pos.get("sold_half", False)

            if current_price > peak_price:
                pos["peak_price"] = current_price
                save_positions(active_positions)
                logging.info(f"📈 [{symbol}] Nowy szczyt: {current_price}")

            dynamic_sl_pct = max(atr_pct * 1.8, 0.015)
            trailing_drop_pct = max(atr_pct * 1.5, 0.010)

            drop_from_peak = (pos["peak_price"] - current_price) / pos["peak_price"]
            total_pnl = (current_price - buy_price) / buy_price
            peak_pnl = (pos["peak_price"] - buy_price) / buy_price

            # MECHANIZM PARTIAL TAKE PROFIT (50% przy +3.0%)
            if not sold_half and total_pnl >= PARTIAL_TP_PCT:
                coin_name = symbol.replace("USDC", "").replace("USDT", "")
                real_balance = get_coin_available_balance(coin_name)
                
                if real_balance > 0:
                    half_qty = real_balance / 2.0
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_to_sell = format_qty_with_step(half_qty, qty_step, min_qty)

                    if qty_to_sell:
                        res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)
                        if res.get("retCode") == 0:
                            pos["sold_half"] = True
                            save_positions(active_positions)
                            sold_qty_val = safe_float(qty_to_sell)
                            record_closed_trade(symbol, strat_name, buy_price, current_price, sold_qty_val, "Partial TP (50%)")
                            logging.info(f"💸 [PARTIAL TP] {symbol} wygenerowało zysk! Zabezpieczam 50% pozycji przy +{total_pnl*100:.2f}%. Reszta jedzie na trailingu.")

            if sold_half or peak_pnl >= BREAKEVEN_TRIGGER_PCT:
                breakeven_price = buy_price * (1.0 + BREAKEVEN_SL_PCT)
                stop_loss_sell = current_price <= breakeven_price
                sl_type = "Breakeven SL (+0.3%)"
            else:
                stop_loss_sell = total_pnl <= -dynamic_sl_pct
                sl_type = f"Dynamic ATR Stop Loss (-{dynamic_sl_pct*100:.1f}%)"

            trailing_sell = drop_from_peak >= trailing_drop_pct and current_price > buy_price

            ema_death_cross_sell = False
            pepe_ema_break_exit = False
            rsi_overbought_sell = False
            vol_dump_sell = False

            if strat_name == "EMA21_89_CROSS":
                ema21 = candle['ema21']
                ema89 = candle['ema89']
                hysteresis_exit = ema21 < (ema89 * (1.0 - EMA_EXIT_HYSTERESIS_PCT))
                two_candles_below_ema89 = (current_price < ema89) and (prev_candle['close'] < prev_candle['ema89'])
                if hysteresis_exit or two_candles_below_ema89:
                    ema_death_cross_sell = True
            elif "PEPE" in symbol:
                ema89 = candle['ema89']
                ema21 = candle['ema21']
                if current_price < ema89 or ema21 < ema89:
                    pepe_ema_break_exit = True
            else:
                rsi_overbought_sell = current_rsi >= RSI_OVERBOUGHT_SELL and total_pnl > 0
                vol_dump_sell = (c_vol > vol_ma * 2.0) and (candle['close'] < candle['open']) and (total_pnl < -0.01)

            btc_emergency_exit = btc_flash_crash and (total_pnl < -0.005)

            if trailing_sell or stop_loss_sell or rsi_overbought_sell or vol_dump_sell or pepe_ema_break_exit or ema_death_cross_sell or btc_emergency_exit:
                if btc_emergency_exit: reason = "BTC Flash Crash Circuit Breaker"
                elif trailing_sell: reason = f"Dynamic ATR Trailing Up (-{trailing_drop_pct*100:.1f}%)"
                elif stop_loss_sell: reason = sl_type
                elif ema_death_cross_sell: reason = "Krzyż Śmierci EMA 21/89"
                elif pepe_ema_break_exit: reason = "PEPE EMA Trend Breakdown"
                elif rsi_overbought_sell: reason = "RSI Overbought (>82)"
                else: reason = "Volume Dump Exit"

                coin_name = symbol.replace("USDC", "").replace("USDT", "")
                real_balance = get_coin_available_balance(coin_name)

                if real_balance > 0:
                    qty_step, min_qty = get_symbol_lot_filter(symbol)
                    qty_to_sell = format_qty_with_step(real_balance, qty_step, min_qty)

                    if not qty_to_sell:
                        logging.warning(f"⚠️ Saldo {coin_name} ({real_balance}) poniżej minimalnego kroku giełdy. Usuwam z pamięci bota.")
                        symbols_to_delete.append(symbol)
                    else:
                        res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell)
                        if res.get("retCode") == 170131:
                            time.sleep(0.5)
                            real_balance_retry = get_coin_available_balance(coin_name) * 0.999
                            qty_to_sell_retry = format_qty_with_step(real_balance_retry, qty_step, min_qty)
                            if qty_to_sell_retry:
                                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_to_sell_retry)

                        if res.get("retCode") == 0:
                            sold_qty_val = safe_float(qty_to_sell)
                            record_closed_trade(symbol, strat_name, buy_price, current_price, sold_qty_val, reason)
                            logging.info(f"💰 [SPOT SPRZEDAŻ - FULL] {symbol} | Powód: {reason} | PnL: {total_pnl*100:.2f}%")
                            symbols_to_delete.append(symbol)
                        else:
                            logging.error(f"❌ Błąd sprzedaży {symbol}: {res} — pozycja pozostaje w pamięci.")
                else:
                    logging.warning(f"⚠️ Saldo {coin_name} zwrócone jako 0. Nie usuwam pozycji bez udanej sprzedaży.")
            else:
                logging.info(f"ℹ️ [{symbol}] PnL: {total_pnl*100:.2f}% | Szczyt: {pos['peak_price']}")

        except Exception as e:
            logging.error(f"Błąd monitorowania {symbol}: {e}")

    for s in symbols_to_delete:
        del active_positions[s]
    if symbols_to_delete:
        save_positions(active_positions)

# ==============================================================================
# PĘTLA GŁÓWNA - 45 SEKUNDOWY CYKL PRACY Z WYŚWIETLANIEM BILANSU
# ==============================================================================
def bot_loop():
    sync_positions_from_bybit()

    while True:
        try:
            logging.info("--------------------------------------------------------------------------------")
            logging.info("🚀 ROZPOCZYNAM NOWY CYKL SKANOWANIA RYNKU SPOT...")

            current_sentiment = update_market_sentiment()
            
            total_equity, free_usdc = get_balances()

            # Odczyt i kalkulacja bilansu handlowego (format zgodny z Python 3.14)
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_stats = trade_history.get("daily", {}).get(today_str, {"pnl_usd": 0.0, "trades": 0, "wins": 0, "losses": 0})
            total_realized = trade_history.get("total_realized_usd", 0.0)
            total_trades = trade_history.get("total_trades", 0)
            wins = trade_history.get("wins", 0)
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
            today_sign = "+" if today_stats['pnl_usd'] >= 0 else ""
            total_sign = "+" if total_realized >= 0 else ""

            logging.info(f"📈 [BILANS HANDLOWY] Dziś: {today_sign}{today_stats['pnl_usd']:.2f} USD ({today_stats['trades']} transakcji) | Łącznie: {total_sign}{total_realized:.2f} USD | Win Rate: {win_rate:.1f}% ({wins}/{total_trades})")
            logging.info(f"💰 [PORTFEL UTA] Całkowita wartość: {total_equity:.2f} USD | Wolne środki: {free_usdc:.2f} USDC")

            sync_positions_from_bybit()

            if is_btc_market_healthy() or "BEARISH" in current_sentiment:
                run_mtf_alignment_strategy()           # Poziom 1
                scan_top150_for_ema_signals()          # Poziom 2
                run_relative_strength_strategy()       # Poziom 4
                run_dip_hunter_strategy()              # Poziom 7

                if "BEARISH" not in current_sentiment:
                    run_top_gainers_strategy()             # Poziom 3
                    run_bb_squeeze_strategy()              # Poziom 5
                    run_ema21_89_pure_crossover_strategy() # Poziom 6
                else:
                    logging.info("🛡️ [TRYB BEARISH] Rynek w spadkach! Pomijam średnie ryzyko. Łowię tylko duże dołki 5M i twarde hossy 4H.")

                if "BULLISH" in current_sentiment:
                    scan_for_pumps()                       # Poziom 8
                    scan_pepe_specialist()                 # Poziom 9
                elif "BEARISH" not in current_sentiment:
                    logging.info("⏸️ [TRYB NEUTRAL] Rynek niezdecydowany. Pomijam pompy i memecoiny.")
            else:
                logging.info("⛔ [ZAKUPY WSTRZYMANE] Wykryto krwotok na BTC. Czekam na uspokojenie sytuacji.")

            monitor_and_close_positions()

            logging.info("💤 CYKL ZAKOŃCZONY. Następna szybka weryfikacja za 45 sekund...")
            logging.info("--------------------------------------------------------------------------------")
        except Exception as e:
            logging.error(f"Błąd pętli bota: {e}")
        time.sleep(45)

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
        self.wfile.write(b"Bybit Spot Trading Bot is Running 24/7!")
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
