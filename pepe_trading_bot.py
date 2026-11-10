import concurrent.futures
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import socket
import sys
import threading
import time
import pandas as pd
from pybit.unified_trading import HTTP
import requests
from requests.adapters import HTTPAdapter

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# ==============================================================================
# SYNCHRONIZACJA CZASU I POŁĄCZENIE
# ==============================================================================
socket.setdefaulttimeout(15)
server_time_offset_sec = 0.0

def sync_bybit_clock():
    global server_time_offset_sec
    for url in ["https://api.bybit.eu/v5/market/time", "https://api.bybit.com/v5/market/time"]:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("retCode") == 0:
                    server_ms = int(data["result"]["timeNano"]) // 1_000_000
                    local_ms = int(time.time() * 1000)
                    server_time_offset_sec = (server_ms - local_ms) / 1000.0
                    logging.info(f"⏱️ Zegar Bybit zsynchronizowany (Offset: {server_time_offset_sec*1000:+.1f} ms)")
                    return True
        except Exception:
            pass
    return False

# ==============================================================================
# KONFIGURACJA GŁÓWNA I PARAMETRY SKANERA
# ==============================================================================
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "TUTAJ_TWÓJ_KEY").strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "TUTAJ_TWÓJ_SECRET").strip()

TESTNET = False
CATEGORY = "spot"
QUOTE_COIN = "USDC"

EXCLUDED_BASE_COINS = {
    "USDT", "EURC", "USD", "DAI", "FDUSD", "TUSD", "BUSD", "USDE", "PYUSD", "USDD"
}

CAPITAL_PER_TRADE_PCT = 0.45       # 45% salda na pozycję (max 2 pozycje)
MAX_TOTAL_POSITIONS = 2            
STANDARD_MIN_ORDER_VALUE = 15.0    
MIN_EXCHANGE_NOTIONAL = 5.2        

MIN_24H_TURNOVER_USD = 60000.0     
USDC_MAX_SPREAD_PCT = 0.0050       
MAX_SCAN_CANDIDATES = 35           

# PARAMETRY ZAPADKI DOLAROWEJ ZE SCHODKIEM CO 5 USD
DOLLAR_STEP_USD = 5.0              # Krok bazowy: 5$, 10$, 15$, 20$...

# AGRESYWNE CIĘCIE PŁASKICH POZYCJI (FLAT MARKET KILLER)
FLAT_MAX_TIME_SEC = 600            # Max 10 minut na potwierdzenie kierunku
FLAT_MIN_GAIN_PCT = 0.0040         # Wymóg min. +0.40% po 10 minutach
MID_TIME_SEC = 900                 # Max 15 minut
MID_MIN_GAIN_PCT = 0.0080          # Wymóg min. +0.80% po 15 minutach
STAGNATION_ROTATION_TIME_SEC = 420 # 7 minut bezruchu pozwala na rotację

# DOLAROWY TRAILING PORTFELA (BASKET LOCK)
USD_PROFIT_TRIGGER = 20.0          
USD_TRAILING_DROP = 5.0            
basket_peak_profit_usd = 0.0       

APP_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://bybit-bot-i1lw.onrender.com")
POSITIONS_FILE = "active_spot_positions.json"
TRADE_HISTORY_FILE = "trade_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

lock_pos = threading.RLock()
lock_history = threading.RLock()
instrument_filters = {}

sync_bybit_clock()

endpoints = [("EU Region / MiCA", "https://api.bybit.eu"), ("Global Mainnet", "https://api.bybit.com")]
session = None
for name, url in endpoints:
    try:
        s = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
        s.endpoint = url
        res = s.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0:
            session = s
            logging.info(f"✅ POŁĄCZONO: {name}")
            break
    except Exception:
        pass

if not session:
    session = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
    session.endpoint = "https://api.bybit.eu"

adapter = HTTPAdapter(pool_connections=40, pool_maxsize=40)
if hasattr(session, "client") and isinstance(session.client, requests.Session):
    session.client.mount("https://", adapter)
    session.client.mount("http://", adapter)

def safe_float(val, default=0.0):
    try:
        if val is None or str(val).strip() == "":
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def get_base_coin(symbol: str) -> str:
    if symbol.endswith(QUOTE_COIN):
        return symbol[:-len(QUOTE_COIN)]
    return symbol

def get_symbol_lot_filter(symbol):
    if symbol in instrument_filters:
        return instrument_filters[symbol]
    try:
        res = session.get_instruments_info(category=CATEGORY, symbol=symbol)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            lot = res["result"]["list"][0].get("lotSizeFilter", {})
            step = lot.get("basePrecision") or lot.get("qtyStep") or "0.000001"
            min_q = lot.get("minOrderQty", "0.000001")
            instrument_filters[symbol] = (step, min_q)
            return instrument_filters[symbol]
    except Exception as e:
        logging.error(f"Błąd lot filter dla {symbol}: {e}")
    return "0.0001", "0.0001"

def format_qty_with_step(raw_qty, qty_step_str, min_qty_str):
    try:
        qty_dec = Decimal(str(raw_qty))
        step_dec = Decimal(str(qty_step_str))
        min_dec = Decimal(str(min_qty_str))
        floored = (qty_dec // step_dec) * step_dec if step_dec > 0 else qty_dec
        if floored < min_dec:
            return None
        norm = step_dec.normalize()
        exp = norm.as_tuple().exponent
        return f"{floored:.{abs(exp)}f}" if exp < 0 else f"{int(floored)}"
    except Exception:
        return None

# ==============================================================================
# MODUŁ ZAPISU I HISTORII
# ==============================================================================
def load_positions():
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_positions(positions):
    with lock_pos:
        try:
            with open(POSITIONS_FILE, "w") as f:
                json.dump(positions, f, indent=4)
        except Exception as e:
            logging.error(f"Błąd zapisu pozycji: {e}")

active_positions = load_positions()

def load_trade_history():
    if os.path.exists(TRADE_HISTORY_FILE):
        try:
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
                if "trades_log" not in data:
                    data["trades_log"] = []
                return data
        except Exception:
            pass
    return {"total_realized_usd": 0.0, "total_trades": 0, "wins": 0, "losses": 0, "trades_log": [], "daily": {}}

def save_trade_history(history):
    with lock_history:
        try:
            with open(TRADE_HISTORY_FILE, "w") as f:
                json.dump(history, f, indent=4)
        except Exception as e:
            logging.error(f"Błąd zapisu historii: {e}")

trade_history = load_trade_history()

def record_closed_trade(symbol, strat, buy_price, sell_price, qty, reason, is_maker=False):
    global trade_history
    with lock_history:
        raw_pnl_usd = (sell_price - buy_price) * qty
        fee_buy = (buy_price * qty) * 0.001
        fee_sell = (sell_price * qty) * (0.0006 if is_maker else 0.001)
        estimated_fee = fee_buy + fee_sell
        
        pnl_usd = round(raw_pnl_usd - estimated_fee, 2)
        pnl_pct = (pnl_usd / (buy_price * qty)) if (buy_price * qty) > 0 else 0.0
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = time.time()

        trade_history["total_realized_usd"] = round(trade_history.get("total_realized_usd", 0.0) + pnl_usd, 2)
        trade_history["total_trades"] = trade_history.get("total_trades", 0) + 1

        if pnl_usd >= 0:
            trade_history["wins"] = trade_history.get("wins", 0) + 1
        else:
            trade_history["losses"] = trade_history.get("losses", 0) + 1

        if "trades_log" not in trade_history:
            trade_history["trades_log"] = []
        trade_history["trades_log"].append({
            "timestamp": now_ts, "symbol": symbol, "turnover": round((buy_price + sell_price) * qty, 2),
            "fee": round(estimated_fee, 4), "gross_pnl": round(raw_pnl_usd, 2), "net_pnl": pnl_usd,
            "reason": reason
        })

        if len(trade_history["trades_log"]) > 250:
            trade_history["trades_log"] = trade_history["trades_log"][-250:]

        save_trade_history(trade_history)
        sign = "+" if pnl_usd >= 0 else ""
        logging.info(f"📊 [BILANS TRANSAKCJI] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | {reason}")

# ==============================================================================
# WSKAŹNIKI I DANE RYNKOWE
# ==============================================================================
def get_all_spot_tickers():
    try:
        res = session.get_tickers(category=CATEGORY)
        if res.get("retCode") == 0:
            return {item["symbol"]: item for item in res.get("result", {}).get("list", [])}
    except Exception as e:
        logging.warning(f"Błąd pobierania tickerów: {e}")
    return {}

def get_balances():
    if not session:
        return 0.0, 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            ad = res["result"]["list"][0]
            te = safe_float(ad.get("totalEquity"))
            fu = 0.0
            for c in ad.get("coin", []):
                if c.get("coin") == QUOTE_COIN:
                    avail = c.get("availableToWithdraw")
                    fu = safe_float(avail) if avail else safe_float(c.get("equity"))
                    break
            return te, fu
    except Exception:
        pass
    return 0.0, 0.0

def get_coin_available_balance(coin_name):
    if not session:
        return 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED", coin=coin_name)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            for c in res["result"]["list"][0].get("coin", []):
                if c.get("coin") == coin_name:
                    return max(safe_float(c.get("walletBalance")), safe_float(c.get("availableToWithdraw")))
    except Exception:
        pass
    return 0.0

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def get_market_data_no_cache(symbol, interval="15", limit=200):
    try:
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles:
            return None
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

        df['bb_mid'] = df['close'].rolling(20).mean()
        df['bb_std'] = df['close'].rolling(20).std()
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        df['bb_upper'] = df['bb_mid'] + (2.0 * df['bb_std'])
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']
        
        return df
    except Exception:
        return None

def fetch_data_worker(symbol):
    return symbol, get_market_data_no_cache(symbol, interval="15", limit=200)

def is_btc_market_healthy():
    try:
        df_btc = get_market_data_no_cache("BTCUSDC", interval="5", limit=5)
        if df_btc is not None and len(df_btc) >= 3:
            c = df_btc.iloc[-1]
            candle_change = (c['close'] - c['open']) / c['open']
            if candle_change <= -0.015:
                logging.warning(f"⚠️ [BTC GUARD] Tąpnięcie 5M na BTC: {candle_change*100:.2f}%. Wstrzymuję zakupy.")
                return False
    except Exception:
        pass
    return True

# ==============================================================================
# SYNCHRONIZACJA PORTFELA
# ==============================================================================
def sync_positions_from_bybit():
    global active_positions
    if not session:
        return
    try:
        with lock_pos:
            res = session.get_wallet_balance(accountType="UNIFIED")
            if res.get("retCode") != 0 or not res.get("result", {}).get("list"):
                return
            
            real_holdings = {}
            for coin_info in res["result"]["list"][0].get("coin", []):
                coin = coin_info.get("coin")
                if coin in ["USDC", "USDT", "EURC", "USD", "EUR"]:
                    continue
                bal = max(safe_float(coin_info.get("walletBalance")), safe_float(coin_info.get("equity")))
                if bal > 0:
                    real_holdings[f"{coin}{QUOTE_COIN}"] = bal

            to_remove = []
            for sym, pos in list(active_positions.items()):
                if not sym.endswith(QUOTE_COIN) or real_holdings.get(sym, 0.0) <= 0:
                    to_remove.append((sym, "brak salda na Bybit"))

            for sym, reason in to_remove:
                if sym in active_positions:
                    del active_positions[sym]
                    logging.info(f"🧹 [SYNC] Usunięto {sym} ({reason}).")

            for sym, bal in real_holdings.items():
                if sym not in active_positions:
                    tickers = get_all_spot_tickers()
                    curr_p = safe_float(tickers.get(sym, {}).get("lastPrice"), 0.0)
                    if (bal * curr_p) >= MIN_EXCHANGE_NOTIONAL:
                        step, minq = get_symbol_lot_filter(sym)
                        q_str = format_qty_with_step(bal, step, minq)
                        if q_str:
                            active_positions[sym] = {
                                "strategy": "SYNCED",
                                "buy_price": curr_p,
                                "peak_price": curr_p,
                                "qty": q_str,
                                "initial_qty": q_str,
                                "trailing_sl": curr_p * 0.985,
                                "entry_time": time.time(),
                                "is_downtrend": False,
                                "locked_dollar_floor": 0.0,
                                "peak_pnl_usd": 0.0
                            }
                            logging.info(f"🎯 [SYNC] Zaimportowano pozycję {sym} | Ilość: {q_str}")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji Bybit: {e}")

# ==============================================================================
# DYNAMICZNY SKANER CAŁEGO RYNKU SPOT USDC
# ==============================================================================
def get_dynamic_usdc_targets(all_tickers):
    valid_candidates = []
    for symbol, t_data in all_tickers.items():
        if not symbol.endswith(QUOTE_COIN):
            continue

        base = get_base_coin(symbol)
        if base in EXCLUDED_BASE_COINS:
            continue

        turnover_24h = safe_float(t_data.get("turnover24h", 0.0))
        if turnover_24h < MIN_24H_TURNOVER_USD:
            continue

        bid = safe_float(t_data.get("bid1Price"))
        ask = safe_float(t_data.get("ask1Price"))
        if bid <= 0 or ask <= 0:
            continue
        spread = (ask - bid) / bid
        if spread > USDC_MAX_SPREAD_PCT:
            continue

        valid_candidates.append({
            "symbol": symbol,
            "turnover": turnover_24h,
            "price24hPcnt": safe_float(t_data.get("price24hPcnt", 0.0))
        })

    valid_candidates.sort(key=lambda x: x["turnover"], reverse=True)
    return [c["symbol"] for c in valid_candidates[:MAX_SCAN_CANDIDATES]]

# ==============================================================================
# STRATEGIA ŁOWCY BONUSÓW + LIVE CAPITULATION SNIPER
# ==============================================================================
def run_bonus_hunter_strategy():
    global active_positions
    total_equity, free_usdc = get_balances()
    now_ts = time.time()

    all_tickers = get_all_spot_tickers()
    if not all_tickers:
        return

    scanned_coins = get_dynamic_usdc_targets(all_tickers)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_data_worker, symbol): symbol for symbol in scanned_coins}
        for future in concurrent.futures.as_completed(futures):
            sym, df = future.result()
            if df is not None:
                results[sym] = df

    candidates = []

    for symbol in scanned_coins:
        with lock_pos:
            if symbol in active_positions:
                continue

        df = results.get(symbol)
        if df is None or len(df) < 60:
            continue

        try:
            c_now = df.iloc[-1]   # Świeca bieżąca (na żywo)
            c = df.iloc[-2]       # Poprzednia zamknięta
            p = df.iloc[-3]
            p2 = df.iloc[-4]

            curr_price = c_now['close']
            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0

            # ==================================================================
            # 1. LIVE CAPITULATION SNIPER (ŁAPANIE DOŁKA DŁUGIEJ ŚWIECY W LOCIE)
            # ==================================================================
            # Badamy zrzut albo na świecy bieżącej (c_now), albo świeżo zamkniętej (c)
            # Scenariusz A: Świeca bieżąca tąpnęła i właśnie dynamicznie zawraca w górę
            now_range = c_now['high'] - c_now['low']
            now_drop_pct = (c_now['high'] - c_now['low']) / c_now['high'] if c_now['high'] > 0 else 0.0
            rebound_from_bottom_pct = (curr_price - c_now['low']) / c_now['low'] if c_now['low'] > 0 else 0.0
            lower_wick_now = min(c_now['open'], curr_price) - c_now['low']

            live_capitulation_active = (
                (now_drop_pct >= 0.022) and                      # Tąpnięcie min. 2.2%
                (rebound_from_bottom_pct >= 0.0070) and          # Odbicie od dna o min. +0.70%
                (now_range > 0 and (lower_wick_now / now_range) >= 0.35) and # Dolny knot >= 35%
                (curr_price > c_now['open'] * 0.998)             # Popyt z powrotem przejmuje kontrolę
            )

            # Scenariusz B: Świeca c była panicznym zrzutem, a c_now wybija w górę
            c_red = c['close'] < c['open']
            c_range = c['high'] - c['low']
            c_lower_wick = min(c['open'], c['close']) - c['low']
            c_drop_pct = (c['open'] - c['close']) / c['open'] if c_red else 0.0
            closed_capitulation_absorbed = (
                c_red and (c_drop_pct >= 0.020) and
                (vol_ratio >= 2.0) and
                (c_range > 0 and (c_lower_wick / c_range) >= 0.30) and
                (curr_price > c['low'] * 1.006) and
                (c_now['close'] >= c_now['open'])
            )

            is_capitulation_snipe = live_capitulation_active or closed_capitulation_absorbed

            # ==================================================================
            # 2. BEZPIECZNIKI STANDARDOWYCH WEJŚĆ (BLOKADA PUŁAPEK)
            # ==================================================================
            p_red = p['close'] < p['open']
            p_drop_pct = (p['open'] - p['close']) / p['open'] if p_red else 0.0
            p_vol_ratio = (p['volume'] / p['vol_ma20']) if (p['vol_ma20'] and p['vol_ma20'] > 0) else 1.0

            dump_in_recent = (p_red and (p_drop_pct >= 0.012 or p_vol_ratio >= 1.35))
            if dump_in_recent and not is_capitulation_snipe:
                if c['close'] <= p['high']:
                    continue

            candle_range = c['high'] - c['low']
            upper_wick = c['high'] - max(c['open'], c['close'])
            if candle_range > 0 and (upper_wick / candle_range > 0.28) and not is_capitulation_snipe:
                continue

            # ==================================================================
            # 3. POZOSTAŁE STRATEGIE (TREND RUNNER, BREAKOUT, SWING)
            # ==================================================================
            is_bull_market = c['close'] > c['ema200']
            is_stacked_bull = is_bull_market and (c['close'] > c['ema50']) and (c['ema9'] > c['ema21'])

            high_4h = df['high'].iloc[-17:-2].max()
            is_golden_breakout = (
                is_bull_market and
                (c['close'] > high_4h) and
                (c['close'] > c['open']) and
                (vol_ratio >= 1.25) and
                (c['rsi'] <= 75.0)
            )

            tested_ema9_or_21 = (p['low'] <= p['ema9'] * 1.003) or (c['low'] <= c['ema9'] * 1.003) or (p['low'] <= p['ema21'] * 1.003)
            dist_from_ema9 = (c['close'] - c['ema9']) / c['ema9']
            is_trend_runner = (
                is_stacked_bull and
                tested_ema9_or_21 and
                (c['close'] > c['open']) and
                (c['close'] >= c['ema9'] * 0.999) and
                (dist_from_ema9 <= 0.016) and
                (c['rsi'] <= 70.0) and
                (vol_ratio >= 0.85)
            )

            bb_width = c.get('bb_width', 0.0)
            recent_low_4 = df['low'].iloc[-5:-1].min()
            recent_bb_lower = df['bb_lower'].iloc[-5:-1].min()
            recent_rsi_min = df['rsi'].iloc[-5:-1].min()
            in_deep_trough = (recent_low_4 <= recent_bb_lower * 1.003) or (recent_rsi_min <= 38.0)

            lower_wick = min(c['open'], c['close']) - c['low']
            is_hammer = (candle_range > 0) and ((lower_wick / candle_range) >= 0.38)
            is_engulfing = (c['close'] > p['open']) and (p['close'] < p['open'])
            is_staircase = (c['low'] >= p['low']) and (c['close'] > p['close']) and (c['close'] >= c['ema9'] * 0.999)

            is_swing_reversal = (
                is_bull_market and              # Twardy wymóg: tylko w rynku wzrostowym!
                (bb_width >= 0.020) and
                in_deep_trough and
                (c['close'] > c['open']) and
                (is_hammer or is_engulfing or is_staircase) and
                (c['close'] <= c['bb_mid'] * 1.003) and
                (vol_ratio >= 0.80)
            )

            if not is_capitulation_snipe and not is_golden_breakout and not is_trend_runner and not is_swing_reversal:
                continue

            live_holding = c_now['low'] >= min(c['low'], p['low']) * 0.997
            live_not_dumping = (c_now['close'] >= c_now['open']) or is_capitulation_snipe
            if not (live_holding and live_not_dumping):
                continue

            ret_1h = (c['close'] - df['close'].iloc[-5]) / df['close'].iloc[-5]

            if is_capitulation_snipe:
                tag = "CAPITULATION_SNIPER"
                score = 400.0 + (rebound_from_bottom_pct * 1000.0) # Bezwzględny priorytet dla złapanego dołka!
            elif is_golden_breakout:
                tag = "GOLDEN_BREAKOUT"
                score = 300.0 + (ret_1h * 1000.0) + (vol_ratio * 40.0)
            elif is_trend_runner:
                tag = "TREND_RUNNER"
                score = 200.0 + (ret_1h * 800.0) + (vol_ratio * 30.0)
            else:
                tag = "SWING_REVERSAL"
                score = 100.0 + ((100.0 - c['rsi']) * 1.5)

            calculated_swing_low = min(c_now['low'], c['low']) if is_capitulation_snipe else min(c['low'], p['low'], c_now['low'])

            candidates.append({
                "symbol": symbol, "score": score, "tag": tag, "df": df,
                "is_downtrend": not is_bull_market,
                "swing_low": calculated_swing_low,
                "ret_1h": ret_1h
            })

        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    if not candidates:
        return

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best_candidate = candidates[0]

    cand_sym = best_candidate["symbol"]
    cand_tag = best_candidate["tag"]
    cand_df = best_candidate["df"]
    is_down = best_candidate["is_downtrend"]
    swing_low = best_candidate["swing_low"]

    with lock_pos:
        current_count = len(active_positions)
        current_items = list(active_positions.items())

    # SCENARIUSZ 1: WOLNY SLOT
    if current_count < MAX_TOTAL_POSITIONS:
        position_pct = CAPITAL_PER_TRADE_PCT if cand_tag == "CAPITULATION_SNIPER" else ((CAPITAL_PER_TRADE_PCT * 0.60) if is_down else CAPITAL_PER_TRADE_PCT)
        target_allocation = total_equity * position_pct

        if free_usdc < (target_allocation * 0.98):
            return

        c_now = cand_df.iloc[-1]
        curr_price = c_now['close']
        
        # Przy łapaniu kapitulacji Stop Loss tuż pod dołkiem knota (ułamek procenta)
        calculated_sl = swing_low * 0.997 if cand_tag == "CAPITULATION_SNIPER" else swing_low * 0.995
        hard_sl_cap = curr_price * (1.0 - (0.012 if cand_tag == "CAPITULATION_SNIPER" else 0.018))
        initial_sl = max(calculated_sl, hard_sl_cap)

        res = session.place_order(category=CATEGORY, symbol=cand_sym, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
        if res.get("retCode") == 0:
            step, minq = get_symbol_lot_filter(cand_sym)
            q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
            if q_str:
                with lock_pos:
                    active_positions[cand_sym] = {
                        "strategy": cand_tag,
                        "buy_price": curr_price,
                        "peak_price": curr_price,
                        "qty": q_str,
                        "initial_qty": q_str,
                        "trailing_sl": initial_sl,
                        "entry_time": time.time(),
                        "is_downtrend": is_down,
                        "locked_dollar_floor": 0.0,
                        "peak_pnl_usd": 0.0
                    }
                    save_positions(active_positions)
                is_micro = any(m in cand_sym for m in ["PEPE", "BONK", "SHIB"])
                sl_fmt = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
                logging.info(f"🎯🔥 [ZŁAPANO DOŁEK: {cand_tag}] {cand_sym} | Kupiono po: {curr_price} | SL tuż pod knotem: {sl_fmt}")
        return

    # SCENARIUSZ 2: ROTACJA Z PŁASKIEJ POZYCJI (PO 7 MINUTACH)
    stagnant_candidate = None
    for def_sym, def_pos in current_items:
        c_curr = safe_float(all_tickers.get(def_sym, {}).get("lastPrice"), def_pos["buy_price"])
        bp = def_pos["buy_price"]
        gain_pct = (c_curr - bp) / bp
        time_in_pos = now_ts - def_pos.get("entry_time", now_ts)

        if def_pos.get("locked_dollar_floor", 0.0) >= DOLLAR_STEP_USD or gain_pct >= 0.008:
            continue

        is_stagnant = (time_in_pos >= STAGNATION_ROTATION_TIME_SEC) and (gain_pct <= FLAT_MIN_GAIN_PCT)
        if is_stagnant:
            if stagnant_candidate is None or gain_pct < stagnant_candidate["gain_pct"]:
                stagnant_candidate = {
                    "symbol": def_sym, "pos": def_pos, "curr_price": c_curr, 
                    "gain_pct": gain_pct, "time_min": int(time_in_pos / 60)
                }

    if stagnant_candidate:
        old_sym = stagnant_candidate["symbol"]
        old_gain = stagnant_candidate["gain_pct"]
        old_time = stagnant_candidate["time_min"]
        logging.info(f"🔄⚡ [ROTACJA Z PŁASKIEJ] Wymiana {old_sym} ({old_gain*100:+.2f}%, {old_time}m) -> Nowy sygnał: {cand_sym} ({cand_tag})!")

        base_old = get_base_coin(old_sym)
        bal_old = get_coin_available_balance(base_old)
        sold_ok = False

        if bal_old > 0:
            step_old, minq_old = get_symbol_lot_filter(old_sym)
            sell_q_str = format_qty_with_step(bal_old, step_old, minq_old)
            if sell_q_str:
                res_sell = session.place_order(category=CATEGORY, symbol=old_sym, side="Sell", orderType="Market", qty=sell_q_str)
                if res_sell.get("retCode") == 0:
                    sold_ok = True
                    reason = f"Rotacja ze stagnacji ({old_time}m) -> {cand_sym}"
                    record_closed_trade(old_sym, stagnant_candidate["pos"].get("strategy"), stagnant_candidate["pos"]["buy_price"], stagnant_candidate["curr_price"], safe_float(sell_q_str), reason)
                    with lock_pos:
                        if old_sym in active_positions:
                            del active_positions[old_sym]
                            save_positions(active_positions)

        if sold_ok:
            time.sleep(1.2)
            _, free_usdc_now = get_balances()
            total_eq_now, _ = get_balances()
            position_pct = CAPITAL_PER_TRADE_PCT if cand_tag == "CAPITULATION_SNIPER" else ((CAPITAL_PER_TRADE_PCT * 0.60) if is_down else CAPITAL_PER_TRADE_PCT)
            buy_capital = min(free_usdc_now * 0.985, total_eq_now * position_pct)

            if buy_capital >= STANDARD_MIN_ORDER_VALUE:
                c_now_c = cand_df.iloc[-1]
                curr_price_c = c_now_c['close']
                calc_sl = swing_low * 0.997 if cand_tag == "CAPITULATION_SNIPER" else swing_low * 0.995
                initial_sl_c = max(calc_sl, curr_price_c * (1.0 - (0.012 if cand_tag == "CAPITULATION_SNIPER" else 0.018)))

                res_buy = session.place_order(category=CATEGORY, symbol=cand_sym, side="Buy", orderType="Market", qty=f"{buy_capital:.2f}")
                if res_buy.get("retCode") == 0:
                    step_c, minq_c = get_symbol_lot_filter(cand_sym)
                    q_str_c = format_qty_with_step(buy_capital / curr_price_c, step_c, minq_c)
                    with lock_pos:
                        active_positions[cand_sym] = {
                            "strategy": f"ROTACJA ({cand_tag})",
                            "buy_price": curr_price_c,
                            "peak_price": curr_price_c,
                            "qty": q_str_c or "1",
                            "initial_qty": q_str_c or "1",
                            "trailing_sl": initial_sl_c,
                            "entry_time": time.time(),
                            "is_downtrend": is_down,
                            "locked_dollar_floor": 0.0,
                            "peak_pnl_usd": 0.0
                        }
                        save_positions(active_positions)
                    logging.info(f"🎉🚀 [ROTACJA UDANA] Zamieniono na {cand_sym} ({cand_tag}) po cenie {curr_price_c}!")

# ==============================================================================
# MONITORING ZYSKÓW: ULTRA-FAST PEAK TRAILING + FLAT MARKET KILLER
# ==============================================================================
def monitoring_worker():
    global active_positions, basket_peak_profit_usd
    logging.info("🚀 [SYSTEM] Uruchomiono monitoring z ekspresowym trailingiem odbić i cięciem stagnacji.")
    while True:
        try:
            with lock_pos:
                items = list(active_positions.items())
            if not items:
                basket_peak_profit_usd = 0.0
                time.sleep(1.0)
                continue

            tickers = get_all_spot_tickers()
            now_ts = time.time()

            # 1. DOLAROWY TRAILING PORTFELA (BASKET LOCK: +20$ SZCZYT -> STOP -5$)
            total_unrealized_usd = 0.0
            position_prices = {}

            for symbol, pos in items:
                ticker = tickers.get(symbol)
                c_curr = safe_float(ticker.get("lastPrice")) if ticker else pos["buy_price"]
                position_prices[symbol] = c_curr
                total_unrealized_usd += (c_curr - pos["buy_price"]) * safe_float(pos.get("qty", 0.0))

            if total_unrealized_usd > basket_peak_profit_usd:
                basket_peak_profit_usd = total_unrealized_usd

            if basket_peak_profit_usd >= USD_PROFIT_TRIGGER:
                if total_unrealized_usd <= (basket_peak_profit_usd - USD_TRAILING_DROP):
                    logging.info(f"🏆💰 [BASKET LOCK] Zrealizowano zysk koszyka: +{total_unrealized_usd:.2f}$ (szczyt: +{basket_peak_profit_usd:.2f}$).")
                    for symbol, pos in items:
                        c_curr = position_prices.get(symbol, pos["buy_price"])
                        base_c = get_base_coin(symbol)
                        bal_coin = get_coin_available_balance(base_c)
                        if bal_coin > 0 and (bal_coin * c_curr) >= MIN_EXCHANGE_NOTIONAL:
                            step, minq = get_symbol_lot_filter(symbol)
                            sell_q_str = format_qty_with_step(bal_coin, step, minq)
                            if sell_q_str:
                                res_s = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=sell_q_str)
                                if res_s.get("retCode") == 0:
                                    record_closed_trade(symbol, pos.get("strategy"), pos["buy_price"], c_curr, safe_float(sell_q_str), "Basket Dollar Lock")
                    with lock_pos:
                        active_positions.clear()
                        save_positions(active_positions)
                    basket_peak_profit_usd = 0.0
                    time.sleep(1.5)
                    continue

            # 2. MONITOROWANIE POJEDYNCZYCH POZYCJI
            to_delete = []
            for symbol, pos in items:
                c_curr = position_prices.get(symbol, 0.0)
                if c_curr <= 0:
                    continue

                bp = pos["buy_price"]
                qty_f = safe_float(pos.get("qty", 0.0))
                entry_time = pos.get("entry_time", now_ts)
                time_in_pos = now_ts - entry_time
                gain_pct = (c_curr - bp) / bp

                trade_cost = bp * qty_f
                curr_value = c_curr * qty_f
                est_fees = (trade_cost + curr_value) * 0.001
                net_pnl_usd = (curr_value - trade_cost) - est_fees

                peak_p = max(pos.get("peak_price", bp), c_curr)
                pos["peak_price"] = peak_p
                
                peak_pnl = max(pos.get("peak_pnl_usd", 0.0), net_pnl_usd)
                pos["peak_pnl_usd"] = peak_pnl

                # Rejestracja schodka wielokrotności 5 USD
                if net_pnl_usd >= DOLLAR_STEP_USD:
                    current_multiple = (net_pnl_usd // DOLLAR_STEP_USD) * DOLLAR_STEP_USD
                    if current_multiple > pos.get("locked_dollar_floor", 0.0):
                        pos["locked_dollar_floor"] = current_multiple
                        logging.info(f"🔒💰 [SCHODEK 5$] {symbol} | Zysk: +{net_pnl_usd:.2f}$! Zablokowano próg: +{current_multiple:.2f}$!")

                locked_floor = pos.get("locked_dollar_floor", 0.0)
                current_sl = pos.get("trailing_sl", bp * 0.985)
                strat_tag = pos.get("strategy", "")

                trigger_exit = False
                exit_reason = ""

                price_drop_from_peak = (peak_p - c_curr) / peak_p if peak_p > 0 else 0.0

                # ==============================================================
                # EKSPRESOWY TRAILING DLA ODBIĆ PO ZRZUCIE (CAPITULATION SNIPER)
                # Trzymasz dopóki rośnie, przy pierwszym cofnięciu (0.35%) natychmiastowa sprzedaż!
                # ==============================================================
                if "CAPITULATION" in strat_tag:
                    if (peak_p - bp) / bp >= 0.0080 or peak_pnl >= 2.0:
                        if price_drop_from_peak >= 0.0035: # Cofnięcie o 0.35% od szczytu odbicia
                            trigger_exit = True
                            exit_reason = f"Capitulation Bounce Peak Cut (-{price_drop_from_peak*100:.2f}% ze szczytu | Zysk: +{net_pnl_usd:.2f}$)"

                # ==============================================================
                # TRAILING DLA POZOSTAŁYCH TRYBÓW
                # ==============================================================
                if not trigger_exit:
                    if peak_pnl >= 2.0 or (peak_p - bp) / bp >= 0.009:
                        if peak_pnl >= 10.0:
                            max_drop_pct = 0.0050
                            max_giveback_usd = min(1.20, peak_pnl * 0.10)
                        elif peak_pnl >= 5.0:
                            max_drop_pct = 0.0040
                            max_giveback_usd = min(0.80, peak_pnl * 0.12)
                        else:
                            max_drop_pct = 0.0035
                            max_giveback_usd = 0.50

                        hit_price_drop = price_drop_from_peak >= max_drop_pct
                        hit_dollar_giveback = (peak_pnl - net_pnl_usd) >= max_giveback_usd
                        breached_locked_floor = (locked_floor >= DOLLAR_STEP_USD) and (net_pnl_usd < locked_floor)

                        if hit_price_drop or hit_dollar_giveback or breached_locked_floor:
                            trigger_exit = True
                            exit_reason = f"Błyskawiczne Cięcie Cofnięcia (-{price_drop_from_peak*100:.2f}% | Zrealizowano: +{net_pnl_usd:.2f}$ ze szczytu +{peak_pnl:.2f}$)"

                # ==============================================================
                # AGRESYWNE CIĘCIE PŁASKICH POZYCJI (FLAT MARKET KILLER)
                # ==============================================================
                if not trigger_exit:
                    # Po 10 minutach brak ruchu powyżej +0.40% -> WYLOT!
                    if time_in_pos >= FLAT_MAX_TIME_SEC and gain_pct < FLAT_MIN_GAIN_PCT:
                        trigger_exit = True
                        exit_reason = f"Flat Market Exit (10m bez ruchu, PnL: {gain_pct*100:+.2f}%)"

                    # Po 15 minutach brak dynamiki powyżej +0.80% -> WYLOT!
                    elif time_in_pos >= MID_TIME_SEC and gain_pct < MID_MIN_GAIN_PCT:
                        trigger_exit = True
                        exit_reason = f"Brak Dynamiki (15m < +0.80%, PnL: {gain_pct*100:+.2f}%)"

                    # Standardowy Stop Loss
                    elif c_curr <= current_sl:
                        trigger_exit = True
                        exit_reason = f"Stop Loss (Wynik: {gain_pct*100:+.2f}%)"

                if trigger_exit:
                    base_c = get_base_coin(symbol)
                    bal_coin = get_coin_available_balance(base_c)
                    if bal_coin > 0 and (bal_coin * c_curr) >= MIN_EXCHANGE_NOTIONAL:
                        step, minq = get_symbol_lot_filter(symbol)
                        sell_q_str = format_qty_with_step(bal_coin, step, minq)
                        if sell_q_str:
                            res_sell = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=sell_q_str)
                            if res_sell.get("retCode") == 0:
                                record_closed_trade(symbol, pos.get("strategy"), bp, c_curr, safe_float(sell_q_str), exit_reason)
                                logging.info(f"🚨 [WYJŚCIE] {symbol} | {exit_reason}")
                    to_delete.append(symbol)

            if to_delete:
                with lock_pos:
                    for s in to_delete:
                        if s in active_positions:
                            del active_positions[s]
                save_positions(active_positions)

        except Exception as e:
            logging.error(f"Błąd monitoringu: {e}")
        time.sleep(1.0)

# ==============================================================================
# PĘTLA GŁÓWNA
# ==============================================================================
def bot_loop():
    sync_positions_from_bybit()
    t_mon = threading.Thread(target=monitoring_worker, daemon=True)
    t_mon.start()

    last_print = 0
    last_hourly_sync = time.time()

    while True:
        try:
            now = time.time()
            if now - last_hourly_sync >= 3600:
                sync_bybit_clock()
                last_hourly_sync = now

            with lock_pos:
                current_items = list(active_positions.items())

            if now - last_print >= 25:
                total_eq, free_usdc = get_balances()
                all_tickers = get_all_spot_tickers()
                total_unrealized_usd = sum(
                    (safe_float(all_tickers.get(s, {}).get("lastPrice"), p["buy_price"]) - p["buy_price"]) * safe_float(p.get("qty", 0.0))
                    for s, p in current_items
                )
                logging.info("--------------------------------------------------------------------------------")
                status_basket = f"Szczyt: +{basket_peak_profit_usd:.2f}$" if basket_peak_profit_usd >= USD_PROFIT_TRIGGER else "Nieaktywny (< 20$)"
                logging.info(f"💰 [PORTFEL] Saldo: {total_eq:.2f}$ | Wolne: {free_usdc:.2f}$ | Niezrealizowany PnL: {total_unrealized_usd:+.2f}$ ({status_basket}) | Pozycje: {len(current_items)}/{MAX_TOTAL_POSITIONS}")
                for s, p in current_items:
                    curr = safe_float(all_tickers.get(s, {}).get("lastPrice"), p["buy_price"])
                    pnl_pct = ((curr - p["buy_price"]) / p["buy_price"]) * 100
                    pnl_usd = (curr - p["buy_price"]) * safe_float(p.get("qty", 0.0))
                    floor_val = p.get("locked_dollar_floor", 0.0)
                    peak_usd = p.get("peak_pnl_usd", pnl_usd)
                    strat_info = p.get("strategy", "")
                    floor_txt = f" | 🔒 Podłoga: +{floor_val:.2f}$ (Szczyt: +{peak_usd:.2f}$)" if floor_val > 0 else f" (Szczyt: +{peak_usd:.2f}$)"
                    min_elapsed = int((now - p.get("entry_time", now)) / 60)
                    logging.info(f"📈 [{s}] ({strat_info}) Zysk: {pnl_usd:+.2f}$ ({pnl_pct:+.2f}%){floor_txt} | Czas: {min_elapsed}m")
                logging.info("--------------------------------------------------------------------------------")
                last_print = now

            if is_btc_market_healthy():
                run_bonus_hunter_strategy()

        except Exception as e:
            logging.error(f"Błąd głównej pętli: {e}")
        time.sleep(10)

def self_ping_loop():
    while True:
        time.sleep(300)
        try:
            requests.get(APP_URL, timeout=10)
        except Exception:
            pass

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bybit Spot Bot Active - Live Capitulation Sniper & Ultra Trailing")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    httpd = HTTPServer(('', port), HealthHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    t_bot = threading.Thread(target=bot_loop, daemon=True)
    t_bot.start()
    t_ping = threading.Thread(target=self_ping_loop, daemon=True)
    t_ping.start()
    run_health_server()
