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
# KONFIGURACJA GŁÓWNA: TYLKO PEPE/USDC (MAX KAPITAŁ)
# ==============================================================================
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "TUTAJ_TWÓJ_KEY").strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "TUTAJ_TWÓJ_SECRET").strip()

TESTNET = False
CATEGORY = "spot"
TARGET_SYMBOL = "PEPEUSDC"
TARGET_BASE_COIN = "PEPE"
QUOTE_COIN = "USDC"

CAPITAL_ALLOCATION_PCT = 0.99      # 99% dostępnego USDC (max kapitał z 1% buforem na zaokrąglenia)
MAX_TOTAL_POSITIONS = 1            # 1 pełna pozycja na PEPE/USDC
MIN_EXCHANGE_NOTIONAL = 5.2        

MIN_24H_TURNOVER_USD = 150000.0    # Filtr ochrony przed martwym rynkiem
USDC_MAX_SPREAD_PCT = 0.0050       # Maksymalny dopuszczalny spread 0.50%

# ==============================================================================
# ZARZĄDZANIE ZYSKIEM I TWARDE BEZPIECZNIKI PORTFELA
# ==============================================================================
EQUITY_DROP_LIMIT_USD = 5.0        # Szczyt salda - 5 USD = NATYCHMIASTOWY ZRZUT DO USDC
MAX_DAILY_LOSS_USD = 20.0          # Maksymalna dzienna strata (-20 USD)
DOLLAR_STEP_USD = 5.0              # Schodek zabezpieczania zysku co 5 USD
TP1_GAIN_PCT = 0.018               # TP1: realizacja 50% pozycji przy +1.8% zysku
COOLDOWN_AFTER_EXIT_SEC = 2700     # 45 minut przerwy po zamknięciu pozycji

APP_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://bybit-bot-i1lw.onrender.com")
POSITIONS_FILE = "active_spot_positions.json"
TRADE_HISTORY_FILE = "trade_history.json"
PORTFOLIO_STATE_FILE = "portfolio_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

lock_pos = threading.RLock()
lock_history = threading.RLock()
lock_state = threading.RLock()
instrument_filters = {}
last_exit_timestamp = 0.0

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
            logging.info(f"✅ POŁĄCZONO Z BYBIT: {name} | Tryb: SNIPER PEPE/USDC (MAX KAPITAŁ)")
            break
    except Exception:
        pass

if not session:
    session = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
    session.endpoint = "https://api.bybit.eu"

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
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

def get_symbol_lot_filter(symbol):
    if symbol in instrument_filters:
        return instrument_filters[symbol]
    try:
        res = session.get_instruments_info(category=CATEGORY, symbol=symbol)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            lot = res["result"]["list"][0].get("lotSizeFilter", {})
            step = lot.get("basePrecision") or lot.get("qtyStep") or "1"
            min_q = lot.get("minOrderQty", "1")
            instrument_filters[symbol] = (step, min_q)
            return instrument_filters[symbol]
    except Exception as e:
        logging.error(f"Błąd lot filter dla {symbol}: {e}")
    return "1", "1"

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
# STAN PORTFELA I HISTORIA
# ==============================================================================
def load_portfolio_state():
    if os.path.exists(PORTFOLIO_STATE_FILE):
        try:
            with open(PORTFOLIO_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"peak_total_equity": 0.0}

def save_portfolio_state(state):
    with lock_state:
        try:
            with open(PORTFOLIO_STATE_FILE, "w") as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            logging.error(f"Błąd zapisu portfolio_state: {e}")

portfolio_state = load_portfolio_state()

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
                if "daily" not in data:
                    data["daily"] = {}
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

def record_closed_trade(symbol, strat, buy_price, sell_price, qty, reason, set_cooldown=True):
    global trade_history, last_exit_timestamp
    if set_cooldown:
        last_exit_timestamp = time.time()
    with lock_history:
        raw_pnl_usd = (sell_price - buy_price) * qty
        fee_est = (buy_price * qty + sell_price * qty) * 0.001
        pnl_usd = round(raw_pnl_usd - fee_est, 2)
        pnl_pct = (pnl_usd / (buy_price * qty)) if (buy_price * qty) > 0 else 0.0
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = time.time()

        trade_history["total_realized_usd"] = round(trade_history.get("total_realized_usd", 0.0) + pnl_usd, 2)
        trade_history["total_trades"] = trade_history.get("total_trades", 0) + 1

        if "daily" not in trade_history:
            trade_history["daily"] = {}
        if today_str not in trade_history["daily"]:
            trade_history["daily"][today_str] = {"realized_usd": 0.0, "trades": 0}

        trade_history["daily"][today_str]["realized_usd"] = round(trade_history["daily"][today_str]["realized_usd"] + pnl_usd, 2)
        trade_history["daily"][today_str]["trades"] += 1

        if pnl_usd >= 0:
            trade_history["wins"] = trade_history.get("wins", 0) + 1
        else:
            trade_history["losses"] = trade_history.get("losses", 0) + 1

        trade_history["trades_log"].append({
            "timestamp": now_ts, "symbol": symbol, "turnover": round((buy_price + sell_price) * qty, 2),
            "fee": round(fee_est, 4), "gross_pnl": round(raw_pnl_usd, 2), "net_pnl": pnl_usd,
            "reason": reason
        })

        if len(trade_history["trades_log"]) > 250:
            trade_history["trades_log"] = trade_history["trades_log"][-250:]

        save_trade_history(trade_history)
        sign = "+" if pnl_usd >= 0 else ""
        logging.info(f"📊 [BILANS TRANSAKCJI] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | {reason}")

def get_today_pnl(curr_pepe_price=0.0):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with lock_history:
        realized_today = trade_history.get("daily", {}).get(today_str, {}).get("realized_usd", 0.0)

    unrealized_today = 0.0
    if curr_pepe_price > 0:
        with lock_pos:
            pos = active_positions.get(TARGET_SYMBOL)
            if pos:
                bp = pos["buy_price"]
                qty_f = safe_float(pos.get("qty", 0.0))
                unrealized_today += (curr_pepe_price - bp) * qty_f

    return realized_today, unrealized_today, (realized_today + unrealized_today)

# ==============================================================================
# WSKAŹNIKI I DANE RYNKOWE DLA PEPE/USDC
# ==============================================================================
def get_pepe_ticker():
    try:
        res = session.get_tickers(category=CATEGORY, symbol=TARGET_SYMBOL)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return res["result"]["list"][0]
    except Exception as e:
        logging.warning(f"Błąd pobierania tickera PEPEUSDC: {e}")
    return None

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

def get_pepe_available_balance():
    if not session:
        return 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED", coin=TARGET_BASE_COIN)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            for c in res["result"]["list"][0].get("coin", []):
                if c.get("coin") == TARGET_BASE_COIN:
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
        
        return df
    except Exception:
        return None

def is_btc_market_healthy():
    try:
        df_btc = get_market_data_no_cache("BTCUSDC", interval="5", limit=5)
        if df_btc is not None and len(df_btc) >= 3:
            c = df_btc.iloc[-1]
            candle_change = (c['close'] - c['open']) / c['open']
            if candle_change <= -0.015:
                logging.warning(f"⚠️ [BTC GUARD] Tąpnięcie 5M na BTC: {candle_change*100:.2f}%. Wstrzymuję zakupy PEPE.")
                return False
    except Exception:
        pass
    return True

# ==============================================================================
# NIEZAWODNY ZRZUT PEPE DO USDC
# ==============================================================================
def force_exit_pepe_to_usdc(qty_to_sell=None, reason="Exit"):
    try:
        session.cancel_all_orders(category=CATEGORY, symbol=TARGET_SYMBOL)
    except Exception:
        pass
    time.sleep(0.15)

    bal = get_pepe_available_balance()
    ticker = get_pepe_ticker()
    curr_p = safe_float(ticker.get("lastPrice"), 0.0) if ticker else 0.0

    target_qty = min(bal, qty_to_sell) if qty_to_sell else bal
    if (target_qty * curr_p) < MIN_EXCHANGE_NOTIONAL:
        return True

    step, minq = get_symbol_lot_filter(TARGET_SYMBOL)
    sell_qty = format_qty_with_step(target_qty, step, minq)

    if sell_qty:
        res = session.place_order(category=CATEGORY, symbol=TARGET_SYMBOL, side="Sell", orderType="Market", qty=sell_qty)
        if res.get("retCode") == 0:
            logging.info(f"✅ [SPRZEDAŻ PEPE UDANA] Ilość: {sell_qty} | Cena: ~{curr_p:.8f} | {reason}")
            return True

    fallback_qty = format_qty_with_step(target_qty * 0.995, step, minq)
    if fallback_qty:
        res2 = session.place_order(category=CATEGORY, symbol=TARGET_SYMBOL, side="Sell", orderType="Market", qty=fallback_qty)
        if res2.get("retCode") == 0:
            logging.info(f"✅ [SPRZEDAŻ PEPE FALLBACK] Ilość: {fallback_qty} | Cena: ~{curr_p:.8f} | {reason}")
            return True

    logging.error(f"❌ [BŁĄD SPRZEDAŻY] Nie udało się sprzedać {TARGET_SYMBOL}!")
    return False

def emergency_close_all_portfolio(reason="Emergency"):
    logging.error(f"🛑🛑🛑 [AWARYJNY ZRZUT PORTFELA DO USDC] Powód: {reason}")
    try:
        ticker = get_pepe_ticker()
        curr_p = safe_float(ticker.get("lastPrice"), 0.0) if ticker else 0.0
        with lock_pos:
            pos = active_positions.get(TARGET_SYMBOL)
        bp = pos["buy_price"] if pos else curr_p
        qty_f = get_pepe_available_balance()

        if force_exit_pepe_to_usdc(None, reason) and qty_f > 0 and curr_p > 0:
            record_closed_trade(TARGET_SYMBOL, "EMERGENCY", bp, curr_p, qty_f, reason, set_cooldown=True)
    except Exception as e:
        logging.error(f"Błąd zrzutu awaryjnego: {e}")
    finally:
        sync_positions_from_bybit()

def sync_positions_from_bybit():
    global active_positions
    if not session:
        return
    try:
        with lock_pos:
            bal = get_pepe_available_balance()
            ticker = get_pepe_ticker()
            curr_p = safe_float(ticker.get("lastPrice"), 0.0) if ticker else 0.0

            if (bal * curr_p) < MIN_EXCHANGE_NOTIONAL:
                if TARGET_SYMBOL in active_positions:
                    del active_positions[TARGET_SYMBOL]
                    save_positions(active_positions)
                return

            if TARGET_SYMBOL not in active_positions:
                step, minq = get_symbol_lot_filter(TARGET_SYMBOL)
                q_str = format_qty_with_step(bal, step, minq)
                if q_str:
                    active_positions[TARGET_SYMBOL] = {
                        "strategy": "RECOVERED",
                        "buy_price": curr_p,
                        "peak_price": curr_p,
                        "qty": q_str,
                        "initial_qty": q_str,
                        "trailing_sl": curr_p * 0.988,
                        "entry_time": time.time(),
                        "locked_dollar_floor": 0.0,
                        "peak_pnl_usd": 0.0,
                        "tp1_executed": False
                    }
                    save_positions(active_positions)
                    logging.warning(f"🚨 [SYNC] Odzyskano pozycję {TARGET_SYMBOL}: {q_str} szt. po ~{curr_p:.8f}")
    except Exception as e:
        logging.error(f"Błąd synchronizacji Bybit: {e}")

# ==============================================================================
# STRATEGIA ZAKUPU NA DOŁKACH TYLKO DLA PEPE/USDC (MAX KAPITAŁ)
# ==============================================================================
def run_pepe_sniper_strategy():
    global active_positions, last_exit_timestamp
    with lock_pos:
        if TARGET_SYMBOL in active_positions:
            return

    now_ts = time.time()
    if (now_ts - last_exit_timestamp) < COOLDOWN_AFTER_EXIT_SEC:
        return

    ticker = get_pepe_ticker()
    if not ticker:
        return

    curr_p_ticker = safe_float(ticker.get("lastPrice"), 0.0)
    _, _, total_today = get_today_pnl(curr_p_ticker)
    if total_today <= -MAX_DAILY_LOSS_USD:
        return

    # Kontrola płynności i spreadu na PEPE/USDC
    turnover_24h = safe_float(ticker.get("turnover24h", 0.0))
    if turnover_24h < MIN_24H_TURNOVER_USD:
        return

    bid = safe_float(ticker.get("bid1Price"))
    ask = safe_float(ticker.get("ask1Price"))
    if bid <= 0 or ask <= 0 or ((ask - bid) / bid) > USDC_MAX_SPREAD_PCT:
        return

    df = get_market_data_no_cache(TARGET_SYMBOL, interval="15", limit=200)
    if df is None or len(df) < 60:
        return

    try:
        c_now = df.iloc[-1]   # Bieżąca świeca 15M
        c = df.iloc[-2]       # Poprzednia zamknięta świeca 15M
        p = df.iloc[-3]       # Świeca sprzed 30 min
        curr_price = c_now['close']

        # ======================================================================
        # ŻELAZNA BARIERA ANTY-SZCZYTOWA: ZAKAZ KUPOWANIA NA GÓRKACH PEPE
        # ======================================================================
        # 1. Zakaz kupna nad EMA 50 i powyżej bufora 0.9% nad EMA 9
        if curr_price > c_now['ema9'] * 1.009 or curr_price >= c_now['ema50']:
            return

        # 2. Zakaz gonienia silnie rozpędzonej zielonej świecy (> +0.8% od otwarcia)
        if c_now['close'] >= (c_now['open'] * 1.008):
            return

        # ======================================================================
        # TRYB 1: CAPITULATION SNIPER (SZYBKIE ODBICIE "V-SHAPE" PO PANICE)
        # ======================================================================
        c_is_red = c['close'] < c['open']
        c_drop_pct = (c['open'] - c['close']) / c['open'] if c_is_red else 0.0
        vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0

        is_panic_dump = (
            c_is_red and 
            (c_drop_pct >= 0.018) and 
            (vol_ratio >= 1.8) and 
            (c['low'] <= c['bb_lower'] * 1.002)
        )

        rebound_from_low = (curr_price - c['low']) / c['low'] if c['low'] > 0 else 0.0
        is_valid_bottom_rebound = (0.0040 <= rebound_from_low <= 0.0110)

        is_capitulation = (
            is_panic_dump and 
            is_valid_bottom_rebound and 
            (curr_price <= c_now['ema9'] * 1.002) and
            (c_now['close'] < (c_now['open'] * 1.004)) and
            (curr_price < c_now['ema21']) and
            (curr_price < c_now['bb_mid']) and
            (c_now['rsi'] <= 38.0)
        )

        # ======================================================================
        # TRYB 2: U-SHAPE BOTTOM (ZAOKRĄGLONE DNO PO SPADKU 6H >= 2.5%)
        # ======================================================================
        high_6h = df['high'].iloc[-26:-1].max()
        low_recent_base = df['low'].iloc[-10:-1].min()
        local_drop_6h = (high_6h - low_recent_base) / high_6h if high_6h > 0 else 0.0
        has_local_6h_drop = (local_drop_6h >= 0.025)
        
        deep_under_ema89 = (curr_price <= c_now['ema89'] * 0.993)
        ema_crossover = (
            ((c['ema9'] <= c['ema21']) and (c_now['ema9'] > c_now['ema21'])) or
            ((p['ema9'] <= p['ema21']) and (c['ema9'] > c['ema21']))
        )
        
        safe_crossover_entry = (
            (curr_price <= c_now['ema9'] * 1.009) and
            (curr_price <= c_now['ema21'] * 1.011) and
            (curr_price <= low_recent_base * 1.030) and
            (38.0 <= c_now['rsi'] <= 56.0)
        )

        is_ushape_bottom = has_local_6h_drop and deep_under_ema89 and ema_crossover and safe_crossover_entry

        if not (is_capitulation or is_ushape_bottom):
            return

        if is_capitulation:
            tag = "CAPITULATION_SNIPER"
            swing_low = c['low']
        else:
            tag = "U_SHAPE_BOTTOM"
            swing_low = low_recent_base

        # ======================================================================
        # EGZEKUCJA ZAKUPU ZA MAKSYMALNĄ DOSTĘPNĄ KWOTĘ USDC (99%)
        # ======================================================================
        total_equity, free_usdc = get_balances()
        target_allocation = round(free_usdc * CAPITAL_ALLOCATION_PCT, 2)

        if target_allocation < MIN_EXCHANGE_NOTIONAL:
            logging.warning(f"⚠️ Za mało wolnych środków USDC ({free_usdc:.2f}$) na zakup PEPE.")
            return

        calculated_sl = swing_low * 0.996
        hard_sl_cap = curr_price * 0.988
        initial_sl = max(calculated_sl, hard_sl_cap)

        res = session.place_order(
            category=CATEGORY,
            symbol=TARGET_SYMBOL,
            side="Buy",
            orderType="Market",
            qty=f"{target_allocation:.2f}"
        )
        if res.get("retCode") == 0:
            time.sleep(0.5)
            actual_pepe_bal = get_pepe_available_balance()
            step, minq = get_symbol_lot_filter(TARGET_SYMBOL)
            est_qty = actual_pepe_bal if actual_pepe_bal > 0 else (target_allocation / curr_price)
            q_str = format_qty_with_step(est_qty, step, minq)
            if q_str:
                with lock_pos:
                    active_positions[TARGET_SYMBOL] = {
                        "strategy": tag,
                        "buy_price": curr_price,
                        "peak_price": curr_price,
                        "qty": q_str,
                        "initial_qty": q_str,
                        "trailing_sl": initial_sl,
                        "entry_time": time.time(),
                        "locked_dollar_floor": 0.0,
                        "peak_pnl_usd": 0.0,
                        "tp1_executed": False
                    }
                    save_positions(active_positions)
                logging.info(f"🎯🔥 [ALL-IN ZAKUP PEPE: {tag}] Kwota: {target_allocation:.2f}$ | Cena wejścia: {curr_price:.8f} | SL pod dnem: {initial_sl:.8f}")
        else:
            logging.error(f"❌ Błąd kupna PEPEUSDC: {res.get('retMsg')}")

    except Exception as e:
        logging.error(f"Błąd analizy PEPEUSDC: {e}")

# ==============================================================================
# MONITORING ZYSKÓW: MAX PROFIT RUNNER + TP1 + BREAK-EVEN + OBRONA SALDA
# ==============================================================================
def monitoring_worker():
    global active_positions, portfolio_state
    logging.info("🚀 [SYSTEM] Uruchomiono monitoring maksymalizacji zysku i ochrony spadków na PEPE/USDC.")
    last_kline_check = 0.0
    cached_climax_spike = False

    while True:
        try:
            with lock_pos:
                pos = active_positions.get(TARGET_SYMBOL)

            ticker = get_pepe_ticker()
            c_curr = safe_float(ticker.get("lastPrice"), 0.0) if ticker else 0.0
            now_ts = time.time()
            total_eq, free_usdc = get_balances()

            # ------------------------------------------------------------------
            # 1. NADRZĘDNA BLOKADA SALDA PORTFELA (SZCZYT - 5$ = ZRZUT DO USDC)
            # ------------------------------------------------------------------
            if total_eq > 0:
                current_peak = portfolio_state.get("peak_total_equity", 0.0)

                if total_eq > current_peak:
                    portfolio_state["peak_total_equity"] = total_eq
                    save_portfolio_state(portfolio_state)
                    current_peak = total_eq
                    logging.info(f"👑 [SZCZYT SALDA] Portfel: {total_eq:.2f}$ | Obrona salda przy: {total_eq - EQUITY_DROP_LIMIT_USD:.2f}$")

                if pos is not None and current_peak > 0:
                    if total_eq <= (current_peak - EQUITY_DROP_LIMIT_USD):
                        emergency_close_all_portfolio(f"Master Equity Drop (-5$ ze szczytu {current_peak:.2f}$)")
                        portfolio_state["peak_total_equity"] = total_eq
                        save_portfolio_state(portfolio_state)
                        time.sleep(2.0)
                        continue

            # ------------------------------------------------------------------
            # 2. DZIENNY LIMIT STRATY (-20 USD)
            # ------------------------------------------------------------------
            _, _, total_today = get_today_pnl(c_curr)
            if total_today <= -MAX_DAILY_LOSS_USD and pos is not None:
                emergency_close_all_portfolio("Daily Loss Limit Hit (-20 USD)")
                time.sleep(2.0)
                continue

            if not pos or c_curr <= 0:
                time.sleep(1.0)
                continue

            # ------------------------------------------------------------------
            # 3. PROWADZENIE POZYCJI PEPE PO MAKSYMALNY ZYSK
            # ------------------------------------------------------------------
            bp = pos["buy_price"]
            qty_f = safe_float(pos.get("qty", 0.0))
            entry_time = pos.get("entry_time", now_ts)
            time_in_pos = now_ts - entry_time
            gain_pct = (c_curr - bp) / bp

            curr_value = c_curr * qty_f
            trade_cost = bp * qty_f
            net_pnl_usd = (curr_value - trade_cost) - (trade_cost + curr_value) * 0.001

            peak_p = max(pos.get("peak_price", bp), c_curr)
            pos["peak_price"] = peak_p

            peak_pnl = max(pos.get("peak_pnl_usd", 0.0), net_pnl_usd)
            pos["peak_pnl_usd"] = peak_pnl

            # ------------------------------------------------------------------
            # KROK A: TP1 PRZY +1.8% (ZRZUT 50% W ZYSKU + SL NA BREAK-EVEN +0.4%)
            # ------------------------------------------------------------------
            if gain_pct >= TP1_GAIN_PCT and not pos.get("tp1_executed", False):
                half_qty = qty_f / 2.0
                step, minq = get_symbol_lot_filter(TARGET_SYMBOL)
                half_qty_str = format_qty_with_step(half_qty, step, minq)

                if half_qty_str and force_exit_pepe_to_usdc(safe_float(half_qty_str), "TP1 (+50% PEPE sprzedane w zysku)"):
                    sold_qty_f = safe_float(half_qty_str)
                    record_closed_trade(TARGET_SYMBOL, pos.get("strategy"), bp, c_curr, sold_qty_f, "TP1 (50% zysku zrealizowane)", set_cooldown=False)
                    pos["tp1_executed"] = True
                    remaining_qty = format_qty_with_step(qty_f - sold_qty_f, step, minq)
                    pos["qty"] = remaining_qty or half_qty_str
                    pos["trailing_sl"] = bp * 1.004  # Break-Even (+0.4% pokrywa prowizje)
                    logging.info(f"💎💰 [TP1 ZREALIZOWANE] PEPE | Połowa pozycji w USDC! SL na Break-Even ({pos['trailing_sl']:.8f}). Reszta jedzie po max zysk!")
                    save_positions(active_positions)
                    qty_f = safe_float(pos["qty"])

            # Rejestracja schodka wielokrotności 5 USD (dla dużej pozycji PEPE)
            if net_pnl_usd >= DOLLAR_STEP_USD:
                current_multiple = (net_pnl_usd // DOLLAR_STEP_USD) * DOLLAR_STEP_USD
                if current_multiple > pos.get("locked_dollar_floor", 0.0):
                    pos["locked_dollar_floor"] = current_multiple
                    logging.info(f"🔒💰 [SCHODEK 5$] PEPE | Zysk: +{net_pnl_usd:.2f}$! Zablokowano podłogę: +{current_multiple:.2f}$!")

            locked_floor = pos.get("locked_dollar_floor", 0.0)
            current_sl = pos.get("trailing_sl", bp * 0.988)

            # ------------------------------------------------------------------
            # KROK B: DYNAMICZNY TRAILING POD MAX ZYSK NA PEPE
            # ------------------------------------------------------------------
            if gain_pct >= 0.060:
                current_sl = max(current_sl, peak_p * (1.0 - 0.0060))
            elif gain_pct >= 0.035:
                current_sl = max(current_sl, peak_p * (1.0 - 0.0080))
            elif gain_pct >= 0.020:
                current_sl = max(current_sl, peak_p * (1.0 - 0.0100))
            elif gain_pct >= 0.012:
                current_sl = max(current_sl, bp * 1.0035)

            pos["trailing_sl"] = current_sl

            # Sprawdzanie świecy 15M co 5 sekund pod kątem Climax Spike (szczyt na górnej wstędze)
            if (now_ts - last_kline_check) >= 5.0:
                last_kline_check = now_ts
                df15 = get_market_data_no_cache(TARGET_SYMBOL, interval="15", limit=25)
                if df15 is not None and len(df15) >= 2:
                    c15 = df15.iloc[-1]
                    cached_climax_spike = (c_curr >= c15['bb_upper'] * 1.002) and (c15['rsi'] >= 74.0) and (gain_pct >= 0.030)
                else:
                    cached_climax_spike = False

            trigger_exit = False
            exit_reason = ""

            # 1. Wyjście na parabolicznej górce (Climax Spike)
            if cached_climax_spike:
                trigger_exit = True
                exit_reason = f"Climax Spike Top Exit (Górna wstęga + RSI >= 74 | Zysk: {gain_pct*100:+.2f}%)"

            # 2. Obrona schodka 5 USD
            elif locked_floor >= DOLLAR_STEP_USD and net_pnl_usd < (locked_floor - 0.25):
                trigger_exit = True
                exit_reason = f"Obrona schodka 5$ (Zysk: +{net_pnl_usd:.2f}$ | Próg: +{locked_floor:.2f}$)"

            # 3. Uderzenie w Trailing SL / Stop Loss pod dnem
            elif c_curr <= current_sl:
                trigger_exit = True
                exit_reason = f"Trailing / SL Exit (Wynik: {gain_pct*100:+.2f}%)"

            # 4. Cięcie stagnacji po 30 minutach
            elif time_in_pos >= 1800 and gain_pct < 0.0035 and not pos.get("tp1_executed", False):
                trigger_exit = True
                exit_reason = f"Stagnation Exit (30m bez wybicia, PnL: {gain_pct*100:+.2f}%)"

            if trigger_exit:
                if force_exit_pepe_to_usdc(None, exit_reason):
                    record_closed_trade(TARGET_SYMBOL, pos.get("strategy"), bp, c_curr, qty_f, exit_reason, set_cooldown=True)
                    with lock_pos:
                        if TARGET_SYMBOL in active_positions:
                            del active_positions[TARGET_SYMBOL]
                        save_positions(active_positions)

        except Exception as e:
            logging.error(f"Błąd monitoringu PEPEUSDC: {e}")
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

            sync_positions_from_bybit()

            ticker = get_pepe_ticker()
            curr_p = safe_float(ticker.get("lastPrice"), 0.0) if ticker else 0.0
            realized_d, unrealized_d, total_day_pnl = get_today_pnl(curr_p)
            daily_limit_hit = total_day_pnl <= -MAX_DAILY_LOSS_USD

            with lock_pos:
                pos = active_positions.get(TARGET_SYMBOL)

            if now - last_print >= 25:
                total_eq, free_usdc = get_balances()
                peak_eq = portfolio_state.get("peak_total_equity", total_eq)
                status_circuit = "🛑 [ZABLOKOWANY - LIMIT -20$]" if daily_limit_hit else "✅ Aktywny"
                stop_floor_eq = max(0.0, peak_eq - EQUITY_DROP_LIMIT_USD)

                logging.info("--------------------------------------------------------------------------------")
                logging.info(f"🐸 [PEPE SNIPER] Cena PEPE: {curr_p:.8f} | Saldo: {total_eq:.2f}$ | Szczyt: {peak_eq:.2f}$ (Obrona: {stop_floor_eq:.2f}$) | Wolne USDC: {free_usdc:.2f}$")
                logging.info(f"📅 [BILANS DNIA] Dzisiaj: {total_day_pnl:+.2f}$ | Status: {status_circuit}")
                if pos and curr_p > 0:
                    pnl_pct = ((curr_p - pos["buy_price"]) / pos["buy_price"]) * 100
                    pnl_usd = (curr_p - pos["buy_price"]) * safe_float(pos.get("qty", 0.0))
                    tp_status = " [TP1 50% W KIESZENI]" if pos.get("tp1_executed", False) else ""
                    min_elapsed = int((now - pos.get("entry_time", now)) / 60)
                    logging.info(f"📈 [POZYCJA PEPE] ({pos.get('strategy')}) Wejście: {pos['buy_price']:.8f} | Zysk: {pnl_usd:+.2f}$ ({pnl_pct:+.2f}%){tp_status} | Czas: {min_elapsed}m | SL: {pos.get('trailing_sl', 0):.8f}")
                logging.info("--------------------------------------------------------------------------------")
                last_print = now

            if not daily_limit_hit and is_btc_market_healthy():
                run_pepe_sniper_strategy()

        except Exception as e:
            logging.error(f"Błąd głównej pętli: {e}")
        time.sleep(5)

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
        self.wfile.write(b"Bybit Spot Bot Active - PEPE/USDC Max Capital Bottom Sniper")
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
