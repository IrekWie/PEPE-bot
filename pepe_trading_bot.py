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
# KONFIGURACJA GŁÓWNA
# ==============================================================================
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "TUTAJ_TWÓJ_KEY").strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "TUTAJ_TWÓJ_SECRET").strip()

TESTNET = False
CATEGORY = "spot"
QUOTE_COIN = "USDC"

EXCLUDED_BASE_COINS = {
    "USDT", "EURC", "USD", "DAI", "FDUSD", "TUSD", "BUSD", "USDE", "PYUSD", "USDD"
}

CAPITAL_PER_TRADE_PCT = 0.45       # 45% salda na transakcję (max 2 pozycje)
MAX_TOTAL_POSITIONS = 2            
STANDARD_MIN_ORDER_VALUE = 15.0    
MIN_EXCHANGE_NOTIONAL = 5.2        

MIN_24H_TURNOVER_USD = 250000.0    # Odsiew śmieci: min. 250 000 USD obrotu (koniec z monetami jak LIT!)
USDC_MAX_SPREAD_PCT = 0.0040       # Ciasny spread max 0.40%
MAX_SCAN_CANDIDATES = 30           

# ==============================================================================
# TWARDE BEZPIECZNIKI PORTFELA
# ==============================================================================
EQUITY_DROP_LIMIT_USD = 5.0        # Szczyt salda - 5 USD = NATYCHMIASTOWY ZRZUT WSZYSTKIEGO DO USDC
MAX_DAILY_LOSS_USD = 20.0          # Maksymalna strata dzienna (-20 USD)
DOLLAR_STEP_USD = 5.0              # Zapadka zysku: 5$, 10$, 15$...
COOLDOWN_AFTER_EXIT_SEC = 5400     # BLOKADA 90 MINUT: zakaz dotykania monety po wyjściu z pozycji!

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
recently_closed_coins = {}         # Symbol -> timestamp zamknięcia (ochrona przed overtradingiem)

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

def record_closed_trade(symbol, strat, buy_price, sell_price, qty, reason):
    global trade_history, recently_closed_coins
    recently_closed_coins[symbol] = time.time()  # Aktywacja 90-minutowego zakazu ponownego kupna
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

def get_today_pnl(tickers=None):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with lock_history:
        realized_today = trade_history.get("daily", {}).get(today_str, {}).get("realized_usd", 0.0)

    unrealized_today = 0.0
    if tickers:
        with lock_pos:
            for s, pos in active_positions.items():
                curr = safe_float(tickers.get(s, {}).get("lastPrice"), pos["buy_price"])
                bp = pos["buy_price"]
                qty_f = safe_float(pos.get("qty", 0.0))
                unrealized_today += (curr - bp) * qty_f

    return realized_today, unrealized_today, (realized_today + unrealized_today)

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
# NIEZAWODNY ZRZUT POZYCJI DO USDC
# ==============================================================================
def force_exit_coin_to_usdc(symbol, qty_to_sell=None, reason="Exit"):
    base_c = get_base_coin(symbol)
    try:
        session.cancel_all_orders(category=CATEGORY, symbol=symbol)
    except Exception:
        pass
    time.sleep(0.15)

    bal = get_coin_available_balance(base_c)
    tickers = get_all_spot_tickers()
    curr_p = safe_float(tickers.get(symbol, {}).get("lastPrice"), 0.0)

    target_qty = min(bal, qty_to_sell) if qty_to_sell else bal
    if (target_qty * curr_p) < MIN_EXCHANGE_NOTIONAL:
        return True

    step, minq = get_symbol_lot_filter(symbol)
    sell_qty = format_qty_with_step(target_qty, step, minq)

    if sell_qty:
        res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=sell_qty)
        if res.get("retCode") == 0:
            logging.info(f"✅ [SPRZEDAŻ UDANA] {symbol} | Ilość: {sell_qty} | {reason}")
            return True

    fallback_qty = format_qty_with_step(target_qty * 0.995, step, minq)
    if fallback_qty:
        res2 = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=fallback_qty)
        if res2.get("retCode") == 0:
            logging.info(f"✅ [SPRZEDAŻ FALLBACK] {symbol} | Ilość: {fallback_qty} | {reason}")
            return True

    logging.error(f"❌ [BŁĄD SPRZEDAŻY] {symbol}!")
    return False

def emergency_close_all_portfolio(reason="Emergency"):
    logging.error(f"🛑🛑🛑 [AWARYJNY ZRZUT CAŁEGO PORTFELA] Powód: {reason}")
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            for c_info in res["result"]["list"][0].get("coin", []):
                coin = c_info.get("coin")
                if coin in ["USDC", "USDT", "EURC", "USD", "EUR"]:
                    continue
                sym = f"{coin}{QUOTE_COIN}"
                force_exit_coin_to_usdc(sym, None, reason)
    except Exception as e:
        logging.error(f"Błąd zrzutu portfela: {e}")
    finally:
        sync_positions_from_bybit()

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

            for sym in list(active_positions.keys()):
                if not sym.endswith(QUOTE_COIN) or real_holdings.get(sym, 0.0) <= 0:
                    del active_positions[sym]

            tickers = get_all_spot_tickers()
            for sym, bal in real_holdings.items():
                if sym not in active_positions:
                    curr_p = safe_float(tickers.get(sym, {}).get("lastPrice"), 0.0)
                    if (bal * curr_p) >= MIN_EXCHANGE_NOTIONAL:
                        step, minq = get_symbol_lot_filter(sym)
                        q_str = format_qty_with_step(bal, step, minq)
                        if q_str:
                            active_positions[sym] = {
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
    except Exception as e:
        logging.error(f"Błąd synchronizacji: {e}")

# ==============================================================================
# DYNAMICZNY SKANER PŁYNNYCH PAR USDC (TYLKO DUŻY OBRÓT)
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
            continue  # Koniec ze śmieciowymi monetami poniżej 250k obrotu!

        bid = safe_float(t_data.get("bid1Price"))
        ask = safe_float(t_data.get("ask1Price"))
        if bid <= 0 or ask <= 0:
            continue
        spread = (ask - bid) / bid
        if spread > USDC_MAX_SPREAD_PCT:
            continue

        valid_candidates.append({
            "symbol": symbol,
            "turnover": turnover_24h
        })

    valid_candidates.sort(key=lambda x: x["turnover"], reverse=True)
    return [c["symbol"] for c in valid_candidates[:MAX_SCAN_CANDIDATES]]

# ==============================================================================
# JEDYNY AKTYWNY SILNIK: CAPITULATION SNIPER (BEZWZGLĘDNY ZAKAZ KUPNA NA GÓRKACH)
# ==============================================================================
def run_bonus_hunter_strategy():
    global active_positions, recently_closed_coins
    all_tickers = get_all_spot_tickers()
    if not all_tickers:
        return

    # Sprawdzenie dziennego limitu straty (-20 USD)
    _, _, total_today = get_today_pnl(all_tickers)
    if total_today <= -MAX_DAILY_LOSS_USD:
        return

    total_equity, free_usdc = get_balances()
    now_ts = time.time()
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

        # BLOKADA 90 MINUT DLA MONET JUŻ ZAMKNIĘTYCH (KONIEC Z MIELENIEM LIT!)
        last_closed = recently_closed_coins.get(symbol, 0)
        if (now_ts - last_closed) < COOLDOWN_AFTER_EXIT_SEC:
            continue

        df = results.get(symbol)
        if df is None or len(df) < 60:
            continue

        try:
            c_now = df.iloc[-1]   # Świeca bieżąca (na żywo)
            c = df.iloc[-2]       # Poprzednia świeca (zamknięta)
            curr_price = c_now['close']

            # ==================================================================
            # ŻELAZNA BARIERA ANTY-SZCZYTOWA (ELIMINACJA WEJŚĆ TYPU LIT, NEAR, PEPE)
            # ==================================================================
            # 1. Bezwzględny zakaz kupowania nad średnimi (EMA 9, EMA 21, EMA 50)
            if curr_price >= c_now['ema9'] or curr_price >= c_now['ema21'] or curr_price >= c_now['ema50']:
                continue

            # 2. Bezwzględny zakaz kupowania w górnej połowie wstęgi Bollingera
            if curr_price >= c_now['bb_mid']:
                continue

            # 3. Wskaźnik RSI MUSI być skrajnie wyprzedany (< 38.0). Zero zakupów przy RSI 45-75!
            if c_now['rsi'] >= 38.0:
                continue

            # 4. Zakaz gonienia zielonej świecy w locie
            if c_now['close'] >= (c_now['open'] * 1.004):
                continue

            # 5. Zakaz kupowania monet, które rosły w ciągu ostatniej godziny
            if curr_price >= df['close'].iloc[-5]:
                continue

            # ==================================================================
            # JEDYNY DOPUSZCZALNY WARUNEK: CZERWONY ZRZUT KAPITULACYJNY Z ABSORPCJĄ
            # ==================================================================
            # Świeca c MUSI być dużą CZERWONĄ świecą spadkową (min. 1.8% spadku)
            c_is_red = c['close'] < c['open']
            c_drop_pct = (c['open'] - c['close']) / c['open'] if c_is_red else 0.0
            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0

            is_panic_dump = (
                c_is_red and 
                (c_drop_pct >= 0.018) and 
                (vol_ratio >= 1.8) and 
                (c['low'] <= c['bb_lower'] * 1.002)
            )

            # Kurs uderzył w dno i dopiero zaczyna lekko zawracać (+0.40% do +0.90% od dna knota)
            rebound_from_low = (curr_price - c['low']) / c['low'] if c['low'] > 0 else 0.0
            is_valid_bottom_rebound = (0.0040 <= rebound_from_low <= 0.0110)

            if not (is_panic_dump and is_valid_bottom_rebound):
                continue

            score = 500.0 + (rebound_from_low * 1000.0) + (vol_ratio * 10.0)
            candidates.append({
                "symbol": symbol, "score": score, "swing_low": c['low'], "curr_price": curr_price
            })

        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    if not candidates:
        return

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]

    cand_sym = best["symbol"]
    curr_price = best["curr_price"]
    swing_low = best["swing_low"]

    with lock_pos:
        current_count = len(active_positions)

    if current_count < MAX_TOTAL_POSITIONS:
        target_allocation = total_equity * CAPITAL_PER_TRADE_PCT
        if free_usdc < (target_allocation * 0.98):
            return

        # Bardzo ciasny Stop Loss pod dnem knota zrzutu
        calculated_sl = swing_low * 0.996
        hard_sl_cap = curr_price * 0.988
        initial_sl = max(calculated_sl, hard_sl_cap)

        res = session.place_order(category=CATEGORY, symbol=cand_sym, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
        if res.get("retCode") == 0:
            step, minq = get_symbol_lot_filter(cand_sym)
            q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
            if q_str:
                with lock_pos:
                    active_positions[cand_sym] = {
                        "strategy": "CAPITULATION_SNIPER",
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
                logging.info(f"🎯🔥 [ZŁAPANO DOŁEK PANIKI] {cand_sym} | Kupiono po zrzucie na cenie: {curr_price} | SL pod knotem: {initial_sl:.4f}")

# ==============================================================================
# MONITORING ZYSKÓW: TP1 (+5$) + BREAK-EVEN + MASTER EQUITY LOCK
# ==============================================================================
def monitoring_worker():
    global active_positions, portfolio_state
    logging.info("🚀 [SYSTEM] Uruchomiono monitoring z twardą obroną salda portfela i realizacją TP1.")
    while True:
        try:
            with lock_pos:
                items = list(active_positions.items())

            tickers = get_all_spot_tickers()
            now_ts = time.time()
            total_eq, free_usdc = get_balances()

            # ------------------------------------------------------------------
            # 1. NADRZĘDNA BLOKADA SALDA PORTFELA (SZCZYT - 5$ = NATYCHMIASTOWY EXIT)
            # ------------------------------------------------------------------
            if total_eq > 0:
                current_peak = portfolio_state.get("peak_total_equity", 0.0)

                if total_eq > current_peak:
                    portfolio_state["peak_total_equity"] = total_eq
                    save_portfolio_state(portfolio_state)
                    current_peak = total_eq
                    logging.info(f"👑 [SZCZYT SALDA] Portfel: {total_eq:.2f}$ | Obrona salda przy: {total_eq - EQUITY_DROP_LIMIT_USD:.2f}$")

                if len(items) > 0 and current_peak > 0:
                    if total_eq <= (current_peak - EQUITY_DROP_LIMIT_USD):
                        emergency_close_all_portfolio(f"Master Equity Drop (-5$ ze szczytu {current_peak:.2f}$)")
                        portfolio_state["peak_total_equity"] = total_eq
                        save_portfolio_state(portfolio_state)
                        time.sleep(2.0)
                        continue

            # ------------------------------------------------------------------
            # 2. DZIENNY LIMIT STRATY (-20 USD)
            # ------------------------------------------------------------------
            _, _, total_today = get_today_pnl(tickers)
            if total_today <= -MAX_DAILY_LOSS_USD and len(items) > 0:
                emergency_close_all_portfolio("Daily Loss Limit Hit (-20 USD)")
                time.sleep(2.0)
                continue

            if not items:
                time.sleep(1.0)
                continue

            # ------------------------------------------------------------------
            # 3. PROWADZENIE OTWARTEJ POZYCJI
            # ------------------------------------------------------------------
            to_delete = []
            for symbol, pos in items:
                ticker = tickers.get(symbol)
                c_curr = safe_float(ticker.get("lastPrice")) if ticker else pos["buy_price"]
                if c_curr <= 0:
                    continue

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

                # --------------------------------------------------------------
                # KROK 1: TP1 PRZY ZYSKU +5$ LUB +1.8% -> ZRZUT 50% I BEZPIECZNE BREAK-EVEN
                # --------------------------------------------------------------
                if (net_pnl_usd >= DOLLAR_STEP_USD or gain_pct >= 0.018) and not pos.get("tp1_executed", False):
                    half_qty = qty_f / 2.0
                    step, minq = get_symbol_lot_filter(symbol)
                    half_qty_str = format_qty_with_step(half_qty, step, minq)

                    if half_qty_str and force_exit_coin_to_usdc(symbol, safe_float(half_qty_str), "TP1 (+50% zysku do gotówki)"):
                        pos["tp1_executed"] = True
                        remaining_qty = format_qty_with_step(qty_f - safe_float(half_qty_str), step, minq)
                        pos["qty"] = remaining_qty or half_qty_str
                        # SL na Break-Even (+0.4% na pokrycie opłat) -> ZERO RYZYKA DALSZEJ STRATY!
                        pos["trailing_sl"] = bp * 1.004
                        logging.info(f"💎💰 [TP1 ZREALIZOWANE] {symbol} | Połowa pozycji zamknięta w zysku! SL na Break-Even (+0.4%).")
                        save_positions(active_positions)

                # Prowadzenie Stop Lossa
                current_sl = pos.get("trailing_sl", bp * 0.988)

                if gain_pct >= 0.025:
                    current_sl = max(current_sl, peak_p * (1.0 - 0.0085))
                elif gain_pct >= 0.012:
                    current_sl = max(current_sl, bp * 1.004)

                pos["trailing_sl"] = current_sl

                trigger_exit = False
                exit_reason = ""

                # Wyjście po uderzeniu w Stop Loss
                if c_curr <= current_sl:
                    trigger_exit = True
                    exit_reason = f"SL / Trailing Exit (Wynik: {gain_pct*100:+.2f}%)"
                # Wyjście ze stagnacji (tylko jeśli po 25 min nie ma nawet +0.3%)
                elif time_in_pos >= 1500 and gain_pct < 0.0030 and not pos.get("tp1_executed", False):
                    trigger_exit = True
                    exit_reason = f"Stagnation Exit (25m bez ruchu, PnL: {gain_pct*100:+.2f}%)"

                if trigger_exit:
                    if force_exit_coin_to_usdc(symbol, None, exit_reason):
                        record_closed_trade(symbol, pos.get("strategy"), bp, c_curr, safe_float(pos.get("qty")), exit_reason)
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

            sync_positions_from_bybit()

            with lock_pos:
                current_items = list(active_positions.items())

            all_tickers = get_all_spot_tickers()
            realized_d, unrealized_d, total_day_pnl = get_today_pnl(all_tickers)
            daily_limit_hit = total_day_pnl <= -MAX_DAILY_LOSS_USD

            if now - last_print >= 25:
                total_eq, free_usdc = get_balances()
                peak_eq = portfolio_state.get("peak_total_equity", total_eq)
                status_circuit = "🛑 [ZABLOKOWANY - LIMIT -20$]" if daily_limit_hit else "✅ Aktywny"
                stop_floor_eq = max(0.0, peak_eq - EQUITY_DROP_LIMIT_USD)

                logging.info("--------------------------------------------------------------------------------")
                logging.info(f"💰 [PORTFEL] Saldo: {total_eq:.2f}$ | Szczyt: {peak_eq:.2f}$ (Obrona przy: {stop_floor_eq:.2f}$) | Wolne: {free_usdc:.2f}$")
                logging.info(f"📅 [BILANS DNIA] Dzisiaj: {total_day_pnl:+.2f}$ | Status bota: {status_circuit} | Aktywne pozycje: {len(current_items)}/{MAX_TOTAL_POSITIONS}")
                for s, p in current_items:
                    curr = safe_float(all_tickers.get(s, {}).get("lastPrice"), p["buy_price"])
                    pnl_pct = ((curr - p["buy_price"]) / p["buy_price"]) * 100
                    pnl_usd = (curr - p["buy_price"]) * safe_float(p.get("qty", 0.0))
                    tp_status = " [TP1 ZABEZPIECZONE]" if p.get("tp1_executed", False) else ""
                    min_elapsed = int((now - p.get("entry_time", now)) / 60)
                    logging.info(f"📈 [{s}] Zysk: {pnl_usd:+.2f}$ ({pnl_pct:+.2f}%){tp_status} | Czas: {min_elapsed}m | SL: {p.get('trailing_sl', 0):.4f}")
                logging.info("--------------------------------------------------------------------------------")
                last_print = now

            if not daily_limit_hit and is_btc_market_healthy():
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
        self.wfile.write(b"Bybit Spot Bot Active - Strict Capitulation Sniper Only")
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
