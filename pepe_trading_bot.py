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
# BEZPIECZEŃSTWO SIECI I SYNCHRONIZACJA CZASU
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

TARGET_COINS = [
    "PEPEUSDC", "SOLUSDC", "DOGEUSDC", "WIFUSDC", "SUIUSDC", "BONKUSDC",
    "ETHUSDC", "XRPUSDC", "NEARUSDC", "AVAXUSDC", "LINKUSDC", "INJUSDC", "SHIBUSDC"
]

CAPITAL_PER_TRADE_PCT = 0.45       # 45% salda na pozycję (max 2 pozycje)
MAX_TOTAL_POSITIONS = 2            
STANDARD_MIN_ORDER_VALUE = 15.0    
MIN_EXCHANGE_NOTIONAL = 5.2        

MIN_24H_TURNOVER_USD = 70000.0     # Odrzucanie pustych par
USDC_MAX_SPREAD_PCT = 0.0050       

MAX_INITIAL_STOP_LOSS_PCT = 0.024  # Bezpieczny początkowy SL: 2.4% pod strukturą dołka

# DOLAROWY TRAILING STOP KOSZYKA (PORTFEL)
USD_PROFIT_TRIGGER = 20.0          
USD_TRAILING_DROP = 5.0            
basket_peak_profit_usd = 0.0       

# STAGNATION & ROTACJA
STAGNATION_MIN_TIME_SEC = 3600     # 60 min bez ruchu zanim uznamy za stagnację
ROTATION_MIN_HOLD_SEC = 2700       # Min. 45 min trzymania przed rozważeniem rotacji
ROTATION_MIN_CHALLENGER_VOL = 1.60 

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

adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
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

        if "daily" not in trade_history:
            trade_history["daily"] = {}
        if today_str not in trade_history["daily"]:
            trade_history["daily"][today_str] = {"pnl_usd": 0.0, "trades": 0, "wins": 0, "losses": 0, "total_fees": 0.0, "total_turnover": 0.0}

        d = trade_history["daily"][today_str]
        d["pnl_usd"] = round(d.get("pnl_usd", 0.0) + pnl_usd, 2)
        d["trades"] = d.get("trades", 0) + 1
        d["total_fees"] = round(d.get("total_fees", 0.0) + estimated_fee, 4)
        d["total_turnover"] = round(d.get("total_turnover", 0.0) + ((buy_price + sell_price) * qty), 2)

        save_trade_history(trade_history)
        sign = "+" if pnl_usd >= 0 else ""
        logging.info(f"📊 [BILANS TRANSAKCJI] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | Fee: -{estimated_fee:.2f}$ | {reason}")

# ==============================================================================
# DANE RYNKOWE I ANALIZA TECHNICZNA
# ==============================================================================
def get_all_spot_tickers():
    try:
        res = session.get_tickers(category=CATEGORY)
        if res.get("retCode") == 0:
            items = res.get("result", {}).get("list", [])
            return {item["symbol"]: item for item in items}
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
            if candle_change <= -0.012:
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
                    real_holdings[f"{coin}USDC"] = bal

            to_remove = []
            for sym, pos in list(active_positions.items()):
                if sym not in TARGET_COINS:
                    to_remove.append((sym, "symbol spoza listy"))
                    continue
                bal = real_holdings.get(sym, 0.0)
                if bal <= 0:
                    to_remove.append((sym, "brak salda na koncie Bybit"))

            for sym, reason in to_remove:
                if sym in active_positions:
                    del active_positions[sym]
                    logging.info(f"🧹 [SYNC] Usunięto {sym} ({reason}).")

            for sym, bal in real_holdings.items():
                if sym in TARGET_COINS and sym not in active_positions:
                    tickers = get_all_spot_tickers()
                    curr_p = safe_float(tickers.get(sym, {}).get("lastPrice"), 0.0)
                    if (bal * curr_p) >= MIN_EXCHANGE_NOTIONAL:
                        step, minq = get_symbol_lot_filter(sym)
                        q_str = format_qty_with_step(bal, step, minq)
                        if q_str:
                            initial_sl = curr_p * (1.0 - MAX_INITIAL_STOP_LOSS_PCT)
                            active_positions[sym] = {
                                "strategy": "SYNCED",
                                "buy_price": curr_p,
                                "peak_price": curr_p,
                                "qty": q_str,
                                "initial_qty": q_str,
                                "tp1_executed": False,
                                "trailing_sl": initial_sl,
                                "entry_time": time.time()
                            }
                            is_micro = any(m in sym for m in ["PEPE", "BONK", "SHIB"])
                            sl_fmt = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
                            logging.info(f"🎯 [SYNC] Zaimportowano pozycję {sym} | Ilość: {q_str} | SL: {sl_fmt}")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji Bybit: {e}")

# ==============================================================================
# STRATEGIA ZAKUPÓW: RETEST & PULLBACK (ZAKAZ KUPOWANIA SZCZYTÓW)
# ==============================================================================
def run_sniper_strategy():
    global active_positions
    total_equity, free_usdc = get_balances()
    target_allocation = total_equity * CAPITAL_PER_TRADE_PCT
    now_ts = time.time()

    tickers = get_all_spot_tickers()
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_data_worker, symbol): symbol for symbol in TARGET_COINS}
        for future in concurrent.futures.as_completed(futures):
            sym, df = future.result()
            if df is not None:
                results[sym] = df

    best_candidate = None

    for symbol in TARGET_COINS:
        with lock_pos:
            if symbol in active_positions:
                continue

        # 1. Płynność i spread
        t_data = tickers.get(symbol, {})
        turnover_24h = safe_float(t_data.get("turnover24h", 0.0))
        if turnover_24h < MIN_24H_TURNOVER_USD:
            continue

        bid = safe_float(t_data.get("bid1Price"))
        ask = safe_float(t_data.get("ask1Price"))
        if bid > 0 and ask > 0:
            if ((ask - bid) / bid) > USDC_MAX_SPREAD_PCT:
                continue

        df = results.get(symbol)
        if df is None or len(df) < 60:
            continue

        try:
            c_now = df.iloc[-1]
            c = df.iloc[-2]
            p = df.iloc[-3]

            # Cooldown po stracie (min. 45 min)
            cooldown_ok = True
            with lock_history:
                for t in reversed(trade_history.get("trades_log", [])):
                    if t.get("symbol") == symbol:
                        if t.get("net_pnl", 0) < 0 and (now_ts - t.get("timestamp", 0)) < 2700:
                            cooldown_ok = False
                        break
            if not cooldown_ok:
                continue

            # ------------------------------------------------------------------
            # FILTRY ANTY-SZCZYTOWE
            # ------------------------------------------------------------------
            is_micro = any(m in symbol for m in ["PEPE", "BONK", "SHIB"])
            max_ema9_dist = 0.013 if is_micro else 0.0095

            # 1. Zakaz kupowania, gdy cena uciekła od EMA 9
            dist_from_ema9 = (c['close'] - c['ema9']) / c['ema9']
            if dist_from_ema9 > max_ema9_dist:
                continue

            # 2. Zakaz kupowania świecy wyczerpania
            candle_body_pct = (c['close'] - c['open']) / c['open']
            if candle_body_pct >= 0.022:
                continue

            # 3. Filtr podaży z knota
            candle_range = c['high'] - c['low']
            upper_wick = c['high'] - max(c['open'], c['close'])
            if candle_range > 0 and (upper_wick / candle_range > 0.25):
                continue

            # 4. Filtr trendu EMA 200
            if c['close'] < c['ema200']:
                dist_to_ema200 = (c['ema200'] - c['close']) / c['close']
                if dist_to_ema200 < 0.025:
                    continue

            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0
            rsi_val = c.get('rsi', 50.0)
            if rsi_val >= 65.0:
                continue

            # ------------------------------------------------------------------
            # WEJŚCIE NA PULLBACKU
            # ------------------------------------------------------------------
            in_solid_trend = (c['close'] > c['ema50']) and (c['ema21'] > c['ema50'])
            tested_ema = (c['low'] <= c['ema9'] * 1.002) or (p['low'] <= p['ema9'] * 1.002) or (c['low'] <= c['ema21'] * 1.003)
            pullback_entry = in_solid_trend and tested_ema and (c['close'] > c['open']) and (vol_ratio >= 1.15)
            flat_compression = in_solid_trend and (abs(c['close'] - c['ema9']) / c['ema9'] < 0.005) and (c['close'] > p['high']) and (vol_ratio >= 1.25)

            live_green = c_now['close'] >= c_now['open']

            if (pullback_entry or flat_compression) and live_green:
                tag = "PULLBACK_RETEST" if pullback_entry else "COMPRESSION_BOUNCE"
                score = vol_ratio * (1.3 if pullback_entry else 1.0)
                if best_candidate is None or score > best_candidate['score']:
                    best_candidate = {
                        "symbol": symbol, "score": score, "tag": tag, 
                        "vol_ratio": vol_ratio, "df": df
                    }
        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    if not best_candidate:
        return

    cand_sym = best_candidate["symbol"]
    cand_tag = best_candidate["tag"]
    cand_df = best_candidate["df"]

    with lock_pos:
        current_count = len(active_positions)
        current_items = list(active_positions.items())

    # SCENARIUSZ 1: WOLNY SLOT
    if current_count < MAX_TOTAL_POSITIONS:
        if free_usdc >= (target_allocation * 0.98):
            c_now = cand_df.iloc[-1]
            curr_price = c_now['close']
            
            recent_low = min(cand_df['low'].iloc[-4:].min(), c_now['low'])
            calculated_sl = recent_low * 0.995
            hard_sl_cap = curr_price * (1.0 - MAX_INITIAL_STOP_LOSS_PCT)
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
                            "tp1_executed": False,
                            "trailing_sl": initial_sl,
                            "entry_time": time.time()
                        }
                        save_positions(active_positions)
                    is_micro = any(m in cand_sym for m in ["PEPE", "BONK", "SHIB"])
                    sl_fmt = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
                    logging.info(f"⚡🎯 [ZAKUP NA RETEŚCIE] {cand_sym} ({cand_tag}) | Cena: {curr_price} | SL: {sl_fmt} (-{((curr_price-initial_sl)/curr_price)*100:.2f}%)")
        return

    # SCENARIUSZ 2: ROTACJA PRZY STAGNACJI (>45 MIN)
    if best_candidate["vol_ratio"] < ROTATION_MIN_CHALLENGER_VOL:
        return

    stagnant_candidate = None
    for def_sym, def_pos in current_items:
        c_curr = safe_float(tickers.get(def_sym, {}).get("lastPrice"), def_pos["buy_price"])
        bp = def_pos["buy_price"]
        gain_pct = (c_curr - bp) / bp
        time_in_pos = now_ts - def_pos.get("entry_time", now_ts)

        if time_in_pos < ROTATION_MIN_HOLD_SEC or gain_pct >= 0.010:
            continue

        if gain_pct <= 0.002:
            stagnant_candidate = {
                "symbol": def_sym, "pos": def_pos, "curr_price": c_curr, 
                "gain_pct": gain_pct, "time_min": int(time_in_pos / 60)
            }
            break

    if stagnant_candidate:
        old_sym = stagnant_candidate["symbol"]
        old_gain = stagnant_candidate["gain_pct"]
        logging.info(f"🔄⚡ [ROZSĄDNA ROTACJA] Zamiana {old_sym} ({old_gain*100:+.2f}%, {stagnant_candidate['time_min']}m) na {cand_sym} (Vol: {best_candidate['vol_ratio']:.2f}x).")

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
                    reason = f"Rotacja ze stagnacji -> {cand_sym}"
                    record_closed_trade(old_sym, stagnant_candidate["pos"].get("strategy"), stagnant_candidate["pos"]["buy_price"], stagnant_candidate["curr_price"], safe_float(sell_q_str), reason)
                    with lock_pos:
                        if old_sym in active_positions:
                            del active_positions[old_sym]
                            save_positions(active_positions)

        if sold_ok:
            time.sleep(1.2)
            _, free_usdc_now = get_balances()
            total_eq_now, _ = get_balances()
            buy_capital = min(free_usdc_now * 0.985, total_eq_now * CAPITAL_PER_TRADE_PCT)
            if buy_capital >= STANDARD_MIN_ORDER_VALUE:
                c_now_c = cand_df.iloc[-1]
                curr_price_c = c_now_c['close']
                recent_low_c = min(cand_df['low'].iloc[-4:].min(), c_now_c['low'])
                initial_sl_c = max(recent_low_c * 0.995, curr_price_c * (1.0 - MAX_INITIAL_STOP_LOSS_PCT))

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
                            "tp1_executed": False,
                            "trailing_sl": initial_sl_c,
                            "entry_time": time.time()
                        }
                        save_positions(active_positions)
                    logging.info(f"🎉🚀 [ROTACJA] Otwarto {cand_sym} na korekcie po cenie {curr_price_c}!")

# ==============================================================================
# MONITORING W CZASIE RZECZYWISTYM (1.5s) - ADAPTACYJNA ZAPADKA ZYSKÓW
# ==============================================================================
def monitoring_worker():
    global active_positions, basket_peak_profit_usd
    logging.info("🚀 [SYSTEM] Uruchomiono monitoring z adaptacyjną zapadką zysków.")
    while True:
        try:
            with lock_pos:
                items = list(active_positions.items())
            if not items:
                basket_peak_profit_usd = 0.0
                time.sleep(1.5)
                continue

            tickers = get_all_spot_tickers()
            now_ts = time.time()

            # 1. DOLAROWY TRAILING STOP KOSZYKA (+20 USD szczyt -> stop po -5 USD)
            total_unrealized_usd = 0.0
            position_prices = {}

            for symbol, pos in items:
                ticker = tickers.get(symbol)
                c_curr = safe_float(ticker.get("lastPrice")) if ticker else 0.0
                if c_curr <= 0:
                    c_curr = pos["buy_price"]
                position_prices[symbol] = c_curr

                bp = pos["buy_price"]
                q = safe_float(pos.get("qty", 0.0))
                total_unrealized_usd += (c_curr - bp) * q

            if total_unrealized_usd > basket_peak_profit_usd:
                basket_peak_profit_usd = total_unrealized_usd

            if basket_peak_profit_usd >= USD_PROFIT_TRIGGER:
                basket_sl_target = basket_peak_profit_usd - USD_TRAILING_DROP
                if total_unrealized_usd <= basket_sl_target:
                    logging.info(f"🏆💰 [BASKET LOCK] Zrealizowano zysk koszyka: +{total_unrealized_usd:.2f}$ (ze szczytu +{basket_peak_profit_usd:.2f}$).")
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
                    time.sleep(2.0)
                    continue

            # 2. ADAPTACYJNA DRABINKA ZYSKÓW DLA KAŻDEJ POZYCJI
            to_delete = []
            for symbol, pos in items:
                c_curr = position_prices.get(symbol, 0.0)
                if c_curr <= 0:
                    continue

                bp = pos["buy_price"]
                entry_time = pos.get("entry_time", now_ts)
                time_in_pos = now_ts - entry_time
                gain_pct = (c_curr - bp) / bp

                # Śledzenie absolutnego szczytu
                peak = max(pos.get("peak_price", bp), c_curr)
                pos["peak_price"] = peak
                peak_pct = (peak - bp) / bp

                current_sl = pos.get("trailing_sl", bp * (1.0 - MAX_INITIAL_STOP_LOSS_PCT))

                # --------------------------------------------------------------
                # DYNAMICZNA ZAPADKA (RATCHET TRAILING LADDER)
                # --------------------------------------------------------------
                if peak_pct >= 0.085:
                    # Poziom 6 (Szczyt >= +8.5%): Trailing 1.6% pod szczytem, min. +6.5% zysku
                    current_sl = max(current_sl, peak * (1.0 - 0.016), bp * 1.065)
                elif peak_pct >= 0.065:
                    # Poziom 5 (Szczyt >= +6.5%): Trailing 1.4% pod szczytem, min. +4.8% zysku
                    current_sl = max(current_sl, peak * (1.0 - 0.014), bp * 1.048)
                elif peak_pct >= 0.045:
                    # Poziom 4 (Szczyt >= +4.5%): Trailing 1.25% pod szczytem, min. +3.2% zysku
                    current_sl = max(current_sl, peak * (1.0 - 0.0125), bp * 1.032)
                elif peak_pct >= 0.032:
                    # Poziom 3 (Szczyt >= +3.2%): TP1 + zabezpieczenie min. +2.0% zysku
                    if not pos.get("tp1_executed", False):
                        full_qty = Decimal(str(pos.get("initial_qty", pos["qty"])))
                        half_qty = full_qty / Decimal("2")
                        step, minq = get_symbol_lot_filter(symbol)
                        half_qty_str = format_qty_with_step(half_qty, step, minq)

                        if half_qty_str and (safe_float(half_qty_str) * c_curr >= MIN_EXCHANGE_NOTIONAL):
                            res_tp1 = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=half_qty_str)
                            if res_tp1.get("retCode") == 0:
                                remaining_qty = format_qty_with_step(full_qty - Decimal(half_qty_str), step, minq)
                                pos["qty"] = remaining_qty or half_qty_str
                                pos["tp1_executed"] = True
                                record_closed_trade(symbol, pos.get("strategy"), bp, c_curr, safe_float(half_qty_str), "TP1 (+3.2%)")
                                logging.info(f"💎💰 [TAKE PROFIT 1] {symbol} | Zabezpieczono 50% pozycji przy +{gain_pct*100:.2f}%!")
                        pos["tp1_executed"] = True
                    
                    current_sl = max(current_sl, peak * (1.0 - 0.0115), bp * 1.020)
                elif peak_pct >= 0.023:
                    # Poziom 2 (Szczyt >= +2.3%): Blokada zysku min. +1.20% (NIE WRACA DO BE!)
                    current_sl = max(current_sl, peak * (1.0 - 0.0110), bp * 1.012)
                elif peak_pct >= 0.016:
                    # Poziom 1 (Szczyt >= +1.6%): Pewny Break-Even (+0.60% zysku na pokrycie opłat)
                    current_sl = max(current_sl, bp * 1.006)

                pos["trailing_sl"] = current_sl

                # --------------------------------------------------------------
                # WARUNKI WYJŚCIA
                # --------------------------------------------------------------
                trigger_exit = False
                exit_reason = ""

                if c_curr <= current_sl:
                    trigger_exit = True
                    exit_reason = f"Ratchet Trailing SL (Zrealizowano: {gain_pct*100:+.2f}% | Szczyt: {peak_pct*100:+.2f}%)"
                elif time_in_pos >= STAGNATION_MIN_TIME_SEC and gain_pct < 0.002:
                    trigger_exit = True
                    exit_reason = f"Stagnation Exit (60m bez ruchu | {gain_pct*100:+.2f}%)"

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
                                logging.info(f"🚨 [WYJŚCIE] {symbol} | Wynik: {gain_pct*100:+.2f}% | {exit_reason}")
                    to_delete.append(symbol)

            if to_delete:
                with lock_pos:
                    for s in to_delete:
                        if s in active_positions:
                            del active_positions[s]
                save_positions(active_positions)

        except Exception as e:
            logging.error(f"Błąd monitoringu: {e}")
        time.sleep(1.5)

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

            if now - last_print >= 30:
                total_eq, free_usdc = get_balances()
                tickers = get_all_spot_tickers()
                total_unrealized_usd = sum(
                    (safe_float(tickers.get(s, {}).get("lastPrice"), p["buy_price"]) - p["buy_price"]) * safe_float(p.get("qty", 0.0))
                    for s, p in current_items
                )
                logging.info("--------------------------------------------------------------------------------")
                status_basket = f"Szczyt: +{basket_peak_profit_usd:.2f}$" if basket_peak_profit_usd >= USD_PROFIT_TRIGGER else "Nieaktywny (< 20$)"
                logging.info(f"💰 [PORTFEL] Saldo: {total_eq:.2f}$ | Wolne: {free_usdc:.2f}$ | Niezrealizowany PnL: {total_unrealized_usd:+.2f}$ ({status_basket}) | Pozycje: {len(current_items)}/{MAX_TOTAL_POSITIONS}")
                for s, p in current_items:
                    curr = safe_float(tickers.get(s, {}).get("lastPrice"), p["buy_price"])
                    pnl = ((curr - p["buy_price"]) / p["buy_price"]) * 100
                    pnl_usd = (curr - p["buy_price"]) * safe_float(p.get("qty", 0.0))
                    sl = p.get("trailing_sl", 0.0)
                    is_micro = any(m in s for m in ["PEPE", "BONK", "SHIB"])
                    sl_fmt = f"{sl:.8f}" if is_micro else f"{sl:.4f}"
                    peak_fmt = f"{p.get('peak_price', curr):.8f}" if is_micro else f"{p.get('peak_price', curr):.4f}"
                    min_elapsed = int((now - p.get("entry_time", now)) / 60)
                    logging.info(f"📈 [{s}] PnL: {pnl:+.2f}% ({pnl_usd:+.2f}$) | Czas: {min_elapsed}m | Szczyt: {peak_fmt} | Zabezpieczenie SL: {sl_fmt}")
                logging.info("--------------------------------------------------------------------------------")
                last_print = now

            if is_btc_market_healthy():
                run_sniper_strategy()

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
        self.wfile.write(b"Bybit Spot Bot Active - Ratchet Trailing Enabled")
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
