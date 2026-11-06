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

# Wymuszenie natychmiastowego wypychania logów na Renderze (brak buforowania stdout)
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ==============================================================================
# BEZPIECZEŃSTWO SIECI I SYNCHRONIZACJA CZASU BYBIT
# ==============================================================================
socket.setdefaulttimeout(15)

server_time_offset_sec = 0.0

def get_synced_timestamp_ms() -> int:
    return int((time.time() + server_time_offset_sec) * 1000)

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

# KOSZYK 6 LIDERÓW ZMIENNOŚCI
TARGET_COINS = ["PEPEUSDC", "SOLUSDC", "DOGEUSDC", "WIFUSDC", "SUIUSDC", "BONKUSDC"]

CAPITAL_PER_TRADE_PCT = 0.45       # 45% salda na transakcję (pozwala na 2 pozycje)
MAX_TOTAL_POSITIONS = 2            # Maksymalnie 2 aktywne transakcje jednocześnie
STANDARD_MIN_ORDER_VALUE = 15.0    # Minimalna wartość wejścia w USDC
MIN_EXCHANGE_NOTIONAL = 5.2        # Minimalna wartość zlecenia na Bybit (z marginesem)

# PARAMETRY OCHRONY KAPITAŁU (MINIMALIZACJA STRAT)
MAX_INITIAL_STOP_LOSS_PCT = 0.012  # Sztywny maksymalny początkowy SL = 1.20%
FAST_INVALIDATION_PCT = -0.0065    # Szybkie ucięcie braku impetu (-0.65% w pierwsze 10 min)
BREAK_EVEN_TRIGGER_PCT = 0.014     # Próg przestawienia na Break-Even (+1.40%)
BREAK_EVEN_LOCK_PCT = 0.0035       # Poziom Break-Even (+0.35% na pokrycie opłat)

# BEZPIECZNIKI RYNKOWE
USDC_MAX_SPREAD_PCT = 0.0080
BTC_FAST_DROP_BUY_BLOCK_PCT = -0.012

APP_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://bybit-bot-i1lw.onrender.com")
POSITIONS_FILE = "active_spot_positions.json"
TRADE_HISTORY_FILE = "trade_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ==============================================================================
# BLOKADY WIELOWĄTKOWE (RLock ZAPOBIEGA DEADLOCKOM)
# ==============================================================================
lock_pos = threading.RLock()
lock_history = threading.RLock()
kline_cache_lock = threading.Lock()
kline_cache_15m = {}
instrument_filters = {}

# Inicjalizacja czasu
sync_bybit_clock()

# Inicjalizacja sesji Bybit
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
# POBIERANIE DANYCH RYNKOWYCH
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

# ==============================================================================
# FILTRY BEZPIECZEŃSTWA (BTC & USDC)
# ==============================================================================
def check_usdc_market_dependencies():
    try:
        res = session.get_tickers(category=CATEGORY, symbol="BTCUSDC")
        if res.get("retCode") == 0:
            ticker_list = res.get("result", {}).get("list", [])
            if ticker_list:
                t = ticker_list[0]
                bid = safe_float(t.get("bid1Price"))
                ask = safe_float(t.get("ask1Price"))
                if bid > 0 and ask > 0:
                    spread_pct = (ask - bid) / bid
                    if spread_pct > USDC_MAX_SPREAD_PCT:
                        return False, f"WIDE_SPREAD_{spread_pct*100:.2f}%"

        df_usdc_1m = get_market_data_no_cache("BTCUSDC", interval="1", limit=3)
        if df_usdc_1m is not None and len(df_usdc_1m) >= 2:
            c1m = df_usdc_1m.iloc[-1]
            chg_1m = (c1m['close'] - c1m['open']) / c1m['open']
            if chg_1m <= -0.005:
                return False, f"USDC_DUMP_{chg_1m*100:.2f}%"

        return True, "USDC_OK"
    except Exception:
        return True, "BYPASS"

def is_btc_market_healthy():
    try:
        df_btc = get_market_data_no_cache("BTCUSDC", interval="5", limit=5)
        if df_btc is not None and len(df_btc) >= 3:
            c = df_btc.iloc[-1]
            candle_change = (c['close'] - c['open']) / c['open']
            if candle_change <= BTC_FAST_DROP_BUY_BLOCK_PCT:
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
                    to_remove.append((sym, "symbol spoza listy 6 par"))
                    continue
                bal = real_holdings.get(sym, 0.0)
                if bal <= 0:
                    to_remove.append((sym, "brak salda na koncie Bybit"))

            for sym, reason in to_remove:
                if sym in active_positions:
                    del active_positions[sym]
                    logging.info(f"🧹 [SYNC] Usunięto {sym} ({reason}).")

            # Przejęcie pozycji z Bybit jeśli nie są w słowniku
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
                            logging.info(f"🎯 [SYNC] Zaimportowano pozycję {sym} | Ilość: {q_str} | SL: {initial_sl:.6f}")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji Bybit: {e}")

# ==============================================================================
# STRATEGIA ZAKUPÓW: BREAKOUT & PULLBACK SNIPER
# ==============================================================================
def run_sniper_strategy():
    global active_positions
    total_equity, free_usdc = get_balances()
    target_allocation = total_equity * CAPITAL_PER_TRADE_PCT
    now_ts = time.time()

    with lock_pos:
        current_count = len(active_positions)
    if current_count >= MAX_TOTAL_POSITIONS:
        return

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
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

        df = results.get(symbol)
        if df is None or len(df) < 50:
            continue

        try:
            c_now = df.iloc[-1]
            c = df.iloc[-2]
            p = df.iloc[-3]

            # Cooldown po stracie (30 min po SL)
            cooldown_ok = True
            with lock_history:
                for t in reversed(trade_history.get("trades_log", [])):
                    if t.get("symbol") == symbol:
                        if t.get("net_pnl", 0) < 0 and (now_ts - t.get("timestamp", 0)) < 1800:
                            cooldown_ok = False
                        break
            if not cooldown_ok:
                continue

            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0
            is_green = c['close'] > c['open']
            rsi_val = c.get('rsi', 50.0)

            # Filtr anty-szczytowy: odrzucenie świec z długim górnym knotem
            candle_range = c['high'] - c['low']
            upper_wick = c['high'] - max(c['open'], c['close'])
            wick_rejection = (upper_wick / candle_range > 0.28) if candle_range > 0 else False

            if rsi_val >= 68.0 or wick_rejection or not is_green:
                continue

            # SYGNAŁ 1: Odbicie po zrzucie (Reclaim EMA9 & EMA21)
            reclaimed_emas = (c['close'] > c['ema9']) and (c['close'] > c['ema21']) and (p['close'] <= p['ema21'])
            is_rebound = reclaimed_emas and (vol_ratio >= 1.15)

            # SYGNAŁ 2: Wybicie w trendzie (Pullback Retest)
            in_trend = (c['close'] > c['ema50']) and (c['ema21'] > c['ema50'])
            tested_ema21 = (p['low'] <= p['ema21'] * 1.003) or (c['low'] <= c['ema21'] * 1.003)
            bounce_trend = in_trend and tested_ema21 and (c['close'] > p['high']) and (vol_ratio >= 1.15)

            # SYGNAŁ 3: V-Shape Reversal (potężne objęcie)
            is_v_shape = (c['close'] > p['high']) and (c['close'] > c['ema21']) and (vol_ratio >= 1.45)

            live_green = c_now['close'] >= c_now['open']

            if (is_rebound or bounce_trend or is_v_shape) and live_green:
                score = vol_ratio * (1.4 if is_v_shape else 1.0)
                tag = "V-SHAPE" if is_v_shape else ("RETEST_TREND" if bounce_trend else "REBOUND")
                if best_candidate is None or score > best_candidate['score']:
                    best_candidate = {"symbol": symbol, "score": score, "tag": tag, "df": df}
        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    if not best_candidate:
        return

    sym = best_candidate["symbol"]
    tag = best_candidate["tag"]
    df_c = best_candidate["df"]

    if free_usdc < (target_allocation * 0.98):
        return

    c_now = df_c.iloc[-1]
    curr_price = c_now['close']

    # CIASNY INITIAL STOP LOSS: pod dołkiem świecy, ale nie więcej niż 1.20%
    recent_low = min(df_c['low'].iloc[-3:].min(), c_now['low'])
    calculated_sl = recent_low * 0.997
    hard_sl_cap = curr_price * (1.0 - MAX_INITIAL_STOP_LOSS_PCT)
    initial_sl = max(calculated_sl, hard_sl_cap)

    res = session.place_order(category=CATEGORY, symbol=sym, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
    if res.get("retCode") == 0:
        step, minq = get_symbol_lot_filter(sym)
        q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
        if q_str:
            with lock_pos:
                active_positions[sym] = {
                    "strategy": tag,
                    "buy_price": curr_price,
                    "peak_price": curr_price,
                    "qty": q_str,
                    "initial_qty": q_str,
                    "tp1_executed": False,
                    "trailing_sl": initial_sl,
                    "entry_time": time.time()
                }
                save_positions(active_positions)
            is_micro = ("PEPE" in sym or "BONK" in sym)
            sl_fmt = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
            logging.info(f"⚡🎯 [ZAKUP] {sym} ({tag}) | Cena: {curr_price} | Ilość: {q_str} | Ciasny SL: {sl_fmt} (-{((curr_price-initial_sl)/curr_price)*100:.2f}%)")

# ==============================================================================
# MONITORING W CZASIE RZECZYWISTYM (REAL-TIME TICKER CO 1.5s)
# ==============================================================================
def monitoring_worker():
    global active_positions
    logging.info("🚀 [SYSTEM] Uruchomiono wątek monitoringu w czasie rzeczywistym (1.5s).")
    while True:
        try:
            with lock_pos:
                items = list(active_positions.items())
            if not items:
                time.sleep(1.5)
                continue

            tickers = get_all_spot_tickers()
            now_ts = time.time()
            to_delete = []

            for symbol, pos in items:
                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                c_curr = safe_float(ticker.get("lastPrice"))
                if c_curr <= 0:
                    continue

                bp = pos["buy_price"]
                entry_time = pos.get("entry_time", now_ts)
                time_in_pos = now_ts - entry_time
                gain_pct = (c_curr - bp) / bp

                # 1. Śledzenie szczytu w czasie rzeczywistym
                peak = max(pos.get("peak_price", bp), c_curr)
                pos["peak_price"] = peak
                peak_pct = (peak - bp) / bp

                current_sl = pos.get("trailing_sl", bp * (1.0 - MAX_INITIAL_STOP_LOSS_PCT))

                # KROK 1: Wybicie wczesne (+1.4%): natychmiastowy Break-Even
                if peak_pct >= BREAK_EVEN_TRIGGER_PCT:
                    current_sl = max(current_sl, bp * (1.0 + BREAK_EVEN_LOCK_PCT))

                # KROK 2: TP1 (+2.8%): Sprzedaż 50% pozycji
                if peak_pct >= 0.028 and not pos.get("tp1_executed", False):
                    full_qty = Decimal(str(pos.get("initial_qty", pos["qty"])))
                    half_qty = full_qty / Decimal("2")
                    step, minq = get_symbol_lot_filter(symbol)
                    half_qty_str = format_qty_with_step(half_qty, step, minq)

                    sold_half_success = False
                    if half_qty_str and (safe_float(half_qty_str) * c_curr >= MIN_EXCHANGE_NOTIONAL):
                        res_tp1 = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=half_qty_str)
                        if res_tp1.get("retCode") == 0:
                            sold_half_success = True
                            remaining_qty = format_qty_with_step(full_qty - Decimal(half_qty_str), step, minq)
                            pos["qty"] = remaining_qty or half_qty_str
                            pos["tp1_executed"] = True
                            record_closed_trade(symbol, pos.get("strategy"), bp, c_curr, safe_float(half_qty_str), "Częściowy TP1 (50% pozycji)", is_maker=False)
                            logging.info(f"💎💰 [CZĘŚCIOWY TAKE PROFIT] {symbol} | Sprzedano 50% ({half_qty_str}) przy +{gain_pct*100:.2f}%! Pozycja zabezpieczona.")

                    if not sold_half_success:
                        pos["tp1_executed"] = True
                    
                    current_sl = max(current_sl, bp * 1.018)

                # KROK 3: Wybicie średnie (+4.0% do +6.0%): Ciasny trailing 0.9% od szczytu
                if peak_pct >= 0.040:
                    trail_sl = peak * (1.0 - 0.009)
                    current_sl = max(current_sl, trail_sl, bp * 1.025)

                # KROK 4: Paraboliczny wystrzał (> +6.5%): Trailing 1.2% od szczytu
                if peak_pct >= 0.065:
                    trail_sl = peak * (1.0 - 0.012)
                    current_sl = max(current_sl, trail_sl, bp * 1.045)

                # KROK 5: Ochrona przed cofnięciem z knota (Wick Rejection Guard)
                wick_drop_exit = False
                if peak_pct >= 0.022:
                    retracement = (peak - c_curr) / (peak - bp) if (peak - bp) > 0 else 0.0
                    if retracement >= 0.35:
                        wick_drop_exit = True

                pos["trailing_sl"] = current_sl

                # WARUNKI WYJŚCIA
                trigger_exit = False
                exit_reason = ""

                if c_curr <= current_sl:
                    trigger_exit = True
                    exit_reason = f"Trailing Stop / Profit Lock ({gain_pct*100:+.2f}%)"
                elif wick_drop_exit:
                    trigger_exit = True
                    exit_reason = f"Wick Rejection Guard (Odrzucenie z {peak_pct*100:+.2f}%)"
                elif time_in_pos >= 600 and gain_pct <= FAST_INVALIDATION_PCT:
                    trigger_exit = True
                    exit_reason = f"Fast Invalidation (Ucięcie straty: {gain_pct*100:+.2f}% w {int(time_in_pos/60)}m)"
                elif time_in_pos >= 2700 and gain_pct < 0.004:
                    trigger_exit = True
                    exit_reason = f"Stagnation Exit (45m bez impetu | {gain_pct*100:+.2f}%)"

                # EGZEKUCJA WYJŚCIA
                if trigger_exit:
                    base_c = get_base_coin(symbol)
                    bal_coin = get_coin_available_balance(base_c)
                    val_usd = bal_coin * c_curr

                    if bal_coin > 0 and val_usd >= MIN_EXCHANGE_NOTIONAL:
                        step, minq = get_symbol_lot_filter(symbol)
                        sell_q_str = format_qty_with_step(bal_coin, step, minq)
                        if sell_q_str:
                            res_sell = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=sell_q_str)
                            if res_sell.get("retCode") == 0:
                                record_closed_trade(symbol, pos.get("strategy"), bp, c_curr, safe_float(sell_q_str), exit_reason, is_maker=False)
                                logging.info(f"🚨💰 [ZAMKNIĘTO POZYCJĘ] {symbol} | Wynik: {gain_pct*100:+.2f}% | Powód: {exit_reason}")
                            else:
                                logging.warning(f"Błąd zlecenia sprzedaży {symbol}: {res_sell.get('retMsg')}")
                    to_delete.append(symbol)

            if to_delete:
                with lock_pos:
                    for s in to_delete:
                        if s in active_positions:
                            del active_positions[s]
                save_positions(active_positions)

        except Exception as e:
            logging.error(f"Błąd pętli monitoringu: {e}")
        time.sleep(1.5)

# ==============================================================================
# GŁÓWNA PĘTLA BOTA (SKANER CO 10 SEKUND)
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
                usdc_ok, usdc_stat = check_usdc_market_dependencies()
                logging.info("--------------------------------------------------------------------------------")
                logging.info(f"💰 [PORTFEL] Saldo: {total_eq:.2f}$ | Wolne: {free_usdc:.2f}$ | Pozycje: {len(current_items)}/{MAX_TOTAL_POSITIONS} | Status USDC: {usdc_stat}")
                for s, p in current_items:
                    tickers = get_all_spot_tickers()
                    curr = safe_float(tickers.get(s, {}).get("lastPrice"), p["buy_price"])
                    pnl = ((curr - p["buy_price"]) / p["buy_price"]) * 100
                    sl = p.get("trailing_sl", 0.0)
                    is_micro = "PEPE" in s or "BONK" in s
                    sl_fmt = f"{sl:.8f}" if is_micro else f"{sl:.4f}"
                    peak_fmt = f"{p.get('peak_price', curr):.8f}" if is_micro else f"{p.get('peak_price', curr):.4f}"
                    logging.info(f"📈 [{s}] PnL: {pnl:+.2f}% | Szczyt: {peak_fmt} | Aktywny SL: {sl_fmt}")
                logging.info("--------------------------------------------------------------------------------")
                last_print = now

            usdc_ok, _ = check_usdc_market_dependencies()
            if is_btc_market_healthy() and usdc_ok:
                run_sniper_strategy()

        except Exception as e:
            logging.error(f"Błąd głównej pętli: {e}")
        time.sleep(10)

# ==============================================================================
# KEEP-ALIVE SERVER (RENDER)
# ==============================================================================
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
        self.wfile.write(b"Bybit Spot Bot Active - 6 USDC Pairs")
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
