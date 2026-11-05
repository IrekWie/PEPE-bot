import concurrent.futures
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import math
import os
import socket
import threading
import time
import pandas as pd
from pybit.unified_trading import HTTP
import requests

# ==============================================================================
# OCHRONA PRZED ZAWIESZENIEM POŁĄCZEŃ SIECIOWYCH
# ==============================================================================
socket.setdefaulttimeout(15)

# ==============================================================================
# SYNCHRONIZACJA ZEGARA Z BYBIT (ZAPOBIEGA BŁĘDOWI 10002)
# ==============================================================================
time_offset_sec = 0.0

def synced_time():
    return (time.time_ns() / 1_000_000_000.0) + time_offset_sec

time.time = synced_time

def sync_bybit_clock():
    global time_offset_sec
    for url in ["https://api.bybit.eu/v5/market/time", "https://api.bybit.com/v5/market/time"]:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("retCode") == 0:
                    server_ms = int(data["result"]["timeNano"]) // 1_000_000
                    local_ms = int(time.time_ns() / 1_000_000)
                    time_offset_sec = (server_ms - local_ms) / 1000.0
                    print(f"⏱️ Zsynchronizowano zegar z Bybit (Offset: {time_offset_sec*1000:+.1f} ms)", flush=True)
                    return True
        except Exception:
            pass
    return False

sync_bybit_clock()

# ==============================================================================
# KONFIGURACJA BOTA SPOT (RYNEK USDC)
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

# --- KOSZYK 6 LIDERÓW ZMIENNOŚCI I PŁYNNOŚCI ---
CAPITAL_PER_TRADE_PCT = 0.45         # 45% kapitału na pozycję (pozwala na 2 jednoczesne wejścia)
MAX_TOTAL_POSITIONS = 2              # Maksymalnie 2 otwarte pozycje naraz

STABLE_PAIRS_BLACKLIST = [
    "USDTUSDC", "USDCUSDT", "EURCUSDC", "USDTEUR", "EURCUSDT", 
    "USDEUSDC", "DAIUSDC", "FDUSDUSDC", "TUSDUSDC", "USDT", "USDC"
]

def get_dynamic_top_coins():
    # 6 par do ciągłego polowania i rotacji kapitału
    return ["PEPEUSDC", "WIFUSDC", "BONKUSDC", "DOGEUSDC", "SUIUSDC", "SOLUSDC"]

# --- LIMITY FINANSOWE I REZERWY ---
STANDARD_MIN_ORDER_VALUE = 20.0
MIN_SYNC_POSITION_VALUE = 8.0     
MIN_EXCHANGE_NOTIONAL = 5.0       
MIN_CASH_RESERVE_PCT = 0.02

# --- BEZPIECZNIKI RYNKOWE ---
USDC_MAX_SPREAD_PCT = 0.0010        
USDC_1M_DUMP_THRESHOLD = -0.005     
BTC_FAST_DROP_BUY_BLOCK_PCT = -0.012
BTC_FLASH_CRASH_EXIT_PCT    = -0.015

CURRENT_SENTIMENT = "NEUTRAL 🟡"

APP_URL = "https://bybit-bot-i1lw.onrender.com"
POSITIONS_FILE = "active_spot_positions.json"
TRADE_HISTORY_FILE = "trade_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

kline_cache_15m = {}
kline_cache_lock = threading.Lock()

def safe_float(val, default=0.0):
    try:
        if val is None or str(val).strip() == "":
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

# ==============================================================================
# MODUŁ HISTORII TRANSAKCJI
# ==============================================================================
def load_trade_history():
    if os.path.exists(TRADE_HISTORY_FILE):
        try:
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
                if "trades_log" not in data: data["trades_log"] = []
                return data
        except Exception:
            pass
    return {"total_realized_usd": 0.0, "total_trades": 0, "wins": 0, "losses": 0, "trades_log": [], "daily": {}}

def save_trade_history(history):
    try:
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu historii: {e}")

trade_history = load_trade_history()
lock_history = threading.Lock()

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

        if pnl_usd >= 0: trade_history["wins"] = trade_history.get("wins", 0) + 1
        else: trade_history["losses"] = trade_history.get("losses", 0) + 1

        if "trades_log" not in trade_history: trade_history["trades_log"] = []
        trade_history["trades_log"].append({
            "timestamp": now_ts, "symbol": symbol, "turnover": round((buy_price + sell_price) * qty, 2),
            "fee": round(estimated_fee, 4), "gross_pnl": round(raw_pnl_usd, 2), "net_pnl": pnl_usd
        })

        if len(trade_history["trades_log"]) > 200:
            trade_history["trades_log"] = trade_history["trades_log"][-200:]

        if "daily" not in trade_history: trade_history["daily"] = {}
        if today_str not in trade_history["daily"]:
            trade_history["daily"][today_str] = {"pnl_usd": 0.0, "trades": 0, "wins": 0, "losses": 0, "total_fees": 0.0, "total_turnover": 0.0}

        d = trade_history["daily"][today_str]
        d["pnl_usd"] = round(d.get("pnl_usd", 0.0) + pnl_usd, 2)
        d["trades"] = d.get("trades", 0) + 1
        d["total_fees"] = round(d.get("total_fees", 0.0) + estimated_fee, 4)
        d["total_turnover"] = round(d.get("total_turnover", 0.0) + ((buy_price + sell_price) * qty), 2)

        save_trade_history(trade_history)
        sign = "+" if pnl_usd >= 0 else ""
        ord_type = "LIMIT (POST-ONLY)" if is_maker else "MARKET"
        logging.info(f"📊 [BILANS ZAMKNIĘCIA ({ord_type})] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | Fee: -{estimated_fee:.2f}$ | Powód: {reason}")

# ==============================================================================
# MODUŁ GODZINOWEJ ANALIZY KOSZTÓW I PROWIZJI
# ==============================================================================
last_hourly_cost_report = 0.0

def report_hourly_costs(total_equity):
    global last_hourly_cost_report, trade_history
    now_ts = time.time()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with lock_history:
        trades_log = trade_history.get("trades_log", [])
        recent_trades = [t for t in trades_log if t.get("timestamp", 0) >= now_ts - 3600]

        h_trades_count = len(recent_trades)
        h_turnover = sum(t.get("turnover", 0.0) for t in recent_trades)
        h_fees = sum(t.get("fee", 0.0) for t in recent_trades)
        h_gross = sum(t.get("gross_pnl", 0.0) for t in recent_trades)
        h_net = sum(t.get("net_pnl", 0.0) for t in recent_trades)

        today_stats = trade_history.get("daily", {}).get(today_str, {})
        d_fees = today_stats.get("total_fees", 0.0)
        d_turnover = today_stats.get("total_turnover", 0.0)
        d_trades = today_stats.get("trades", 0)
        avg_fee_per_trade = (d_fees / d_trades) if d_trades > 0 else 0.0
        fee_drag_pct = ((d_fees / total_equity) * 100) if total_equity > 0 else 0.0

    sign_gross = "+" if h_gross >= 0 else ""
    sign_net = "+" if h_net >= 0 else ""

    logging.info("================================================================================")
    logging.info("💸 [GODZINNY RAPORT KOSZTÓW I PROWIZJI GIEŁDOWYCH]")
    logging.info("--------------------------------------------------------------------------------")
    logging.info(f"⏱️ Ostatnie 60 minut: Transakcje: {h_trades_count} | Obrót: {h_turnover:.2f}$ | Fee Bybit: -{h_fees:.2f}$ | Netto: {sign_net}{h_net:.2f}$")
    logging.info(f"📅 Dzisiaj (od 00:00 UTC): Prowizje: -{d_fees:.2f}$ ({fee_drag_pct:.2f}% konta) | Obrót: {d_turnover:.2f}$ | Śr. prowizja: -{avg_fee_per_trade:.2f}$")
    logging.info("================================================================================")

    last_hourly_cost_report = now_ts

# ==============================================================================
# MODUŁ ZALEŻNOŚCI I PŁYNNOŚCI RYNKU USDC
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
                        logging.warning(f"⚠️ [USDC SPREAD GUARD] Zbyt szeroki spread na BTCUSDC ({spread_pct*100:.3f}%).")
                        return False, f"WIDE_SPREAD_{spread_pct*100:.2f}%"

        df_usdc_1m = get_market_data_no_cache("BTCUSDC", interval="1", limit=3)
        if df_usdc_1m is not None and len(df_usdc_1m) >= 2:
            c1m = df_usdc_1m.iloc[-1]
            chg_1m = (c1m['close'] - c1m['open']) / c1m['open']
            if chg_1m <= USDC_1M_DUMP_THRESHOLD:
                logging.warning(f"⚠️ [USDC MICRO-DUMP GUARD] Nagły spadek BTCUSDC 1M ({chg_1m*100:.2f}%).")
                return False, f"USDC_DUMP_{chg_1m*100:.2f}%"

        try:
            r = requests.get("https://api.bybit.com/v5/market/tickers?category=spot&symbol=USDCUSDT", timeout=2)
            if r.status_code == 200:
                d = r.json()
                t_list = d.get("result", {}).get("list", [])
                if t_list:
                    peg_price = safe_float(t_list[0].get("lastPrice"), 1.0)
                    if abs(peg_price - 1.0) >= 0.003:
                        logging.warning(f"⚠️ [GLOBAL USDC PEG ALERT] Odchylenie kursu USDC: {peg_price:.4f}!")
                        return False, f"GLOBAL_DEPEG_{peg_price:.4f}"
        except Exception:
            pass

        return True, "USDC_HEALTHY"
    except Exception as e:
        logging.warning(f"Błąd analizy zależności USDC: {e}")
        return True, "BYPASS"

# ==============================================================================
# INICJALIZACJA SESJI BYBIT
# ==============================================================================
endpoints = [("EU Region / MiCA", "https://api.bybit.eu"), ("Global Mainnet", "https://api.bybit.com")]
session = None
for name, url in endpoints:
    try:
        s = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
        s.endpoint = url
        res = s.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0:
            session = s
            print(f"✅ POŁĄCZONO: {name}", flush=True)
            break
    except Exception:
        pass

if not session:
    session = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
    session.endpoint = "https://api.bybit.eu"

instrument_filters = {}
def get_symbol_lot_filter(symbol):
    if symbol in instrument_filters: return instrument_filters[symbol]
    try:
        res = session.get_instruments_info(category=CATEGORY, symbol=symbol)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            lot = res["result"]["list"][0].get("lotSizeFilter", {})
            step = lot.get("basePrecision") or lot.get("qtyStep") or "0.000001"
            instrument_filters[symbol] = (step, lot.get("minOrderQty", "0.000001"))
            return instrument_filters[symbol]
    except Exception:
        pass
    return "0.0001", "0.0001"

def format_qty_with_step(raw_qty, qty_step_str, min_qty_str):
    try:
        qty_dec = Decimal(str(raw_qty))
        step_dec = Decimal(str(qty_step_str))
        min_dec = Decimal(str(min_qty_str))
        floored = (qty_dec // step_dec) * step_dec if step_dec > 0 else qty_dec
        if floored < min_dec and floored <= 0: return None
        norm = step_dec.normalize()
        exp = norm.as_tuple().exponent
        return f"{floored:.{abs(exp)}f}" if exp < 0 else f"{int(floored)}"
    except Exception:
        return str(raw_qty)

active_positions = {}
lock_pos = threading.Lock()

def load_positions():
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, "r") as f: return json.load(f)
        except Exception: pass
    return {}

def save_positions(positions):
    try:
        with open(POSITIONS_FILE, "w") as f: json.dump(positions, f, indent=4)
    except Exception as e: logging.error(f"Błąd zapisu pozycji: {e}")

active_positions = load_positions()

def get_balances():
    if not session: return 0.0, 0.0
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
    if not session: return 0.0
    try:
        res = session.get_wallet_balance(accountType="UNIFIED", coin=coin_name)
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            for c in res["result"]["list"][0].get("coin", []):
                if c.get("coin") == coin_name:
                    return max(safe_float(c.get("walletBalance")), safe_float(c.get("availableToWithdraw")))
    except Exception: pass
    return 0.0

# ==============================================================================
# DWUKIERUNKOWA SYNCHRONIZACJA PORTFELA Z BYBIT
# ==============================================================================
def sync_positions_from_bybit():
    global active_positions
    if not session: return
    try:
        allowed_symbols = get_dynamic_top_coins()
        with lock_pos:
            res = session.get_wallet_balance(accountType="UNIFIED")
            if res.get("retCode") != 0 or not res.get("result", {}).get("list"): return
            
            # Pobieramy fizyczne salda z Bybit
            real_holdings = {}
            for coin_info in res["result"]["list"][0].get("coin", []):
                coin = coin_info.get("coin")
                if coin in ["USDC", "USDT", "EURC", "USD", "EUR", "PLN", "GBP", "TRY", "UAH", "RUB"]: 
                    continue
                bal = max(safe_float(coin_info.get("walletBalance")), safe_float(coin_info.get("equity")))
                if bal > 0:
                    real_holdings[f"{coin}USDC"] = bal

            # KROK 1: Usuwanie pozycji sprzedanych z ręki lub o wartości śmieciowej (< 8$)
            to_remove = []
            for sym, pos in list(active_positions.items()):
                if sym not in allowed_symbols:
                    to_remove.append((sym, "niedozwolony symbol"))
                    continue
                
                bal = real_holdings.get(sym, 0.0)
                df = get_market_data_no_cache(sym, interval="15", limit=2)
                cp = df.iloc[-1]['close'] if (df is not None and len(df) > 0) else pos.get("buy_price", 1.0)
                val_usd = bal * cp
                
                if bal <= 0 or val_usd < MIN_SYNC_POSITION_VALUE:
                    to_remove.append((sym, f"brak na koncie Bybit lub resztki ({val_usd:.2f}$ < {MIN_SYNC_POSITION_VALUE:.0f}$)"))

            for sym, reason in to_remove:
                if sym in active_positions:
                    del active_positions[sym]
                    logging.info(f"🧹 [SYNC] Usunięto {sym} z monitora ({reason}).")

            # KROK 2: Przejmowanie realnych pozycji istniejących na Bybit (>= 8$)
            for sym, bal in real_holdings.items():
                if sym not in allowed_symbols: 
                    continue

                if sym not in active_positions:
                    df = get_market_data_no_cache(sym, interval="15", limit=30)
                    if df is not None and len(df) > 0:
                        cp = df.iloc[-1]['close']
                        val_usd = bal * cp
                        if val_usd >= MIN_SYNC_POSITION_VALUE:
                            buy_p = cp
                            buy_time = time.time()
                            try:
                                ex = session.get_executions(category=CATEGORY, symbol=sym, limit=5)
                                if ex.get("retCode") == 0:
                                    buys = [e for e in ex.get("result", {}).get("list", []) if e.get("side") == "Buy"]
                                    if buys:
                                        buy_p = safe_float(buys[0].get("execPrice"), cp)
                                        exec_ts = safe_float(buys[0].get("execTime"))
                                        if exec_ts > 0:
                                            buy_time = exec_ts / 1000.0
                            except Exception: pass

                            step, minq = get_symbol_lot_filter(sym)
                            q_str = format_qty_with_step(bal, step, minq)
                            if q_str:
                                recent_low = df['low'].iloc[-4:-1].min() if len(df) >= 4 else buy_p * 0.98
                                initial_sl = recent_low * 0.980
                                recent_peak = max(cp, buy_p)
                                if len(df) >= 8:
                                    recent_peak = max(recent_peak, df['high'].iloc[-8:].max())
                                
                                active_positions[sym] = {
                                    "strategy": "BOTTOM_SNIPER",
                                    "buy_price": buy_p,
                                    "peak_price": recent_peak,
                                    "qty": q_str,
                                    "sold_half": False,
                                    "trailing_sl": initial_sl,
                                    "entry_time": buy_time
                                }
                                is_micro = ("PEPE" in sym or "BONK" in sym)
                                sl_log = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
                                logging.info(f"🎯 [SYNC] Przejęto {sym} ({val_usd:.2f}$) | SL: {sl_log}")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji: {e}")

# ==============================================================================
# POBIERANIE ŚWIEC I WSKAŹNIKÓW (15M)
# ==============================================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def get_market_data_no_cache(symbol, interval="15", limit=350):
    try:
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None
        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]: df[col] = df[col].astype(float)
        
        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = calculate_rsi(df['close'], period=14)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        df['bb_mid'] = df['close'].rolling(window=20).mean()
        df['bb_std'] = df['close'].rolling(window=20).std()
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        
        df['tr0'] = abs(df['high'] - df['low'])
        df['tr1'] = abs(df['high'] - df['close'].shift())
        df['tr2'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        
        return df
    except Exception:
        return None

def fetch_data_worker(symbol):
    return symbol, get_market_data_no_cache(symbol, interval="15", limit=350)

def get_market_data(symbol, interval="15", limit=350):
    global kline_cache_15m
    with kline_cache_lock:
        if interval == "15" and symbol in kline_cache_15m:
            return kline_cache_15m[symbol]
    
    df = get_market_data_no_cache(symbol, interval, limit)
    if interval == "15" and df is not None:
        with kline_cache_lock:
            kline_cache_15m[symbol] = df
    return df

# ==============================================================================
# FILTRY MAKROEKONOMICZNE BTC + ETH
# ==============================================================================
def update_market_sentiment():
    global CURRENT_SENTIMENT
    try:
        df_btc = get_market_data("BTCUSDC", interval="240", limit=100)
        df_eth = get_market_data("ETHUSDC", interval="240", limit=100)

        if df_btc is None or len(df_btc) < 50:
            return "NEUTRAL"

        btc_candle = df_btc.iloc[-1]
        btc_close = btc_candle['close']
        btc_ema21 = btc_candle['ema21']
        btc_ema50 = btc_candle['ema50']

        if btc_close > btc_ema50 and btc_ema21 > btc_ema50: btc_sentiment = "BULLISH 🟢"
        elif btc_close < btc_ema50 or btc_ema21 < btc_ema50: btc_sentiment = "BEARISH 🔴"
        else: btc_sentiment = "NEUTRAL 🟡"

        eth_sentiment = "NEUTRAL"
        if df_eth is not None and len(df_eth) >= 50:
            eth_candle = df_eth.iloc[-1]
            if eth_candle['close'] > eth_candle['ema50'] and eth_candle['ema21'] > eth_candle['ema50']: eth_sentiment = "BULLISH 🟢"
            else: eth_sentiment = "BEARISH 🔴"

        logging.info(f"📊 [MAKRO TREND 4H] BTC: {btc_sentiment} | ETH: {eth_sentiment}")
        CURRENT_SENTIMENT = btc_sentiment
        return btc_sentiment
    except Exception as e:
        logging.error(f"Błąd sentymentu: {e}")
        return "UNKNOWN"

def is_btc_market_healthy():
    try:
        df_btc = get_market_data_no_cache("BTCUSDC", interval="5", limit=6)
        if df_btc is not None and len(df_btc) >= 4:
            c = df_btc.iloc[-1]
            candle_change = (c['close'] - c['open']) / c['open']
            recent_high = df_btc['high'].iloc[-3:].max()
            drop_from_high = (c['close'] - recent_high) / recent_high
            if candle_change <= BTC_FAST_DROP_BUY_BLOCK_PCT or drop_from_high <= BTC_FAST_DROP_BUY_BLOCK_PCT:
                logging.warning(f"⚠️ [BTC GUARD] Tąpnięcie 5M na BTC: {candle_change*100:.2f}%. Wstrzymuję zakupy.")
                return False

        df_btc_15 = get_market_data_no_cache("BTCUSDC", interval="15", limit=3)
        if df_btc_15 is not None and len(df_btc_15) > 1:
            c15 = df_btc_15.iloc[-1]
            chg15 = (c15['close'] - c15['open']) / c15['open']
            if chg15 <= -0.006:
                logging.warning(f"⚠️ [BTC 15M GUARD] Silny spadek BTC 15M ({chg15*100:.2f}%). Zakaz zakupów!")
                return False
    except Exception:
        pass
    return True

# ==============================================================================
# STRATEGIA BOTTOM SNIPER Z ROTACJĄ RELATYWNEJ SIŁY
# ==============================================================================
def run_bottom_sniper_strategy():
    target_coins = get_dynamic_top_coins()
    global active_positions, kline_cache_15m, trade_history
    total_equity, free_usdc = get_balances()
    target_allocation = total_equity * CAPITAL_PER_TRADE_PCT
    now_ts = time.time()

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_data_worker, symbol): symbol for symbol in target_coins}
        for future in concurrent.futures.as_completed(futures):
            sym, df = future.result()
            if df is not None:
                results[sym] = df
                with kline_cache_lock: kline_cache_15m[sym] = df

    with lock_pos: 
        current_items = list(active_positions.items())
        current_count = len(current_items)

    best_candidate = None

    for symbol in target_coins:
        with lock_pos: 
            if symbol in active_positions: 
                continue

        df = results.get(symbol)
        if df is None or len(df) < 60:
            continue

        try:
            c_now = df.iloc[-1]
            c = df.iloc[-2]
            p = df.iloc[-3]

            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            daily_loss_hit = False
            with lock_history:
                t_log = trade_history.get("trades_log", [])
                losses_today = sum(1 for t in t_log if t.get("symbol") == symbol and t.get("net_pnl", 0) < 0 and datetime.fromtimestamp(t.get("timestamp", 0), timezone.utc).strftime("%Y-%m-%d") == today_str)
                if losses_today >= 1:
                    daily_loss_hit = True

            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0
            is_green = c['close'] > c['open']
            is_v_shape = is_green and (c['close'] > p['high']) and (vol_ratio >= 1.35)

            macro_ok = True
            df_1h = get_market_data(symbol, interval="60", limit=50)
            if df_1h is not None and len(df_1h) >= 20:
                c1h = df_1h.iloc[-2]
                if c1h['close'] < c1h['ema50'] and c1h['close'] < c1h['ema89']:
                    macro_ok = False

            if daily_loss_hit or (not macro_ok and not is_v_shape):
                continue

            ema_spread = abs(c['ema21'] - c['ema50']) / c['ema50']
            is_flat_chop = ema_spread < 0.0035

            highest_8 = df['high'].iloc[-11:-3].max()
            breakout_confirmed = (c['close'] > highest_8) and is_green and (vol_ratio >= 1.20)
            is_breakout_entry = breakout_confirmed and not is_flat_chop

            dist_from_ema9 = (c['close'] - c['ema9']) / c['ema9']
            not_overextended = dist_from_ema9 <= 0.022

            lookback_peak = min(len(df) - 3, 96)
            local_peak = df['high'].iloc[-lookback_peak:-2].max()
            drop_from_peak = (local_peak - c['close']) / local_peak if local_peak > 0 else 0.0
            is_valid_dip = drop_from_peak >= 0.025

            curr_low = min(p['low'], c['low'], c_now['low'])
            at_support = (curr_low <= c['bb_lower'] * 1.005) or \
                         ((curr_low <= c['ema89'] * 1.008) and (c['close'] >= c['ema89'] * 0.995)) or \
                         ((curr_low <= c['ema200'] * 1.008) and (c['close'] >= c['ema200'] * 0.995))

            bounce_trigger = is_green and (c['close'] > p['high']) and (vol_ratio >= 1.15)
            is_dip_entry = is_valid_dip and at_support and bounce_trigger and not is_flat_chop

            cooldown_ok = True
            with lock_history:
                trades_log = trade_history.get("trades_log", [])
                for t in reversed(trades_log):
                    if t.get("symbol") == symbol:
                        time_since = now_ts - t.get("timestamp", 0)
                        if t.get("net_pnl", 0) < 0 and time_since < 1800:
                            cooldown_ok = False
                        elif time_since < 300:
                            cooldown_ok = False
                        break
                        
            live_candle_green = c_now['close'] > c_now['open']

            should_buy = (is_breakout_entry or is_dip_entry or is_v_shape) and not_overextended and cooldown_ok and live_candle_green

            if should_buy:
                mode_tag = "V-SHAPE REVERSAL" if is_v_shape else ("WYBICIE 2H" if is_breakout_entry else "DIP W TRENDZIE")
                candidate_score = vol_ratio * (1.5 if is_v_shape else 1.0)
                if best_candidate is None or candidate_score > best_candidate['score']:
                    best_candidate = {
                        "symbol": symbol,
                        "mode_tag": mode_tag,
                        "df": df,
                        "score": candidate_score,
                        "vol_ratio": vol_ratio,
                        "is_v_shape": is_v_shape
                    }
        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

    if best_candidate is None:
        return

    cand_sym = best_candidate["symbol"]
    cand_mode = best_candidate["mode_tag"]
    cand_df = best_candidate["df"]

    # PRZYPADEK A: Mamy wolne sloty na pozycje
    if current_count < MAX_TOTAL_POSITIONS:
        if free_usdc < (target_allocation * 0.99):
            logging.warning(f"⏳ [{cand_sym}] Sygnał {cand_mode}, brak wolnego USDC ({free_usdc:.2f}$ < {target_allocation:.2f}$).")
            return

        c_now = cand_df.iloc[-1]
        c = cand_df.iloc[-2]
        p = cand_df.iloc[-3]
        swing_low = min(c['low'], p['low'], c_now['low'])
        initial_sl = swing_low * 0.980

        res = session.place_order(category=CATEGORY, symbol=cand_sym, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
        if res.get("retCode") == 0:
            curr_price = cand_df.iloc[-1]['close']
            step, minq = get_symbol_lot_filter(cand_sym)
            q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
            with lock_pos:
                active_positions[cand_sym] = {
                    "strategy": "WAVE_SNIPER", 
                    "buy_price": curr_price, 
                    "peak_price": curr_price, 
                    "qty": q_str or "1", 
                    "trailing_sl": initial_sl,
                    "entry_type": cand_mode,
                    "entry_time": now_ts
                }
                save_positions(active_positions)
            is_micro = "PEPE" in cand_sym or "BONK" in cand_sym
            sl_print = f"{initial_sl:.8f}" if is_micro else f"{initial_sl:.4f}"
            logging.info(f"⚡🎯 [ZAKUP WYKONANY] {cand_sym} | Tryb: {cand_mode} | Cena: {curr_price} | SL: {sl_print}")
        return

    # PRZYPADEK B: Brak wolnych slotów -> ROTACJA KAPITAŁU
    for def_sym, def_pos in current_items:
        def_df = results.get(def_sym)
        if def_df is None or len(def_df) < 20:
            continue

        def_curr = def_df.iloc[-1]['close']
        def_bp = def_pos["buy_price"]
        def_gain = (def_curr - def_bp) / def_bp
        def_entry_time = def_pos.get("entry_time", now_ts)
        def_time_in_pos = now_ts - def_entry_time

        if def_gain >= 0.015:
            continue

        if def_time_in_pos < 1800:
            continue

        c_def = def_df.iloc[-2]
        c_now_def = def_df.iloc[-1]
        under_ema21 = (c_def['close'] < c_def['ema21']) and (c_now_def['close'] < c_now_def['ema21'])
        is_stagnant = (def_time_in_pos >= 2700) and (def_gain < 0.004)
        strong_challenger = (best_candidate["vol_ratio"] >= 1.25) or best_candidate["is_v_shape"]

        if (under_ema21 or is_stagnant) and strong_challenger:
            logging.info(f"🔄⚡ [ROTACJA KAPITAŁU] Ospały {def_sym} (PnL: {def_gain*100:+.2f}%, czas: {int(def_time_in_pos/60)}m) traci impet!")
            logging.info(f"🚀 Odpala nowy lider: {cand_sym} ({cand_mode}, Wolumen: {best_candidate['vol_ratio']:.2f}x)! Przenoszę kapitał...")

            rb = get_coin_available_balance(def_sym.replace(QUOTE_COIN, "").replace("USDT", ""))
            sold_ok = False
            if rb > 0:
                st, mq = get_symbol_lot_filter(def_sym)
                qs = format_qty_with_step(rb, st, mq)
                if qs:
                    res_s = session.place_order(category=CATEGORY, symbol=def_sym, side="Sell", orderType="Market", qty=qs)
                    if res_s.get("retCode") == 0:
                        sold_ok = True
                        reason = f"Rotacja kapitału -> Skok na {cand_sym} ({def_gain*100:+.2f}%)"
                        record_closed_trade(def_sym, "WAVE_SNIPER", def_bp, def_curr, safe_float(qs), reason, is_maker=False)
                        with lock_pos:
                            if def_sym in active_positions:
                                del active_positions[def_sym]
                                save_positions(active_positions)
                        logging.info(f"💸 [ZAMKNIĘTO W ROTACJI] {def_sym} sprzedany po cenie {def_curr}!")

            if sold_ok:
                time.sleep(1.2)
                _, free_usdc_now = get_balances()
                total_eq_now, _ = get_balances()
                alloc_now = total_eq_now * CAPITAL_PER_TRADE_PCT
                alloc_buy = min(free_usdc_now * 0.99, alloc_now)

                if alloc_buy >= STANDARD_MIN_ORDER_VALUE:
                    c_now_c = cand_df.iloc[-1]
                    c_c = cand_df.iloc[-2]
                    p_c = cand_df.iloc[-3]
                    swing_low_c = min(c_c['low'], p_c['low'], c_now_c['low'])
                    initial_sl_c = swing_low_c * 0.980

                    res_b = session.place_order(category=CATEGORY, symbol=cand_sym, side="Buy", orderType="Market", qty=f"{alloc_buy:.2f}")
                    if res_b.get("retCode") == 0:
                        curr_p_c = cand_df.iloc[-1]['close']
                        step_c, minq_c = get_symbol_lot_filter(cand_sym)
                        q_str_c = format_qty_with_step(alloc_buy / curr_p_c, step_c, minq_c)
                        with lock_pos:
                            active_positions[cand_sym] = {
                                "strategy": "WAVE_SNIPER",
                                "buy_price": curr_p_c,
                                "peak_price": curr_p_c,
                                "qty": q_str_c or "1",
                                "trailing_sl": initial_sl_c,
                                "entry_type": f"ROTACJA ({cand_mode})",
                                "entry_time": time.time()
                            }
                            save_positions(active_positions)
                        is_micro_c = "PEPE" in cand_sym or "BONK" in cand_sym
                        sl_print_c = f"{initial_sl_c:.8f}" if is_micro_c else f"{initial_sl_c:.4f}"
                        logging.info(f"🎉🚀 [ROTACJA ZAKOŃCZONA] Otwarto {cand_sym} za {alloc_buy:.2f}$ | SL: {sl_print_c}")
            break

# ==============================================================================
# MONITORING Z OCHRONĄ ZYSKU I EWAKUACJĄ
# ==============================================================================
def monitoring_worker():
    global active_positions, trade_history
    while True:
        try:
            with lock_pos: items = list(active_positions.items())
            if not items:
                time.sleep(3)
                continue

            symbols_to_del = []
            now_ts = time.time()

            for symbol, pos in items:
                try:
                    df_mon = get_market_data_no_cache(symbol, interval="15", limit=60)
                    if df_mon is None or len(df_mon) < 20: 
                        continue

                    c_curr = df_mon.iloc[-1]['close']
                    bp = pos["buy_price"]
                    peak = max(pos.get("peak_price", bp), c_curr)
                    entry_time = pos.get("entry_time", now_ts)
                    time_in_pos = now_ts - entry_time

                    with lock_pos:
                        if symbol in active_positions:
                            active_positions[symbol]["peak_price"] = peak
                            if "entry_time" not in active_positions[symbol]:
                                active_positions[symbol]["entry_time"] = entry_time

                    gain_pct = (c_curr - bp) / bp
                    peak_pct = (peak - bp) / bp
                    current_sl = pos.get("trailing_sl", bp * 0.980)

                    # 1. Break-Even przy zysku >= +1.5%
                    if peak_pct >= 0.015:
                        current_sl = max(current_sl, bp * 1.004)

                    # 2. Agresywna blokada zysku z wystrzału
                    if peak_pct >= 0.020:
                        if peak_pct >= 0.050:
                            pullback_buffer = 0.010
                            min_guaranteed_sl = bp * 1.035
                        elif peak_pct >= 0.035:
                            pullback_buffer = 0.009
                            min_guaranteed_sl = bp * 1.022
                        else:
                            pullback_buffer = 0.008
                            min_guaranteed_sl = bp * 1.010

                        trail_from_peak = peak * (1.0 - pullback_buffer)
                        current_sl = max(current_sl, trail_from_peak, min_guaranteed_sl)

                    with lock_pos:
                        if symbol in active_positions:
                            active_positions[symbol]["trailing_sl"] = current_sl

                    # 3. Warunki ewakuacji
                    trigger_exit = False
                    exit_reason = ""

                    if c_curr <= current_sl:
                        trigger_exit = True
                        exit_reason = f"Profit Lock / Trailing Stop ({gain_pct*100:+.2f}%)"
                    elif time_in_pos >= 9000 and gain_pct < 0.008:
                        trigger_exit = True
                        exit_reason = f"Stagnation Exit ({time_in_pos/3600:.1f}h bez ruchu | {gain_pct*100:+.2f}%)"
                    elif time_in_pos >= 3600 and gain_pct <= 0.005:
                        c1 = df_mon.iloc[-2]
                        c2 = df_mon.iloc[-3]
                        under_emas = (c1['close'] < c1['ema21'] and c1['close'] < c1['ema50'] and
                                      c2['close'] < c2['ema21'] and c2['close'] < c2['ema50'])
                        if under_emas:
                            trigger_exit = True
                            exit_reason = f"Breakdown Exit (Utrata pędu pod EMA | {gain_pct*100:+.2f}%)"

                    # 4. Egzekucja wyjścia (weryfikacja realnego salda)
                    if trigger_exit:
                        net_result = ((c_curr - bp) / bp) * 100
                        rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                        val_rb = rb * c_curr
                        if rb > 0 and val_rb >= MIN_EXCHANGE_NOTIONAL:
                            st, mq = get_symbol_lot_filter(symbol)
                            qs = format_qty_with_step(rb, st, mq)
                            if qs:
                                res_s = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qs)
                                if res_s.get("retCode") == 0:
                                    record_closed_trade(symbol, "WAVE_SNIPER", bp, c_curr, safe_float(qs), exit_reason, is_maker=False)
                                    symbols_to_del.append(symbol)
                                    logging.info(f"💰 [ZAMKNIĘTO POZYCJĘ] {symbol} | Wynik: {net_result:+.2f}% | Powód: {exit_reason}")
                                else:
                                    err_msg = res_s.get("retMsg", "")
                                    logging.warning(f"⚠️ [BYBIT ORDER] {symbol}: {err_msg}. Usuwam pozycję.")
                                    symbols_to_del.append(symbol)
                            else:
                                symbols_to_del.append(symbol)
                        else:
                            logging.info(f"ℹ️ [{symbol}] Pozycja już nie istnieje na koncie Bybit. Usuwam z monitora.")
                            symbols_to_del.append(symbol)
                except Exception as e:
                    pass

            if symbols_to_del:
                with lock_pos:
                    for s in symbols_to_del:
                        if s in active_positions: del active_positions[s]
                    save_positions(active_positions)
        except Exception:
            pass
        time.sleep(3)

def auto_liquidate_stables_to_btc():
    global active_positions
    for coin, sym in [("USDT", "USDTUSDC"), ("EURC", "EURCUSDC")]:
        try:
            bal = get_coin_available_balance(coin)
            if (bal * 1.0 >= MIN_EXCHANGE_NOTIONAL) or (sym in active_positions):
                logging.info(f"🧹 [AUTO-WYMIANA] Wykryto {bal} {coin}. Wymieniam na USDC...")
                step, mq = get_symbol_lot_filter(sym)
                qs = format_qty_with_step(bal, step, mq)
                sold_ok = True
                if qs and safe_float(qs) > 0:
                    r = session.place_order(category=CATEGORY, symbol=sym, side="Sell", orderType="Market", qty=qs)
                    sold_ok = (r.get("retCode") == 0)

                if sold_ok:
                    with lock_pos:
                        if sym in active_positions: 
                            del active_positions[sym]
                            save_positions(active_positions)
        except Exception:
            pass

# ==============================================================================
# PĘTLA GŁÓWNA SKANERA RYNKU (SKAN W TLE CO 10 SEKUND)
# ==============================================================================
def bot_loop():
    global last_hourly_cost_report, kline_cache_15m
    sync_positions_from_bybit()
    auto_liquidate_stables_to_btc()
    last_hourly_cost_report = time.time()
    last_scan_print_time = 0

    t_mon = threading.Thread(target=monitoring_worker, daemon=True)
    t_mon.start()

    while True:
        try:
            with kline_cache_lock: kline_cache_15m.clear()
            
            now_ts = time.time()
            if now_ts - last_scan_print_time >= 60:
                logging.info("--------------------------------------------------------------------------------")
                logging.info("🚀 NOWY CYKL SKANOWANIA RYNKU (Skan 6 par w tle co 10 sekund)...")
                last_scan_print_time = now_ts

            if now_ts - last_hourly_cost_report >= 3600:
                sync_bybit_clock()
                total_equity, _ = get_balances()
                report_hourly_costs(total_equity)

            if now_ts - last_scan_print_time <= 1:
                update_market_sentiment()

            total_equity, free_usdc = get_balances()
            usdc_ok, usdc_stat = check_usdc_market_dependencies()

            if now_ts - last_scan_print_time <= 1:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                with lock_history:
                    t_stats = trade_history.get("daily", {}).get(today_str, {"pnl_usd": 0.0, "trades": 0})
                start_eq = safe_float(trade_history.get("equity_baseline", {}).get("starting_equity"), total_equity)
                diff = total_equity - start_eq
                logging.info(f"📈 [BILANS DNIA] Netto: {t_stats['pnl_usd']:+.2f}$ ({t_stats['trades']} trade) | Zmiana Bybit: {diff:+.2f}$")
                
                sync_positions_from_bybit()
                auto_liquidate_stables_to_btc()

                with lock_pos: current_items = list(active_positions.items())
                logging.info(f"💰 [PORTFEL] Wartość: {total_equity:.2f}$ | Wolne: {free_usdc:.2f}$ | Pozycje: {len(current_items)}/{MAX_TOTAL_POSITIONS} | Płynność USDC: {usdc_stat}")
                logging.info(f"=== [MONITOROWANIE POZYCJI: {len(current_items)} AKTYWNYCH] ===")
                for symbol, pos in current_items:
                    try:
                        df = get_market_data(symbol, interval="15", limit=2)
                        cp = df.iloc[-1]['close'] if df is not None else pos.get("peak_price", pos["buy_price"])
                        pnl_pct = ((cp - pos["buy_price"]) / pos["buy_price"]) * 100
                        sl_val = pos.get('trailing_sl', pos.get('trailing_sl_low', 0.0))
                        is_micro = ("PEPE" in symbol or "BONK" in symbol)
                        sl_fmt = f"{sl_val:.8f}" if is_micro else f"{sl_val:.4f}"
                        peak_val = pos.get("peak_price", cp)
                        peak_fmt = f"{peak_val:.8f}" if is_micro else f"{peak_val:.4f}"
                        entry_time = pos.get("entry_time", now_ts)
                        elapsed_min = int((now_ts - entry_time) / 60)
                        logging.info(f"ℹ️ [{symbol}] PnL: {pnl_pct:+.2f}% | Czas: {elapsed_min}m | Szczyt: {peak_fmt} | SL: {sl_fmt}")
                    except Exception: pass

            # Zakupy i Rotacja Kapitału
            if is_btc_market_healthy() and usdc_ok:
                run_bottom_sniper_strategy()
            else:
                if now_ts - last_scan_print_time <= 1:
                    if not usdc_ok: logging.info(f"⛔ [ZAKUPY ZABLOKOWANE] Ochrona płynności USDC: {usdc_stat}")
                    else: logging.info("⛔ [ZAKUPY WSTRZYMANE] Czerwona flaga z BTC Guard (gwałtowny spadek).")

            if now_ts - last_scan_print_time <= 1:
                logging.info("💤 Skan zakończony. Radar wejść i rotacji czuwa (odpytywanie co 10s)...")
                logging.info("--------------------------------------------------------------------------------")
                
        except Exception as e:
            logging.error(f"Błąd głównej pętli: {e}")
            
        time.sleep(10)

def self_ping_loop():
    while True:
        time.sleep(300)
        try:
            requests.get(APP_URL, timeout=10)
            logging.info("PING: Serwer aktywny (Render Keep-Alive OK)")
        except Exception: pass

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bybit Spot Trading Bot 24/7 - Multi-Coin Momentum Rotation Active")
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
