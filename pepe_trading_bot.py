import os
import time
import json
import logging
import threading
import socket
from decimal import Decimal
import math
import concurrent.futures
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

# --- FLAGA KONTROLI STRATEGII (PRZEŁĄCZNIK) ---
ENABLE_LEGACY_STRATEGIES = False     # False = Aktywna TYLKO strategia EMA 29/89 Swing na 12 monetach
                                     # True = Odblokowuje wszystkie 10 starych strategii (Poziomy 0-9)

# --- STRATEGIA GŁÓWNA: EMA 29/89 (KOSZYK 12 MONET x 20% KAPITAŁU) ---
EMA29_89_COINS = [
    "SOLUSDC", "SUIUSDC", "NEARUSDC", "DOGEUSDC", "PEPEUSDC",
    "APTUSDC", "AVAXUSDC", "SEIUSDC", "WIFUSDC", "BONKUSDC",
    "RENDERUSDC", "LINKUSDC", "INJUSDC", "TIAUSDC", "FETUSDC",
    "ONDOUSDC"
]
EMA29_89_CAPITAL_PCT = 0.20          # Dokładnie 20% kapitału na pozycję
MAX_TOTAL_POSITIONS = 5              # Maksymalnie 5 pozycji (5 x 20% = 100%)

# --- USTAWIENIA SKANOWANIA I PŁYNNOŚCI (DLA STRATEGII POBOCZNYCH) ---
TOP_MARKETS_COUNT = 150
MIN_24H_TURNOVER_USDC = 150000.0     # Odrzucanie niepłynnych monet (obrót < 150k USD dziennie)
TOP_DIPS_COUNT = 6
TOP_GAINERS_COUNT = 4
MAX_PUMP_POSITIONS = 3
MAX_EMA_CROSS_POSITIONS = 5

STABLE_PAIRS_BLACKLIST = [
    "USDTUSDC", "USDCUSDT", "EURCUSDC", "USDTEUR", "EURCUSDT", 
    "USDEUSDC", "DAIUSDC", "FDUSDUSDC", "TUSDUSDC"
]

# --- LIMITY FINANSOWE I REZERWY ---
STANDARD_MIN_ORDER_VALUE = 20.0      # Minimalna wartość wejścia
MIN_SYNC_POSITION_VALUE = 8.0     
MIN_EXCHANGE_NOTIONAL = 5.0       
MIN_CASH_RESERVE_PCT = 0.02          # 2% rezerwy na fee giełdowe

# Bezpieczniki rynkowe (realistyczne progi)
USDC_MAX_SPREAD_PCT = 0.0010         # Max spread na BTCUSDC: 0.100%
USDC_1M_DUMP_THRESHOLD = -0.005      # Szybki zrzut BTCUSDC na 1M (-0.5%) blokuje zakupy
BTC_FAST_DROP_BUY_BLOCK_PCT = -0.012 # Zrzut BTC 5M > -1.2% blokuje zakupy
BTC_FLASH_CRASH_EXIT_PCT    = -0.015

# ==============================================================================
# PARAMETRY DLA STRATEGII POBOCZNYCH (LEGACY)
# ==============================================================================
SENTIMENT_STATE = "NEUTRAL"
ETH_SENTIMENT_STATE = "NEUTRAL"
ALTCOIN_FLOW_STATE = "ALT_PRESSURE"

SAFE_RISK_PCT = 0.15
MODERATE_RISK_PCT = 0.10
HIGH_RISK_PCT = 0.07

EMA_CROSS_MIN_SPREAD_PCT = 0.0020  
EMA_CROSS_VOL_MULTIPLIER = 1.10    
EMA_EXIT_HYSTERESIS_PCT  = 0.0020  

DIP_RSI_5M_MAX = 30          
DIP_VOL_MULTIPLIER = 2.5     

PARTIAL_TP_PCT = 0.030          
BREAKEVEN_TRIGGER_PCT = 0.015   
BREAKEVEN_SL_PCT = 0.003        

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
    logging.info(f"⏱️ Ostatnie 60 minut:")
    logging.info(f"   • Transakcje: {h_trades_count} | Obrót: {h_turnover:.2f} USD")
    logging.info(f"   • Prowizje Bybit: -{h_fees:.2f} USD")
    logging.info(f"   • Wynik Brutto: {sign_gross}{h_gross:.2f} USD | Wynik Netto: {sign_net}{h_net:.2f} USD")
    logging.info("📅 Podsumowanie dzisiejsze (od 00:00 UTC):")
    logging.info(f"   • Suma prowizji oddanych giełdzie: -{d_fees:.2f} USD ({fee_drag_pct:.2f}% wartości konta)")
    logging.info(f"   • Łączny obrót dzienny: {d_turnover:.2f} USD")
    logging.info(f"   • Średni koszt prowizji na transakcję: -{avg_fee_per_trade:.2f} USD")
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
# ZARZĄDZANIE BUDŻETEM Z REZERWĄ GOTÓWKOWĄ
# ==============================================================================
def get_order_budget(total_equity, free_usdc, target_pct):
    target_val = total_equity * target_pct
    effective_target = max(target_val, STANDARD_MIN_ORDER_VALUE)

    reserve_required = total_equity * MIN_CASH_RESERVE_PCT
    if (free_usdc - effective_target) < reserve_required:
        available = math.floor((free_usdc - reserve_required) * 0.995 * 100) / 100.0
        if available >= MIN_EXCHANGE_NOTIONAL:
            return available, f"Zredukowane ({available:.2f}$) by zachować rezerwę ({MIN_CASH_RESERVE_PCT*100:.0f}%)"
        return 0.0, f"Brak gotówki (Chronię {MIN_CASH_RESERVE_PCT*100:.0f}% rezerwy)"

    if free_usdc >= effective_target:
        label = f"{target_pct * 100:.0f}%" if target_val >= STANDARD_MIN_ORDER_VALUE else f"Min. {STANDARD_MIN_ORDER_VALUE:.0f}$"
        return round(effective_target, 2), label

    return 0.0, "Brak środków"

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

def sync_positions_from_bybit():
    global active_positions
    if not session: return
    try:
        with lock_pos:
            res = session.get_wallet_balance(accountType="UNIFIED")
            if res.get("retCode") != 0 or not res.get("result", {}).get("list"): return
            for coin_info in res["result"]["list"][0].get("coin", []):
                coin = coin_info.get("coin")
                if coin in ["USDC", "USDT", "EURC", "USD", "EUR", "PLN", "GBP", "TRY", "UAH", "RUB"]: continue
                bal = max(safe_float(coin_info.get("walletBalance")), safe_float(coin_info.get("equity")))
                if bal <= 0: continue
                sym = f"{coin}USDC"
                if sym in STABLE_PAIRS_BLACKLIST: continue

                if sym not in active_positions:
                    df = get_market_data_no_cache(sym, interval="15", limit=10)
                    if df is not None and len(df) > 0:
                        cp = df.iloc[-1]['close']
                        val_usd = bal * cp
                        if val_usd >= MIN_SYNC_POSITION_VALUE:
                            buy_p = cp
                            try:
                                ex = session.get_executions(category=CATEGORY, symbol=sym, limit=5)
                                if ex.get("retCode") == 0:
                                    buys = [e for e in ex.get("result", {}).get("list", []) if e.get("side") == "Buy"]
                                    if buys: buy_p = safe_float(buys[0].get("execPrice"), cp)
                            except Exception: pass

                            step, minq = get_symbol_lot_filter(sym)
                            q_str = format_qty_with_step(bal, step, minq)
                            if q_str:
                                if sym in EMA29_89_COINS:
                                    recent_low = df['low'].iloc[-4:-1].min() if len(df) >= 4 else buy_p * 0.98
                                    initial_sl = recent_low * 0.998
                                    active_positions[sym] = {
                                        "strategy": "EMA29_89_SWING",
                                        "buy_price": buy_p,
                                        "peak_price": max(cp, buy_p),
                                        "qty": q_str,
                                        "sold_half": False,
                                        "trailing_sl_low": initial_sl
                                    }
                                    logging.info(f"🎯 [RĘCZNY ZAKUP / SYNC] Wykryto {sym} ({val_usd:.2f}$)! Przejęto pod strategię EMA29_89_SWING | SL pod dołkiem: {initial_sl:.4f}")
                                else:
                                    active_positions[sym] = {
                                        "strategy": "AUTO_RESTORED",
                                        "buy_price": buy_p,
                                        "peak_price": max(cp, buy_p),
                                        "qty": q_str,
                                        "sold_half": False,
                                        "trailing_sl_low": buy_p * 0.98
                                    }
                                    logging.info(f"🔄 [SYNC] Przywrócono {sym}: {q_str} szt. (Tryb obronny)")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji: {e}")

# ==============================================================================
# POBIERANIE ŚWIEC I WSKAŹNIKÓW (LIMIT=300 DLA PEŁNEJ ZGODNOŚCI EMA 89)
# ==============================================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def get_market_data_no_cache(symbol, interval="15", limit=300):
    try:
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=interval, limit=limit)
        candles = res.get("result", {}).get("list", [])
        if not candles: return None
        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]: df[col] = df[col].astype(float)
        
        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema29'] = df['close'].ewm(span=29, adjust=False).mean()
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
        
        df['tr0'] = abs(df['high'] - df['low'])
        df['tr1'] = abs(df['high'] - df['close'].shift())
        df['tr2'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_pct'] = (df['atr'] / df['close']).fillna(0.02)
        return df
    except Exception:
        return None

def fetch_data_worker(symbol):
    return symbol, get_market_data_no_cache(symbol, interval="15", limit=300)

def get_market_data(symbol, interval="15", limit=300):
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
# FILTRY PŁYNNOŚCI (DLA STRATEGII POBOCZNYCH)
# ==============================================================================
def get_top_volume_symbols(limit_count=TOP_MARKETS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        valid = [
            t for t in tickers 
            if t.get("symbol", "").endswith(QUOTE_COIN) 
            and t.get("symbol") not in STABLE_PAIRS_BLACKLIST
            and safe_float(t.get("turnover24h")) >= MIN_24H_TURNOVER_USDC
        ]
        if valid:
            valid.sort(key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
            res = [t["symbol"] for t in valid[:limit_count]]
            if "PEPEUSDC" not in res: res.append("PEPEUSDC")
            return res
    except Exception:
        pass
    return []

def get_top_losers_symbols(limit_count=TOP_DIPS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        valid = [
            t for t in tickers 
            if t.get("symbol", "").endswith(QUOTE_COIN) 
            and t.get("symbol") not in STABLE_PAIRS_BLACKLIST
            and safe_float(t.get("turnover24h")) >= MIN_24H_TURNOVER_USDC
        ]
        if valid:
            losers = [t for t in valid if safe_float(t.get("price24hPcnt")) <= -0.03]
            losers.sort(key=lambda x: safe_float(x.get("price24hPcnt")))
            return [t["symbol"] for t in losers[:limit_count]]
    except Exception:
        pass
    return []

def get_top_gainers_symbols(limit_count=TOP_GAINERS_COUNT):
    try:
        tickers = session.get_tickers(category=CATEGORY).get("result", {}).get("list", [])
        valid = [
            t for t in tickers 
            if t.get("symbol", "").endswith(QUOTE_COIN) 
            and t.get("symbol") not in STABLE_PAIRS_BLACKLIST
            and safe_float(t.get("turnover24h")) >= MIN_24H_TURNOVER_USDC
        ]
        if valid:
            gainers = [t for t in valid if safe_float(t.get("price24hPcnt")) >= 0.02]
            gainers.sort(key=lambda x: safe_float(x.get("price24hPcnt")), reverse=True)
            return [t["symbol"] for t in gainers[:limit_count]]
    except Exception:
        pass
    return []

# ==============================================================================
# FILTRY MAKROEKONOMICZNE BTC + ETH
# ==============================================================================
def update_market_sentiment():
    global SENTIMENT_STATE, ETH_SENTIMENT_STATE, ALTCOIN_FLOW_STATE
    try:
        df_btc = get_market_data("BTCUSDC", interval="240", limit=100)
        df_eth = get_market_data("ETHUSDC", interval="240", limit=100)

        if df_btc is None or len(df_btc) < 50:
            SENTIMENT_STATE = "NEUTRAL"
            return "NEUTRAL (Brak danych BTC)"

        btc_candle = df_btc.iloc[-1]
        btc_close = btc_candle['close']
        btc_ema21 = btc_candle['ema21']
        btc_ema50 = btc_candle['ema50']
        btc_ema89 = btc_candle['ema89']

        if btc_close > btc_ema50 and btc_ema21 > btc_ema50: btc_sentiment = "BULLISH"
        elif btc_close < btc_ema89 or btc_ema21 < btc_ema89: btc_sentiment = "BEARISH"
        else: btc_sentiment = "NEUTRAL"

        eth_sentiment = "NEUTRAL"
        alt_flow = "NEUTRAL"

        if df_eth is not None and len(df_eth) >= 50:
            eth_candle = df_eth.iloc[-1]
            if eth_candle['close'] > eth_candle['ema50'] and eth_candle['ema21'] > eth_candle['ema50']: eth_sentiment = "BULLISH 🟢"
            elif eth_candle['close'] < eth_candle['ema89'] or eth_candle['ema21'] < eth_candle['ema89']: eth_sentiment = "BEARISH 🔴"
            else: eth_sentiment = "NEUTRAL 🟡"

            min_len = min(len(df_btc), len(df_eth))
            ratio = df_eth['close'].iloc[-min_len:].reset_index(drop=True) / df_btc['close'].iloc[-min_len:].reset_index(drop=True)
            ratio_ema = ratio.ewm(span=21, adjust=False).mean()
            alt_flow = "ALT_FLOW 🟢" if ratio.iloc[-1] > ratio_ema.iloc[-1] else "ALT_PRESSURE 🔴"

        SENTIMENT_STATE = btc_sentiment
        ETH_SENTIMENT_STATE = eth_sentiment
        ALTCOIN_FLOW_STATE = alt_flow
        mode_str = "WSZYSTKIE STRATEGIE (10 POZIOMÓW)" if ENABLE_LEGACY_STRATEGIES else "TYLKO EMA 29/89"
        logging.info(f"📊 [MAKRO BTC+ETH] BTC 4H: {btc_sentiment} | ETH 4H: {eth_sentiment} | Rotacja: {alt_flow} | Tryb: {mode_str}")
        return btc_sentiment
    except Exception as e:
        logging.error(f"Błąd sentymentu: {e}")
        SENTIMENT_STATE = "NEUTRAL"
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
# STRATEGIA GŁÓWNA: EMA 29/89 SWING (SZYBKIE SKANOWANIE RÓWNOLEGŁE)
# ==============================================================================
def run_ema29_89_swing_strategy():
    global active_positions, kline_cache_15m, trade_history
    total_equity, free_usdc = get_balances()
    target_allocation = total_equity * EMA29_89_CAPITAL_PCT

    with lock_pos: current_count = len(active_positions)
    if current_count >= MAX_TOTAL_POSITIONS:
        logging.info(f"ℹ️ [LIMIT POZYCJI] Osiągnięto limit {current_count}/{MAX_TOTAL_POSITIONS}. Czekam na zamknięcie.")
        return

    logging.info(f"🎯 [SKANER EMA 29/89] Sprawdzam {len(EMA29_89_COINS)} wyselekcjonowanych monet | Cel: {target_allocation:.2f}$ (20%) | Wolne: {free_usdc:.2f}$")

    # Pobieranie danych równolegle dla 16 monet
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_data_worker, symbol): symbol for symbol in EMA29_89_COINS}
        for future in concurrent.futures.as_completed(futures):
            sym, df = future.result()
            if df is not None:
                results[sym] = df
                with kline_cache_lock: kline_cache_15m[sym] = df

    now_ts = time.time()

    for symbol in EMA29_89_COINS:
        with lock_pos: is_active = (symbol in active_positions)
        if is_active: continue

        df = results.get(symbol)
        if df is None or len(df) < 100:
            continue

        try:
            c_now = df.iloc[-1]   # Świeca bieżąca (w trakcie trwania)
            c = df.iloc[-2]       # Świeca zamknięta 15M temu
            p = df.iloc[-3]
            p2 = df.iloc[-4]
            p3 = df.iloc[-5]

            # ------------------------------------------------------------------
            # SYGNAŁ 1: KLASYCZNE PRZECIĘCIE (ZŁOTY KRZYŻ)
            # ------------------------------------------------------------------
            crossed_recently = (p['ema29'] <= p['ema89'] and c['ema29'] > c['ema89']) or                                (p2['ema29'] <= p2['ema89'] and c['ema29'] > c['ema89']) or                                (p3['ema29'] <= p3['ema89'] and c['ema29'] > c['ema89'])
            spread_pct = (c['ema29'] - c['ema89']) / c['ema89']
            is_bullish_trend = c['ema29'] > c['ema89'] and spread_pct >= 0.0005
            candle_ok = (c['close'] >= c['open']) or (c['close'] >= c['ema29'])
            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0
            vol_ok = vol_ratio >= 1.10

            is_valid_signal = crossed_recently and is_bullish_trend and candle_ok and vol_ok

            # ------------------------------------------------------------------
            # SYGNAŁ 2: HYBRYDOWE WEJŚCIE W LOCIE
            # ------------------------------------------------------------------
            spread_now = (c_now['ema29'] - c_now['ema89']) / c_now['ema89']
            crossed_now = (c['ema29'] <= c['ema89']) and (c_now['ema29'] > c_now['ema89'])
            vol_now_ratio = (c_now['volume'] / c_now['vol_ma20']) if (c_now['vol_ma20'] and c_now['vol_ma20'] > 0) else 1.0
            
            is_strong_breakout = crossed_now and (spread_now >= 0.0010) and (c_now['close'] > c_now['open']) and (c_now['close'] > c_now['ema29']) and (vol_now_ratio >= 0.50 or vol_ratio >= 1.0)
            
            # ------------------------------------------------------------------
            # SYGNAŁ 3: PULLBACK RE-ENTRY (PONOWNE WEJŚCIE PO ODBICIU OD EMA29)
            # ------------------------------------------------------------------
            is_reentry = False
            # 1. Trend bazowy musi nadal trwać i być silny
            if not crossed_recently and not crossed_now and c['ema29'] > c['ema89'] and spread_pct >= 0.0010:
                # 2. Sprawdzamy czy ostatnio nas wyrzuciło (cooldown min. 15 minut)
                cooldown_ok = True
                with lock_history:
                    trades_log = trade_history.get("trades_log", [])
                    # Szukamy ostatniej transakcji na tym symbolu
                    for t in reversed(trades_log):
                        if t.get("symbol") == symbol:
                            if now_ts - t.get("timestamp", 0) < 900:  # Zablokuj na 15 minut (900 sek) po wyrzuceniu na SL
                                cooldown_ok = False
                            break
                
                if cooldown_ok:
                    # 3. Kształt korekty - cena musiała naruszyć/zbliżyć się do EMA29 i odbić w górę
                    touched_ema = (c['low'] <= c['ema29'] * 1.002) or (p['low'] <= p['ema29'] * 1.002)
                    bounced_up = c['close'] > c['open'] and c['close'] > p['high'] and c['close'] > c['ema29']
                    reentry_vol = vol_ratio >= 1.05  # Odbicie poparte wolumenem
                    
                    if touched_ema and bounced_up and reentry_vol:
                        is_reentry = True
                        is_valid_signal = True

            # ------------------------------------------------------------------
            # LOGIKA WYŚWIETLANIA I SKŁADANIA ZAMÓWIEŃ
            # ------------------------------------------------------------------
            disp_ema29 = c_now['ema29'] if crossed_now else c['ema29']
            disp_ema89 = c_now['ema89'] if crossed_now else c['ema89']
            disp_spread = spread_now if crossed_now else spread_pct
            
            dec = 8 if disp_ema29 < 0.01 else 4
            spread_tag = f"🟢 +{disp_spread*100:.3f}%" if disp_spread >= 0 else f"🔴 {disp_spread*100:.3f}%"
            vol_tag = f"Vol: {vol_ratio:.2f}x ({'🟢' if vol_ok or (is_reentry and vol_ratio>=1.05) else '🔴'})"
            log_msg = f"🔍 [{symbol}] EMA29: {disp_ema29:.{dec}f} | EMA89: {disp_ema89:.{dec}f} | Spread: {spread_tag} | {vol_tag} | "
            
            if is_strong_breakout:
                logging.info(log_msg + f"🚀 HYBRYDOWE PRZEBICIE W LOCIE (Kupno natychmiastowe!)")
                is_valid_signal = True
            elif is_reentry:
                logging.info(log_msg + f"♻️ PULLBACK RE-ENTRY (Odbicie od EMA29 w trendzie!)")
            else:
                logging.info(log_msg + f"Krzyż w 1h: {crossed_recently} | Świeca OK: {candle_ok}")

            if is_valid_signal:
                if free_usdc < (target_allocation * 0.99):
                    logging.warning(f"⏳ [{symbol}] Sygnał zakupu, ale brak środków ({free_usdc:.2f}$ < {target_allocation:.2f}$).")
                    continue

                if is_reentry:
                    recent_low = min(c['low'], p['low'])
                else:
                    recent_low = df['low'].iloc[-4:-1].min()
                    
                initial_sl = recent_low * 0.997

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
                if res.get("retCode") == 0:
                    curr_price = df.iloc[-1]['close']
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
                    with lock_pos:
                        active_positions[symbol] = {
                            "strategy": "EMA29_89_SWING", 
                            "buy_price": curr_price, 
                            "peak_price": curr_price, 
                            "qty": q_str or "1", 
                            "trailing_sl_low": initial_sl
                        }
                        save_positions(active_positions)
                    p_fmt = f"{curr_price:.8f}" if curr_price < 0.01 else f"{curr_price:.4f}"
                    sl_fmt = f"{initial_sl:.8f}" if initial_sl < 0.01 else f"{initial_sl:.4f}"
                    tag = "PONOWNE WEJŚCIE (RE-ENTRY)" if is_reentry else "KUPNO 20%"
                    logging.info(f"⚡🚀 [SUKCES - {tag}] {symbol} | Kwota: {target_allocation:.2f}$ | Cena: {p_fmt} | Początkowy SL: {sl_fmt}")
                    free_usdc -= target_allocation
                else:
                    logging.error(f"❌ Błąd zlecenia {symbol}: {res}")
        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")
# ==============================================================================
# PEŁNA PALETA 9 POZOSTAŁYCH STRATEGII (ZACHOWANA W PLIKU)
# ==============================================================================
def run_support_bounce_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    logging.info("=== [POZ 0: DNO SINUSOIDY] CONFIRMED BOLLINGER / SUPPORT BOUNCE (15M) ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None or len(df) < 60: continue
            c_prev = df.iloc[-2]
            c_curr = df.iloc[-1]

            if (c_prev['low'] <= c_prev['bb_lower'] * 1.002 or c_prev['rsi'] <= 35.0):
                body = abs(c_prev['close'] - c_prev['open'])
                wick = min(c_prev['open'], c_prev['close']) - c_prev['low']
                reversal = (c_prev['close'] > c_prev['open']) or (wick >= body * 1.5)
                vol_ok = c_prev['volume'] >= (c_prev['vol_ma20'] * 0.85)
                upside = (c_curr['close'] < c_prev['ema200']) or (c_curr['close'] < c_prev['bb_mid'])

                if reversal and vol_ok and upside and c_curr['close'] >= c_prev['low']:
                    res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                    if res.get("retCode") == 0:
                        step, minq = get_symbol_lot_filter(symbol)
                        qty_str = format_qty_with_step(order_val / c_curr['close'], step, minq)
                        with lock_pos:
                            active_positions[symbol] = {
                                "strategy": "SUPPORT_BOUNCE", "buy_price": c_curr['close'], 
                                "peak_price": c_curr['close'], "qty": qty_str or "1", 
                                "sold_half": False, "structural_sl": round(c_prev['low'] * 0.996, 6)
                            }
                            save_positions(active_positions)
                        logging.info(f"🎯 [KUPNO] {symbol} (Dno Sinusoidy) | Kwota: {order_val:.2f}$ ({risk_label})")
                        break
        except Exception: pass

def run_mtf_alignment_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    logging.info("=== [POZ 1: NAJBEZPIECZNIEJSZA] MULTI-TIMEFRAME ALIGNMENT 4H+15M ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df_4h = get_market_data_no_cache(symbol, interval="240", limit=60)
            if df_4h is None or len(df_4h) < 45: continue
            c4h = df_4h.iloc[-1]
            if not (c4h['close'] > c4h['ema50'] and c4h['ema21'] > c4h['ema50']): continue

            df_15m = get_market_data(symbol, interval="15", limit=60)
            if df_15m is None: continue
            c15 = df_15m.iloc[-2]
            if c15['rsi'] <= 36 and ((c15['close'] > c15['open']) or ((c15['open'] - c15['low']) > abs(c15['close'] - c15['open']) * 1.5)):
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                if res.get("retCode") == 0:
                    step, minq = get_symbol_lot_filter(symbol)
                    qty_str = format_qty_with_step(order_val / c15['close'], step, minq)
                    with lock_pos:
                        active_positions[symbol] = {"strategy": "MTF_ALIGNMENT", "buy_price": c15['close'], "peak_price": c15['close'], "qty": q_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"💎 [KUPNO] {symbol} (MTF Alignment) | Kwota: {order_val:.2f}$ ({risk_label})")
                    break
        except Exception: pass

def scan_top150_for_ema_signals(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    logging.info("=== [POZ 2: BARDZO BEZPIECZNA] EMA TREND SCANNER ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None: continue
            c = df.iloc[-2]
            if c['ema21'] > c['ema89'] and (c['low'] <= max(c['ema21'], c['ema89'])) and (c['low'] >= min(c['ema21'], c['ema89'])):
                if c['close'] > c['open'] and 40 <= c['rsi'] <= 65 and c['volume'] >= c['vol_ma20']:
                    res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                    if res.get("retCode") == 0:
                        step, minq = get_symbol_lot_filter(symbol)
                        q_str = format_qty_with_step(order_val / c['close'], step, minq)
                        with lock_pos:
                            active_positions[symbol] = {"strategy": "EMA_SCAN", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                            save_positions(active_positions)
                        logging.info(f"🔥 [KUPNO] {symbol} (EMA Scan) | Kwota: {order_val:.2f}$ ({risk_label})")
                        break
        except Exception: pass

def run_top_gainers_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
    logging.info("=== [POZ 3: UMIARKOWANIE BEZPIECZNA] TOP GAINERS PULLBACK ===")
    if order_val <= 0: return

    for symbol in get_top_gainers_symbols():
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None: continue
            c = df.iloc[-2]
            if (c['close'] > c['ema89']) and (c['ema21'] > c['ema89']) and (c['low'] <= max(c['ema9'], c['ema21'])) and (c['close'] >= c['ema21']) and (c['close'] > c['open']):
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                if res.get("retCode") == 0:
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(order_val / c['close'], step, minq)
                    with lock_pos:
                        active_positions[symbol] = {"strategy": "TOP_GAINER", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"🚀 [KUPNO] {symbol} (Gainer Pullback) | Kwota: {order_val:.2f}$ ({risk_label})")
                    break
        except Exception: pass

def run_relative_strength_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    logging.info("=== [POZ 4: BEZPIECZNA / OBRONNA] RELATIVE STRENGTH VS BTC ===")
    if order_val <= 0: return

    try:
        df_btc = get_market_data("BTCUSDC", interval="15", limit=10)
        if df_btc is None or len(df_btc) < 6: return
        btc_change = (df_btc.iloc[-2]['close'] - df_btc.iloc[-5]['open']) / df_btc.iloc[-5]['open']
        if not (-0.015 <= btc_change <= -0.003): return

        for symbol in top_symbols:
            if symbol in active_positions or symbol == "BTCUSDC" or symbol in STABLE_PAIRS_BLACKLIST: continue
            if len(active_positions) >= MAX_TOTAL_POSITIONS: break
            try:
                df_alt = get_market_data(symbol, interval="15", limit=60)
                if df_alt is None or len(df_alt) < 40: continue
                c_now = df_alt.iloc[-2]
                alt_change = (c_now['close'] - df_alt.iloc[-5]['open']) / df_alt.iloc[-5]['open']
                if alt_change >= 0.010 and c_now['close'] > c_now['ema21'] and c_now['volume'] >= (c_now['vol_ma20'] * 1.3):
                    res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                    if res.get("retCode") == 0:
                        step, minq = get_symbol_lot_filter(symbol)
                        q_str = format_qty_with_step(order_val / c_now['close'], step, minq)
                        with lock_pos:
                            active_positions[symbol] = {"strategy": "RELATIVE_STRENGTH", "buy_price": c_now['close'], "peak_price": c_now['close'], "qty": q_str or "1", "sold_half": False}
                            save_positions(active_positions)
                        logging.info(f"🛡️ [KUPNO] {symbol} (RS vs BTC) | Kwota: {order_val:.2f}$ ({risk_label})")
                        break
            except Exception: pass
    except Exception: pass

def run_bb_squeeze_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
    logging.info("=== [POZ 5: ŚREDNIE RYZYKO] BOLLINGER BANDS SQUEEZE ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=60)
            if df is None or len(df) < 40: continue
            c = df.iloc[-2]
            recent_widths = df['bb_width'].iloc[-20:-2]
            avg_width = df['bb_width'].iloc[-40:-2].mean()
            if recent_widths.min() <= (avg_width * 0.70) and c['close'] > c['bb_upper'] and c['close'] > c['open'] and c['volume'] >= (c['vol_ma20'] * 2.5):
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                if res.get("retCode") == 0:
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(order_val / c['close'], step, minq)
                    with lock_pos:
                        active_positions[symbol] = {"strategy": "BB_SQUEEZE", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"💥 [KUPNO] {symbol} (BB Squeeze) | Kwota: {order_val:.2f}$ ({risk_label})")
                    break
        except Exception: pass

def run_ema21_89_pure_crossover_strategy(top_symbols):
    global active_positions
    cross_cnt = sum(1 for p in active_positions.values() if p.get("strategy") == "EMA21_89_CROSS")
    if cross_cnt >= MAX_EMA_CROSS_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
    logging.info("=== [POZ 6: ŚREDNIO-WYŻSZE RYZYKO] CZYSTY CROSS EMA 21/89 ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None or len(df) < 95: continue
            c = df.iloc[-2]
            p = df.iloc[-3]
            if (p['ema21'] <= p['ema89']) and (c['ema21'] > c['ema89']) and ((c['ema21'] - c['ema89']) / c['ema89'] >= EMA_CROSS_MIN_SPREAD_PCT):
                if c['close'] > c['ema21'] and c['close'] > c['open'] and c['volume'] >= (c['vol_ma20'] * EMA_CROSS_VOL_MULTIPLIER):
                    res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                    if res.get("retCode") == 0:
                        step, minq = get_symbol_lot_filter(symbol)
                        q_str = format_qty_with_step(order_val / c['close'], step, minq)
                        with lock_pos:
                            active_positions[symbol] = {"strategy": "EMA21_89_CROSS", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                            save_positions(active_positions)
                        logging.info(f"⚡ [KUPNO] {symbol} (Złoty Krzyż EMA 21/89) | Kwota: {order_val:.2f}$ ({risk_label})")
                        break
        except Exception: pass

def run_dip_hunter_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)
    logging.info("=== [POZ 7: WYŻSZE RYZYKO] REAL-TIME DIP HUNTER (5M) ===")
    if order_val <= 0: return

    for symbol in get_top_losers_symbols(limit_count=6):
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        try:
            df = get_market_data_no_cache(symbol, interval="5", limit=30)
            if df is None or len(df) < 25: continue
            c = df.iloc[-1]
            body = abs(c['close'] - c['open'])
            wick = min(c['open'], c['close']) - c['low']
            if wick > (body * 2.0) and c['volume'] >= (c['vol_ma20'] * DIP_VOL_MULTIPLIER) and c['rsi'] <= DIP_RSI_5M_MAX:
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                if res.get("retCode") == 0:
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(order_val / c['close'], step, minq)
                    with lock_pos:
                        active_positions[symbol] = {"strategy": "DIP_HUNTER_5M", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"🔥 [KUPNO] {symbol} (Dip Hunter 5M) | Kwota: {order_val:.2f}$ ({risk_label})")
                    break
        except Exception: pass

def scan_for_pumps(top_symbols):
    global active_positions
    pumps_cnt = sum(1 for p in active_positions.values() if p.get("strategy") == "PUMP_HUNTER")
    if pumps_cnt >= MAX_PUMP_POSITIONS or len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)
    logging.info("=== [POZ 8: WYSOKIE RYZYKO] MOMENTUM PUMP HUNTER ===")
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions or symbol in STABLE_PAIRS_BLACKLIST: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=25)
            if df is None: continue
            c = df.iloc[-2]
            if (c['close'] - c['open']) / c['open'] >= PUMP_PRICE_CHANGE_PCT and c['volume'] >= (c['vol_ma20'] * PUMP_VOL_MULTIPLIER) and 45 <= c['rsi'] <= 75:
                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                if res.get("retCode") == 0:
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(order_val / c['close'], step, minq)
                    with lock_pos:
                        active_positions[symbol] = {"strategy": "PUMP_HUNTER", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"🚀 [KUPNO] {symbol} (Pump Hunter) | Kwota: {order_val:.2f}$ ({risk_label})")
                    break
        except Exception: pass

def scan_pepe_specialist():
    global active_positions
    sym = "PEPEUSDC"
    if sym in active_positions or len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, HIGH_RISK_PCT)
    logging.info("=== [POZ 9: NAJBARDZIEJ RYZYKOWNA / MEME] PEPE EMA SPECIALIST ===")
    if order_val <= 0: return

    try:
        df = get_market_data(sym, interval="15", limit=250)
        if df is None or len(df) < 200: return
        c = df.iloc[-2]
        p = df.iloc[-3]
        macro = (c['close'] > c['ema200']) and (c['ema21'] > c['ema200'])
        ema_bounce = (c['ema21'] > c['ema89']) and (c['low'] <= c['ema21']) and (c['close'] > c['ema21']) and (c['close'] > c['open'])
        if macro and ema_bounce and 45 <= c['rsi'] <= 70 and c['volume'] >= c['vol_ma20']:
            res = session.place_order(category=CATEGORY, symbol=sym, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
            if res.get("retCode") == 0:
                step, minq = get_symbol_lot_filter(sym)
                q_str = format_qty_with_step(order_val / c['close'], step, minq)
                with lock_pos:
                    active_positions[sym] = {"strategy": "PEPE_SPECIALIST", "buy_price": c['close'], "peak_price": c['close'], "qty": q_str or "1", "sold_half": False}
                    save_positions(active_positions)
                logging.info(f"🐸 [KUPNO] PEPEUSDC (Odbicie EMA 21) | Kwota: {order_val:.2f}$ ({risk_label})")
    except Exception: pass

# ==============================================================================
# SZYBKI ASYNCHRONICZNY WĄTEK MONITOROWANIA (ODŚWIEŻANIE CO 3 SEKUNDY)
# ==============================================================================
def monitoring_worker():
    global active_positions
    while True:
        try:
            with lock_pos: items = list(active_positions.items())
            if not items:
                time.sleep(3)
                continue

            btc_flash_crash = False
            try:
                res = session.get_kline(category=CATEGORY, symbol="BTCUSDC", interval="1", limit=15)
                c_list = res.get("result", {}).get("list", [])
                if c_list and len(c_list) >= 15:
                    c_close = safe_float(c_list[0][4])
                    highest_15m = max(safe_float(x[2]) for x in c_list)
                    if (c_close - highest_15m) / highest_15m <= BTC_FLASH_CRASH_EXIT_PCT:
                        btc_flash_crash = True
            except: pass

            symbols_to_del = []

            for symbol, pos in items:
                try:
                    res = session.get_kline(category=CATEGORY, symbol=symbol, interval="15", limit=15)
                    c_list = res.get("result", {}).get("list", [])
                    if not c_list: continue

                    c_curr = safe_float(c_list[0][4])
                    bp = pos["buy_price"]
                    peak = max(pos.get("peak_price", bp), c_curr)

                    with lock_pos:
                        if symbol in active_positions:
                            active_positions[symbol]["peak_price"] = peak

                    total_pnl = (c_curr - bp) / bp
                    strat = pos.get("strategy", "")

                    sl_hit = False
                    be_hit = False
                    dyn_hit = False
                    trail_hit = False
                    sl_type = ""

                    # 1. DEDYKOWANE PROWADZENIE DLA NOWEJ STRATEGII EMA 29/89
                    if strat == "EMA29_89_SWING":
                        sold_half = pos.get("sold_half", False)
                        
                        # 1. Cząstkowy TP (33% przy +2.5%)
                        if not sold_half and total_pnl >= 0.025:
                            rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                            if rb > 0:
                                st, mq = get_symbol_lot_filter(symbol)
                                qs = format_qty_with_step(rb * 0.33, st, mq)
                                if qs:
                                    ticker = session.get_tickers(category=CATEGORY, symbol=symbol)
                                    ask_p = safe_float(ticker.get("result", {}).get("list", [{}])[0].get("ask1Price", c_curr))
                                    session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Limit", qty=qs, price=str(ask_p), timeInForce="PostOnly")
                                    with lock_pos:
                                        if symbol in active_positions:
                                            active_positions[symbol]["sold_half"] = True
                                            save_positions(active_positions)
                                    logging.info(f"💸 [PARTIAL TP] {symbol} LIMIT ORDER po {ask_p:.4f} USD (+2.5%).")
                        
                        # 2. Obliczenie aktywnego poziomu obrony (BreakEven vs Trailing)
                        be_level = bp * 1.003
                        if (peak - bp) / bp >= 0.015 or sold_half:
                            # Próg BreakEven aktywowany
                            curr_sl = max(pos.get("trailing_sl_low", 0.0), be_level)
                        else:
                            curr_sl = pos.get("trailing_sl_low", 0.0)

                        # 3. Dynamiczne podnoszenie za dołkami 15M
                        if len(c_list) >= 4:
                            recent_lows = [safe_float(c_list[i][3]) for i in range(1, 4)]
                            local_swing_low = min(recent_lows) * 0.998
                            
                            if local_swing_low > pos.get("trailing_sl_low", 0.0) and c_curr > bp:
                                with lock_pos:
                                    if symbol in active_positions:
                                        active_positions[symbol]["trailing_sl_low"] = local_swing_low
                                        save_positions(active_positions)
                                logging.info(f"📈 [TRAILING SL] Podniesiono bazowy SL dla {symbol} na {local_swing_low:.4f} (za lokalnym dołkiem).")

                        # Aktualizacja bieżącego Stop Lossa po ewentualnym wyznaczeniu nowego dołka
                        curr_sl = max(curr_sl, pos.get("trailing_sl_low", 0.0))

                        # 4. Sprawdzenie wybicia obrony dołem
                        if curr_sl > 0 and c_curr <= curr_sl:
                            sl_hit = True
                            if curr_sl == be_level:
                                sl_type = f"BreakEven (Obrona kapitału)"
                            else:
                                sl_type = f"Wyjście z trendu (Stop za dołkiem: {curr_sl:.4f})"
                                
                        # 5. Ochrona zysku prosto ze szczytu (-1.0% od peaku przy zysku min +2.0%)
                        if (peak - bp) / bp >= 0.020 and (peak - c_curr) / peak >= 0.010:
                            trail_hit = True
                            sl_type = f"Ochrona zysku ze szczytu (Trailing -1.0%)"
                    
                    # 2. STANDARDOWA OBSŁUGA POZYCJI DLA STARYCH / PRZYWRÓCONYCH MONET
                    else:
                        sold_half = pos.get("sold_half", False)
                        struc_sl = pos.get("structural_sl")
                        
                        tr_list = []
                        for i in range(len(c_list)-1):
                            h = safe_float(c_list[i][2])
                            l = safe_float(c_list[i][3])
                            pc = safe_float(c_list[i+1][4])
                            tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
                        atr_pct = ((sum(tr_list) / len(tr_list)) / c_curr) if tr_list and c_curr > 0 else 0.02
                        dynamic_sl_pct = max(atr_pct * 1.8, 0.015)
                        trailing_drop_pct = max(atr_pct * 1.5, 0.010)
                        drop_from_peak = (peak - c_curr) / peak

                        if not sold_half and total_pnl >= PARTIAL_TP_PCT:
                            rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                            if rb > 0:
                                st, mq = get_symbol_lot_filter(symbol)
                                qs = format_qty_with_step(rb * 0.33, st, mq)
                                if qs:
                                    ticker = session.get_tickers(category=CATEGORY, symbol=symbol)
                                    ask_p = safe_float(ticker.get("result", {}).get("list", [{}])[0].get("ask1Price", c_curr))
                                    session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Limit", qty=qs, price=str(ask_p), timeInForce="PostOnly")
                                    with lock_pos:
                                        if symbol in active_positions:
                                            active_positions[symbol]["sold_half"] = True
                                            save_positions(active_positions)
                                    logging.info(f"💸 [PARTIAL TP] {symbol} LIMIT ORDER po {ask_p:.4f} USD.")

                        sl_hit = struc_sl and c_curr <= struc_sl
                        if sl_hit: sl_type = "Stop Loss Strukturalny"
                        be_hit = (sold_half or (peak - bp)/bp >= BREAKEVEN_TRIGGER_PCT) and c_curr <= bp * (1.0 + BREAKEVEN_SL_PCT)
                        dyn_hit = total_pnl <= -dynamic_sl_pct
                        trail_hit = drop_from_peak >= trailing_drop_pct and c_curr > bp

                    panic_hit = btc_flash_crash and total_pnl < -0.005

                    if sl_hit or be_hit or dyn_hit or trail_hit or panic_hit:
                        reason = "BTC Flash Crash" if panic_hit else (sl_type if sl_type else ("Trailing Stop" if trail_hit else "Stop Loss / BreakEven"))
                        rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                        if rb > 0:
                            st, mq = get_symbol_lot_filter(symbol)
                            qs = format_qty_with_step(rb, st, mq)
                            if qs:
                                res_s = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qs)
                                if res_s.get("retCode") == 0:
                                    record_closed_trade(symbol, strat, bp, c_curr, safe_float(qs), reason, is_maker=False)
                                    symbols_to_del.append(symbol)
                        else:
                            symbols_to_del.append(symbol)
                except Exception:
                    pass

            if symbols_to_del:
                with lock_pos:
                    for s in symbols_to_del:
                        if s in active_positions: del active_positions[s]
                    save_positions(active_positions)
        except Exception:
            pass
        time.sleep(3)

# ==============================================================================
# AUTOMATYCZNA LIKWIDACJA STABLECOINÓW I REINWESTYCJA W BTC
# ==============================================================================
def auto_liquidate_stables_to_btc():
    global active_positions
    for coin, sym in [("USDT", "USDTUSDC"), ("EURC", "EURCUSDC")]:
        try:
            bal = get_coin_available_balance(coin)
            if (bal * 1.0 >= MIN_EXCHANGE_NOTIONAL) or (sym in active_positions):
                logging.info(f"🧹 [AUTO-WYMIANA] Wykryto {bal} {coin}. Sprzedaję na Spot Market i kupuję BTC...")
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
                    time.sleep(1)
                    _, fu = get_balances()
                    if fu >= MIN_EXCHANGE_NOTIONAL:
                        btc_val = round(fu * 0.99, 2)
                        r2 = session.place_order(category=CATEGORY, symbol="BTCUSDC", side="Buy", orderType="Market", qty=f"{btc_val:.2f}")
                        if r2.get("retCode") == 0:
                            logging.info(f"💎🚀 [REINWESTYCJA] Za {btc_val} USDC zakupiono Bitcoina (BTCUSDC)!")
        except Exception:
            pass

# ==============================================================================
# PĘTLA GŁÓWNA SKANERA RYNKU (CO 10 SEKUND ZAMIAST 60)
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
                logging.info("🚀 NOWY CYKL SKANOWANIA RYNKU (Skan w tle co 10 sekund)...")
                last_scan_print_time = now_ts

            if now_ts - last_hourly_cost_report >= 3600:
                sync_bybit_clock()
                total_equity, _ = get_balances()
                report_hourly_costs(total_equity)

            current_sentiment = update_market_sentiment() if now_ts - last_scan_print_time <= 1 else SENTIMENT_STATE
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
                        sl_info = f" | SL: {pos.get('trailing_sl_low'):.4f}" if 'trailing_sl_low' in pos else ""
                        logging.info(f"ℹ️ [{symbol}] PnL: {pnl_pct:+.2f}% | Szczyt: {pos.get('peak_price', cp)}{sl_info}")
                    except Exception: pass

            # ==================================================================
            # SKANOWANIE I ZAKUPY - Uruchamiane co 10 sekund
            # ==================================================================
            if is_btc_market_healthy() and usdc_ok:
                run_ema29_89_swing_strategy()
                
                if ENABLE_LEGACY_STRATEGIES:
                    top_vols = get_top_volume_symbols(TOP_MARKETS_COUNT)
                    if top_vols:
                        run_support_bounce_strategy(top_vols)
                        run_relative_strength_strategy(top_vols)
                        if "BEARISH" not in current_sentiment:
                            run_mtf_alignment_strategy(top_vols)
                            scan_top150_for_ema_signals(top_vols)
                            run_top_gainers_strategy()
                            run_bb_squeeze_strategy(top_vols)
                            run_ema21_89_pure_crossover_strategy(top_vols)
                            run_dip_hunter_strategy()
                        if "BULLISH" in current_sentiment:
                            scan_for_pumps(top_vols)
                            scan_pepe_specialist()
            else:
                if now_ts - last_scan_print_time <= 1:
                    if not usdc_ok: logging.info(f"⛔ [ZAKUPY ZABLOKOWANE] Ochrona płynności USDC: {usdc_stat}")
                    else: logging.info("⛔ [ZAKUPY WSTRZYMANE] Czerwona flaga z BTC Guard (gwałtowny spadek).")

            if now_ts - last_scan_print_time <= 1:
                logging.info("💤 Skan główny zakończony. Skaner wejść w locie czuwa (odpytywanie co 10s)...")
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
        self.wfile.write(b"Bybit Spot Trading Bot is Running 24/7 (V9: Hybrydowe skanowanie w locie + Wielowatkowosc)")
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
