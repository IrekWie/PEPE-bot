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

# --- USTAWIENIA SKANOWANIA I PŁYNNOŚCI ---
TOP_MARKETS_COUNT = 150
MIN_24H_TURNOVER_USDC = 150000.0     # Odrzucamy wszystkie shitcoiny o obrocie < 150k USD dziennie
TOP_DIPS_COUNT = 6
TOP_GAINERS_COUNT = 4
MAX_PUMP_POSITIONS = 3

STABLE_PAIRS_BLACKLIST = [
    "USDTUSDC", "USDCUSDT", "EURCUSDC", "USDTEUR", "EURCUSDT", 
    "USDEUSDC", "DAIUSDC", "FDUSDUSDC", "TUSDUSDC"
]

# --- LIMITY FINANSOWE I REZERWY ---
STANDARD_MIN_ORDER_VALUE = 65.0      # Obniżone ze 100 USD, by nie zablokować konta przy wielu pozycjach
MIN_SYNC_POSITION_VALUE = 8.0     
MIN_EXCHANGE_NOTIONAL = 5.0       
MIN_CASH_RESERVE_PCT = 0.20          # 20% kapitału ZAWSZE musi leżeć w gotówce na sytuacje awaryjne

# --- BEZWZGLĘDNE BEZPIECZNIKI PORTFELA (GLOBALNY TRAILING DRAWDOWN) ---
MAX_TRAILING_EQUITY_DROP_USD = 15.0  # Spadek o 15 USD od MAKSYMALNEGO salda dnia = BEZWZGLĘDNY STOP ZAKUPÓW
MAX_CONSECUTIVE_LOSSES = 3           # 3 błędne decyzje pod rząd = STOP ZAKUPÓW
LOSS_COOLDOWN_HOURS = 12             # Czas blokady zakupów po 3 stratach (w godzinach)
BTC_SURGE_UNLOCK_PCT = 0.015         # +1.5% wzrostu BTC w 2h zwalnia blokadę zakupów

USDC_MAX_SPREAD_PCT = 0.0008         # Max spread na BTCUSDC: 0.080%
USDC_1M_DUMP_THRESHOLD = -0.003      # Szybki zrzut BTCUSDC na 1M (-0.3%) blokuje zakupy

# ==============================================================================
# ZMIENNE GLOBALNE I CACHE
# ==============================================================================
MAX_TOTAL_POSITIONS = 6              # Obniżone z 10, by zachować większą kontrolę i wolną gotówkę
MAX_EMA_CROSS_POSITIONS = 3
SENTIMENT_STATE = "NEUTRAL"
ETH_SENTIMENT_STATE = "NEUTRAL"
ALTCOIN_FLOW_STATE = "ALT_PRESSURE"

SAFE_RISK_PCT = 0.15
MODERATE_RISK_PCT = 0.10
HIGH_RISK_PCT = 0.07

BTC_FAST_DROP_BUY_BLOCK_PCT = -0.008
BTC_FLASH_CRASH_EXIT_PCT    = -0.010

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

_original_time = time.time

# CACHE ŚWIEC (Czyszczony co cykl)
kline_cache_15m = {}

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
                data = json.load(f)
                if "consecutive_losses" not in data: data["consecutive_losses"] = 0
                if "cooldown_until" not in data: data["cooldown_until"] = 0
                if "equity_baseline" not in data: data["equity_baseline"] = {}
                if "trades_log" not in data: data["trades_log"] = []
                return data
        except Exception:
            pass
    return {
        "total_realized_usd": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "consecutive_losses": 0,
        "cooldown_until": 0,
        "equity_baseline": {},
        "trades_log": [],
        "daily": {}
    }

def save_trade_history(history):
    try:
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu historii transakcji: {e}")

trade_history = load_trade_history()
lock_history = threading.Lock()

def record_closed_trade(symbol, strat, buy_price, sell_price, qty, reason, is_maker=False):
    global trade_history
    with lock_history:
        raw_pnl_usd = (sell_price - buy_price) * qty
        
        fee_buy = (buy_price * qty) * 0.001
        fee_sell = (sell_price * qty) * (0.001 if not is_maker else 0.001)
        estimated_fee = fee_buy + fee_sell
        
        pnl_usd = round(raw_pnl_usd - estimated_fee, 2)
        pnl_pct = (pnl_usd / (buy_price * qty)) if (buy_price * qty) > 0 else 0.0

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = time.time()

        trade_history["total_realized_usd"] = round(trade_history.get("total_realized_usd", 0.0) + pnl_usd, 2)
        trade_history["total_trades"] = trade_history.get("total_trades", 0) + 1

        if "trades_log" not in trade_history:
            trade_history["trades_log"] = []

        trade_history["trades_log"].append({
            "timestamp": now_ts,
            "symbol": symbol,
            "turnover": round((buy_price + sell_price) * qty, 2),
            "fee": round(estimated_fee, 4),
            "gross_pnl": round(raw_pnl_usd, 2),
            "net_pnl": pnl_usd
        })

        if len(trade_history["trades_log"]) > 200:
            trade_history["trades_log"] = trade_history["trades_log"][-200:]

        if pnl_usd >= 0:
            trade_history["wins"] = trade_history.get("wins", 0) + 1
            trade_history["consecutive_losses"] = 0
        else:
            trade_history["losses"] = trade_history.get("losses", 0) + 1
            streak = trade_history.get("consecutive_losses", 0) + 1
            trade_history["consecutive_losses"] = streak
            
            if streak >= MAX_CONSECUTIVE_LOSSES:
                trade_history["cooldown_until"] = now_ts + (LOSS_COOLDOWN_HOURS * 3600)

        if "daily" not in trade_history:
            trade_history["daily"] = {}
        if today_str not in trade_history["daily"]:
            trade_history["daily"][today_str] = {
                "pnl_usd": 0.0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "total_fees": 0.0,
                "total_turnover": 0.0
            }

        d = trade_history["daily"][today_str]
        d["pnl_usd"] = round(d.get("pnl_usd", 0.0) + pnl_usd, 2)
        d["trades"] = d.get("trades", 0) + 1
        d["total_fees"] = round(d.get("total_fees", 0.0) + estimated_fee, 4)
        d["total_turnover"] = round(d.get("total_turnover", 0.0) + ((buy_price + sell_price) * qty), 2)

        if pnl_usd >= 0:
            d["wins"] = d.get("wins", 0) + 1
        else:
            d["losses"] = d.get("losses", 0) + 1

        save_trade_history(trade_history)
        sign = "+" if pnl_usd >= 0 else ""
        ord_type = "LIMIT (MAKER)" if is_maker else "MARKET (TAKER)"
        logging.info(f"📊 [BILANS ({ord_type})] {symbol} | Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | Fee: -{estimated_fee:.2f}$ | Powód: {reason}")

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
                        logging.warning(f"⚠️ [USDC SPREAD GUARD] Zbyt szeroki spread na BTCUSDC ({spread_pct*100:.3f}%). Ryzyko poślizgu cenowego!")
                        return False, f"WIDE_SPREAD_{spread_pct*100:.2f}%"

        df_usdc_1m = get_market_data_no_cache("BTCUSDC", interval="1", limit=3)
        if df_usdc_1m is not None and len(df_usdc_1m) >= 2:
            c1m = df_usdc_1m.iloc[-1]
            chg_1m = (c1m['close'] - c1m['open']) / c1m['open']
            if chg_1m <= USDC_1M_DUMP_THRESHOLD:
                logging.warning(f"⚠️ [USDC MICRO-DUMP GUARD] Nagły spadek BTCUSDC 1M ({chg_1m*100:.2f}%). Chwilowa pauza przed zakupem!")
                return False, f"USDC_DUMP_{chg_1m*100:.2f}%"

        try:
            r = requests.get("https://api.bybit.com/v5/market/tickers?category=spot&symbol=USDCUSDT", timeout=2)
            if r.status_code == 200:
                d = r.json()
                t_list = d.get("result", {}).get("list", [])
                if t_list:
                    peg_price = safe_float(t_list[0].get("lastPrice"), 1.0)
                    if abs(peg_price - 1.0) >= 0.003:
                        logging.warning(f"⚠️ [GLOBAL USDC PEG ALERT] Globalne odchylenie kursu USDC: {peg_price:.4f}! Rynek niestabilny.")
                        return False, f"GLOBAL_DEPEG_{peg_price:.4f}"
        except Exception:
            pass

        return True, "USDC_HEALTHY"
    except Exception as e:
        logging.warning(f"Błąd analizy zależności USDC: {e}")
        return True, "BYPASS"

def check_btc_significant_surge():
    try:
        df = get_market_data_no_cache("BTCUSDC", interval="60", limit=5)
        if df is not None and len(df) >= 3:
            curr_p = df.iloc[-1]['close']
            open_2h = df.iloc[-3]['open']
            growth = (curr_p - open_2h) / open_2h
            if growth >= BTC_SURGE_UNLOCK_PCT:
                return True, growth
    except Exception:
        pass
    return False, 0.0

def check_and_update_circuit_breakers(current_total_equity, current_sentiment):
    global trade_history
    with lock_history:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = time.time()

        if "equity_baseline" not in trade_history:
            trade_history["equity_baseline"] = {}
        baseline = trade_history["equity_baseline"]

        if baseline.get("date") != today_str:
            baseline["date"] = today_str
            baseline["starting_equity"] = current_total_equity
            baseline["peak_equity"] = current_total_equity
            save_trade_history(trade_history)
            logging.info(f"🌅 [NOWY DZIEŃ UTC] Ustalono kapitał bazowy i szczytowy dla dnia {today_str}: {current_total_equity:.2f} USD")

        starting_equity = safe_float(baseline.get("starting_equity"), current_total_equity)
        prev_peak = safe_float(baseline.get("peak_equity"), starting_equity)
        peak_equity = max(prev_peak, current_total_equity)

        if peak_equity > prev_peak:
            baseline["peak_equity"] = peak_equity
            save_trade_history(trade_history)
            logging.info(f"🚀 [NOWY SZCZYT DNIA] Szczyt kapitału przesunięty w górę: {peak_equity:.2f} USD")

        drop_from_peak = peak_equity - current_total_equity
        cooldown_until = safe_float(trade_history.get("cooldown_until", 0))
        consecutive_losses = trade_history.get("consecutive_losses", 0)

        is_blocked_by_drop = drop_from_peak >= MAX_TRAILING_EQUITY_DROP_USD
        is_blocked_by_losses = (consecutive_losses >= MAX_CONSECUTIVE_LOSSES) or (now_ts < cooldown_until)

        if is_blocked_by_drop or is_blocked_by_losses:
            btc_surging, btc_gain = check_btc_significant_surge()
            if btc_surging:
                logging.info(f"🚀⚡ [BTC SURGE UNLOCK] Bitcoin rośnie (+{btc_gain*100:.2f}% w 2h)! Znoszę blokadę zakupów i resetuję szczyt.")
                trade_history["cooldown_until"] = 0
                trade_history["consecutive_losses"] = 0
                baseline["peak_equity"] = current_total_equity
                save_trade_history(trade_history)
                return True, 0.0, current_total_equity

        if is_blocked_by_drop:
            logging.warning(f"🛑 [TRAILING BREAKER] Kapitał spadł o {drop_from_peak:.2f}$ od szczytu {peak_equity:.2f}$. Zakupy ZABLOKOWANE.")
            return False, drop_from_peak, peak_equity

        if is_blocked_by_losses:
            if now_ts < cooldown_until:
                rem_minutes = int((cooldown_until - now_ts) / 60)
                logging.warning(f"⏳ [BLOKADA: 3 STRATY] Zakupy wstrzymane na {rem_minutes} min.")
            else:
                logging.warning(f"⏳ [BLOKADA: 3 STRATY] Zakupy zablokowane na {LOSS_COOLDOWN_HOURS}h.")
            return False, drop_from_peak, peak_equity

        return True, drop_from_peak, peak_equity

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
            return available, f"Zredukowane ({available:.2f}$) by zachować rezerwę"
        return 0.0, f"Brak gotówki (Chronię {MIN_CASH_RESERVE_PCT*100:.0f}% rezerwy)"

    if free_usdc >= effective_target:
        label = f"{target_pct * 100:.0f}%" if target_val >= STANDARD_MIN_ORDER_VALUE else f"Min. {STANDARD_MIN_ORDER_VALUE:.0f}$"
        return round(effective_target, 2), label

    return 0.0, "Brak środków"

# ==============================================================================
# INICJALIZACJA PUNKTU KOŃCOWEGO
# ==============================================================================
endpoints_to_test = [("EU Region / MiCA", "https://api.bybit.eu"), ("Global Mainnet", "https://api.bybit.com")]
session = None
for name, url in endpoints_to_test:
    try:
        test_sess = HTTP(testnet=TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, recv_window=20000)
        test_sess.endpoint = url
        res_bal = test_sess.get_wallet_balance(accountType="UNIFIED")
        if res_bal.get("retCode") == 0:
            session = test_sess
            print(f"✅ POŁĄCZONO PRZEZ: {name}", flush=True)
            res_time = test_sess.get_server_time()
            bybit_ms = int(res_time["result"]["timeNano"]) // 1000000
            local_ms = int(_original_time() * 1000)
            time.time = lambda: _original_time() + (bybit_ms - local_ms) / 1000.0
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
            b_prec = lot.get("basePrecision")
            step = lot.get("qtyStep")
            eff = b_prec if b_prec and str(b_prec).strip() != "" else (step if step else "0.000001")
            instrument_filters[symbol] = (eff, lot.get("minOrderQty", "0.000001"))
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
        if floored <= 0 and qty_dec > 0: floored = qty_dec
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
            with open(POSITIONS_FILE, "r") as f:
                pos = json.load(f)
                for p in pos.values(): p.setdefault("sold_half", False)
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
                    a = c.get("availableToWithdraw")
                    fu = safe_float(a) if a else safe_float(c.get("equity"))
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
                    wb = safe_float(c.get("walletBalance"))
                    avail = safe_float(c.get("availableToWithdraw"))
                    return wb if wb > 0 else avail
    except Exception:
        pass
    return 0.0

# ==============================================================================
# NOWOŚĆ: FILTR PŁYNNOŚCI (TURNOVER24H) 
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
            return [t["symbol"] for t in valid[:limit_count]]
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

def sync_positions_from_bybit():
    global active_positions
    if not session: return
    try:
        with lock_pos:
            res = session.get_wallet_balance(accountType="UNIFIED")
            if res.get("retCode") != 0 or not res.get("result", {}).get("list"): return
            
            for coin_info in res["result"]["list"][0].get("coin", []):
                coin = coin_info.get("coin")
                if coin in ["USDC", "USDT", "EURC", "USD", "EUR", "PLN", "GBP", "TRY", "UAH", "RUB"]: 
                    continue

                bal = max(safe_float(coin_info.get("walletBalance")), safe_float(coin_info.get("equity")))
                if bal <= 0: continue

                sym = f"{coin}USDC"
                if sym in STABLE_PAIRS_BLACKLIST: continue

                if sym not in active_positions:
                    df = get_market_data_no_cache(sym, interval="15", limit=5)
                    if df is not None and len(df) > 0:
                        cp = df.iloc[-1]['close']
                        if bal * cp >= MIN_SYNC_POSITION_VALUE:
                            buy_p = cp
                            try:
                                ex = session.get_executions(category=CATEGORY, symbol=sym, limit=10)
                                if ex.get("retCode") == 0:
                                    buys = [e for e in ex.get("result", {}).get("list", []) if e.get("side") == "Buy"]
                                    if buys: buy_p = safe_float(buys[0].get("execPrice"), cp)
                            except Exception: pass
                            
                            step, minq = get_symbol_lot_filter(sym)
                            q_str = format_qty_with_step(bal, step, minq)
                            if q_str:
                                active_positions[sym] = {"strategy": "AUTO", "buy_price": buy_p, "peak_price": cp, "qty": q_str, "sold_half": False}
                                logging.info(f"🔄 [SYNC] Przywrócono {sym}: {q_str} szt.")
            save_positions(active_positions)
    except Exception as e:
        logging.error(f"Błąd synchronizacji: {e}")

# ==============================================================================
# WYZNACZANIE WSKAŹNIKÓW I GLOBALNY CACHE ŚWIEC
# ==============================================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def get_market_data_no_cache(symbol, interval="15", limit=250):
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
        df['bb_upper'] = df['bb_mid'] + (2.0 * df['bb_std'])
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        df['tr0'] = abs(df['high'] - df['low'])
        df['tr1'] = abs(df['high'] - df['close'].shift())
        df['tr2'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_pct'] = (df['atr'] / df['close']).fillna(0.02)
        return df
    except Exception:
        return None

def get_market_data(symbol, interval="15", limit=250):
    """Zwraca dane z cache 15M dla danego cyklu, zapobiegając spamowaniu API."""
    global kline_cache_15m
    if interval == "15" and symbol in kline_cache_15m:
        return kline_cache_15m[symbol]
    
    df = get_market_data_no_cache(symbol, interval, limit)
    if interval == "15" and df is not None:
        kline_cache_15m[symbol] = df
    return df

# ==============================================================================
# STRATEGIE HANDLOWE
# ==============================================================================
def run_support_bounce_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
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
                            active_positions[symbol] = {"strategy": "SUPPORT_BOUNCE", "buy_price": c_curr['close'], "peak_price": c_curr['close'], "qty": qty_str or "1", "sold_half": False, "structural_sl": round(c_prev['low'] * 0.996, 6)}
                            save_positions(active_positions)
                        logging.info(f"🎯 [KUPNO] {symbol} (Dno Sinusoidy) | Z: {order_val}$")
                        break
        except Exception: pass

def run_mtf_alignment_strategy(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions: continue
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
                        active_positions[symbol] = {"strategy": "MTF_ALIGNMENT", "buy_price": c15['close'], "peak_price": c15['close'], "qty": qty_str or "1", "sold_half": False}
                        save_positions(active_positions)
                    logging.info(f"💎 [KUPNO] {symbol} (MTF Alignment) | Z: {order_val}$")
                    break
        except Exception: pass

def scan_top150_for_ema_signals(top_symbols):
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, SAFE_RISK_PCT)
    if order_val <= 0: return

    for symbol in top_symbols:
        if symbol in active_positions: continue
        if len(active_positions) >= MAX_TOTAL_POSITIONS: break
        try:
            df = get_market_data(symbol, interval="15", limit=120)
            if df is None: continue
            c_prev = df.iloc[-2]
            if c_prev['ema21'] > c_prev['ema89'] and (c_prev['low'] <= max(c_prev['ema21'], c_prev['ema89'])) and (c_prev['low'] >= min(c_prev['ema21'], c_prev['ema89'])):
                if c_prev['close'] > c_prev['open'] and 40 <= c_prev['rsi'] <= 65 and c_prev['volume'] >= c_prev['vol_ma20']:
                    res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{order_val:.2f}")
                    if res.get("retCode") == 0:
                        step, minq = get_symbol_lot_filter(symbol)
                        q_str = format_qty_with_step(order_val / c_prev['close'], step, minq)
                        with lock_pos:
                            active_positions[symbol] = {"strategy": "EMA_SCAN", "buy_price": c_prev['close'], "peak_price": c_prev['close'], "qty": q_str or "1", "sold_half": False}
                            save_positions(active_positions)
                        logging.info(f"🔥 [KUPNO] {symbol} (EMA Scan) | Z: {order_val}$")
                        break
        except Exception: pass

def run_top_gainers_strategy():
    global active_positions
    if len(active_positions) >= MAX_TOTAL_POSITIONS: return
    total_equity, free_usdc = get_balances()
    order_val, risk_label = get_order_budget(total_equity, free_usdc, MODERATE_RISK_PCT)
    if order_val <= 0: return

    for symbol in get_top_gainers_symbols():
        if symbol in active_positions: continue
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
                    logging.info(f"🚀 [KUPNO] {symbol} (Gainer Pullback) | Z: {order_val}$")
                    break
        except Exception: pass

# ==============================================================================
# SZYBKI WĄTEK MONITOROWANIA OTWARTYCH POZYCJI (URUCHAMIANY CO 3 SEKUNDY)
# ==============================================================================
def monitoring_worker():
    global active_positions
    while True:
        try:
            if not active_positions:
                time.sleep(10)
                continue

            # Szybki odczyt kondycji BTC (bez pełnego DataFrame)
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
            
            with lock_pos:
                items = list(active_positions.items())
                
            for symbol, pos in items:
                try:
                    # Pobieramy tylko ostatnie 2 świece na potrzeby szybkiego Trail / TP
                    res = session.get_kline(category=CATEGORY, symbol=symbol, interval="15", limit=2)
                    c_list = res.get("result", {}).get("list", [])
                    if not c_list: continue
                    
                    c_curr = safe_float(c_list[0][4]) # zamkniecie (aktualna cena)
                    bp = pos["buy_price"]
                    peak = max(pos.get("peak_price", bp), c_curr)
                    
                    with lock_pos:
                        if symbol in active_positions:
                            active_positions[symbol]["peak_price"] = peak
                    
                    total_pnl = (c_curr - bp) / bp
                    drop_from_peak = (peak - c_curr) / peak
                    
                    strat = pos.get("strategy", "")
                    sold_half = pos.get("sold_half", False)
                    struc_sl = pos.get("structural_sl")

                    # Hardcoded dynamika: ATR można aproksymować jako stały np. 1.5% - 2% dla oszczędności RAM
                    dynamic_sl_pct = 0.02
                    trailing_drop_pct = 0.012

                    if SENTIMENT_STATE == "BULLISH": tp_ratio = 0.25
                    elif SENTIMENT_STATE == "BEARISH": tp_ratio = 0.50
                    else: tp_ratio = 0.33

                    # Częściowy TP realizujemy Limit Orderem (Post-Only)
                    if not sold_half and total_pnl >= PARTIAL_TP_PCT:
                        rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                        if rb > 0:
                            st, mq = get_symbol_lot_filter(symbol)
                            qs = format_qty_with_step(rb * tp_ratio, st, mq)
                            if qs:
                                ticker = session.get_tickers(category=CATEGORY, symbol=symbol)
                                ask_p = safe_float(ticker.get("result", {}).get("list", [{}])[0].get("ask1Price", c_curr))
                                session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Limit", qty=qs, price=str(ask_p), timeInForce="PostOnly")
                                
                                with lock_pos:
                                    if symbol in active_positions:
                                        active_positions[symbol]["sold_half"] = True
                                        save_positions(active_positions)
                                logging.info(f"💸 [PARTIAL TP] {symbol} LIMIT ORDER po {ask_p}. PnL: +{total_pnl*100:.2f}%")

                    # Wyjścia Awaryjne Marketem
                    sl_hit = struc_sl and c_curr <= struc_sl
                    be_hit = (sold_half or (peak - bp)/bp >= BREAKEVEN_TRIGGER_PCT) and c_curr <= bp * (1.0 + BREAKEVEN_SL_PCT)
                    dyn_hit = total_pnl <= -dynamic_sl_pct
                    trail_hit = drop_from_peak >= trailing_drop_pct and c_curr > bp
                    panic_hit = btc_flash_crash and total_pnl < -0.005

                    if sl_hit or be_hit or dyn_hit or trail_hit or panic_hit:
                        reason = "BTC Flash Crash" if panic_hit else ("Trailing Stop" if trail_hit else "Stop Loss / BreakEven")
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
                except Exception as e:
                    pass

            if symbols_to_del:
                with lock_pos:
                    for s in symbols_to_del:
                        if s in active_positions: del active_positions[s]
                    save_positions(active_positions)

        except Exception:
            pass
        time.sleep(3) # Szybka pętla asynchroniczna

# ==============================================================================
# PĘTLA GŁÓWNA (WOLNA)
# ==============================================================================
def auto_liquidate_stables_to_btc():
    """Wykrywa pozycje/salda USDT i EURC, sprzedaje na Spot i kupuje BTC."""
    global active_positions
    for coin, sym in [("USDT", "USDTUSDC"), ("EURC", "EURCUSDC")]:
        try:
            bal = get_coin_available_balance(coin)
            if (bal * 1.0 >= MIN_EXCHANGE_NOTIONAL) or (sym in active_positions):
                logging.info(f"🧹 [CLEANUP] Zamieniam {bal} {coin} na BTCUSDC...")
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
                            logging.info(f"✅ Kupiono BTC za {btc_val} USDC z odzysku stablecoinów.")
        except: pass

def bot_loop():
    global last_hourly_cost_report, kline_cache_15m
    sync_positions_from_bybit()
    auto_liquidate_stables_to_btc()
    last_hourly_cost_report = time.time()

    # Odpal wątek monitorujący w tle
    t_mon = threading.Thread(target=monitoring_worker, daemon=True)
    t_mon.start()

    while True:
        try:
            kline_cache_15m.clear() # Czyszczenie pamięci przed nowym cyklem skanowania

            logging.info("--------------------------------------------------------------------------------")
            logging.info("🚀 NOWY CYKL SKANOWANIA RYNKU (Skaner w tle, Monitor aktywny 24/7)...")

            current_sentiment = update_market_sentiment()
            total_equity, free_usdc = get_balances()

            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            t_stats = trade_history.get("daily", {}).get(today_str, {"pnl_usd": 0.0, "trades": 0})
            
            can_buy, drop_peak, peak_eq = check_and_update_circuit_breakers(total_equity, current_sentiment)
            usdc_ok, usdc_stat = check_usdc_market_dependencies()

            start_eq = safe_float(trade_history.get("equity_baseline", {}).get("starting_equity"), total_equity)
            diff = total_equity - start_eq

            logging.info(f"📈 [BILANS DNIA] Netto: {t_stats['pnl_usd']:+.2f}$ ({t_stats['trades']} trade) | Portfel: {diff:+.2f}$")
            logging.info(f"🛡️ [DRAWDOWN] Szczyt: {peak_eq:.2f}$ | Spadek: -{drop_peak:.2f}/{MAX_TRAILING_EQUITY_DROP_USD}$")
            
            with lock_pos:
                act_cnt = len(active_positions)
                
            logging.info(f"💰 [PORTFEL] Wartość: {total_equity:.2f}$ | Wolne: {free_usdc:.2f}$ | Pozycje: {act_cnt}/{MAX_TOTAL_POSITIONS}")

            if time.time() - last_hourly_cost_report >= 3600:
                report_hourly_costs(total_equity)

            sync_positions_from_bybit()
            auto_liquidate_stables_to_btc()

            if can_buy and is_btc_market_healthy() and usdc_ok:
                top_vols = get_top_volume_symbols()
                if top_vols:
                    run_support_bounce_strategy(top_vols)
                    run_mtf_alignment_strategy(top_vols)
                    scan_top150_for_ema_signals(top_vols)
                
                run_top_gainers_strategy()
                # Pomijam dla czytelności mniejsze moduły (Pumps, Dips, PEPE)
            else:
                if not can_buy: logging.info("⛔ [ZAKUPY ZABLOKOWANE] Ochrona kapitału/Drawdown aktywna.")
                elif not usdc_ok: logging.info(f"⛔ [ZAKUPY ZABLOKOWANE] Problem USDC: {usdc_stat}")

            logging.info("💤 SKAN ZAKOŃCZONY. Monitor działa w tle. Następny skan za 60 sek...")
        except Exception as e:
            logging.error(f"Błąd głównej pętli: {e}")
            
        time.sleep(60)

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
        self.wfile.write(b"Bybit Spot Trading Bot is Running 24/7 (V8: Async Monitor & Flow Engine)!")
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
