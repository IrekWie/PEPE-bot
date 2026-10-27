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

# --- KOSZYK 20 WYSELEKCJONOWANYCH MONET SPOT USDC ---
SNIPER_COINS = [
    "SOLUSDC", "SUIUSDC", "NEARUSDC", "DOGEUSDC", "PEPEUSDC",
    "APTUSDC", "AVAXUSDC", "SEIUSDC", "WIFUSDC", "BONKUSDC",
    "RENDERUSDC", "LINKUSDC", "INJUSDC", "TIAUSDC", "FETUSDC",
    "ONDOUSDC", "XRPUSDC", "ENAUSDC", "ARBUSDC", "ADAUSDC"
]
CAPITAL_PER_TRADE_PCT = 0.20         # 20% kapitału na transakcję
MAX_TOTAL_POSITIONS = 5              # Maksymalnie 5 otwartych pozycji (5 x 20% = 100%)

STABLE_PAIRS_BLACKLIST = [
    "USDTUSDC", "USDCUSDT", "EURCUSDC", "USDTEUR", "EURCUSDT", 
    "USDEUSDC", "DAIUSDC", "FDUSDUSDC", "TUSDUSDC"
]

# --- LIMITY FINANSOWE I REZERWY ---
STANDARD_MIN_ORDER_VALUE = 20.0
MIN_SYNC_POSITION_VALUE = 8.0     
MIN_EXCHANGE_NOTIONAL = 5.0       
MIN_CASH_RESERVE_PCT = 0.02          # 2% rezerwy na fee giełdowe

# --- BEZPIECZNIKI RYNKOWE ---
USDC_MAX_SPREAD_PCT = 0.0010         # Max spread rynkowy 0.10%
USDC_1M_DUMP_THRESHOLD = -0.005      # Szybki zrzut BTCUSDC 1M (-0.5%) blokuje zakupy
BTC_FAST_DROP_BUY_BLOCK_PCT = -0.012 # Zrzut BTC 5M > -1.2% blokuje zakupy
BTC_FLASH_CRASH_EXIT_PCT    = -0.015 # Paniczny zrzut BTC 15M > -1.5%

CURRENT_SENTIMENT = "NEUTRAL 🟡"

# --- PARAMETRY RYZYKA I REALIZACJI ZYSKU ---
PARTIAL_TP_PCT = 0.025               # Częściowy TP: 33% przy +2.5%
BREAKEVEN_TRIGGER_PCT = 0.015        # Próg aktywacji BreakEven przy +1.5%
BREAKEVEN_SL_PCT = 0.003             # Poziom SL BreakEven (+0.3% zysku - pokrywa fee)
PEAK_TRAIL_TRIGGER_PCT = 0.020       # Ochrona ze szczytu: aktywacja powyżej +2.0%
PEAK_TRAIL_PULLBACK_PCT = 0.010      # Zamknięcie przy cofnięciu 1.0% od szczytu

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
                                recent_low = df['low'].iloc[-4:-1].min() if len(df) >= 4 else buy_p * 0.98
                                initial_sl = recent_low * 0.997
                                active_positions[sym] = {
                                    "strategy": "BOTTOM_SNIPER",
                                    "buy_price": buy_p,
                                    "peak_price": max(cp, buy_p),
                                    "qty": q_str,
                                    "sold_half": False,
                                    "trailing_sl_low": initial_sl
                                }
                                logging.info(f"🎯 [SYNC / RĘCZNY] Przejęto {sym} ({val_usd:.2f}$) pod BOTTOM_SNIPER | SL: {initial_sl:.4f}")
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

def get_market_data_no_cache(symbol, interval="15", limit=120):
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
        df['rsi'] = calculate_rsi(df['close'], period=14)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        df['bb_mid'] = df['close'].rolling(window=20).mean()
        df['bb_std'] = df['close'].rolling(window=20).std()
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        return df
    except Exception:
        return None

def fetch_data_worker(symbol):
    return symbol, get_market_data_no_cache(symbol, interval="15", limit=120)

def get_market_data(symbol, interval="15", limit=120):
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

        logging.info(f"📊 [MAKRO TREND 4H] BTC: {btc_sentiment} | ETH: {eth_sentiment} | Strategia: JEDNOLITY BOTTOM SNIPER (20 MONET)")
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
# GŁÓWNA I JEDYNA STRATEGIA: ZINTEGROWANY BOTTOM SNIPER (SEKWENCJA DNO -> SPUST)
# ==============================================================================
def run_bottom_sniper_strategy():
    global active_positions, kline_cache_15m, trade_history
    total_equity, free_usdc = get_balances()
    target_allocation = total_equity * CAPITAL_PER_TRADE_PCT

    with lock_pos: current_count = len(active_positions)
    if current_count >= MAX_TOTAL_POSITIONS:
        logging.info(f"ℹ️ [LIMIT POZYCJI] Osiągnięto limit {current_count}/{MAX_TOTAL_POSITIONS}. Czekam na realizację zysków.")
        return

    logging.info(f"🎯 [BOTTOM SNIPER 15M] Skanowanie {len(SNIPER_COINS)} par | Alokacja: {target_allocation:.2f}$ (20%) | Wolne: {free_usdc:.2f}$")

    # Pobieranie równoległe (8 wątków - czas ~300ms)
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_data_worker, symbol): symbol for symbol in SNIPER_COINS}
        for future in concurrent.futures.as_completed(futures):
            sym, df = future.result()
            if df is not None:
                results[sym] = df
                with kline_cache_lock: kline_cache_15m[sym] = df

    now_ts = time.time()

    for symbol in SNIPER_COINS:
        with lock_pos: is_active = (symbol in active_positions)
        if is_active: continue

        df = results.get(symbol)
        if df is None or len(df) < 50:
            continue

        try:
            c_now = df.iloc[-1]   # Świeca bieżąca (w locie)
            c = df.iloc[-2]       # Ostatnia zamknięta świeca 15M
            p = df.iloc[-3]
            p2 = df.iloc[-4]
            p3 = df.iloc[-5]

            # ------------------------------------------------------------------
            # KROK 1: FILTR DNA / SCHŁODZENIA (ANTI-TOP GUARD)
            # Moneta MUSI być lokalnie tania i po schłodzeniu, a NIE na szczycie
            # ------------------------------------------------------------------
            recent_min_rsi = min(p3['rsi'], p2['rsi'], p['rsi'], c['rsi'])
            was_cooled_down = recent_min_rsi <= 44.0 or (min(p['low'], c['low']) <= c['bb_lower'] * 1.005)
            is_not_overbought = (c['rsi'] <= 65.0) and (c_now['rsi'] <= 68.0)
            not_extended = ((c['close'] - c['ema9']) / c['ema9']) <= 0.012

            # ------------------------------------------------------------------
            # KROK 2: SPUST EGZEKUCJI (CONFIRMED BOUNCE / V-REVERSAL)
            # ------------------------------------------------------------------
            vol_ratio = (c['volume'] / c['vol_ma20']) if (c['vol_ma20'] and c['vol_ma20'] > 0) else 1.0
            
            # TRIGGER A: Klasyczne odwrócenie przez krzyż EMA 9/21
            fast_cross_closed = ((p['ema9'] <= p['ema21'] and c['ema9'] > c['ema21']) or \
                                 (p2['ema9'] <= p2['ema21'] and c['ema9'] > c['ema21'])) and \
                                (c['close'] > c['open']) and (c['close'] >= c['ema9']) and (vol_ratio >= 1.05)

            cross_in_flight = (c['ema9'] <= c['ema21']) and (c_now['ema9'] > c_now['ema21'])
            spread_now = (c_now['ema9'] - c_now['ema21']) / c_now['ema21']
            fast_cross_live = cross_in_flight and (spread_now >= 0.0010) and (c_now['close'] > c_now['open']) and (c_now['close'] > c_now['ema9'])

            # TRIGGER B: Formacja "V-Reversal" ze skokiem wolumenu (Złapanie dna Price Action)
            is_dump_candle = (p['close'] < p['open']) and ((p['open'] - p['close']) / p['open'] >= 0.0025)
            is_rebound_candle = (c['close'] > c['open'])
            rebound_strength = (c['close'] - p['close']) / (p['open'] - p['close']) if (p['open'] > p['close']) else 0.0
            is_engulfing = rebound_strength >= 0.70  # Zamknięcie zielonej znosi min. 70% spadku poprzedniej
            vol_spike = (c['volume'] >= (c['vol_ma20'] * 1.15)) or (p['volume'] >= (p['vol_ma20'] * 1.15))
            
            v_reversal_signal = is_dump_candle and is_rebound_candle and is_engulfing and vol_spike

            # ------------------------------------------------------------------
            # KROK 3: COOLDOWN PO STOP LOSS (CHRONI PRZED SPADAJĄCYM NOŻEM)
            # ------------------------------------------------------------------
            cooldown_ok = True
            with lock_history:
                trades_log = trade_history.get("trades_log", [])
                for t in reversed(trades_log):
                    if t.get("symbol") == symbol:
                        if now_ts - t.get("timestamp", 0) < 600:  # 10 minut przerwy po SL
                            cooldown_ok = False
                        break

            # Weryfikacja sygnału ostatecznego
            is_signal = was_cooled_down and is_not_overbought and not_extended and cooldown_ok and (fast_cross_closed or fast_cross_live or v_reversal_signal)

            # Prezentacja logów
            spread_disp = spread_now if fast_cross_live else ((c['ema9'] - c['ema21']) / c['ema21'])
            dec = 8 if c['ema9'] < 0.01 else 4
            spread_tag = f"🟢 +{spread_disp*100:.2f}%" if spread_disp >= 0 else f"🔴 {spread_disp*100:.2f}%"
            vol_tag = f"Vol: {vol_ratio:.2f}x ({'🟢' if vol_ratio >= 1.05 or vol_spike else '🔴'})"
            rsi_tag = f"RSI: {c['rsi']:.1f} (Min4: {recent_min_rsi:.1f})"
            log_msg = f"🔍 [{symbol}] EMA9: {c['ema9']:.{dec}f} | EMA21: {c['ema21']:.{dec}f} | {spread_tag} | {vol_tag} | {rsi_tag} | "

            if is_signal:
                if v_reversal_signal and not fast_cross_closed and not fast_cross_live:
                    mode = "V-REVERSAL PA 📉⬆️"
                else:
                    mode = "W LOCIE ⚡" if fast_cross_live else "POTWIERDZONE 15M 🕯️"
                logging.info(log_msg + f"🚀 [SNIPER DNA: {mode}] Zakup w dołku!")
            else:
                logging.info(log_msg + f"Dno OK: {was_cooled_down} | Cross 9/21: {fast_cross_closed or fast_cross_live} | V-Reversal: {v_reversal_signal}")

            # EGZEKUCJA TRANSAKCJI
            if is_signal:
                if free_usdc < (target_allocation * 0.99):
                    logging.warning(f"⏳ [{symbol}] Sygnał zakupu na dnie, ale brak wolnego USDC ({free_usdc:.2f}$ < {target_allocation:.2f}$).")
                    continue

                # Ciasny Stop Loss tuż pod lokalnym dołkiem zwrotnym (ryzyko zazwyczaj poniżej 1%)
                swing_low = min(c['low'], p['low'], c_now['low'])
                initial_sl = swing_low * 0.997

                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{target_allocation:.2f}")
                if res.get("retCode") == 0:
                    curr_price = df.iloc[-1]['close']
                    step, minq = get_symbol_lot_filter(symbol)
                    q_str = format_qty_with_step(target_allocation / curr_price, step, minq)
                    with lock_pos:
                        active_positions[symbol] = {
                            "strategy": "BOTTOM_SNIPER", 
                            "buy_price": curr_price, 
                            "peak_price": curr_price, 
                            "qty": q_str or "1", 
                            "sold_half": False,
                            "trailing_sl_low": initial_sl
                        }
                        save_positions(active_positions)
                    p_fmt = f"{curr_price:.8f}" if curr_price < 0.01 else f"{curr_price:.4f}"
                    sl_fmt = f"{initial_sl:.8f}" if initial_sl < 0.01 else f"{initial_sl:.4f}"
                    risk_pct = ((curr_price - initial_sl) / curr_price) * 100
                    logging.info(f"⚡🎯 [SUKCES - KUPNO NA DNIE] {symbol} | Kwota: {target_allocation:.2f}$ | Cena: {p_fmt} | SL: {sl_fmt} (Ryzyko: {risk_pct:.2f}%)")
                    free_usdc -= target_allocation
                else:
                    logging.error(f"❌ Błąd zlecenia {symbol}: {res}")
        except Exception as e:
            logging.error(f"Błąd analizy {symbol}: {e}")

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
                    peak_pnl = (peak - bp) / bp
                    sold_half = pos.get("sold_half", False)

                    sl_hit = False
                    trail_hit = False
                    sl_type = ""

                    # ----------------------------------------------------------
                    # DYNAMICZNA ADAPTACJA PARAMETRÓW RYZYKA (BEARISH vs RESZTA)
                    # ----------------------------------------------------------
                    is_bearish = ("BEARISH" in CURRENT_SENTIMENT)
                    if is_bearish:
                        # Tryb BEARISH: Szybka ucieczka z zyskiem (Hit & Run)
                        eff_partial_tp = 0.020        # Szybszy TP: 33% przy +2.0%
                        eff_be_trigger = 0.010        # BreakEven aktywuje się już przy +1.0%
                        eff_be_sl = 0.003             # SL przestawiony na +0.3% (pokrywa prowizje)
                        eff_peak_trigger = 0.014      # Ochrona ze szczytu czuwa już od +1.4%
                        eff_peak_pullback = 0.006     # Wystarczy -0.6% cofnięcia ze szczytu by zamknąć zysk
                    else:
                        # Tryb STANDARDOWY (BULLISH / NEUTRAL): Szerszy oddech dla pozycji
                        eff_partial_tp = PARTIAL_TP_PCT         # 0.025 (+2.5%)
                        eff_be_trigger = BREAKEVEN_TRIGGER_PCT  # 0.015 (+1.5%)
                        eff_be_sl = BREAKEVEN_SL_PCT            # 0.003 (+0.3%)
                        eff_peak_trigger = PEAK_TRAIL_TRIGGER_PCT     # 0.020 (+2.0%)
                        eff_peak_pullback = PEAK_TRAIL_PULLBACK_PCT   # 0.010 (-1.0%)

                    # ----------------------------------------------------------
                    # 1. CZĘŚCIOWY TAKE PROFIT (33% pozycji zleceniem limit ask)
                    # ----------------------------------------------------------
                    if not sold_half and total_pnl >= eff_partial_tp:
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
                                tp_label = f"{eff_partial_tp*100:.1f}%"
                                mode_tag = "[BEARISH HIT&RUN]" if is_bearish else "[SWING]"
                                logging.info(f"💸 [PARTIAL TP {mode_tag}] {symbol} LIMIT ORDER po {ask_p:.4f} USD (+{tp_label} zrealizowane).")

                    # ----------------------------------------------------------
                    # 2. PRÓG BREAKEVEN (OBRONA PRZED POWROTEM NA STRATĘ)
                    # ----------------------------------------------------------
                    be_level = bp * (1.0 + eff_be_sl)
                    if peak_pnl >= eff_be_trigger or sold_half:
                        curr_sl = max(pos.get("trailing_sl_low", 0.0), be_level)
                    else:
                        curr_sl = pos.get("trailing_sl_low", 0.0)

                    # ----------------------------------------------------------
                    # 3. KROCZĄCY TRAILING ZA WYŻSZYMI DOŁKAMI 15M
                    # ----------------------------------------------------------
                    if len(c_list) >= 4:
                        recent_lows = [safe_float(c_list[i][3]) for i in range(1, 4)]
                        local_swing_low = min(recent_lows) * 0.998
                        
                        if local_swing_low > pos.get("trailing_sl_low", 0.0) and c_curr > bp:
                            with lock_pos:
                                if symbol in active_positions:
                                    active_positions[symbol]["trailing_sl_low"] = local_swing_low
                                    save_positions(active_positions)
                            logging.info(f"📈 [TRAILING SL] Podniesiono SL dla {symbol} na {local_swing_low:.4f} (za wyższym dołkiem).")

                    curr_sl = max(curr_sl, pos.get("trailing_sl_low", 0.0))

                    # Sprawdzenie uderzenia w Stop Loss / BreakEven
                    if curr_sl > 0 and c_curr <= curr_sl:
                        sl_hit = True
                        if curr_sl == be_level:
                            mode_info = "BEARISH Obrona (+0.3%)" if is_bearish else "BreakEven (+0.3%)"
                            sl_type = f"BreakEven ({mode_info})"
                        else:
                            sl_type = f"Wyjście ze Stop Lossa ({curr_sl:.4f})"

                    # ----------------------------------------------------------
                    # 4. KROCZĄCA OCHRONA ZE SZCZYTU (TRAILING FROM PEAK)
                    # ----------------------------------------------------------
                    if peak_pnl >= eff_peak_trigger and ((peak - c_curr) / peak) >= eff_peak_pullback:
                        trail_hit = True
                        p_drop = eff_peak_pullback * 100
                        mode_info = "BEARISH HIT&RUN" if is_bearish else "Peak Trailing"
                        sl_type = f"Ochrona zysku ze szczytu [{mode_info}] (-{p_drop:.1f}% od peaku)"

                    panic_hit = btc_flash_crash and total_pnl < -0.005

                    if sl_hit or trail_hit or panic_hit:
                        reason = "BTC Flash Crash" if panic_hit else (sl_type if sl_type else "Trailing Stop")
                        rb = get_coin_available_balance(symbol.replace(QUOTE_COIN, "").replace("USDT", ""))
                        if rb > 0:
                            st, mq = get_symbol_lot_filter(symbol)
                            qs = format_qty_with_step(rb, st, mq)
                            if qs:
                                res_s = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qs)
                                if res_s.get("retCode") == 0:
                                    record_closed_trade(symbol, "BOTTOM_SNIPER", bp, c_curr, safe_float(qs), reason, is_maker=False)
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
                logging.info(f"🧹 [AUTO-WYMIANA] Wykryto {bal} {coin}. Wymieniam na USDC/BTC...")
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
                logging.info("🚀 NOWY CYKL SKANOWANIA RYNKU (Skan w tle co 10 sekund)...")
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
                        sl_info = f" | SL: {pos.get('trailing_sl_low'):.4f}" if 'trailing_sl_low' in pos else ""
                        logging.info(f"ℹ️ [{symbol}] PnL: {pnl_pct:+.2f}% | Szczyt: {pos.get('peak_price', cp)}{sl_info}")
                    except Exception: pass

            # ==================================================================
            # SKANOWANIE I ZAKUPY - Uruchamiane co 10 sekund
            # ==================================================================
            if is_btc_market_healthy() and usdc_ok:
                run_bottom_sniper_strategy()
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
        self.wfile.write(b"Bybit Spot Trading Bot 24/7 - Unified Bottom Sniper Active (20 Pairs)")
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
