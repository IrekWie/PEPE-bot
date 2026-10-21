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

socket.setdefaulttimeout(10)

# ==============================================================================
# KONFIGURACJA SCALPERA MULTI-COIN SPOT (TYLKO DLA HOSSY)
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

CATEGORY = "spot"
QUOTE_COIN = "USDC"
INTERVAL = "1"

# --- ZAAWANSOWANE PARAMETRY DLA KAŻDEJ MONETY ---
SCALP_PARAMS = {
    "SOLUSDC": {
        "budget": 80.0,
        "tp": 0.008,          # Take Profit: +0.80%
        "sl": 0.005,          # Stop Loss: -0.50%
        "be_trigger": 0.0035, # Aktywacja Breakeven: +0.35%
        "be_lock": 0.0010,    # Poziom Breakeven: +0.10%
        "max_hold": 480       # Time-Stop: 8 minut
    },
    "PEPEUSDC": {
        "budget": 80.0,
        "tp": 0.012,          # Take Profit: +1.20%
        "sl": 0.008,          # Stop Loss: -0.80%
        "be_trigger": 0.0050, # Aktywacja Breakeven: +0.50%
        "be_lock": 0.0015,    # Poziom Breakeven: +0.15%
        "max_hold": 600       # Time-Stop: 10 minut
    }
}

SCALP_SYMBOLS = list(SCALP_PARAMS.keys())

RSI_OVERSOLD = 25                # Poziom wyprzedania RSI 7 na 1M
POLL_INTERVAL_SEC = 2            # Szybki cykl odpytywania rynku (co 2 sekundy)
HEARTBEAT_INTERVAL_SEC = 60      # Częstotliwość raportu w logach (co 60s)
SENTIMENT_CHECK_INTERVAL = 300   # Weryfikacja sentymentu BTC 4H co 5 minut

SCALP_APP_URL = "https://bybit-scalper-d4xb.onrender.com"
SCALP_POS_FILE = "scalp_active_pos.json"
SCALP_HIST_FILE = "scalp_trade_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCALPER] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ==============================================================================
# INICJALIZACJA POŁĄCZENIA BYBIT
# ==============================================================================
session = HTTP(
    testnet=False,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=15000
)
session.endpoint = "https://api.bybit.eu"

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and str(val).strip() != "" else default
    except (ValueError, TypeError):
        return default

# ==============================================================================
# PRECYZJA ZLECEŃ DLA RYNKU SPOT
# ==============================================================================
instrument_cache = {}

def get_symbol_precision(symbol):
    if symbol in instrument_cache:
        return instrument_cache[symbol]
    try:
        res = session.get_instruments_info(category=CATEGORY, symbol=symbol)
        if res.get("retCode") == 0:
            item = res["result"]["list"][0]
            lot_filter = item.get("lotSizeFilter", {})
            base_prec = lot_filter.get("basePrecision")
            qty_step = lot_filter.get("qtyStep")
            min_qty = lot_filter.get("minOrderQty", "0.001")
            step = base_prec if base_prec else (qty_step if qty_step else "0.001")
            instrument_cache[symbol] = (step, min_qty)
            return step, min_qty
    except Exception:
        pass
    return "0.001", "0.001"

def format_qty(raw_qty, step_str, min_str):
    try:
        q_dec = Decimal(str(raw_qty))
        s_dec = Decimal(str(step_str))
        m_dec = Decimal(str(min_str))
        floored = (q_dec // s_dec) * s_dec if s_dec > 0 else q_dec
        if floored < m_dec:
            return None
        norm = s_dec.normalize()
        decimals = abs(norm.as_tuple().exponent) if norm.as_tuple().exponent < 0 else 0
        return f"{floored:.{decimals}f}" if decimals > 0 else f"{int(floored)}"
    except Exception:
        return None

# ==============================================================================
# ZARZĄDZANIE PAMIĘCIĄ I REJESTR TRANSAKCJI
# ==============================================================================
def load_positions():
    if os.path.exists(SCALP_POS_FILE):
        try:
            with open(SCALP_POS_FILE, "r") as f:
                data = json.load(f)
                if "buy_price" in data and "symbol" in data:
                    return {data["symbol"]: data}
                return data if isinstance(data, dict) else {}
        except Exception:
            pass
    return {}

def save_positions(pos_dict):
    try:
        with open(SCALP_POS_FILE, "w") as f:
            json.dump(pos_dict, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu pozycji scalpera: {e}")

def load_trade_history():
    if os.path.exists(SCALP_HIST_FILE):
        try:
            with open(SCALP_HIST_FILE, "r") as f:
                data = json.load(f)
                if "trades" not in data:
                    data["trades"] = []
                if "daily" not in data:
                    data["daily"] = {}
                return data
        except Exception:
            pass
    return {"trades": [], "daily": {}}

def save_trade_history(hist):
    try:
        with open(SCALP_HIST_FILE, "w") as f:
            json.dump(hist, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu historii transakcji scalpera: {e}")

scalp_trade_history = load_trade_history()

def record_trade(symbol, buy_price, sell_price, qty, reason):
    global scalp_trade_history
    raw_pnl_usd = (sell_price - buy_price) * qty
    turnover = (buy_price + sell_price) * qty
    estimated_fee = turnover * 0.001  # Bybit Spot Taker ~0.1% kupno + ~0.1% sprzedaż
    net_pnl_usd = round(raw_pnl_usd - estimated_fee, 4)
    net_pnl_pct = (net_pnl_usd / (buy_price * qty)) if (buy_price * qty) > 0 else 0.0

    now_ts = time.time()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    scalp_trade_history["trades"].append({
        "timestamp": now_ts,
        "date_str": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "buy": buy_price,
        "sell": sell_price,
        "qty": qty,
        "turnover": round(turnover, 2),
        "fee": round(estimated_fee, 4),
        "gross_pnl": round(raw_pnl_usd, 4),
        "net_pnl": net_pnl_usd,
        "reason": reason
    })

    # Ograniczenie pamięci do ostatnich 200 mikro-transakcji
    if len(scalp_trade_history["trades"]) > 200:
        scalp_trade_history["trades"] = scalp_trade_history["trades"][-200:]

    if "daily" not in scalp_trade_history:
        scalp_trade_history["daily"] = {}
    if today_str not in scalp_trade_history["daily"]:
        scalp_trade_history["daily"][today_str] = {
            "trades": 0,
            "total_turnover": 0.0,
            "total_fees": 0.0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0
        }

    d = scalp_trade_history["daily"][today_str]
    d["trades"] = d.get("trades", 0) + 1
    d["total_turnover"] = round(d.get("total_turnover", 0.0) + turnover, 2)
    d["total_fees"] = round(d.get("total_fees", 0.0) + estimated_fee, 4)
    d["gross_pnl"] = round(d.get("gross_pnl", 0.0) + raw_pnl_usd, 4)
    d["net_pnl"] = round(d.get("net_pnl", 0.0) + net_pnl_usd, 4)

    save_trade_history(scalp_trade_history)

    sign = "+" if net_pnl_usd >= 0 else ""
    logging.info(f"📊 [BILANS SCALP] {symbol} | Netto: {sign}{net_pnl_usd:.4f} USD ({sign}{net_pnl_pct*100:.2f}%) | Fee: -{estimated_fee:.4f}$ | Powód: {reason}")

# ==============================================================================
# MODUŁ GODZINOWEJ ANALIZY KOSZTÓW I PROWIZJI SCALPERA
# ==============================================================================
last_hourly_cost_report = 0.0

def report_hourly_costs(total_equity):
    """Generuje co godzinę raport prowizji giełdowych, obrotu i efektywności scalpera."""
    global last_hourly_cost_report, scalp_trade_history
    now_ts = time.time()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    trades_log = scalp_trade_history.get("trades", [])
    one_hour_ago = now_ts - 3600

    recent_trades = [t for t in trades_log if t.get("timestamp", 0) >= one_hour_ago]

    h_trades_count = len(recent_trades)
    h_turnover = sum(t.get("turnover", 0.0) for t in recent_trades)
    h_fees = sum(t.get("fee", 0.0) for t in recent_trades)
    h_gross = sum(t.get("gross_pnl", 0.0) for t in recent_trades)
    h_net = sum(t.get("net_pnl", 0.0) for t in recent_trades)

    today_stats = scalp_trade_history.get("daily", {}).get(today_str, {})
    d_fees = today_stats.get("total_fees", 0.0)
    d_turnover = today_stats.get("total_turnover", 0.0)
    d_trades = today_stats.get("trades", 0)
    d_net = today_stats.get("net_pnl", 0.0)

    avg_fee_per_trade = (d_fees / d_trades) if d_trades > 0 else 0.0
    fee_drag_pct = ((d_fees / total_equity) * 100) if total_equity > 0 else 0.0

    sign_gross = "+" if h_gross >= 0 else ""
    sign_net = "+" if h_net >= 0 else ""
    sign_d_net = "+" if d_net >= 0 else ""

    logging.info("================================================================================")
    logging.info("💸 [SCALPER: GODZINNY RAPORT KOSZTÓW I PROWIZJI GIEŁDOWYCH]")
    logging.info("--------------------------------------------------------------------------------")
    logging.info("⏱️ Ostatnie 60 minut:")
    logging.info(f"   • Zamknięte skalpy: {h_trades_count} | Obrót: {h_turnover:.2f} USD")
    logging.info(f"   • Prowizje Bybit (Taker 0.1%): -{h_fees:.4f} USD")
    logging.info(f"   • Wynik Brutto: {sign_gross}{h_gross:.4f} USD | Wynik Netto: {sign_net}{h_net:.4f} USD")
    logging.info("📅 Podsumowanie dzisiejsze (od 00:00 UTC):")
    logging.info(f"   • Suma prowizji oddanych giełdzie: -{d_fees:.4f} USD ({fee_drag_pct:.2f}% wartości portfela)")
    logging.info(f"   • Łączny obrót dzienny scalpera: {d_turnover:.2f} USD ({d_trades} transakcji)")
    logging.info(f"   • Średni koszt prowizji na transakcję: -{avg_fee_per_trade:.4f} USD")
    logging.info(f"   • Bilans dzienny netto scalpera: {sign_d_net}{d_net:.4f} USD")
    logging.info("================================================================================")

    last_hourly_cost_report = now_ts

# ==============================================================================
# SALDO I DANE RYNKOWE
# ==============================================================================
def get_wallet_metrics():
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0:
            account_data = res["result"]["list"][0]
            total_equity = safe_float(account_data.get("totalEquity"))
            free_usdc = 0.0
            for coin in account_data.get("coin", []):
                if coin.get("coin") == QUOTE_COIN:
                    avail = coin.get("availableToWithdraw")
                    eq = coin.get("equity")
                    free_usdc = safe_float(avail) if avail else safe_float(eq)
                    break
            return total_equity, free_usdc
    except Exception:
        pass
    return 0.0, 0.0

def get_coin_balance(coin_name):
    try:
        res = session.get_wallet_balance(accountType="UNIFIED", coin=coin_name)
        if res.get("retCode") == 0:
            for coin in res["result"]["list"][0].get("coin", []):
                if coin.get("coin") == coin_name:
                    wb = safe_float(coin.get("walletBalance"))
                    avail = safe_float(coin.get("availableToWithdraw"))
                    return wb if wb > 0 else avail
    except Exception:
        pass
    return 0.0

def get_1m_candles(symbol):
    try:
        res = session.get_kline(category=CATEGORY, symbol=symbol, interval=INTERVAL, limit=50)
        candles = res.get("result", {}).get("list", [])
        if not candles or len(candles) < 30:
            return None
        df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(7).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(7).mean()
        rs = gain / loss
        df['rsi7'] = 100 - (100 / (1 + rs))

        df['bb_mid'] = df['close'].rolling(20).mean()
        df['bb_std'] = df['close'].rolling(20).std()
        df['bb_lower'] = df['bb_mid'] - (2.0 * df['bb_std'])
        df['bb_upper'] = df['bb_mid'] + (2.0 * df['bb_std'])
        return df
    except Exception as e:
        logging.error(f"Błąd pobierania świec 1M dla {symbol}: {e}")
        return None

# ==============================================================================
# FILTR MAKRO-HOSSY DLA SCALPERA (BTC 4H)
# ==============================================================================
cached_sentiment = "UNKNOWN"
last_sentiment_check = 0.0

def update_macro_sentiment():
    """Weryfikuje, czy rynek jest w stanie HOSSY (BULLISH) na BTC 4H."""
    global cached_sentiment, last_sentiment_check
    now = time.time()
    if now - last_sentiment_check < SENTIMENT_CHECK_INTERVAL and cached_sentiment != "UNKNOWN":
        return cached_sentiment

    try:
        res = session.get_kline(category=CATEGORY, symbol="BTCUSDC", interval="240", limit=100)
        candles = res.get("result", {}).get("list", [])
        if candles and len(candles) >= 55:
            df = pd.DataFrame(candles, columns=["startTime", "open", "high", "low", "close", "volume", "turnover"])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ["open", "high", "low", "close"]:
                df[col] = df[col].astype(float)

            df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
            df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
            df['ema89'] = df['close'].ewm(span=89, adjust=False).mean()

            c_close = df.iloc[-1]['close']
            ema21 = df.iloc[-1]['ema21']
            ema50 = df.iloc[-1]['ema50']
            ema89 = df.iloc[-1]['ema89']

            if c_close > ema50 and ema21 > ema50:
                cached_sentiment = "BULLISH"
            elif c_close < ema89 or ema21 < ema89:
                cached_sentiment = "BEARISH"
            else:
                cached_sentiment = "NEUTRAL"

            last_sentiment_check = now
            return cached_sentiment
    except Exception as e:
        logging.warning(f"Błąd sprawdzania makro sentymentu BTC dla scalpera: {e}")

    last_sentiment_check = now
    return cached_sentiment

# ==============================================================================
# SILNIK SCALPINGOWY
# ==============================================================================
active_scalps = load_positions()

def scalper_loop():
    global active_scalps, last_hourly_cost_report
    logging.info(f"⚡ Start Scalpera MULTI-COIN: {SCALP_SYMBOLS} (Warunek startu: HOSSA / BULLISH)...")
    last_heartbeat = 0.0
    last_hourly_cost_report = time.time()

    while True:
        try:
            now = time.time()
            do_heartbeat = False
            
            # Weryfikacja sentymentu BTC 4H
            current_sentiment = update_macro_sentiment()
            is_bullish = (current_sentiment == "BULLISH")
            total_equity, free_usdc = get_wallet_metrics()

            # Globalny Raport pulsu co 60 sekund
            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                mode_str = "AKTYWNY (HOSSA 🟢)" if is_bullish else f"UŚPIONY (Czekam na hossę | Stan BTC: {current_sentiment})"
                logging.info(f"💓 --- RAPORT SCALPERA | Tryb: {mode_str} | Wolne środki: {free_usdc:.2f} USDC ---")
                do_heartbeat = True
                last_heartbeat = now

            # GODZINNY RAPORT KOSZTÓW I PROWIZJI (Co 3600 sekund)
            if now - last_hourly_cost_report >= 3600:
                report_hourly_costs(total_equity)

            for symbol in SCALP_SYMBOLS:
                df = get_1m_candles(symbol)
                if df is None:
                    continue

                current_candle = df.iloc[-1]
                current_price = current_candle['close']
                rsi7 = current_candle['rsi7']
                params = SCALP_PARAMS[symbol]
                
                pos = active_scalps.get(symbol)
                
                if do_heartbeat:
                    if pos:
                        pnl_now = ((current_price - pos['buy_price']) / pos['buy_price']) * 100
                        status_str = f"POZYCJA OTWARTA (PnL: {pnl_now:+.2f}%)"
                    else:
                        status_str = "Szukam wejścia" if is_bullish else "Blokada zakupów (Poza hossą)"
                    logging.info(f"  > [1M] {symbol}: {current_price} USD | RSI(7): {rsi7:.1f} | Stan: {status_str}")

                # FAZA 1: ZARZĄDZANIE OTWARTĄ POZYCJĄ (Działa ZAWSZE, nawet po utracie hossy)
                if pos:
                    buy_price = pos["buy_price"]
                    peak_price = max(pos.get("peak_price", buy_price), current_price)
                    pos["peak_price"] = peak_price
                    
                    pnl = (current_price - buy_price) / buy_price
                    peak_pnl = (peak_price - buy_price) / buy_price
                    elapsed_time = now - pos["entry_timestamp"]

                    hit_tp = pnl >= params["tp"] or (current_price >= current_candle['bb_upper'] and pnl >= 0.005)
                    hit_be = peak_pnl >= params["be_trigger"] and current_price <= (buy_price * (1.0 + params["be_lock"]))
                    hit_sl = pnl <= -params["sl"]
                    hit_time_stop = elapsed_time >= params["max_hold"] and pnl < 0.003

                    if hit_tp or hit_be or hit_sl or hit_time_stop:
                        reason = f"Take Profit (+{params['tp']*100:.1f}%)" if hit_tp else ("Breakeven Lock" if hit_be else (f"Stop Loss (-{params['sl']*100:.1f}%)" if hit_sl else f"Time-Stop ({params['max_hold']//60} min)"))
                        base_coin = symbol.replace(QUOTE_COIN, "")
                        real_bal = get_coin_balance(base_coin)

                        if real_bal > 0:
                            step, min_qty = get_symbol_precision(symbol)
                            qty_str = format_qty(real_bal, step, min_qty)
                            if qty_str:
                                res = session.place_order(category=CATEGORY, symbol=symbol, side="Sell", orderType="Market", qty=qty_str)
                                if res.get("retCode") == 0:
                                    record_trade(symbol, buy_price, current_price, safe_float(qty_str), reason)
                                    logging.info(f"🎯 [WYJŚCIE SCALP] {symbol} | {reason} | Cena: {current_price} | PnL: {pnl*100:.2f}%")
                                    del active_scalps[symbol]
                                    save_positions(active_scalps)
                        else:
                            del active_scalps[symbol]
                            save_positions(active_scalps)
                    else:
                        save_positions(active_scalps)

                # FAZA 2: POSZUKIWANIE MIKRO-WEJŚCIA (WYŁĄCZNIE W HOSSIE)
                else:
                    if not is_bullish:
                        continue

                    c_open = current_candle['open']
                    c_low = current_candle['low']
                    bb_lower = current_candle['bb_lower']
                    
                    touched_bb = c_low <= (bb_lower * 1.0005)
                    oversold = rsi7 <= RSI_OVERSOLD
                    rebounding = (current_price > c_open) or ((min(c_open, current_price) - c_low) > abs(current_price - c_open) * 1.5)

                    if touched_bb and oversold and rebounding:
                        if free_usdc >= params["budget"]:
                            step, min_qty = get_symbol_precision(symbol)
                            order_qty_coin = format_qty(params["budget"] / current_price, step, min_qty)
                            
                            if order_qty_coin:
                                res = session.place_order(category=CATEGORY, symbol=symbol, side="Buy", orderType="Market", qty=f"{params['budget']:.2f}")
                                if res.get("retCode") == 0:
                                    active_scalps[symbol] = {
                                        "symbol": symbol,
                                        "buy_price": current_price,
                                        "peak_price": current_price,
                                        "qty": order_qty_coin,
                                        "entry_timestamp": now
                                    }
                                    save_positions(active_scalps)
                                    logging.info(f"⚡ [WEJŚCIE SCALP - HOSSA] {symbol} po {current_price} USD | RSI7: {rsi7:.1f} | Budżet: {params['budget']}$")

        except Exception as e:
            logging.error(f"Błąd w pętli scalpera: {e}")

        time.sleep(POLL_INTERVAL_SEC)

# ==============================================================================
# KEEP-ALIVE SERVER ORAZ SELF-PING LOOP (RENDER COMPATIBILITY 24/7)
# ==============================================================================
def self_ping_loop():
    time.sleep(15)
    while True:
        try:
            requests.get(SCALP_APP_URL, timeout=10)
            logging.info("PING: Scalper aktywny (Render Keep-Alive 24/7 OK)")
        except Exception:
            pass
        time.sleep(240)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bybit Scalper Bot is Running 24/7 (Bullish Filter & Fee Tracker Active)!")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    httpd = HTTPServer(('', port), HealthHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    t_scalp = threading.Thread(target=scalper_loop, daemon=True)
    t_scalp.start()

    t_ping = threading.Thread(target=self_ping_loop, daemon=True)
    t_ping.start()

    run_health_server()
