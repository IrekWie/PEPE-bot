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
# KONFIGURACJA SCALPERA SPOT
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

CATEGORY = "spot"
SCALP_SYMBOL = "SOLUSDC"
QUOTE_COIN = "USDC"
INTERVAL = "1"

# --- PARAMETRY WEJŚCIA I WYJŚCIA ---
SCALP_BUDGET_USD = 80.0          # Kwota pojedynczego wejścia (dostosowana do wolnego salda ~93 USDC)
TAKE_PROFIT_PCT = 0.008          # TP: +0.80% (czysty zysk ~0.60% po prowizjach Bybit)
HARD_STOP_LOSS_PCT = 0.005       # Sztywny SL: -0.50% (szybkie ucięcie błędu)
BREAKEVEN_TRIGGER_PCT = 0.0035   # Aktywacja BE po osiągnięciu +0.35% zysku
BREAKEVEN_LOCK_PCT = 0.0010      # Przesunięcie SL na +0.10% (zabezpieczenie prowizji)
MAX_HOLD_SECONDS = 480           # Time-Stop: 8 minut (zrzut zastygłej pozycji)

RSI_OVERSOLD = 25                # Poziom wyprzedania RSI 7 na 1M
POLL_INTERVAL_SEC = 2            # Szybki cykl odpytywania rynku (co 2 sekundy)
HEARTBEAT_INTERVAL_SEC = 60      # Częstotliwość raportu w logach (co 60s)

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
# ZARZĄDZANIE PAMIĘCIĄ I BILANSEM
# ==============================================================================
def load_position():
    if os.path.exists(SCALP_POS_FILE):
        try:
            with open(SCALP_POS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_position(pos):
    try:
        with open(SCALP_POS_FILE, "w") as f:
            json.dump(pos, f, indent=4)
    except Exception as e:
        logging.error(f"Błąd zapisu pozycji scalpera: {e}")

def record_trade(buy_price, sell_price, qty, reason):
    pnl_usd = (sell_price - buy_price) * qty
    pnl_pct = (sell_price - buy_price) / buy_price
    history = {"trades": []}
    if os.path.exists(SCALP_HIST_FILE):
        try:
            with open(SCALP_HIST_FILE, "r") as f:
                history = json.load(f)
        except Exception:
            pass
    history["trades"].append({
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": SCALP_SYMBOL,
        "buy": buy_price,
        "sell": sell_price,
        "qty": qty,
        "pnl_usd": round(pnl_usd, 3),
        "pnl_pct": round(pnl_pct * 100, 2),
        "reason": reason
    })
    try:
        with open(SCALP_HIST_FILE, "w") as f:
            json.dump(history, f, indent=4)
    except Exception:
        pass
    sign = "+" if pnl_usd >= 0 else ""
    logging.info(f"📊 [BILANS SCALP] Wynik: {sign}{pnl_usd:.2f} USD ({sign}{pnl_pct*100:.2f}%) | Powód: {reason}")

# ==============================================================================
# SALDO I DANE RYNKOWE
# ==============================================================================
def get_free_usdc():
    try:
        res = session.get_wallet_balance(accountType="UNIFIED")
        if res.get("retCode") == 0:
            for coin in res["result"]["list"][0].get("coin", []):
                if coin.get("coin") == QUOTE_COIN:
                    avail = coin.get("availableToWithdraw")
                    eq = coin.get("equity")
                    return safe_float(avail) if avail else safe_float(eq)
    except Exception:
        pass
    return 0.0

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

def get_1m_candles():
    try:
        res = session.get_kline(category=CATEGORY, symbol=SCALP_SYMBOL, interval=INTERVAL, limit=50)
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
        logging.error(f"Błąd pobierania świec 1M: {e}")
        return None

# ==============================================================================
# SILNIK SCALPINGOWY
# ==============================================================================
active_scalp = load_position()

def scalper_loop():
    global active_scalp
    logging.info(f"⚡ Start Scalpera na parze {SCALP_SYMBOL} (Interwał 1M, polling co {POLL_INTERVAL_SEC}s)...")
    last_heartbeat = 0.0

    while True:
        try:
            now = time.time()
            df = get_1m_candles()
            if df is None:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            current_candle = df.iloc[-1]
            current_price = current_candle['close']
            rsi7 = current_candle['rsi7']

            # Raport co 60 sekund
            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                free_bal = get_free_usdc()
                if active_scalp:
                    pnl_now = ((current_price - active_scalp['buy_price']) / active_scalp['buy_price']) * 100
                    status_str = f"POZYCJA OTWARTA (PnL: {pnl_now:+.2f}%)"
                else:
                    status_str = "Szukam wejścia (Oczekiwanie)"
                
                logging.info(f"💓 [STATUS 1M] {SCALP_SYMBOL}: {current_price:.2f} USD | RSI(7): {rsi7:.1f} | Stan: {status_str} | Wolne środki: {free_bal:.2f} USDC")
                last_heartbeat = now
            
            # FAZA 1: ZARZĄDZANIE OTWARTĄ POZYCJĄ
            if active_scalp:
                buy_price = active_scalp["buy_price"]
                peak_price = max(active_scalp.get("peak_price", buy_price), current_price)
                active_scalp["peak_price"] = peak_price
                
                pnl = (current_price - buy_price) / buy_price
                peak_pnl = (peak_price - buy_price) / buy_price
                elapsed_time = now - active_scalp["entry_timestamp"]

                hit_tp = pnl >= TAKE_PROFIT_PCT or (current_price >= current_candle['bb_upper'] and pnl >= 0.005)
                hit_be = peak_pnl >= BREAKEVEN_TRIGGER_PCT and current_price <= (buy_price * (1.0 + BREAKEVEN_LOCK_PCT))
                hit_sl = pnl <= -HARD_STOP_LOSS_PCT
                hit_time_stop = elapsed_time >= MAX_HOLD_SECONDS and pnl < 0.003

                if hit_tp or hit_be or hit_sl or hit_time_stop:
                    reason = "Take Profit (+0.8%)" if hit_tp else ("Breakeven Lock" if hit_be else ("Stop Loss (-0.5%)" if hit_sl else "Time-Stop (8 min)"))
                    base_coin = SCALP_SYMBOL.replace(QUOTE_COIN, "")
                    real_bal = get_coin_balance(base_coin)

                    if real_bal > 0:
                        step, min_qty = get_symbol_precision(SCALP_SYMBOL)
                        qty_str = format_qty(real_bal, step, min_qty)
                        if qty_str:
                            res = session.place_order(category=CATEGORY, symbol=SCALP_SYMBOL, side="Sell", orderType="Market", qty=qty_str)
                            if res.get("retCode") == 0:
                                record_trade(buy_price, current_price, safe_float(qty_str), reason)
                                logging.info(f"🎯 [WYJŚCIE SCALP] {reason} | Cena: {current_price} | PnL: {pnl*100:.2f}%")
                                active_scalp = None
                                save_position({})
                                if os.path.exists(SCALP_POS_FILE): os.remove(SCALP_POS_FILE)
                    else:
                        active_scalp = None
                        if os.path.exists(SCALP_POS_FILE): os.remove(SCALP_POS_FILE)
                else:
                    save_position(active_scalp)

            # FAZA 2: POSZUKIWANIE MIKRO-WEJŚCIA
            else:
                c_open = current_candle['open']
                c_low = current_candle['low']
                bb_lower = current_candle['bb_lower']
                
                touched_bb = c_low <= (bb_lower * 1.0005)
                oversold = rsi7 <= RSI_OVERSOLD
                rebounding = (current_price > c_open) or ((min(c_open, current_price) - c_low) > abs(current_price - c_open) * 1.5)

                if touched_bb and oversold and rebounding:
                    free_usdc = get_free_usdc()
                    if free_usdc >= SCALP_BUDGET_USD:
                        step, min_qty = get_symbol_precision(SCALP_SYMBOL)
                        order_qty_coin = format_qty(SCALP_BUDGET_USD / current_price, step, min_qty)
                        
                        if order_qty_coin:
                            res = session.place_order(category=CATEGORY, symbol=SCALP_SYMBOL, side="Buy", orderType="Market", qty=f"{SCALP_BUDGET_USD:.2f}")
                            if res.get("retCode") == 0:
                                active_scalp = {
                                    "symbol": SCALP_SYMBOL,
                                    "buy_price": current_price,
                                    "peak_price": current_price,
                                    "qty": order_qty_coin,
                                    "entry_timestamp": now
                                }
                                save_position(active_scalp)
                                logging.info(f"⚡ [WEJŚCIE SCALP] {SCALP_SYMBOL} po {current_price} USD | RSI7: {rsi7:.1f} | Budżet: {SCALP_BUDGET_USD}$")

        except Exception as e:
            logging.error(f"Błąd w pętli scalpera: {e}")

        time.sleep(POLL_INTERVAL_SEC)

# ==============================================================================
# KEEP-ALIVE SERVER (RENDER COMPATIBILITY)
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bybit 1M Scalper Bot is Running!")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

if __name__ == "__main__":
    t_scalp = threading.Thread(target=scalper_loop, daemon=True)
    t_scalp.start()

    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('', port), HealthHandler)
    server.serve_forever()
