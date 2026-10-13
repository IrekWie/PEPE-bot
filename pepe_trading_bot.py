import os
import time
import json
import logging
import threading
import socket
from decimal import Decimal
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import requests
from pybit.unified_trading import HTTP

# ==============================================================================
# OCHRONA PRZED ZAWIESZENIEM POŁĄCZEŃ SIECIOWYCH (SOCKET TIMEOUT)
# ==============================================================================
socket.setdefaulttimeout(15)  # Maksymalnie 15 sekund na odpowiedź sieciową

# ==============================================================================
# KONFIGURACJA BOTA SPOT (RYNEK USDC - MICA COMPLIANT)
# ==============================================================================
FALLBACK_KEY = "TUTAJ_TWÓJ_KEY"
FALLBACK_SECRET = "TUTAJ_TWÓJ_SECRET"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", FALLBACK_KEY).strip()
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", FALLBACK_SECRET).strip()

print(f"🔑 DEBUG KEY: [{BYBIT_API_KEY[:5]}...] (Długość: {len(BYBIT_API_KEY)})", flush=True)
print(f"🔑 DEBUG SEC: [{BYBIT_API_SECRET[:5]}...] (Długość: {len(BYBIT_API_SECRET)})", flush=True)

TESTNET = False
CATEGORY = "spot"  # Rynek SPOT (zgodny z Bybit EU / MiCA)
QUOTE_COIN = "USDC" # Waluta bazowa do handlu

# --- USTAWIENIA SKANOWANIA ---
TOP_MARKETS_COUNT = 150      # Skanuj TOP 150 par USDC
TOP_DIPS_COUNT = 4           # Skanuj 4 monety o największym spadku 24h
TOP_GAINERS_COUNT = 4        # Skanuj 4 monety o największym wzroście 24h
MAX_EMA_CROSS_POSITIONS = 5  # Maksymalnie 5 pozycji dla czystego przecięcia EMA 21/89
MAX_PUMP_POSITIONS = 4       # Maksymalnie 4 pozycje typu PUMP
MAX_TOTAL_POSITIONS = 8      # Łączny limit otwartych pozycji

# --- ZARZĄDZANIE KAPITAŁEM ---
RISK_PCT_PER_TRADE = 0.15    # Standardowo 15% na pozycję
PEPE_RISK_HIGH_PCT = 0.20    # 20% kapitału dla PEPE (gdy jest wystarczająco środków)
PEPE_RISK_LOW_PCT = 0.10     # 10% kapitału dla PEPE (gdy jest mniej środków)
MIN_ORDER_VALUE = 100.0      # Minimalna wartość zakupu to 100 USDC

# --- FILTRY ANTY-SZUMOWE EMA 21/89 ---
EMA_CROSS_MIN_SPREAD_PCT = 0.0020  # Wymagana separacja min. +0.20% ponad EMA 89
EMA_CROSS_VOL_MULTIPLIER = 1.10    # Wolumen na świecy min. 1.1x MA20
EMA_EXIT_HYSTERESIS_PCT  = 0.0020  # Histereza wyjścia: EMA 21 musi spaść min. 0.20% poniżej EMA 89

# --- STRATEGIA 2 (TOP DIP HUNTER) ---
MIN_DIP_24H_PCT = -0.050     # Wymagany SPADEK minimum -5.0% w 24h
DIP_RSI_15M_MAX = 40         # Głębokie wyprzedanie na 15M (RSI <= 40)
DIP_VOL_MULTIPLIER = 1.5     # Wolumen odbicia co najmniej 1.5x większy od MA20

# --- PROGI ZABEZPIECZAJĄCE ---
TRAILING_DROP_PCT = 0.015       # Standardowy Trailing Stop (-1.5% od szczytu)
PEPE_TRAILING_DROP_PCT = 0.028  # Dedykowany Trailing Stop dla PEPE (-2.8% od szczytu)
MEME_TRAILING_DROP_PCT = 0.025  # Trailing Stop dla pozostałych Memecoinów (-2.5%)
HARD_STOP_LOSS_PCT = 0.02       # Sztywny Stop Loss (-2.0% od ceny zakupu)

# --- MECHANIZM BREAKEVEN ---
BREAKEVEN_TRIGGER_PCT = 0.015   # Aktywacja po osiągnięciu szczytu +1.5% PnL
BREAKEVEN_SL_PCT = 0.002        # Przesunięcie Stop Loss na +0.2% PnL (pokrycie opłat)

# --- PROGI RSI & VOL ---
RSI_OVERBOUGHT_SELL = 80     # Sprzedaj, gdy RSI przekroczy 80
RSI_MAX_BUY_EMA = 65         # Maksymalne RSI dla zakupu w trendzie
PUMP_VOL_MULTIPLIER = 2.5    # Wolumen 2.5x wyższy od średniej MA20 dla PUMP
PUMP_PRICE_CHANGE_PCT = 0.025# Wzrost ceny o min. +2.5% w świecy 15M

APP_URL = "https://bybit-bot-i1lw.onrender.com"
POSITIONS_FILE = "active_spot_positions.json"
