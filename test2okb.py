# =================================================================================
# OKX SOLUSDT LIVE TRADING BOT - LONG STRATEGY WITH CPR
# (full patched version: WS auth fixed, algo orders for TP/SL, precision fixes,
#  removed secret logging, and account-mode gating removed per request)
# =================================================================================

"""
================================================================================
OKX SOLUSDT LIVE TRADING BOT - LONG STRATEGY WITH CPR
================================================================================

STRATEGY OVERVIEW:
- Daily CPR-based long strategy for SOL-USDT perpetuals on OKX.

TEST MODES:
- MANUAL: immediate test entry
- AUTO: accelerated simulation
- PRODUCTION: live schedule

NOTE:
- Per your request, API key/secret/passphrase remain hard-coded in this file.
  This is insecure in production — consider environment variables later.
================================================================================
"""

import os
import datetime
import hashlib
import hmac
import base64
import json
import requests
import pandas as pd
import ta
import numpy as np
from time import time, sleep
import threading
import traceback
import gc
import websocket
import _thread

# ================================================================================

# TEST MODE SELECTION
TEST_MODE = "PRODUCTION"  # Options: "MANUAL", "AUTO", "PRODUCTION"

# Manual test configuration
MANUAL_TEST_CONFIG = {
    'use_current_price': True,
    'price_offset_percent': 0.1,
    'force_entry': True,
    'immediate_order': True,
    'test_tp_after_seconds': 60,
    'test_eod_after_seconds': 120,
}

# ================================================================================

LOG_DIR = "trading_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"bot_{TEST_MODE.lower()}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

def log_message(message, timestamp=None, section="GENERAL"):
    """Enhanced logging with sections and error suppression"""
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
    section_tag = f"[{section:8s}]" if section != "GENERAL" else ""
    log_entry = f"[{timestamp_str}] {section_tag} {message}\n"
    print(log_entry, end='')
    try:
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(log_entry)
    except Exception:
        pass

# ================================================================================

# OKX API Configuration (hard-coded as requested)
API_KEY_TESTNET = "YOUR_OKX_TESTNET_API_KEY"
API_SECRET_TESTNET = "YOUR_OKX_TESTNET_API_SECRET"
API_PASSPHRASE_TESTNET = "YOUR_OKX_TESTNET_API_PASSPHRASE"
REST_API_BASE_URL_TESTNET = "https://www.okx.com"


API_KEY_LIVE = "2b38b2de-2369-4898-9c03-c5b34a86c963"
API_SECRET_LIVE = "4F4B909E7BE28C53F03FEEDFF924B67E"
API_PASSPHRASE_LIVE = "Cosmos1&&"
REST_API_BASE_URL_LIVE = "https://www.okx.com"


# Active Configuration
USE_TESTNET = False  # True uses testnet placeholders above; set False to use live keys
if USE_TESTNET:
    API_KEY = API_KEY_TESTNET
    API_SECRET = API_SECRET_TESTNET
    API_PASSPHRASE = API_PASSPHRASE_TESTNET
    REST_API_BASE_URL = REST_API_BASE_URL_TESTNET
    OKX_SIMULATED_TRADING_HEADER = {'x-simulated-trading': '1'}
else:
    API_KEY = API_KEY_LIVE
    API_SECRET = API_SECRET_LIVE
    API_PASSPHRASE = API_PASSPHRASE_LIVE
    REST_API_BASE_URL = REST_API_BASE_URL_LIVE
    OKX_SIMULATED_TRADING_HEADER = {}

# ================================================================================

SYMBOL = 'SOL-USDT-SWAP'
CURRENCY = 'USDT'
POSITION_SIDE = "net"
TIMEFRAME_CPR = '1d'
TIMEFRAME_TRADE = '1h'

RISK_PERCENT = 2.0
LEVERAGE = 8

SL_PERCENT = 5
TP_PERCENT = 0.7
TP_PERCENT_REDUCED = 0.2

HISTORICAL_DATA_MONTHS = 3
MIN_CONTRACT_QTY = 0

ws_authenticated = threading.Event()
ws_subscriptions_ready = threading.Event()
bot_startup_complete = False

entry_sl_price = 0.0
sl_hit_triggered = False
sl_hit_lock = threading.Lock()

entry_order_with_sl = None
entry_order_sl_lock = threading.Lock()

now_utc = datetime.datetime.now(datetime.timezone.utc)

# Schedule config based on TEST_MODE
if TEST_MODE == "MANUAL":
    DAILY_ENTRY_CHECK_HOUR = 0
    DAILY_ENTRY_CHECK_MINUTE = 0
    DAILY_ENTRY_CHECK_SECOND = 0
    EOD_EXIT_HOUR_UTC = 22
    EOD_EXIT_MINUTE_UTC = 59
    log_message(" MANUAL TEST MODE - Immediate order placement", section="SYSTEM")
elif TEST_MODE == "AUTO":
    DAILY_ENTRY_CHECK_HOUR = now_utc.hour
    DAILY_ENTRY_CHECK_MINUTE = (now_utc.minute + 1) % 60
    DAILY_ENTRY_CHECK_SECOND = 0
    EOD_EXIT_HOUR_UTC = now_utc.hour
    EOD_EXIT_MINUTE_UTC = (now_utc.minute + 2) % 60
    log_message(" AUTO TEST MODE - Accelerated schedule", section="SYSTEM")
else:
    DAILY_ENTRY_CHECK_HOUR = 0
    DAILY_ENTRY_CHECK_MINUTE = 0
    DAILY_ENTRY_CHECK_SECOND = 5
    EOD_EXIT_HOUR_UTC = 23
    EOD_EXIT_MINUTE_UTC = 50
    log_message(" PRODUCTION MODE - Live trading schedule", section="SYSTEM")

tp_hit_triggered = False
tp_hit_lock = threading.Lock()

manual_test_order_placed = False
manual_test_tp_scheduled = False
manual_test_eod_scheduled = False

PRODUCT_INFO = {
    "pricePrecision": None,
    "qtyPrecision": None,
    "priceTickSize": None,
    "qtyStepSize": None,
    "minOrderQty": None,
    "contractSize": None,
}

latest_trade_price = None
latest_trade_timestamp = None
trade_data_lock = threading.Lock()

account_balance = 0.0
available_balance = 0.0
account_info_lock = threading.Lock()

ws = None

in_position = False
position_entry_price = 0.0
position_qty = 0.0
current_stop_loss = 0.0
current_take_profit = 0.0
position_lock = threading.Lock()

pending_entry_order_id = None
pending_entry_order_details = {}
position_exit_orders = {}
entry_reduced_tp_flag = False

historical_data_store = {'1d': None, '1h': None}
data_lock = threading.Lock()

trading_logic_thread = None
position_manager_thread = None
live_monitor_thread = None
manual_test_thread = None

shutdown_flag = threading.Event()

intervals = {
    '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
    '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
    '12h': 43200, '1d': 86400, '1w': 604800, '1M': 2592000
}
interval_to_timeframe_str = {v: k for k, v in intervals.items()}

# ================================================================================

server_time_offset = 0

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_okx_server_time_and_offset():
    global server_time_offset
    try:
        response = requests.get(f"{REST_API_BASE_URL}/api/v5/public/time", timeout=5)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get('code') == '0' and json_response.get('data'):
            server_timestamp_ms = int(json_response['data'][0]['ts'])
            local_timestamp_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            server_time_offset = server_timestamp_ms - local_timestamp_ms
            log_message(f"OKX server time synchronized. Offset: {server_time_offset}ms", section="SYSTEM")
            return True
        else:
            log_message(f"Failed to get OKX server time: {json_response.get('msg', 'Unknown error')}", section="ERROR")
            return False
    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching OKX server time: {e}", section="ERROR")
        return False
    except Exception as e:
        log_message(f"Unexpected error in get_okx_server_time_and_offset: {e}", section="ERROR")
        return False

# ================================================================================

def generate_okx_signature(timestamp, method, request_path, body_str=''):
    """
    Generate HMAC SHA256 signature for OKX API.
    Returns Base64-encoded HMAC-SHA256 digest.
    """
    message = str(timestamp) + method.upper() + request_path + body_str
    log_message(f"String to sign: '{message}'", section="DEBUG")
    hashed = hmac.new(API_SECRET.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(hashed.digest()).decode('utf-8')
    return signature

# ================================================================================

def okx_request(method, path, params=None, body_dict=None, max_retries=3):
    local_dt = datetime.datetime.now(datetime.timezone.utc)
    adjusted_dt = local_dt + datetime.timedelta(milliseconds=server_time_offset)
    timestamp = adjusted_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    log_message(f"Generated OK-ACCESS-TIMESTAMP: {timestamp}", section="API")

    body_str = ''
    if body_dict:
        body_str = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

    request_path_for_signing = path
    final_url = f"{REST_API_BASE_URL}{path}"

    if params and method.upper() == 'GET':
        query_string = '?' + '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
        request_path_for_signing += query_string
        final_url += query_string

    signature = generate_okx_signature(timestamp, method, request_path_for_signing, body_str)

    headers = {
        "OK-ACCESS-KEY": API_KEY,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }

    if USE_TESTNET:
        headers.update(OKX_SIMULATED_TRADING_HEADER)

    for attempt in range(max_retries):
        try:
            req_func = getattr(requests, method.lower(), None)
            if not req_func:
                log_message(f"Unsupported HTTP method: {method}", section="ERROR")
                return None

            kwargs = {'headers': headers, 'timeout': 15}

            if body_dict and method.upper() in ['POST', 'PUT', 'DELETE']:
                kwargs['data'] = body_str

            log_message(f"{method} {path} (Attempt {attempt + 1}/{max_retries})", section="API")
            response = req_func(final_url, **kwargs)

            if response.status_code != 200:
                try:
                    error_json = response.json()
                    log_message(f"API Error: Status={response.status_code}, Code={error_json.get('code')}, Msg={error_json.get('msg')}", section="ERROR")
                    okx_error_code = error_json.get('code')
                    if okx_error_code:
                        return error_json
                except json.JSONDecodeError:
                    log_message(f"API Error: Status={response.status_code}, Response: {response.text[:200]}", section="ERROR")

                if attempt < max_retries - 1:
                    sleep(2 ** attempt)
                    continue
                return None

            try:
                json_response = response.json()
                if json_response.get('code') != '0':
                    log_message(f"OKX API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}. Full Response: {json_response}", section="API")
                return json_response
            except json.JSONDecodeError:
                log_message(f"Failed to decode JSON for {method} {path}. Status: {response.status_code}, Resp: {response.text[:200]}", section="ERROR")
                if attempt < max_retries - 1:
                    sleep(2 ** attempt)
                    continue
                return None

        except requests.exceptions.Timeout:
            log_message(f"API request timeout (Attempt {attempt + 1}/{max_retries})", section="ERROR")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.RequestException as e:
            status_code = e.response.status_code if e.response is not None else "N/A"
            err_text = e.response.text[:200] if e.response is not None else 'No response text'
            log_message(f"OKX API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}", section="ERROR")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
                continue
            return None
        except Exception as e:
            log_message(f"Unexpected error during OKX API request ({method} {path}): {e}", section="ERROR")
            import traceback; traceback.print_exc()
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
                continue
            return None
    return None

# ================================================================================

def get_current_market_price():
    try:
        path = "/api/v5/market/ticker"
        params = {"instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        if response and response.get('code') == '0':
            data = response.get('data', [])
            if data and isinstance(data, list) and len(data) > 0:
                ticker_info = data[0]
                last_price = ticker_info.get('last')
                if last_price is not None:
                    current_price = safe_float(last_price)
                    log_message(f"Current market price (REST): ${current_price:.2f}", section="SYSTEM")
                    return current_price
                else:
                    log_message("'last' price not found in OKX ticker response. Falling back to WS.", section="ERROR")
            else:
                log_message("OKX ticker data is empty or malformed. Falling back to WS.", section="ERROR")

        log_message("Failed to fetch current price from REST. Falling back to WS.", section="ERROR")
        with trade_data_lock:
            if latest_trade_price:
                log_message(f"Current market price (from WS): ${latest_trade_price:.2f}", section="SYSTEM")
                return latest_trade_price
        log_message("Failed to fetch current price from any source", section="ERROR")
        return None
    except Exception as e:
        log_message(f"Exception in get_current_market_price: {e}", section="ERROR")
        return None

def fetch_product_info(target_symbol):
    global PRODUCT_INFO
    try:
        path = "/api/v5/public/instruments"
        params = {"instType": "SWAP", "instId": target_symbol}
        response = okx_request("GET", path, params=params)

        if response and response.get('code') == '0':
            product_data = None
            if isinstance(response.get('data'), list):
                for item in response['data']:
                    if item.get('instId') == target_symbol:
                        product_data = item
                        break
            elif isinstance(response.get('data'), dict) and response.get('data').get('instId') == target_symbol:
                product_data = response.get('data')

            if not product_data:
                log_message(f"Product {target_symbol} not found in OKX instruments response.", section="ERROR")
                return False

            PRODUCT_INFO['priceTickSize'] = safe_float(product_data.get('tickSz'))
            PRODUCT_INFO['qtyStepSize'] = safe_float(product_data.get('lotSz'))
            PRODUCT_INFO['minOrderQty'] = safe_float(product_data.get('minSz'))

            if PRODUCT_INFO['priceTickSize'] > 0:
                PRODUCT_INFO['pricePrecision'] = int(np.abs(np.log10(PRODUCT_INFO['priceTickSize'])))
            else:
                PRODUCT_INFO['pricePrecision'] = 0

            if PRODUCT_INFO['qtyStepSize'] > 0:
                PRODUCT_INFO['qtyPrecision'] = int(np.abs(np.log10(PRODUCT_INFO['qtyStepSize'])))
            else:
                PRODUCT_INFO['qtyPrecision'] = 0

            PRODUCT_INFO['contractSize'] = safe_float(product_data.get('ctVal', '1'), 1.0)

            log_message(f"Product info loaded for linear: {PRODUCT_INFO}", section="SYSTEM")
            return True
        else:
            log_message(f"Failed to fetch product info for linear contracts (code: {response.get('code') if response else 'N/A'}, msg: {response.get('msg') if response else 'N/A'})", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in fetch_product_info (linear): {e}", section="ERROR")
        return False

def okx_set_leverage(symbol, leverage_val):
    try:
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": symbol,
            "lever": str(int(leverage_val)),
            "mgnMode": "cross"
        }

        log_message(f"Setting leverage to {leverage_val}x for {symbol}", section="SYSTEM")
        response = okx_request("POST", path, body_dict=body)

        if response and response.get('code') == '0':
            log_message(f"✓ Leverage set successfully for {symbol}", section="SYSTEM")
            return True
        else:
            log_message(f"Failed to set leverage for {symbol}: {response.get('msg') if response else 'No response'}", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in okx_set_leverage: {e}", section="ERROR")
        return False

# ================================================================================

def okx_place_order(symbol, side, qty, price=None, order_type="Market",
                      time_in_force=None, reduce_only=False,
                      stop_loss_price=None, take_profit_price=None):
    """
    Places an order on OKX for perpetual swap contracts (ordinary orders: market/limit).
    Note: Stop market / conditional orders should use algo endpoint (order-algo).
    """
    try:
        path = "/api/v5/trade/order"
        price_precision = PRODUCT_INFO.get('pricePrecision', 4)
        qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

        order_qty_str = f"{qty:.{qty_precision}f}"

        body = {
            "instId": symbol,
            "tdMode": "cross",
            "side": side.lower(),
            "ordType": order_type.lower(),
            "sz": order_qty_str,
        }

        if order_type.lower() == "limit" and price is not None:
            body["px"] = f"{price:.{price_precision}f}"

        if time_in_force:
            if time_in_force == "GoodTillCancel":
                body["timeInForce"] = "GTC"
            else:
                body["timeInForce"] = time_in_force

        if reduce_only:
            body["reduceOnly"] = True

        log_message(f"Placing {order_type} {side} order for {order_qty_str} {symbol} at {price}", section="TRADE")
        response = okx_request("POST", path, body_dict=body)

        if response and response.get('code') == '0':
            order_data = response.get('data', [])
            if order_data and order_data[0].get('ordId'):
                log_message(f"✓ Order placed: OrderID={order_data[0]['ordId']}", section="TRADE")
                return order_data[0]
            else:
                log_message(f"✗ Order placement failed: No order ID in response. Response: {response}", section="ERROR")
                return None
        else:
            error_msg = response.get('msg', 'Unknown error') if response else 'No response'
            log_message(f"✗ Order placement failed: {error_msg}. Response: {response}", section="ERROR")
            return None
    except Exception as e:
        log_message(f"Exception in okx_place_order: {e}", section="ERROR")
        import traceback
        traceback.print_exc()
        return None

# ================================================================================

def okx_place_algo_order(body):
    """
    Places an algo (conditional) order on OKX via /api/v5/trade/order-algo.
    Returns the first data object (with algoId) on success or None.
    """
    try:
        path = "/api/v5/trade/order-algo"
        log_message(f"Placing algo order: {body}", section="TRADE")
        response = okx_request("POST", path, body_dict=body)
        if response and response.get('code') == '0':
            data = response.get('data', [])
            if data and (data[0].get('algoId') or data[0].get('ordId')):
                log_message(f"✓ Algo order placed: {data[0]}", section="TRADE")
                return data[0]
            else:
                log_message(f"✗ Algo order placed but no algoId/ordId returned: {response}", section="ERROR")
                return None
        else:
            log_message(f"✗ Algo order failed: {response}", section="ERROR")
            return None
    except Exception as e:
        log_message(f"Exception in okx_place_algo_order: {e}", section="ERROR")
        import traceback
        traceback.print_exc()
        return None

# ================================================================================

def verify_order_sl_tp(order_id):
    try:
        path = "/api/v5/trade/order"
        params = {"instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        if response and response.get('code') == '0':
            order_data = response.get('data', [])
            if order_data and order_data[0].get('ordId') == order_id:
                log_message(f"✓ Verified entry order {order_id[:12]}... exists.", section="TRADE")
                return True

        log_message(f"Could not verify entry order {order_id[:12]}...", section="ERROR")
        return False
    except Exception as e:
        log_message(f"Exception in verify_order_sl_tp (linear): {e}", section="ERROR")
        return False

def close_all_entry_orders():
    try:
        log_message("Attempting to close unfilled linear entry orders...", section="TRADE")

        path = "/api/v5/trade/orders-pending"
        params = {"instType": "SWAP", "instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        if not response or response.get('code') != '0':
            log_message("No orders found or API error (OK if no orders)", section="TRADE")
            return True

        orders = response.get('data', [])
        cancelled_count = 0

        for order in orders:
            try:
                order_id = order.get('ordId')
                status = order.get('state')
                side = order.get('side')
                if side == 'buy' and status not in ['filled', 'canceled', 'rejected']:
                    if okx_cancel_order(SYMBOL, order_id):
                        cancelled_count += 1
                        sleep(0.1)
            except Exception as e:
                log_message(f"Error processing OKX order: {e}", section="ERROR")

        if cancelled_count > 0:
            log_message(f"✓ Closed {cancelled_count} unfilled linear entry orders", section="TRADE")
        else:
            log_message(f"No unfilled linear entry orders to close (OK)", section="TRADE")

        return True
    except Exception as e:
        log_message(f"Exception in close_all_entry_orders (linear): {e} (continuing)", section="ERROR")
        return True

# ================================================================================

def handle_tp_hit():
    global tp_hit_triggered, in_position, position_qty
    try:
        log_message("=" * 80, section="TRADE")
        log_message("🎯 TP HIT (0.7%) - EXECUTING PROTOCOL", section="TRADE")
        log_message("=" * 80, section="TRADE")

        log_message("Step 1: Closing unfilled entry orders...", section="TRADE")
        close_all_entry_orders()

        sleep(1)

        log_message("Step 2: Checking OKX position status...", section="TRADE")
        path = "/api/v5/account/positions"
        params = {"instType": "SWAP", "instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        position_still_open = False
        open_qty = 0.0

        if response and response.get('code') == '0':
            positions = response.get('data', [])
            for pos in positions:
                if pos.get('instId') == SYMBOL:
                    pos_qty_str = pos.get('pos', '0')
                    size_val = safe_float(pos_qty_str)
                    if size_val > 0:
                        position_still_open = True
                        open_qty = size_val
                        log_message(f"OKX position still open: {open_qty} {SYMBOL} (partial fill)", section="TRADE")
                        break

        if position_still_open and open_qty > 0:
            log_message("Step 3: Waiting 3 seconds (monitoring 3 x 1-second candles)...", section="TRADE")
            for i in range(3):
                log_message(f"  [{i+1}/3 seconds elapsed]", section="TRADE")
                sleep(1)

            log_message("Step 4: Market closing remaining OKX position...", section="TRADE")
            exit_order_response = okx_place_order(
                SYMBOL,
                "Sell",
                open_qty,
                order_type="Market",
                reduce_only=True
            )

            if exit_order_response and exit_order_response.get('ordId'):
                log_message(f"✓ Market close order placed for {open_qty} {SYMBOL}", section="TRADE")
            else:
                log_message(f"⚠ Market close order may have failed (OK if already closed)", section="TRADE")

            sleep(1)
            cancel_all_exit_orders_and_reset("TP hit - OKX position closed")
        else:
            log_message("OKX position fully closed by TP order. No market close needed.", section="TRADE")
            cancel_all_exit_orders_and_reset("TP hit - fully closed")

        with tp_hit_lock:
            tp_hit_triggered = False

        log_message("=" * 80, section="TRADE")
        log_message("✓ TP HIT PROTOCOL COMPLETE (OKX)", section="TRADE")
        log_message("=" * 80, section="TRADE")

    except Exception as e:
        log_message(f"Exception in handle_tp_hit (OKX): {e} (continuing)", section="ERROR")
        with tp_hit_lock:
            tp_hit_triggered = False

# ================================================================================

def handle_eod_exit():
    try:
        log_message("=" * 80, section="TRADE")
        log_message("🕐 EOD EXIT TRIGGERED (OKX)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        with position_lock:
            is_in_pos = in_position
            pos_qty = position_qty

        log_message("Step 1: Checking for open OKX positions...", section="TRADE")

        try:
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": SYMBOL}
            response = okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == SYMBOL:
                        pos_qty_str = pos.get('pos', '0')
                        size_val = safe_float(pos_qty_str)
                        if size_val > 0:
                            log_message(f"Found open long OKX position: {size_val} {SYMBOL} - closing...", section="TRADE")
                            exit_order_response = okx_place_order(
                                SYMBOL,
                                "Sell",
                                size_val,
                                order_type="Market",
                                reduce_only=True
                            )
                            if exit_order_response and exit_order_response.get('ordId'):
                                log_message(f"✓ Market close order placed", section="TRADE")
                            else:
                                log_message(f"⚠ Market close failed (OK if already closed)", section="TRADE")
                            sleep(1)
                            break
            else:
                log_message("No OKX positions found or API error (OK)", section="TRADE")
        except Exception as e:
            log_message(f"Error closing OKX position: {e} (OK, continuing)", section="TRADE")

        log_message("Step 2: Closing unfilled entry orders...", section="TRADE")
        try:
            close_all_entry_orders()
        except Exception as e:
            log_message(f"Error closing entry orders: {e} (OK, continuing)", section="TRADE")

        sleep(0.5)

        log_message("Step 3: Force cancelling all remaining OKX orders...", section="TRADE")
        try:
            path = "/api/v5/trade/cancel-all-after"
            body = {"timeOut": "0", "instType": "SWAP"}
            response = okx_request("POST", path, body_dict=body)
            if response and response.get('code') == '0':
                log_message(f"✓ All OKX orders cancelled", section="TRADE")
            else:
                log_message(f"⚠ All OKX orders cancel response: {response} (OK)", section="TRADE")
        except Exception as e:
            log_message(f"Error force cancelling OKX orders: {e} (OK, continuing)", section="ERROR")

        log_message("=" * 80, section="TRADE")
        log_message("✓ EOD EXIT COMPLETE (OKX)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        cancel_all_exit_orders_and_reset("EOD Exit")

    except Exception as e:
        log_message(f"Exception in handle_eod_exit (OKX): {e} (continuing)", section="ERROR")
        cancel_all_exit_orders_and_reset("EOD Exit - forced")

# ================================================================================

def update_account_info():
    """Updates global account balance for OKX contracts and prints it.
    Now correctly prioritizes overall equity for unified accounts.
    """
    global account_balance, available_balance

    try:
        # Step 1: Now check Trading Account for balance
        path_trading = "/api/v5/account/balance"
        params_trading = {"ccy": CURRENCY}
        response_trading = okx_request("GET", path_trading, params=params_trading)
        log_message(f"Trading Account Balance Response: {response_trading}", section="DEBUG")

        if response_trading and response_trading.get('code') == '0':
            data = response_trading.get('data', [])
            if data and isinstance(data, list) and len(data) > 0:
                account_details = data[0]
                new_total_balance = safe_float(account_details.get('totalEq', '0'))

                # For unified accounts, 'availEq' at the top level is the key metric for new positions.
                # The 'details' array might not contain the futures balance directly.
                new_avail_balance = safe_float(account_details.get('availEq', '0'))

                log_message(f"Unified Account Equity: Total={new_total_balance:.8f}, Available={new_avail_balance:.8f}", section="ACCOUNT")

                log_message(f"DEBUG: Parsed new_total_balance: {new_total_balance}, new_avail_balance: {new_avail_balance}", section="DEBUG")

                if new_avail_balance <= 0:
                    log_message(f"Available equity is zero or less. This suggests no usable trading balance.", section="WARNING")

                with account_info_lock:
                    account_balance = new_total_balance
                    available_balance = new_avail_balance
                    log_message(f"Total Balance Updated: {account_balance:.8f} {CURRENCY}", section="ACCOUNT")
                    log_message(f"Available Balance Updated: {available_balance:.8f} {CURRENCY}", section="ACCOUNT")
                return True
            else:
                log_message("OKX Trading Account data missing or malformed in response", section="ERROR")
                return False
        else:
            log_message(f"Failed to fetch OKX Trading Account info: {response_trading.get('msg') if response_trading else 'No response'}", section="ERROR")
            return False

    except Exception as e:
        log_message(f"Exception in update_account_info (OKX): {e}", section="ERROR")
        return False

# ================================================================================

def okx_cancel_order(symbol, order_id):
    try:
        path = "/api/v5/trade/cancel-order"
        body = {
            "instId": symbol,
            "ordId": order_id,
        }

        log_message(f"Cancelling OKX order {order_id[:12]}...", section="TRADE")
        response = okx_request("POST", path, body_dict=body)

        if response and response.get('code') == '0':
            log_message(f"✓ Order cancelled", section="TRADE")
            return True
        elif response and response.get('code') == '51001':
            log_message(f"Order already filled/cancelled (OK)", section="TRADE")
            return True
        else:
            log_message(f"Failed to cancel order (OK, continuing): {response.get('msg') if response else 'No response'}", section="TRADE")
            return False
    except Exception as e:
        log_message(f"Exception in okx_cancel_order: {e}", section="ERROR")
        return False

def okx_cancel_algo_order(symbol, algo_id):
    try:
        path = "/api/v5/trade/cancel-algo-order"
        body = {
            "instId": symbol,
            "algoId": algo_id,
        }

        log_message(f"Cancelling OKX algo order {str(algo_id)[:12]}...", section="TRADE")
        response = okx_request("POST", path, body_dict=body)

        if response and response.get('code') == '0':
            log_message(f"✓ Algo order cancelled", section="TRADE")
            return True
        elif response and response.get('code') == '51001':
            log_message(f"Algo order already filled/cancelled (OK)", section="TRADE")
            return True
        else:
            log_message(f"Failed to cancel algo order (OK, continuing): {response.get('msg') if response else 'No response'}", section="TRADE")
            return False
    except Exception as e:
        log_message(f"Exception in okx_cancel_algo_order: {e}", section="ERROR")
        return False

# ================================================================================

# Note: per your instruction, account-mode gating removed — this function simply returns True.
def check_account_mode():
    """
    NO GATING: Skip strict account-mode checks and behave like the test script.
    This function intentionally does not block the bot based on acctLv.
    """
    try:
        log_message("Skipping strict account-mode gating (behaving like test script).", section="ACCOUNT")
        return True
    except Exception as e:
        log_message(f"Exception in check_account_mode (skipped): {e}", section="ERROR")
        return True

# ================================================================================

def fetch_historical_data_okx(symbol, timeframe, start_date_str, end_date_str):
    try:
        path = "/api/v5/market/history-candles"

        okx_timeframe_map = {
            '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
            '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
        }
        okx_timeframe = okx_timeframe_map.get(timeframe)

        if not okx_timeframe:
            log_message(f"Invalid timeframe for OKX: {timeframe}", section="ERROR")
            return []

        start_dt = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
        # Ensure the end date is inclusive by setting the time to the end of the day
        end_dt = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=datetime.timezone.utc)

        start_ts_ms = int(start_dt.timestamp() * 1000)
        end_ts_ms = int(end_dt.timestamp() * 1000)

        all_data = []
        max_candles_limit = 100

        current_before_ms = None

        while True:
            params = {
                "instId": symbol,
                "bar": okx_timeframe,
                "limit": str(max_candles_limit)
            }
            if current_before_ms:
                params["before"] = str(current_before_ms)

            response = okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                rows = response.get('data', [])
                if rows:
                    log_message(f"Fetched {len(rows)} candles for {timeframe}", section="DATA")
                    parsed_klines = []
                    for kline in rows:
                        try:
                            parsed_klines.append([
                                int(kline[0]),
                                float(kline[1]),
                                float(kline[2]),
                                float(kline[3]),
                                float(kline[4]),
                                float(kline[5])
                            ])
                        except (ValueError, TypeError, IndexError) as e:
                            log_message(f"Error parsing OKX kline: {kline} - {e}", section="ERROR")
                            continue

                    all_data.extend(parsed_klines)

                    oldest_ts = int(rows[-1][0])
                    current_before_ms = oldest_ts

                    if oldest_ts <= start_ts_ms or len(rows) < max_candles_limit:
                        break
                else:
                    break

                sleep(0.3)
            else:
                log_message(f"Error fetching OKX klines: {response}", section="ERROR")
                return []

        # Filter data to be within the requested range and remove duplicates
        final_data = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        final_data = final_data.drop_duplicates(subset=['Timestamp'])
        final_data = final_data[(final_data['Timestamp'] >= start_ts_ms) & (final_data['Timestamp'] <= end_ts_ms)]

        return final_data.values.tolist()
    except Exception as e:
        log_message(f"Exception in fetch_historical_data_okx: {e}", section="ERROR")
        return []

def fetch_initial_historical_data(symbol, timeframe, start_date_str, end_date_str):
    global historical_data_store

    try:
        raw_data = fetch_historical_data_okx(symbol, timeframe, start_date_str, end_date_str)

        if raw_data:
            df = pd.DataFrame(raw_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)

            if df.empty:
                log_message(f"No valid data for {timeframe}", section="ERROR")
                return False

            invalid_rows = df[(df['Low'] > df['High']) |
                             (df['Open'] < df['Low']) | (df['Open'] > df['High']) |
                             (df['Close'] < df['Low']) | (df['Close'] > df['High'])]

            if not invalid_rows.empty:
                log_message(f"WARNING: Found {len(invalid_rows)} invalid OHLC rows", section="ERROR")
                df = df[(df['Low'] <= df['High'])]

            df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
            df = df.set_index('Datetime')
            df = df[~df.index.duplicated(keep='first')]
            df = df.sort_index()

            with data_lock:
                historical_data_store[timeframe] = df

            log_message(f"Loaded {len(df)} candles for {timeframe}", section="SYSTEM")
            return True
        else:
            log_message(f"Failed to fetch data for {timeframe}", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in fetch_initial_historical_data: {e}", section="ERROR")
        return False

# ================================================================================

def update_historical_data_from_ws(timeframe_key, klines_ws):
    global historical_data_store

    try:
        if not klines_ws:
            return

        with data_lock:
            df = historical_data_store.get(timeframe_key)
            if df is None:
                return

            new_data_points = []
            for kline in klines_ws:
                try:
                    ts_ms = int(kline[0])
                    o = float(kline[1])
                    h = float(kline[2])
                    l = float(kline[3])
                    c = float(kline[4])
                    v = float(kline[5])

                    if not (l <= h):
                        continue

                    dt_utc = datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)

                    if not df.empty and dt_utc == df.index[-1]:
                        df.at[dt_utc, 'Open'] = o
                        df.at[dt_utc, 'High'] = h
                        df.at[dt_utc, 'Low'] = l
                        df.at[dt_utc, 'Close'] = c
                        df.at[dt_utc, 'Volume'] = v
                        continue

                    new_data_points.append({
                        'Datetime': dt_utc,
                        'Open': o,
                        'High': h,
                        'Low': l,
                        'Close': c,
                        'Volume': v
                    })

                except (ValueError, TypeError, IndexError):
                    continue

            if not new_data_points:
                return

            temp_df = pd.DataFrame(new_data_points).set_index('Datetime')
            original_last_time = df.index[-1] if not df.empty else None

            combined_df = pd.concat([df, temp_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()

            if len(combined_df) > 1000:
                combined_df = combined_df.iloc[-1000:]

            historical_data_store[timeframe_key] = combined_df

            if original_last_time is not None and not combined_df.empty:
                current_last_time = combined_df.index[-1]
                if current_last_time > original_last_time:
                    log_message(f"New {timeframe_key} candle: {current_last_time}", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in update_historical_data_from_ws: {e}", section="ERROR")

# ================================================================================
# Raw WebSocket Implementation
# ================================================================================

class RawOkxSocketClient:
    def __init__(self, url, on_message, on_error, on_close, on_open):
        self.ws = None
        self.url = url
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.on_open = on_open
        self.is_running = False

    def start(self):
        self.is_running = True
        self.ws = websocket.WebSocketApp(
            self.url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        _thread.start_new_thread(self.ws.run_forever, ())

    def send(self, data):
        if self.ws:
            self.ws.send(data)

    def subscribe(self, channels):
        if self.ws:
            sub_payload = {
                "op": "subscribe",
                "args": channels
            }
            self.send(json.dumps(sub_payload))

    def close(self):
        self.is_running = False
        if self.ws:
            self.ws.close()

def get_ws_url():
    # Per OKX documentation, demo trading uses the production WebSocket endpoint
    # with `x-simulated-trading: 1` in the REST API calls, which is already handled.
    return "wss://ws.okx.com:8443/ws/v5/public"

# ================================================================================
# WebSocket Implementation
# ================================================================================

def on_websocket_message(ws_app, message):
    global latest_trade_price, latest_trade_timestamp, bot_startup_complete
    try:
        msg = json.loads(message)

        # Handle event messages (login, subscribe)
        if 'event' in msg:
            if msg['event'] == 'login' and msg.get('code') == '0':
                log_message("WebSocket authenticated.", section="WS")
                ws_authenticated.set()
            elif msg['event'] == 'subscribe':
                log_message(f"Subscription response: {msg}", section="WS")
                # This is a simplified check. A robust implementation would
                # track each channel subscription individually.
                ws_subscriptions_ready.set()
            return

        if 'data' in msg:
            channel = msg.get('arg', {}).get('channel', '')
            data = msg.get('data', [])

            if channel == 'trades' and data:
                with trade_data_lock:
                    latest_trade_timestamp = int(data[-1].get('ts'))
                    latest_trade_price = safe_float(data[-1].get('px'))

            elif channel.startswith('candle') and data:
                timeframe_key = channel.replace('candle', '')
                if timeframe_key.endswith('H'):
                    timeframe_key = timeframe_key.replace('H', 'h').lower()
                elif timeframe_key.endswith('D'):
                    timeframe_key = timeframe_key.replace('D', 'd').lower()

                parsed_klines = [
                    [int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])]
                    for k in data if len(k) >= 6
                ]
                update_historical_data_from_ws(timeframe_key, parsed_klines)

            elif channel == 'orders' and bot_startup_complete and data:
                handle_order_update(data)

            elif channel == 'positions' and data:
                detect_sl_from_position_update(data)

    except json.JSONDecodeError:
        pass
    except Exception as e:
        log_message(f"Exception in on_websocket_message: {e}", section="ERROR")

def on_websocket_open(ws_app):
    log_message("OKX WebSocket connection opened.", section="WS")

    # Subscribe to channels
    okx_timeframe_map = {
        '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
        '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
    }

    channels = [
        {"channel": "trades", "instId": SYMBOL},
    ]

    for tf in [TIMEFRAME_CPR, TIMEFRAME_TRADE]:
        okx_channel = okx_timeframe_map.get(tf.lower())
        if okx_channel:
            channels.append({"channel": f"candle{okx_channel}", "instId": SYMBOL})

    ws.subscribe(channels)

def on_websocket_error(ws_app, error):
    log_message(f"OKX WebSocket error: {error}", section="ERROR")

def on_websocket_close(ws_app, close_status_code, close_msg):
    log_message("OKX WebSocket closed.", section="SYSTEM")

def initialize_websocket():
    """
    Initializes a raw WebSocket connection.
    """
    global ws
    ws_url = get_ws_url()

    try:
        ws = RawOkxSocketClient(
            url=ws_url,
            on_message=on_websocket_message,
            on_error=on_websocket_error,
            on_close=on_websocket_close,
            on_open=on_websocket_open
        )
        ws.start()
        log_message(f"Raw WebSocket client started on URL: {ws_url}", section="SYSTEM")
        return ws
    except Exception as e:
        log_message(f"Exception initializing raw WebSocket: {e}", section="ERROR")
        return None

# ================================================================================

def calculate_cpr_levels(H, L, C):
    try:
        if H < L:
            H, L = L, H
        P = (H + L + C) / 3.0
        TC = (H + L) / 2.0
        BC = P - TC + P
        if TC < BC:
            TC, BC = BC, TC
        R1 = (2 * P) - L
        S1 = (2 * P) - H
        R2 = P + (H - L)
        S2 = P - (H - L)
        R3 = P + 2 * (H - L)
        S3 = P - 2 * (H - L)
        R4 = R3 + (R2 - R1)
        return {
            'P': P, 'TC': TC, 'BC': BC,
            'R1': R1, 'S1': S1,
            'R2': R2, 'S2': S2,
            'R3': R3, 'S3': S3,
            'R4': R4
        }
    except Exception as e:
        log_message(f"Exception in calculate_cpr_levels: {e}", section="ERROR")
        return None

def calculate_indicators(df):
    try:
        if df is None or df.empty:
            return None
        if not isinstance(df.index, pd.DatetimeIndex):
            return None
        df_copy = df.copy()
        price_data_ep = pd.to_numeric(df_copy['Close'], errors='coerce').dropna()
        if price_data_ep.empty:
            return None
        window_ema21 = 21
        if len(price_data_ep) >= window_ema21:
            df_copy['EMA_21'] = ta.trend.ema_indicator(price_data_ep, window=window_ema21)
        else:
            df_copy['EMA_21'] = np.nan
        window_ema50 = 50
        if len(price_data_ep) >= window_ema50:
            df_copy['EMA_50'] = ta.trend.ema_indicator(price_data_ep, window=window_ema50)
        else:
            df_copy['EMA_50'] = np.nan
        window_rsi = 14
        if len(price_data_ep) >= (window_rsi + 1):
            df_copy['RSI'] = ta.momentum.rsi(price_data_ep, window=window_rsi)
        else:
            df_copy['RSI'] = np.nan
        min_macd_candles = 35
        if len(price_data_ep) >= min_macd_candles:
            macd = ta.trend.MACD(price_data_ep, window_slow=26, window_fast=12, window_sign=9)
            df_copy['MACD'] = macd.macd()
            df_copy['MACD_Signal'] = macd.macd_signal()
            df_copy['MACD_Histo'] = macd.macd_diff()
        else:
            df_copy['MACD'], df_copy['MACD_Signal'], df_copy['MACD_Histo'] = np.nan, np.nan, np.nan
        return df_copy
    except Exception as e:
        log_message(f"Exception in calculate_indicators: {e}", section="ERROR")
        return None

# ================================================================================

def check_entry_conditions(calculated_data):
    try:
        with position_lock:
            if in_position:
                log_message("Entry check: Already in position", section="TRADE")
                return False, 0.0, False
            if pending_entry_order_id:
                log_message("Entry check: Pending entry order exists", section="TRADE")
                return False, 0.0, False

        if not calculated_data:
            log_message("Entry check: No calculated data", section="ERROR")
            return False, 0.0, False

        daily_cpr = calculated_data['daily_cpr']
        daily_open = calculated_data['daily_open']
        tc_level = daily_cpr['TC']

        ema_21 = calculated_data['ema_21']
        ema_50 = calculated_data['ema_50']

        if TEST_MODE == "MANUAL" and MANUAL_TEST_CONFIG.get('force_entry'):
            log_message(f"🧪 MANUAL TEST: Forcing entry (skipping conditions)", section="TEST")
            reduced_tp = False
            return True, tc_level, reduced_tp

        if daily_open <= tc_level:
            log_message(f"Entry check FAIL: Open ({daily_open:.2f}) not above TC ({tc_level:.2f})", section="TRADE")
            return False, 0.0, False

        log_message(f"✓ Entry check PASS: Open ({daily_open:.2f}) above TC ({tc_level:.2f})", section="TRADE")

        reduced_tp = False
        if pd.notna(ema_21) and pd.notna(ema_50):
            if ema_21 < ema_50:
                reduced_tp = True
                log_message(f"⚠ EMA Filter: 21 EMA < 50 EMA - TP reduced to {TP_PERCENT_REDUCED}%", section="TRADE")
            else:
                log_message(f"✓ EMA Filter: 21 EMA >= 50 EMA - Normal TP {TP_PERCENT}%", section="TRADE")
        else:
            log_message("Warning: EMA values not available, using normal TP", section="TRADE")

        return True, tc_level, reduced_tp
    except Exception as e:
        log_message(f"Exception in check_entry_conditions: {e}", section="ERROR")
        return False, 0.0, False

# ================================================================================

def calculate_position_size(entry_price, current_avail_balance):
    try:
        if current_avail_balance <= 0:
            log_message("Position sizing: Insufficient available balance", section="ERROR")
            return 0.0
        if not entry_price or entry_price <= 0:
            log_message(f"Position sizing: Invalid entry price {entry_price}", section="ERROR")
            return 0.0

        risk_amount_usdt = current_avail_balance * (RISK_PERCENT / 100.0)
        position_value_usdt = risk_amount_usdt * LEVERAGE
        qty_base_asset = position_value_usdt / entry_price

        qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)
        qty_base_asset = round(qty_base_asset, qty_precision)

        if qty_base_asset < MIN_CONTRACT_QTY:
            log_message(f"Position sizing: Calculated quantity {qty_base_asset} < minimum {MIN_CONTRACT_QTY}", section="ERROR")
            return 0.0

        log_message(f"Position size: {qty_base_asset:.{qty_precision}f} {SYMBOL} (Available USDT: {current_avail_balance:.2f})", section="CALC")
        return qty_base_asset
    except Exception as e:
        log_message(f"Exception in calculate_position_size: {e}", section="ERROR")
        return 0.0

# ================================================================================

def calculate_tp_sl(entry_price, reduced_tp_flag):
    try:
        if reduced_tp_flag:
            tp_price = entry_price * (1 + TP_PERCENT_REDUCED / 100.0)
            tp_reason = f"Reduced TP {TP_PERCENT_REDUCED}%"
        else:
            tp_price = entry_price * (1 + TP_PERCENT / 100.0)
            tp_reason = f"Normal TP {TP_PERCENT}%"
        sl_price = entry_price * (1 - SL_PERCENT / 100.0)
        log_message(f"TP/SL: TP={tp_price:.2f} ({tp_reason}), SL={sl_price:.2f}", section="CALC")
        return tp_price, sl_price, tp_reason
    except Exception as e:
        log_message(f"Exception in calculate_tp_sl: {e}", section="ERROR")
        return 0, 0, "Error"

# ================================================================================

def initiate_entry_sequence(calculated_data, limit_entry_price, reduced_tp_flag):
    global pending_entry_order_id, entry_reduced_tp_flag, pending_entry_order_details
    global position_manager_thread, entry_sl_price, entry_order_with_sl

    try:
        if not update_account_info():
            log_message("Entry aborted: Could not update account info", section="ERROR")
            return

        with account_info_lock:
            current_avail = available_balance

        qty = calculate_position_size(limit_entry_price, current_avail)
        if qty == 0:
            log_message("Entry aborted: Position size is 0", section="ERROR")
            return

        sl_price = limit_entry_price * (1 - SL_PERCENT / 100.0)

        log_message("=" * 80, section="TRADE")
        log_message(f"PLACING ENTRY ORDER", section="TRADE")
        log_message(f"Entry Price: ${limit_entry_price:.2f}", section="TRADE")
        log_message(f"Stop Loss: ${sl_price:.2f} (separate order)", section="TRADE")
        log_message(f"Quantity: {qty} {SYMBOL}", section="TRADE")
        log_message("=" * 80, section="TRADE")

        entry_order = okx_place_order(
            SYMBOL,
            "Buy",
            qty,
            price=limit_entry_price,
            order_type="Limit",
            time_in_force="GoodTillCancel"
        )

        if entry_order and entry_order.get('ordId'):
            order_id = entry_order['ordId']
            with position_lock:
                pending_entry_order_id = order_id
                entry_reduced_tp_flag = reduced_tp_flag
                entry_sl_price = sl_price

                pending_entry_order_details = {
                    'order_id': order_id,
                    'side': 'Buy',
                    'qty': qty,
                    'limit_price': limit_entry_price,
                    'sl_price': sl_price,
                    'order_type': 'Limit',
                    'status': 'New',
                    'placed_at': datetime.datetime.now(datetime.timezone.utc)
                }

            with entry_order_sl_lock:
                entry_order_with_sl = order_id

            log_message(f"✓ Entry order placed (SL will be placed separately)", section="TRADE")

            if position_manager_thread is None or not position_manager_thread.is_alive():
                position_manager_thread = threading.Thread(
                    target=manage_position_lifecycle,
                    name="PositionManager",
                    daemon=True
                )
                position_manager_thread.start()
                log_message("✓ Position manager started", section="SYSTEM")
        else:
            log_message(f"Entry order placement failed", section="ERROR")
    except Exception as e:
        log_message(f"Exception in initiate_entry_sequence: {e}", section="ERROR")
        import traceback
        traceback.print_exc()

# ================================================================================

def handle_sl_hit():
    global sl_hit_triggered, in_position, position_qty, entry_order_with_sl
    try:
        log_message("=" * 80, section="TRADE")
        log_message("🛑 STOP LOSS HIT - EXECUTING CLEANUP", section="TRADE")
        log_message("=" * 80, section="TRADE")

        log_message("Position already closed by exchange SL", section="TRADE")

        try:
            close_all_entry_orders()
        except Exception as e:
            log_message(f"Entry order cleanup: {e} (OK)", section="TRADE")

        sleep(0.5)

        log_message("Cancelling TP order and resetting state...", section="TRADE")
        cancel_all_exit_orders_and_reset("SL hit - position closed by exchange")

        with entry_order_sl_lock:
            entry_order_with_sl = None

        with sl_hit_lock:
            sl_hit_triggered = False

        log_message("=" * 80, section="TRADE")
        log_message("✓ SL CLEANUP COMPLETE", section="TRADE")
        log_message("=" * 80, section="TRADE")
    except Exception as e:
        log_message(f"Exception in handle_sl_hit: {e}", section="ERROR")
        try:
            cancel_all_exit_orders_and_reset("SL hit - forced reset")
        except:
            pass
        with sl_hit_lock:
            sl_hit_triggered = False
        with entry_order_sl_lock:
            entry_order_with_sl = None

# ================================================================================

def handle_order_update(orders_data):
    global pending_entry_order_id, in_position, tp_hit_triggered, sl_hit_triggered
    global entry_order_with_sl, position_qty

    try:
        if not isinstance(orders_data, list):
            orders_data = [orders_data]

        with position_lock:
            current_pending_id = pending_entry_order_id
            is_in_pos = in_position
            active_exit_orders = dict(position_exit_orders)
            tracked_qty = position_qty

        with entry_order_sl_lock:
            tracked_entry_order = entry_order_with_sl

        for order in orders_data:
            if not isinstance(order, dict):
                continue

            order_id = order.get('ordId') or order.get('algoId')
            status = order.get('state')
            symbol = order.get('instId')
            cum_qty = order.get('accFillSz', 0)
            order_qty = order.get('sz', 0)
            exec_status = order.get('execType', '')
            order_type = order.get('ordType', '')

            if not order_id or not status:
                continue

            if symbol and symbol != SYMBOL:
                continue

            if active_exit_orders.get('sl') and order_id == active_exit_orders.get('sl') and status in ['filled', 'partially_filled']:
                log_message("=" * 80, section="TRADE")
                log_message(f"🛑 SL HIT DETECTED via SL Order Fill!", section="TRADE")
                log_message(f"Order ID: {str(order_id)[:12]}...", section="TRADE")
                log_message(f"Status: {status} | ExecType: {exec_status}", section="TRADE")
                log_message("=" * 80, section="TRADE")

                with sl_hit_lock:
                    if not sl_hit_triggered:
                        sl_hit_triggered = True
                        threading.Timer(0.5, handle_sl_hit).start()
                return

            if current_pending_id and order_id == current_pending_id:
                with position_lock:
                    if pending_entry_order_details:
                        pending_entry_order_details['status'] = status
                        pending_entry_order_details['cumQty'] = cum_qty

                if status in ['filled', 'partially_filled'] or safe_float(cum_qty) > 0:
                    log_message("=" * 80, section="TRADE")
                    log_message(f"🎉 ENTRY FILLED: {cum_qty}/{order_qty} {SYMBOL}", section="TRADE")
                    log_message("=" * 80, section="TRADE")

                    if status in ['filled']:
                        threading.Timer(2.0, lambda: confirm_and_set_active_position(order_id)).start()
                    else:
                        threading.Timer(5.0, lambda: confirm_and_set_active_position(order_id)).start()
                    return

                elif status in ['canceled', 'live', 'failed'] and not is_in_pos:
                    log_message(f"❌ Entry order {status}", section="TRADE")
                    reset_entry_state(f"Entry order {status}")
                    with entry_order_sl_lock:
                        entry_order_with_sl = None
                    return

            elif is_in_pos and order_id == active_exit_orders.get('tp'):
                if status in ['filled', 'partially_filled'] or safe_float(cum_qty) > 0:
                    log_message("=" * 80, section="TRADE")
                    log_message(f"!!! TP HIT !!! {cum_qty}/{order_qty} {SYMBOL}", section="TRADE")
                    log_message("=" * 80, section="TRADE")

                    with tp_hit_lock:
                        if not tp_hit_triggered:
                            tp_hit_triggered = True
                            threading.Timer(0.5, handle_tp_hit).start()
                    break

    except Exception as e:
        log_message(f"Exception in handle_order_update: {e}", section="ERROR")
        import traceback
        traceback.print_exc()

# ================================================================================

def detect_sl_from_position_update(positions_msg):
    global in_position, position_qty, sl_hit_triggered, entry_order_with_sl

    try:
        if not isinstance(positions_msg, list):
            positions_msg = [positions_msg]

        with position_lock:
            was_in_position = in_position
            expected_qty = position_qty

        if not was_in_position or expected_qty == 0:
            return

        current_position_size = 0
        for pos in positions_msg:
            if pos.get('instId') == SYMBOL:
                size_rv = safe_float(pos.get('pos', 0))
                current_position_size = size_rv
                break

        if was_in_position and current_position_size == 0 and expected_qty > 0:
            log_message("=" * 80, section="TRADE")
            log_message("🛑 SL/CLOSURE DETECTED via Position Update!", section="TRADE")
            log_message(f"Expected Qty: {expected_qty} → Current Qty: 0", section="TRADE")
            log_message("=" * 80, section="TRADE")

            with sl_hit_lock:
                if not sl_hit_triggered:
                    sl_hit_triggered = True
                    with entry_order_sl_lock:
                        entry_order_with_sl = None
                    threading.Timer(0.1, handle_sl_hit).start()

    except Exception as e:
        log_message(f"Exception in detect_sl_from_position_update: {e}", section="ERROR")

# ================================================================================

def test_order_subscription():
    try:
        log_message("=" * 80, section="TEST")
        log_message("Testing order subscription...", section="TEST")
        log_message("=" * 80, section="TEST")

        if ws_subscriptions_ready.wait(timeout=10):
            log_message("✓ Subscriptions ready", section="TEST")
        else:
            log_message("✗ Subscription timeout", section="TEST")
            return

        log_message("Fetching active orders...", section="TEST")
        path = "/api/v5/trade/orders-pending"
        params = {"instType": "SWAP", "instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        if response and response.get('code') == '0':
            orders = response.get('data', [])
            log_message(f"Found {len(orders)} active orders", section="TEST")

            for order in orders[:3]:
                log_message(f"  Order: {order.get('ordId')[:12]}... - {order.get('state')} - {order.get('side')}", section="TEST")
        else:
            log_message("No active orders found", section="TEST")

        log_message("=" * 80, section="TEST")
    except Exception as e:
        log_message(f"Exception in test_order_subscription: {e}", section="ERROR")

# ================================================================================

def confirm_and_set_active_position(filled_order_id):
    global in_position, position_entry_price, position_qty
    global current_take_profit, current_stop_loss, pending_entry_order_id
    global position_exit_orders, entry_sl_price, entry_reduced_tp_flag

    try:
        log_message(f"Confirming OKX position...", section="TRADE")

        path = "/api/v5/account/positions"
        params = {"instType": "SWAP", "instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        entry_confirmed = False
        actual_entry_price = 0.0
        actual_qty = 0.0

        if response and response.get('code') == '0':
            positions = response.get('data', [])
            for pos in positions:
                if pos.get('instId') == SYMBOL:
                    size_rv = safe_float(pos.get('pos', 0))
                    if size_rv > 0:
                        avg_entry_price_rv = safe_float(pos.get('avgPx', 0))
                        actual_entry_price = avg_entry_price_rv
                        actual_qty = size_rv
                        entry_confirmed = True
                        break

        if not entry_confirmed or actual_entry_price <= 0:
            log_message("CRITICAL: Could not confirm OKX position!", section="ERROR")
            return

        reduced_tp = entry_reduced_tp_flag if 'entry_reduced_tp_flag' in globals() else False

        if reduced_tp:
            tp_price = actual_entry_price * (1 + TP_PERCENT_REDUCED / 100.0)
            tp_reason = f"Reduced TP {TP_PERCENT_REDUCED}%"
        else:
            tp_price = actual_entry_price * (1 + TP_PERCENT / 100.0)
            tp_reason = f"Normal TP {TP_PERCENT}%"

        sl_price = actual_entry_price * (1 - SL_PERCENT / 100.0)

        with position_lock:
            in_position = True
            position_entry_price = actual_entry_price
            position_qty = actual_qty
            current_take_profit = tp_price
            current_stop_loss = sl_price
            pending_entry_order_id = None
            position_exit_orders = {}

        log_message("=" * 80, section="TRADE")
        log_message("OKX POSITION OPENED", section="TRADE")
        log_message(f"Entry: ${actual_entry_price:.2f} | Qty: {actual_qty}", section="TRADE")
        log_message(f"TP: ${tp_price:.2f} ({tp_reason})", section="TRADE")
        log_message(f"SL: ${sl_price:.2f} (separate algo order)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        price_precision = PRODUCT_INFO.get('pricePrecision', 4)
        qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

        # Place TP and SL as algo (conditional) orders via /api/v5/trade/order-algo
        tp_body = {
            "instId": SYMBOL,
            "tdMode": "cross",
            "side": "sell",
            "posSide": "long",
            "ordType": "conditional",
            "sz": f"{actual_qty:.{qty_precision}f}",
            "tpTriggerPx": f"{tp_price:.{price_precision}f}",
            "tpOrdPx": "market",
            "reduceOnly": "true"
        }

        tp_order = okx_place_algo_order(tp_body)
        if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
            with position_lock:
                position_exit_orders['tp'] = tp_order.get('algoId') or tp_order.get('ordId')
            log_message(f"✓ TP algo order placed", section="TRADE")
        else:
            log_message(f"CRITICAL: TP algo order failed! Closing position", section="ERROR")
            execute_trade_exit("Failed to place TP")
            return

        sl_body = {
            "instId": SYMBOL,
            "tdMode": "cross",
            "side": "sell",
            "posSide": "long",
            "ordType": "conditional",
            "sz": f"{actual_qty:.{qty_precision}f}",
            "slTriggerPx": f"{sl_price:.{price_precision}f}",
            "slOrdPx": "market",
            "reduceOnly": "true"
        }

        sl_order = okx_place_algo_order(sl_body)
        if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
            with position_lock:
                position_exit_orders['sl'] = sl_order.get('algoId') or sl_order.get('ordId')
            log_message(f"✓ SL algo order placed", section="TRADE")
        else:
            log_message(f"CRITICAL: SL algo order failed! Closing position", section="ERROR")
            execute_trade_exit("Failed to place SL")
            return

        log_message("=" * 80, section="TRADE")
        log_message("✓ OKX POSITION CONFIGURED (SL and TP active)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        update_account_info()
    except Exception as e:
        log_message(f"Exception in confirm_and_set_active_position (OKX): {e}", section="ERROR")

# ================================================================================

def manage_position_lifecycle():
    try:
        log_message("Position lifecycle manager started", section="SYSTEM")
        log_message(f"EOD exit time: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")

        eod_triggered = False

        while not shutdown_flag.is_set():
            sleep(5)

            now_utc = datetime.datetime.now(datetime.timezone.utc)
            current_hour = now_utc.hour
            current_minute = now_utc.minute

            with position_lock:
                is_in_pos = in_position
                has_pending_entry = (pending_entry_order_id is not None)

            if not eod_triggered and (current_hour == EOD_EXIT_HOUR_UTC and current_minute >= EOD_EXIT_MINUTE_UTC):
                eod_triggered = True
                log_message(f"⚠️ EOD EXIT TIME REACHED: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")
                handle_eod_exit()
                sleep(5)
                with position_lock:
                    final_pos = in_position
                    final_pending = (pending_entry_order_id is not None)
                if not final_pos and not final_pending:
                    log_message("✓ EOD cleanup complete. Position manager exiting.", section="SYSTEM")
                    break

            if current_hour == 0 and current_minute == 0:
                eod_triggered = False

            if not is_in_pos and not has_pending_entry and not eod_triggered:
                log_message("Position manager exiting (no active position/order)", section="SYSTEM")
                break

        log_message("Position manager thread finished", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in manage_position_lifecycle: {e}", section="ERROR")

# ================================================================================

def execute_trade_exit(reason):
    try:
        log_message(f"=== MANUAL EXIT === Reason: {reason}", section="TRADE")

        with position_lock:
            if not in_position:
                log_message("Exit aborted: Not in position", section="TRADE")
                return
            qty_to_close = position_qty

        with position_lock:
            orders_to_cancel = list(position_exit_orders.values())

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    okx_cancel_algo_order(SYMBOL, order_id)
                    sleep(0.2)
                except Exception as e:
                    log_message(f"Error cancelling order: {e} (OK, continuing)", section="ERROR")

        try:
            log_message(f"Placing market sell for {qty_to_close} {SYMBOL}", section="TRADE")
            exit_order = okx_place_order(
                SYMBOL,
                "Sell",
                qty_to_close,
                order_type="Market",
                reduce_only=True
            )

            if not (exit_order and exit_order.get('ordId')):
                log_message(f"WARNING: Market exit order may have failed (OK if already closed)", section="TRADE")
        except Exception as e:
            log_message(f"Exception during market exit: {e} (OK, continuing)", section="ERROR")

        sleep(1)
        cancel_all_exit_orders_and_reset(reason)
    except Exception as e:
        log_message(f"Exception in execute_trade_exit: {e}", section="ERROR")

# ================================================================================

def check_and_close_any_open_position():
    try:
        log_message("Checking for any open OKX positions...", section="SYSTEM")
        path = "/api/v5/account/positions"
        params = {"instType": "SWAP", "instId": SYMBOL}
        response = okx_request("GET", path, params=params)

        if response and response.get('code') == '0':
            positions = response.get('data', [])
            for pos in positions:
                if pos.get('instId') == SYMBOL:
                    size_rv = safe_float(pos.get('pos', 0))
                    pos_side = pos.get('posSide') or pos.get('side')
                    if size_rv > 0:
                        log_message(f"⚠️ Found open {pos_side} OKX position: {size_rv} {SYMBOL}", section="SYSTEM")
                        close_side = "Sell" if size_rv > 0 else "Buy"
                        log_message(f"Closing {size_rv} {SYMBOL} with market {close_side} order", section="SYSTEM")
                        close_order = okx_place_order(
                            SYMBOL,
                            close_side,
                            size_rv,
                            order_type="Market",
                            reduce_only=True
                        )
                        if close_order and close_order.get('ordId'):
                            log_message(f"✓ Position close order placed", section="SYSTEM")
                            return True
                        else:
                            log_message(f"❌ Failed to place close order", section="ERROR")
                            return False

        log_message("No open OKX positions found", section="SYSTEM")
        return False
    except Exception as e:
        log_message(f"Exception in check_and_close_any_open_position (OKX): {e}", section="ERROR")
        return False

# ================================================================================

def reset_entry_state(reason):
    global pending_entry_order_id, entry_reduced_tp_flag, pending_entry_order_details
    global entry_order_with_sl
    try:
        with position_lock:
            pending_entry_order_id = None
            entry_reduced_tp_flag = False
            pending_entry_order_details = {}
        with entry_order_sl_lock:
            entry_order_with_sl = None
        log_message(f"Entry state reset. Reason: {reason}", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in reset_entry_state: {e}", section="ERROR")

def cancel_all_exit_orders_and_reset(reason):
    global in_position, position_entry_price, position_qty
    global current_take_profit, current_stop_loss, position_exit_orders
    global pending_entry_order_id, entry_reduced_tp_flag, entry_order_with_sl

    try:
        with position_lock:
            orders_to_cancel = list(position_exit_orders.values())

            in_position = False
            position_entry_price = 0.0
            position_qty = 0.0
            current_take_profit = 0.0
            current_stop_loss = 0.0
            position_exit_orders = {}
            pending_entry_order_id = None
            entry_reduced_tp_flag = False

        with entry_order_sl_lock:
            entry_order_with_sl = None

        log_message("=" * 80, section="TRADE")
        log_message(f"POSITION CLOSED - Reason: {reason}", section="TRADE")
        log_message("=" * 80, section="TRADE")

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    okx_cancel_algo_order(SYMBOL, order_id)
                    sleep(0.1)
                except Exception as e:
                    log_message(f"Error cancelling order: {e} (OK, continuing)", section="ERROR")

        update_account_info()
    except Exception as e:
        log_message(f"Exception in cancel_all_exit_orders_and_reset: {e}", section="ERROR")

# ================================================================================

def get_latest_data_and_indicators():
    try:
        with data_lock:
            daily_df = historical_data_store.get(TIMEFRAME_CPR)

        if daily_df is None or len(daily_df) < 2:
            log_message(f"Insufficient daily data: got {len(daily_df) if daily_df is not None else 0} candles", section="ERROR")
            return None

        # CRITICAL SAFETY CHECK for data staleness
        now_utc = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(milliseconds=server_time_offset)
        today = now_utc.date()
        last_candle_date = daily_df.index[-1].date()

        if (today - last_candle_date).days > 1:
            log_message(message=f"CRITICAL STALENESS: Last available data is from {last_candle_date}, but today is {today}. Aborting trade check.", section="CRITICAL_ERROR")
            return None

        # To be robust against API data lag, use the last two candles available.
        # [-2] is the last fully closed candle ("previous day").
        # [-1] is the current, incomplete candle ("today").
        prev_candle = daily_df.iloc[-2]
        current_candle = daily_df.iloc[-1]

        prev_date = prev_candle.name.date()
        current_date = current_candle.name.date()

        # Sanity check for large gaps, though we proceed anyway.
        if (current_date - prev_date).days > 2:
             log_message(f"Warning: Large gap between last two candles. Previous: {prev_date}, Current: {current_date}", section="CALC")


        prev_open = prev_candle['Open']
        prev_high = prev_candle['High']
        prev_low = prev_candle['Low']
        prev_close = prev_candle['Close']
        daily_open = current_candle['Open']

        if prev_high < prev_low:
            log_message(f"CRITICAL: Data corruption! Prev High < Low on {prev_date}", section="ERROR")
            return None

        daily_cpr = calculate_cpr_levels(prev_high, prev_low, prev_close)

        if not daily_cpr:
            log_message("Failed to calculate CPR levels", section="ERROR")
            return None

        # Indicators are calculated on the full history, result is based on the last candle.
        indicator_results_df = calculate_indicators(daily_df.copy())

        if indicator_results_df is None or indicator_results_df.empty:
            log_message("Failed to calculate indicators", section="ERROR")
            return None

        latest_indicators = indicator_results_df.iloc[-1]
        ema_21 = latest_indicators['EMA_21']
        ema_50 = latest_indicators['EMA_50']

        log_message("=" * 80, section="CALC")
        log_message("MARKET DATA CALCULATION", section="CALC")
        log_message("=" * 80, section="CALC")
        log_message(f"Previous Day ({prev_date}):", section="CALC")
        log_message(f"  Open={prev_open:.2f}, High={prev_high:.2f}, Low={prev_low:.2f}, Close={prev_close:.2f}", section="CALC")
        log_message(f"Current Day ({current_date}): Open={daily_open:.2f}", section="CALC")
        log_message(f"CPR TC={daily_cpr['TC']:.2f}, P={daily_cpr['P']:.2f}, BC={daily_cpr['BC']:.2f}", section="CALC")
        log_message(f"EMA 21={ema_21:.2f}, EMA 50={ema_50:.2f}", section="CALC")
        log_message("=" * 80, section="CALC")

        return {
            'daily_cpr': daily_cpr,
            'daily_open': daily_open,
            'ema_21': ema_21,
            'ema_50': ema_50,
            'prev_daily_high': prev_high,
            'prev_daily_low': prev_low,
            'prev_daily_close': prev_close,
        }

    except Exception as e:
        log_message(f"Exception in get_latest_data_and_indicators: {e}", section="ERROR")
        return None

# ================================================================================

def manual_test_entry():
    global manual_test_order_placed, manual_test_tp_scheduled, manual_test_eod_scheduled

    try:
        log_message("=" * 80, section="TEST")
        log_message("🧪 MANUAL TEST MODE - INITIATING IMMEDIATE ENTRY", section="TEST")
        log_message("=" * 80, section="TEST")

        current_price = get_current_market_price()

        if not current_price:
            log_message("❌ Failed to get current price. Cannot proceed with test.", section="TEST")
            return

        offset_percent = MANUAL_TEST_CONFIG.get('price_offset_percent', -0.5)
        entry_price = current_price * (1 + offset_percent / 100.0)

        log_message(f"Current Price: ${current_price:.2f}", section="TEST")
        log_message(f"Entry Price (offset {offset_percent}%): ${entry_price:.2f}", section="TEST")

        calculated_data = {
            'daily_cpr': {'TC': entry_price, 'P': entry_price, 'BC': entry_price * 0.99},
            'daily_open': entry_price * 1.01,
            'ema_21': entry_price,
            'ema_50': entry_price * 0.99,
            'prev_daily_high': entry_price * 1.05,
            'prev_daily_low': entry_price * 0.95,
            'prev_daily_close': entry_price,
        }

        log_message("Placing test entry order...", section="TEST")
        initiate_entry_sequence(calculated_data, entry_price, False)

        manual_test_order_placed = True

        if MANUAL_TEST_CONFIG.get('test_tp_after_seconds'):
            test_tp_delay = MANUAL_TEST_CONFIG['test_tp_after_seconds']
            log_message(f"📅 TP hit test scheduled in {test_tp_delay} seconds", section="TEST")
            manual_test_tp_scheduled = True

        if MANUAL_TEST_CONFIG.get('test_eod_after_seconds'):
            test_eod_delay = MANUAL_TEST_CONFIG['test_eod_after_seconds']
            log_message(f"📅 EOD exit test scheduled in {test_eod_delay} seconds", section="TEST")
            manual_test_eod_scheduled = True

        log_message("=" * 80, section="TEST")
        log_message("✓ MANUAL TEST ORDER PLACED", section="TEST")
        log_message("Monitor the logs for order fills and TP/SL placement", section="TEST")
        log_message("=" * 80, section="TEST")

    except Exception as e:
        log_message(f"Exception in manual_test_entry: {e}", section="ERROR")

def manual_test_monitor():
    global manual_test_order_placed, manual_test_tp_scheduled, manual_test_eod_scheduled

    try:
        start_time = time()
        tp_triggered = False
        eod_triggered = False

        log_message("🧪 Manual test monitor started", section="TEST")

        while not shutdown_flag.is_set():
            sleep(1)
            elapsed = time() - start_time

            with position_lock:
                is_in_pos = in_position

            if (manual_test_tp_scheduled and not tp_triggered and
                elapsed >= MANUAL_TEST_CONFIG.get('test_tp_after_seconds', 30) and is_in_pos):
                tp_triggered = True
                log_message("=" * 80, section="TEST")
                log_message("🧪 MANUAL TEST: Simulating TP HIT", section="TEST")
                log_message("=" * 80, section="TEST")
                threading.Timer(1.0, handle_tp_hit).start()

            if (manual_test_eod_scheduled and not eod_triggered and
                elapsed >= MANUAL_TEST_CONFIG.get('test_eod_after_seconds', 60)):
                eod_triggered = True
                log_message("=" * 80, section="TEST")
                log_message("🧪 MANUAL TEST: Simulating EOD EXIT", section="TEST")
                log_message("=" * 80, section="TEST")
                threading.Timer(1.0, handle_eod_exit).start()
                sleep(10)
                break

        log_message("🧪 Manual test monitor finished", section="TEST")
    except Exception as e:
        log_message(f"Exception in manual_test_monitor: {e}", section="ERROR")

# ================================================================================

def live_position_monitor():
    try:
        log_message("Live monitor started", section="SYSTEM")

        while not shutdown_flag.is_set():
            sleep(60)

            now_utc = datetime.datetime.now(datetime.timezone.utc)

            with position_lock:
                is_in_pos = in_position
                has_pending = (pending_entry_order_id is not None)
                pos_entry = position_entry_price
                pos_qty_val = position_qty
                tp_val = current_take_profit
                sl_val = current_stop_loss

            next_entry_check = datetime.datetime(
                now_utc.year, now_utc.month, now_utc.day,
                DAILY_ENTRY_CHECK_HOUR, DAILY_ENTRY_CHECK_MINUTE, DAILY_ENTRY_CHECK_SECOND,
                tzinfo=datetime.timezone.utc
            )

            if now_utc >= next_entry_check:
                next_entry_check += datetime.timedelta(days=1)

            time_until_entry = next_entry_check - now_utc
            hours_entry = int(time_until_entry.total_seconds() // 3600)
            minutes_entry = int((time_until_entry.total_seconds() % 3600) // 60)
            seconds_entry = int(time_until_entry.total_seconds() % 60)

            next_eod_exit = datetime.datetime(
                now_utc.year, now_utc.month, now_utc.day,
                EOD_EXIT_HOUR_UTC, EOD_EXIT_MINUTE_UTC, 0,
                tzinfo=datetime.timezone.utc
            )

            if now_utc >= next_eod_exit:
                next_eod_exit += datetime.timedelta(days=1)

            time_until_exit = next_eod_exit - now_utc
            hours_exit = int(time_until_exit.total_seconds() // 3600)
            minutes_exit = int((time_until_exit.total_seconds() % 3600) // 60)
            seconds_exit = int(time_until_exit.total_seconds() % 60)

            with trade_data_lock:
                current_price = latest_trade_price

            log_message("=" * 80, section="MONITOR")
            log_message("🤖 BOT STATUS", section="MONITOR")
            log_message("=" * 80, section="MONITOR")
            log_message(f"Time: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}", section="MONITOR")
            log_message(f"Mode: {TEST_MODE}", section="MONITOR")

            if current_price:
                log_message(f"Current Price: ${current_price:.2f}", section="MONITOR")

            log_message("-" * 80, section="MONITOR")
            log_message(f"Position: {'🟢 ACTIVE' if is_in_pos else '⚪ NONE'}", section="MONITOR")
            log_message(f"Pending Entry Order: {'🟡 YES' if has_pending else '⚪ NO'}", section="MONITOR")

            if is_in_pos:
                qty_prec = PRODUCT_INFO.get('qtyPrecision', 8)
                log_message(f"Entry Price: ${pos_entry:.2f} | Qty: {pos_qty_val:.{qty_prec}f}", section="MONITOR")
                log_message(f"TP: ${tp_val:.2f} | SL: ${sl_val:.2f}", section="MONITOR")

                if current_price and pos_entry > 0:
                    pnl_pct = (current_price - pos_entry) / pos_entry * 100
                    log_message(f"Unrealized P&L: {pnl_pct:+.2f}%", section="MONITOR")

            if TEST_MODE != "MANUAL":
                log_message("-" * 80, section="MONITOR")
                log_message(f"Next Entry Check: {next_entry_check.strftime('%H:%M:%S UTC')}", section="MONITOR")
                log_message(f"Time Until: {hours_entry:02d}h {minutes_entry:02d}m {seconds_entry:02d}s", section="MONITOR")
                log_message("-" * 80, section="MONITOR")
                log_message(f"Next EOD Exit: {next_eod_exit.strftime('%H:%M:%S UTC')}", section="MONITOR")
                log_message(f"Time Until: {hours_exit:02d}h {minutes_exit:02d}m {seconds_exit:02d}s", section="MONITOR")

            log_message("=" * 80, section="MONITOR")

    except Exception as e:
        log_message(f"Exception in live_position_monitor: {e}", section="ERROR")

# ================================================================================

def cleanup_memory():
    while not shutdown_flag.is_set():
        sleep(3600)
        gc.collect()
        log_message(f"Memory cleaned: {gc.get_count()[0]} objects collected", section="SYSTEM")

# ================================================================================

def process_new_candle_and_check_entry():
    try:
        log_message("=" * 80, section="TRADE")
        log_message(f"🕐 NEW DAILY CANDLE - Entry Check @ {datetime.datetime.now(datetime.timezone.utc)}", section="TRADE")
        log_message("=" * 80, section="TRADE")

        calculated_data = get_latest_data_and_indicators()

        if not calculated_data:
            log_message("Failed to calculate data. Skipping entry check.", section="ERROR")
            return

        with position_lock:
            is_in_pos = in_position
            has_pending = (pending_entry_order_id is not None)

        if is_in_pos or has_pending:
            log_message("Skipping entry: Already in position or pending order exists", section="TRADE")
            return

        log_message("Checking entry conditions...", section="TRADE")
        entry_signal, limit_price, reduced_tp = check_entry_conditions(calculated_data)

        if entry_signal:
            log_message(f">>> ENTRY SIGNAL <<< Limit order at TC: ${limit_price:.2f}", section="TRADE")
            initiate_entry_sequence(calculated_data, limit_price, reduced_tp)
        else:
            log_message("No entry signal. Waiting for next check.", section="TRADE")
    except Exception as e:
        log_message(f"Exception in process_new_candle_and_check_entry: {e}", section="ERROR")

def main_trading_logic():
    try:
        log_message("=== MAIN TRADING LOGIC STARTED ===", section="SYSTEM")

        if TEST_MODE != "MANUAL":
            log_message(f"Daily entry check time: {DAILY_ENTRY_CHECK_HOUR:02d}:{DAILY_ENTRY_CHECK_MINUTE:02d}:{DAILY_ENTRY_CHECK_SECOND:02d} UTC", section="SYSTEM")
        else:
            log_message("Manual test mode - entry check handled by test thread", section="SYSTEM")

        last_check_date = None

        while not shutdown_flag.is_set():
            try:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_date = now_utc.date()

                if TEST_MODE == "MANUAL":
                    sleep(5)
                    continue

                if (now_utc.hour == DAILY_ENTRY_CHECK_HOUR and
                    now_utc.minute == DAILY_ENTRY_CHECK_MINUTE and
                    now_utc.second >= DAILY_ENTRY_CHECK_SECOND and
                    current_date != last_check_date):

                    log_message(f"Daily entry check triggered at {now_utc.strftime('%H:%M:%S UTC')}", section="SYSTEM")
                    process_new_candle_and_check_entry()
                    last_check_date = current_date

                sleep(1)

            except Exception as e:
                log_message(f"Error in trading logic loop: {e} (continuing)", section="ERROR")
                sleep(30)

    except Exception as e:
        log_message(f"CRITICAL ERROR in main_trading_logic: {e}", section="ERROR")

# ================================================================================

def main():
    global bot_startup_complete, trading_logic_thread, position_manager_thread, live_monitor_thread, manual_test_thread

    log_message("================================================================================", section="SYSTEM")
    log_message("                                OKX SOL BOT STARTING                            ", section="SYSTEM")
    log_message("================================================================================", section="SYSTEM")

    try:
        log_message("Synchronizing OKX server time...", section="SYSTEM")
        if not get_okx_server_time_and_offset():
            log_message("Failed to synchronize server time. Please check network connection or API.", section="CRITICAL_ERROR")
            return

        # Account mode gating removed: proceed (check_account_mode returns True)
        log_message("Checking account mode...", section="SYSTEM")
        if not check_account_mode():
            log_message("Account not configured for derivatives trading. Please adjust settings on OKX.", section="CRITICAL_ERROR")
            return

        log_message(f"Fetching product info for {SYMBOL}...", section="SYSTEM")
        if not fetch_product_info(SYMBOL):
            log_message("Failed to fetch product info. Exiting.", section="CRITICAL_ERROR")
            return

        log_message(f"Setting leverage to {LEVERAGE}x for {SYMBOL}...", section="SYSTEM")
        if not okx_set_leverage(SYMBOL, LEVERAGE):
            log_message("Failed to set leverage. Exiting.", section="CRITICAL_ERROR")
            return

        log_message("Fetching initial historical data...", section="SYSTEM")
        adjusted_now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(milliseconds=server_time_offset)
        today = adjusted_now.date()
        start_date = today - datetime.timedelta(days=HISTORICAL_DATA_MONTHS * 30)

        if not fetch_initial_historical_data(SYMBOL, TIMEFRAME_CPR, start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')):
            log_message("Failed to fetch initial daily historical data. Exiting.", section="CRITICAL_ERROR")
            return
        if not fetch_initial_historical_data(SYMBOL, TIMEFRAME_TRADE, start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')):
            log_message("Failed to fetch initial trade timeframe historical data. Exiting.", section="CRITICAL_ERROR")
            return

        log_message("Initializing WebSocket connection...", section="SYSTEM")
        ws_client = initialize_websocket()
        if ws_client is None:
            log_message("Failed to initialize WebSocket. Exiting.", section="CRITICAL_ERROR")
            return

        log_message("Waiting for WebSocket subscriptions to be ready...", section="SYSTEM")
        if not ws_subscriptions_ready.wait(timeout=20):
            log_message("WebSocket subscriptions not ready within timeout. Exiting.", section="CRITICAL_ERROR")
            return

        log_message("Updating initial account info...", section="SYSTEM")
        if not update_account_info():
            log_message("Failed to update initial account info. Exiting.", section="CRITICAL_ERROR")
            return

        log_message("Checking for and closing any existing open positions...", section="SYSTEM")
        check_and_close_any_open_position()

        bot_startup_complete = True
        log_message("Bot startup sequence complete.", section="SYSTEM")

        if TEST_MODE == "MANUAL":
            manual_test_thread = threading.Thread(target=manual_test_entry, name="ManualTest", daemon=True)
            manual_test_thread.start()
            manual_test_monitor_thread = threading.Thread(target=manual_test_monitor, name="ManualTestMonitor", daemon=True)
            manual_test_monitor_thread.start()

            manual_test_thread.join()
            manual_test_monitor_thread.join()
        else:
            trading_logic_thread = threading.Thread(target=main_trading_logic, name="TradingLogic", daemon=True)
            trading_logic_thread.start()

            live_monitor_thread = threading.Thread(target=live_position_monitor, name="LiveMonitor", daemon=True)
            live_monitor_thread.start()

            cleanup_thread = threading.Thread(target=cleanup_memory, name="GarbageCollector", daemon=True)
            cleanup_thread.start()

            while not shutdown_flag.is_set():
                sleep(1)

    except KeyboardInterrupt:
        log_message("Shutdown initiated by user (KeyboardInterrupt)", section="SYSTEM")
    except Exception as e:
        log_message(f"CRITICAL ERROR in main execution: {e}", section="CRITICAL_ERROR")
        import traceback
        traceback.print_exc()
    finally:
        shutdown_flag.set()
        log_message("Shutting down...", section="SYSTEM")
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        log_message("OKX SOL BOT SHUTDOWN COMPLETE", section="SYSTEM")

if __name__ == "__main__":
    main()
