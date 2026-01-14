#!/usr/bin/env python3
"""
ICT PRO BOT V7.5 - AWS LAMBDA SERVERLESS
✅ DynamoDB State Management | ✅ Mangum Lambda Handler
✅ No Background Threads | ✅ Stateless Architecture
"""

from flask import Flask, request, jsonify, redirect
import requests
import json
from datetime import datetime, time as dt_time
import logging
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import pytz
from mangum import Mangum

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage" if TELEGRAM_TOKEN else ""

# Upstox Credentials
UPSTOX_API_KEY = os.environ.get("UPSTOX_API_KEY")
UPSTOX_API_SECRET = os.environ.get("UPSTOX_API_SECRET")
# Lambda uses API Gateway URL
UPSTOX_REDIRECT_URI = os.environ.get("UPSTOX_REDIRECT_URI", "https://your-api-gateway-url.execute-api.region.amazonaws.com/prod/callback")

# Trading Configuration
MAX_ORDER_RETRIES = 3
ORDER_FILL_TIMEOUT = 30

# AWS DynamoDB
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "TradingBotState")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ict-pro-bot-v7-5-lambda'

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# DYNAMODB STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def decimal_to_float(obj):
    """Convert DynamoDB Decimal to float recursively"""
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj

def float_to_decimal(obj):
    """Convert float to Decimal for DynamoDB"""
    if isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def save_token_to_db(access_token, generated_at):
    """Save access token to DynamoDB"""
    try:
        table.put_item(Item={
            'state_type': 'token',
            'state_key': 'current',
            'data': {
                'access_token': access_token,
                'generated_at': generated_at
            },
            'updated_at': int(datetime.now().timestamp())
        })
        logger.info("✅ Token saved to DynamoDB")
        return True
    except Exception as e:
        logger.error(f"❌ Token save failed: {e}")
        return False

def get_token_from_db():
    """Retrieve access token from DynamoDB"""
    try:
        response = table.get_item(Key={
            'state_type': 'token',
            'state_key': 'current'
        })
        
        if 'Item' in response:
            data = response['Item']['data']
            access_token = data.get('access_token')
            generated_at = float(data.get('generated_at', 0))
            
            # Check if token is still valid (< 20 hours)
            hours_elapsed = (datetime.now().timestamp() - generated_at) / 3600
            if hours_elapsed < 20:
                return access_token
        
        return None
    except Exception as e:
        logger.error(f"❌ Token retrieval failed: {e}")
        return None

def save_position_to_db(symbol, position_data):
    """Save single position to DynamoDB"""
    try:
        # Convert floats to Decimal for DynamoDB
        position_decimal = float_to_decimal(position_data)
        
        table.put_item(Item={
            'state_type': 'position',
            'state_key': symbol,
            'data': position_decimal,
            'updated_at': int(datetime.now().timestamp())
        })
        logger.info(f"✅ Position saved: {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ Position save failed {symbol}: {e}")
        return False

def get_position_from_db(symbol):
    """Retrieve single position from DynamoDB"""
    try:
        response = table.get_item(Key={
            'state_type': 'position',
            'state_key': symbol
        })
        
        if 'Item' in response:
            return decimal_to_float(response['Item']['data'])
        return None
    except Exception as e:
        logger.error(f"❌ Position retrieval failed {symbol}: {e}")
        return None

def get_all_positions_from_db():
    """Retrieve all positions from DynamoDB"""
    try:
        response = table.query(
            KeyConditionExpression=Key('state_type').eq('position')
        )
        
        positions = {}
        for item in response.get('Items', []):
            symbol = item['state_key']
            positions[symbol] = decimal_to_float(item['data'])
        
        logger.info(f"✅ Loaded {len(positions)} positions from DynamoDB")
        return positions
    except Exception as e:
        logger.error(f"❌ Positions retrieval failed: {e}")
        return {}

def delete_position_from_db(symbol):
    """Delete position from DynamoDB"""
    try:
        table.delete_item(Key={
            'state_type': 'position',
            'state_key': symbol
        })
        logger.info(f"✅ Position deleted: {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ Position deletion failed {symbol}: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════
# MARKET HOURS VALIDATION
# ═══════════════════════════════════════════════════════════════════════════
def is_market_open():
    """Check if NSE market is open (9:15 AM - 3:30 PM IST, Mon-Fri)"""
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        if now.weekday() >= 5:
            logger.warning("⚠️ Market closed: Weekend")
            return False
        
        market_open = dt_time(9, 15)
        market_close = dt_time(15, 30)
        current_time = now.time()
        
        if current_time < market_open:
            logger.warning(f"⚠️ Market not yet open (opens at 9:15 AM IST)")
            return False
        
        if current_time > market_close:
            logger.warning(f"⚠️ Market closed (closed at 3:30 PM IST)")
            return False
        
        return True
    except Exception as e:
        logger.error(f"❌ Market hours check failed: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════
def send_telegram_message(message, parse_mode='HTML'):
    """Send Telegram notification"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    try:
        payload = {
            'chat_id': CHAT_ID,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': True
        }
        response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False

def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        if value is None or (isinstance(value, str) and ("{" in str(value) or "}" in str(value))):
            return default
        return float(value)
    except:
        return default

def format_buy_alert(data):
    """Format BUY signal alert message"""
    symbol = data.get('symbol', 'N/A')
    price = safe_float(data.get('price'))
    sl = safe_float(data.get('sl'))
    tp = safe_float(data.get('tp'))
    partial_tp = safe_float(data.get('partial_tp'))
    qty = safe_float(data.get('qty'))
    risk = safe_float(data.get('risk'))
    rr = safe_float(data.get('rr'), 1)
    confluence = data.get('confluence', 0)
    regime = data.get('regime', 'N/A')
    killzone = data.get('killzone', 'N/A')

    risk_amount = abs(price - sl)
    reward_amount = abs(tp - price)

    message = f"""
🚨 <b>NEW BUY SIGNAL</b> 🚨
━━━━━━━━━━━━━━━━━━━━━
📊 <b>{symbol}</b>
💰 <b>Entry:</b> ₹{price:.2f}
🔻 <b>Stop Loss:</b> ₹{sl:.2f} (-{risk_amount:.2f})
🎯 <b>Partial TP (50%):</b> ₹{partial_tp:.2f}
🔺 <b>Full TP:</b> ₹{tp:.2f} (+{reward_amount:.2f})

💼 <b>Position Details:</b>
• Quantity: {qty:.0f}
• Risk Amount: ₹{risk:.2f}
• Risk-Reward: 1:{rr:.2f}

🎯 <b>Analysis:</b>
• Market Regime: {regime}
• Confluence Score: {confluence}/15
• Kill Zone: {killzone}

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

✅ <b>EXECUTING BUY ORDER...</b>
━━━━━━━━━━━━━━━━━━━━━
"""
    return message.strip()

def format_sell_alert(data):
    """Format SELL signal alert message"""
    symbol = data.get('symbol', 'N/A')
    price = safe_float(data.get('price'))
    sl = safe_float(data.get('sl'))
    tp = safe_float(data.get('tp'))
    partial_tp = safe_float(data.get('partial_tp'))
    qty = safe_float(data.get('qty'))
    risk = safe_float(data.get('risk'))
    rr = safe_float(data.get('rr'), 1)
    confluence = data.get('confluence', 0)
    regime = data.get('regime', 'N/A')
    killzone = data.get('killzone', 'N/A')

    risk_amount = abs(sl - price)
    reward_amount = abs(price - tp)

    message = f"""
⚠️ <b>NEW SELL SIGNAL</b> ⚠️
━━━━━━━━━━━━━━━━━━━━━
📊 <b>{symbol}</b>
💰 <b>Entry:</b> ₹{price:.2f}
🔺 <b>Stop Loss:</b> ₹{sl:.2f} (+{risk_amount:.2f})
🎯 <b>Partial TP (50%):</b> ₹{partial_tp:.2f}
🔻 <b>Full TP:</b> ₹{tp:.2f} (-{reward_amount:.2f})

💼 <b>Position Details:</b>
• Quantity: {qty:.0f}
• Risk Amount: ₹{risk:.2f}
• Risk-Reward: 1:{rr:.2f}

🎯 <b>Analysis:</b>
• Market Regime: {regime}
• Confluence Score: {confluence}/15
• Kill Zone: {killzone}

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

❌ <b>EXECUTING SELL ORDER...</b>
━━━━━━━━━━━━━━━━━━━━━
"""
    return message.strip()

# ═══════════════════════════════════════════════════════════════════════════
# UPSTOX TOKEN MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════
def generate_access_token(auth_code):
    """Generate Upstox access token from authorization code"""
    url = "https://api.upstox.com/v2/login/authorization/token"
    data = {
        'code': auth_code,
        'client_id': UPSTOX_API_KEY,
        'client_secret': UPSTOX_API_SECRET,
        'redirect_uri': UPSTOX_REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        response = requests.post(url, data=data, headers=headers, timeout=10)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data['access_token']
            generated_at = datetime.now().timestamp()
            
            # Save to DynamoDB
            save_token_to_db(access_token, generated_at)
            
            logger.info("✅ Upstox Access Token Generated Successfully!")
            send_telegram_message("✅ <b>Upstox Token Auto-Generated!</b>\nBot अब live trading के लिए ready है।")
            return True
        else:
            logger.error(f"❌ Token generation failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Token generation error: {e}")
        return False

def get_token():
    """Get valid token from DynamoDB"""
    return get_token_from_db()

# ═══════════════════════════════════════════════════════════════════════════
# INSTRUMENT KEY LOADER (Cached in Lambda memory for efficiency)
# ═══════════════════════════════════════════════════════════════════════════
instruments_dict = {}

def load_instruments():
    """Load NSE instrument keys from Upstox (cached in Lambda execution context)"""
    global instruments_dict
    
    # If already loaded in this Lambda execution, skip
    if instruments_dict:
        return
    
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        response = requests.get(url, timeout=30)
        
        import gzip
        import io
        data = json.loads(gzip.decompress(response.content))
        
        for key, info in data.items():
            if info.get('instrument_type') == 'EQUITY' and info.get('exchange') == 'NSE':
                trading_symbol = info['trading_symbol'].upper()
                instruments_dict[trading_symbol] = key
                if trading_symbol.endswith('-EQ'):
                    base_symbol = trading_symbol.replace('-EQ', '')
                    instruments_dict[base_symbol] = key
        
        logger.info(f"✅ Loaded {len(instruments_dict)} NSE instruments")
    except Exception as e:
        logger.error(f"❌ Instruments load failed: {e}")

def get_instrument_key(symbol):
    """Get Upstox instrument key for symbol"""
    # Load instruments if not cached
    if not instruments_dict:
        load_instruments()
    
    symbol_clean = symbol.upper().replace("NSE:", "").strip()
    
    if symbol_clean in instruments_dict:
        return instruments_dict[symbol_clean]
    
    if not symbol_clean.endswith('-EQ'):
        eq_symbol = f"{symbol_clean}-EQ"
        if eq_symbol in instruments_dict:
            return instruments_dict[eq_symbol]
    
    logger.error(f"❌ Instrument not found: {symbol_clean}")
    return None

# ═══════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════
def get_order_status(order_id):
    """Get current status of an order"""
    token = get_token()
    if not token or not order_id:
        return None
    
    try:
        url = f"https://api.upstox.com/v2/order/details?order_id={order_id}"
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('data', {}).get('status')
        return None
    except Exception as e:
        logger.error(f"Order status check failed {order_id}: {e}")
        return None

def verify_order_fill(order_id, timeout=ORDER_FILL_TIMEOUT):
    """Wait and verify if order is filled"""
    if not order_id:
        return False, 0
    
    import time
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        status = get_order_status(order_id)
        
        if status == "complete":
            logger.info(f"✅ Order {order_id} FILLED")
            return True, get_filled_quantity(order_id)
        elif status in ["rejected", "cancelled"]:
            logger.error(f"❌ Order {order_id} {status.upper()}")
            return False, 0
        
        time.sleep(2)
    
    logger.warning(f"⚠️ Order {order_id} fill timeout")
    return False, 0

def get_filled_quantity(order_id):
    """Get actual filled quantity from order"""
    token = get_token()
    if not token or not order_id:
        return 0
    
    try:
        url = f"https://api.upstox.com/v2/order/details?order_id={order_id}"
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            order_data = data.get('data', {})
            return int(order_data.get('filled_quantity', 0))
        return 0
    except Exception as e:
        logger.error(f"Get filled qty failed {order_id}: {e}")
        return 0

def place_order(order_data, label="Order", retry_count=0):
    """Place order with retry logic"""
    token = get_token()
    if not token:
        logger.error("❌ Cannot place order: Token missing")
        return {"success": False, "error": "Token missing", "order_id": None}

    url = "https://api.upstox.com/v2/order/place"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, json=order_data, timeout=10)
        result = response.json()
        order_id = result.get('data', {}).get('order_id')
        success = response.status_code == 200 and result.get('status') == 'success'
        
        if success:
            logger.info(f"✅ {label} SUCCESS | ID: {order_id}")
            if TELEGRAM_TOKEN:
                qty = order_data.get('quantity')
                trans_type = order_data.get('transaction_type')
                send_telegram_message(f"✅ {label}: {trans_type} {qty} | ID: {order_id}")
        else:
            logger.error(f"❌ {label} FAILED | Response: {result}")
            if retry_count < MAX_ORDER_RETRIES:
                logger.info(f"🔄 Retrying... ({retry_count + 1}/{MAX_ORDER_RETRIES})")
                import time
                time.sleep(2)
                return place_order(order_data, label, retry_count + 1)
        
        return {
            "success": success,
            "order_id": order_id,
            "raw": result
        }
    except Exception as e:
        logger.error(f"❌ {label} exception: {e}")
        if retry_count < MAX_ORDER_RETRIES:
            import time
            time.sleep(2)
            return place_order(order_data, label, retry_count + 1)
        return {"success": False, "error": str(e), "order_id": None}

def cancel_order(order_id):
    """Cancel an order"""
    if not order_id or not get_token():
        return False
    try:
        url = f"https://api.upstox.com/v2/order/cancel?order_id={order_id}"
        headers = {'Authorization': f'Bearer {get_token()}'}
        response = requests.delete(url, headers=headers, timeout=10)
        if response.status_code == 200:
            logger.info(f"✅ Cancelled order: {order_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Cancel failed {order_id}: {e}")
        return False

def emergency_exit_position(symbol, quantity, action):
    """Emergency market exit for unprotected positions"""
    logger.critical(f"🚨 EMERGENCY EXIT: {symbol} | Qty: {quantity}")
    
    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        logger.error(f"❌ Emergency exit failed: Symbol not found")
        return False
    
    exit_action = "SELL" if action == "BUY" else "BUY"
    exit_order = {
        "quantity": quantity,
        "product": "I",
        "validity": "DAY",
        "price": 0,
        "instrument_token": instrument_key,
        "order_type": "MARKET",
        "transaction_type": exit_action,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False
    }
    
    result = place_order(exit_order, "EMERGENCY EXIT")
    
    if result["success"]:
        send_telegram_message(f"🚨 <b>EMERGENCY EXIT EXECUTED</b>\n\nSymbol: {symbol}\nQuantity: {quantity}\nReason: SL placement failed")
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════
# WEBHOOK HANDLER
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'No data'}), 400

        action = data.get('action', '').upper()
        symbol_raw = data.get('symbol', '')
        symbol = symbol_raw.replace("-EQ", "").replace("NSE:", "").strip().upper()
        qty_requested = max(1, int(round(safe_float(data.get('qty', 1)))))
        sl_price = safe_float(data.get('sl'))
        tp_price = safe_float(data.get('tp'))
        partial_tp_price = safe_float(data.get('partial_tp'))

        if action not in ["BUY", "SELL"]:
            return jsonify({'error': 'Invalid action'}), 400

        if not is_market_open():
            logger.warning(f"⚠️ Order rejected: Market closed")
            send_telegram_message(f"⚠️ <b>Order Rejected</b>\n\nMarket is closed. Signal: {action} {symbol}")
            return jsonify({'error': 'Market closed'}), 400

        instrument_key = get_instrument_key(symbol)
        if not instrument_key:
            return jsonify({'error': f'Symbol {symbol} not found in NSE_EQ'}), 400

        opposite_action = "SELL" if action == "BUY" else "BUY"

        # Check if position exists in DB
        existing_position = get_position_from_db(symbol)
        if existing_position:
            logger.info(f"🔄 REVERSAL: Squaring off {symbol}")
            
            # Cancel all pending orders
            for oid in [existing_position.get('sl_order_id'), existing_position.get('tp_order_id'), existing_position.get('partial_order_id')]:
                if oid:
                    cancel_order(oid)
            
            # Market exit
            exit_order = {
                "quantity": existing_position['filled_qty'],
                "product": "I",
                "validity": "DAY",
                "price": 0,
                "instrument_token": instrument_key,
                "order_type": "MARKET",
                "transaction_type": opposite_action,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False
            }
            place_order(exit_order, "REVERSAL EXIT")
            delete_position_from_db(symbol)

        # Send entry alert
        if action == "BUY":
            message = format_buy_alert(data)
        else:
            message = format_sell_alert(data)
        send_telegram_message(message)

        # Place ENTRY order
        entry_order_data = {
            "quantity": qty_requested,
            "product": "I",
            "validity": "DAY",
            "price": 0,
            "instrument_token": instrument_key,
            "order_type": "MARKET",
            "transaction_type": action,
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False
        }
        
        entry_res = place_order(entry_order_data, "ENTRY ORDER")
        if not entry_res["success"]:
            send_telegram_message(f"❌ <b>ENTRY FAILED</b>\n\nSymbol: {symbol}\nAction: {action}")
            return jsonify({'error': 'Entry order failed'}), 500

        # Verify entry fill
        is_filled, filled_qty = verify_order_fill(entry_res["order_id"])
        if not is_filled or filled_qty == 0:
            logger.error(f"❌ Entry not filled: {symbol}")
            send_telegram_message(f"❌ <b>ENTRY NOT FILLED</b>\n\nSymbol: {symbol}\nOrder ID: {entry_res['order_id']}")
            return jsonify({'error': 'Entry not filled'}), 500

        logger.info(f"✅ Entry filled: {symbol} | Qty: {filled_qty}")

        # Initialize position state
        import time
        position_state = {
            "symbol": symbol,
            "action": action,
            "qty_requested": qty_requested,
            "filled_qty": filled_qty,
            "entry_order_id": entry_res["order_id"],
            "entry_order_data": entry_order_data,
            "sl_order_id": None,
            "tp_order_id": None,
            "partial_order_id": None,
            "sl_order_data": None,
            "tp_order_data": None,
            "partial_order_data": None,
            "partial_filled": False,
            "created_at": time.time()
        }

        # Place PARTIAL TP
        if partial_tp_price and filled_qty >= 2:
            partial_qty = filled_qty // 2
            partial_order_data = {
                "quantity": partial_qty,
                "product": "I",
                "validity": "DAY",
                "price": round(partial_tp_price, 2),
                "instrument_token": instrument_key,
                "order_type": "LIMIT",
                "transaction_type": opposite_action,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False
            }
            partial_res = place_order(partial_order_data, "PARTIAL TP (50%)")
            if partial_res["success"]:
                position_state["partial_order_id"] = partial_res["order_id"]
                position_state["partial_order_data"] = partial_order_data

        # Place FULL TP
        if tp_price:
            remaining_qty = filled_qty - (filled_qty // 2 if partial_tp_price and filled_qty >= 2 else 0)
            if remaining_qty > 0:
                tp_order_data = {
                    "quantity": remaining_qty,
                    "product": "I",
                    "validity": "DAY",
                    "price": round(tp_price, 2),
                    "instrument_token": instrument_key,
                    "order_type": "LIMIT",
                    "transaction_type": opposite_action,
                    "disclosed_quantity": 0,
                    "trigger_price": 0,
                    "is_amo": False
                }
                tp_res = place_order(tp_order_data, "FULL TP")
                if tp_res["success"]:
                    position_state["tp_order_id"] = tp_res["order_id"]
                    position_state["tp_order_data"] = tp_order_data

        # Place STOP LOSS
        if sl_price:
            sl_order_data = {
                "quantity": filled_qty,
                "product": "I",
                "validity": "DAY",
                "price": 0,
                "instrument_token": instrument_key,
                "order_type": "SL-M",
                "transaction_type": opposite_action,
                "disclosed_quantity": 0,
                "trigger_price": round(sl_price, 2),
                "is_amo": False
            }
            sl_res = place_order(sl_order_data, "STOP LOSS")
            
            if sl_res["success"]:
                position_state["sl_order_id"] = sl_res["order_id"]
                position_state["sl_order_data"] = sl_order_data
            else:
                logger.critical(f"🚨 SL PLACEMENT FAILED: {symbol}")
                emergency_exit_position(symbol, filled_qty, action)
                send_telegram_message(f"🚨 <b>CRITICAL ERROR</b>\n\nSL placement failed for {symbol}\nEmergency market exit executed!")
                return jsonify({'error': 'SL placement failed - emergency exit'}), 500

        # Save position to DynamoDB
        save_position_to_db(symbol, position_state)
        
        logger.info(f"✅ Position opened: {symbol} | Filled: {filled_qty}/{qty_requested}")
        
        success_msg = f"""
✅ <b>POSITION OPENED</b>
━━━━━━━━━━━━━━━━━━━━━
📊 Symbol: {symbol}
🎯 Action: {action}
📈 Filled Qty: {filled_qty}
🔻 SL Order: {'✅ Placed' if position_state['sl_order_id'] else '❌ Failed'}
🔺 TP Order: {'✅ Placed' if position_state['tp_order_id'] else '⚠️ Not Placed'}
🎯 Partial TP: {'✅ Placed' if position_state['partial_order_id'] else '⚠️ Not Placed'}

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━
"""
        send_telegram_message(success_msg)

        return jsonify({
            "status": "success",
            "symbol": symbol,
            "action": action,
            "filled_qty": filled_qty,
            "orders_placed": {
                "entry": entry_res["order_id"],
                "sl": position_state["sl_order_id"],
                "tp": position_state["tp_order_id"],
                "partial_tp": position_state["partial_order_id"]
            }
        }), 200

    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}")
        send_telegram_message(f"❌ <b>WEBHOOK ERROR</b>\n\n{str(e)}")
        return jsonify({'error': str(e)}), 500

# ═══════════════════════════════════════════════════════════════════════════
# POSITION STATUS CHECKER (Called via API Gateway/CloudWatch Events)
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/check_positions', methods=['POST'])
def check_positions():
    """
    Check all active positions for fills/SL/TP hits
    Call this route via CloudWatch Events every 30 seconds
    """
    try:
        active_positions = get_all_positions_from_db()
        
        for symbol, pos in list(active_positions.items()):
            # Check partial TP fill
            if pos.get('partial_order_id') and not pos.get('partial_filled'):
                status = get_order_status(pos['partial_order_id'])
                
                if status == "complete":
                    logger.info(f"✅ Partial TP filled: {symbol}")
                    pos['partial_filled'] = True
                    
                    partial_qty = pos['partial_order_data']['quantity']
                    remaining_qty = pos['filled_qty'] - partial_qty
                    
                    # Cancel and replace SL
                    if pos.get('sl_order_id'):
                        cancel_order(pos['sl_order_id'])
                        
                        instrument_key = get_instrument_key(symbol)
                        if instrument_key:
                            opposite_action = "SELL" if pos['action'] == "BUY" else "BUY"
                            
                            new_sl_order = {
                                "quantity": remaining_qty,
                                "product": "I",
                                "validity": "DAY",
                                "price": 0,
                                "instrument_token": instrument_key,
                                "order_type": "SL-M",
                                "transaction_type": opposite_action,
                                "disclosed_quantity": 0,
                                "trigger_price": pos['sl_order_data']['trigger_price'],
                                "is_amo": False
                            }
                            
                            sl_res = place_order(new_sl_order, "ADJUSTED SL")
                            if sl_res["success"]:
                                pos['sl_order_id'] = sl_res["order_id"]
                                pos['sl_order_data'] = new_sl_order
                                
                                send_telegram_message(f"""
✅ <b>PARTIAL PROFIT TAKEN</b>
━━━━━━━━━━━━━━━━━━━━━
📊 Symbol: {symbol}
💰 Qty Exited: {partial_qty}
📈 Remaining: {remaining_qty}
🔄 SL Adjusted: {remaining_qty}

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━
""")
                            else:
                                emergency_exit_position(symbol, remaining_qty, pos['action'])
                    
                    save_position_to_db(symbol, pos)
            
            # Check full TP
            if pos.get('tp_order_id'):
                status = get_order_status(pos['tp_order_id'])
                if status == "complete":
                    logger.info(f"✅ Full TP hit: {symbol}")
                    if pos.get('sl_order_id'):
                        cancel_order(pos['sl_order_id'])
                    delete_position_from_db(symbol)
                    
                    send_telegram_message(f"""
🎯 <b>TAKE PROFIT HIT</b>
━━━━━━━━━━━━━━━━━━━━━
📊 Symbol: {symbol}
✅ Position fully closed
💰 Target achieved!

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━
""")
            
            # Check SL hit
            if pos.get('sl_order_id'):
                status = get_order_status(pos['sl_order_id'])
                if status == "complete":
                    logger.info(f"🛑 Stop Loss hit: {symbol}")
                    if pos.get('tp_order_id'):
                        cancel_order(pos['tp_order_id'])
                    if pos.get('partial_order_id'):
                        cancel_order(pos['partial_order_id'])
                    delete_position_from_db(symbol)
                    
                    send_telegram_message(f"""
🛑 <b>STOP LOSS HIT</b>
━━━━━━━━━━━━━━━━━━━━━
📊 Symbol: {symbol}
❌ Position closed at loss
🔒 Risk protected

⏰ {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━
""")
        
        return jsonify({"status": "checked", "positions": len(active_positions)}), 200
        
    except Exception as e:
        logger.error(f"❌ Position check error: {e}")
        return jsonify({'error': str(e)}), 500

# ═══════════════════════════════════════════════════════════════════════════
# ROUTES - AUTH, TEST, STATS
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/')
def home():
    """Home endpoint with bot status"""
    token = get_token()
    token_status = "✅ Active" if token else "❌ Expired/Missing"
    market_status = "✅ Open" if is_market_open() else "❌ Closed"
    
    active_positions = get_all_positions_from_db()
    
    return jsonify({
        'bot': 'ICT Pro Bot V7.5 - AWS Lambda Serverless',
        'status': 'active',
        'upstox_token': token_status,
        'market_status': market_status,
        'active_positions': len(active_positions),
        'positions': list(active_positions.keys()),
        'login_url': f"{request.url_root}login",
        'webhook_url': f"{request.url_root}webhook"
    })

@app.route('/login')
def login():
    """Initiate Upstox OAuth login"""
    if not UPSTOX_API_KEY or not UPSTOX_API_SECRET:
        return "<h2 style='color:red;'>❌ Error: Upstox credentials missing!</h2>", 500
    
    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={UPSTOX_API_KEY}&redirect_uri={UPSTOX_REDIRECT_URI}"
    )
    return redirect(auth_url)

@app.route('/callback')
def callback():
    """Handle Upstox OAuth callback"""
    code = request.args.get('code')
    if not code:
        return "<h2 style='color:red;'>❌ Error: No authorization code received</h2>", 400
    
    if generate_access_token(code):
        return f"""
        <html>
        <head><title>Success</title></head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
            <h1 style="color:green;">✅ SUCCESS!</h1>
            <h2>Upstox Token Generated & Saved to DynamoDB!</h2>
            <p style="font-size: 18px;">Bot is now ready for live trading</p>
            <p style="font-size: 16px; color: #666;">Token valid for 20 hours</p>
            <p><a href="/" style="color: blue;">← Back to Dashboard</a></p>
        </body>
        </html>
        """
    else:
        return "<h2 style='color:red;'>❌ Token Generation Failed</h2>", 500

@app.route('/test', methods=['GET'])
def test_alert():
    """Test webhook with sample data"""
    test_data = {
        'action': 'BUY',
        'symbol': 'RELIANCE-EQ',
        'price': 2980.50,
        'sl': 2950.00,
        'tp': 3100.00,
        'partial_tp': 3040.25,
        'qty': 10,
        'risk': 305.00,
        'rr': 3.93,
        'regime': 'TRENDING',
        'confluence': 12,
        'killzone': 'NSE/BSE Session'
    }
    
    message = format_buy_alert(test_data)
    send_telegram_message(message)
    
    return jsonify({'status': 'Test alert sent', 'data': test_data})

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get bot statistics"""
    token = get_token()
    active_positions = get_all_positions_from_db()
    
    positions_detail = []
    for symbol, pos in active_positions.items():
        positions_detail.append({
            'symbol': symbol,
            'action': pos['action'],
            'filled_qty': pos['filled_qty'],
            'partial_filled': pos.get('partial_filled', False),
            'created_at': datetime.fromtimestamp(pos['created_at']).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return jsonify({
        'bot_version': 'V7.5 Lambda',
        'status': 'active',
        'upstox_token_valid': token is not None,
        'market_open': is_market_open(),
        'active_positions_count': len(active_positions),
        'positions': positions_detail,
        'storage': 'DynamoDB',
        'architecture': 'Serverless'
    })

@app.route('/positions', methods=['GET'])
def get_positions():
    """Get detailed position information"""
    active_positions = get_all_positions_from_db()
    return jsonify({
        'active_positions': active_positions,
        'count': len(active_positions)
    })

@app.route('/close/<symbol>', methods=['POST'])
def manual_close(symbol):
    """Manually close a position"""
    symbol = symbol.upper().replace('-EQ', '')
    
    pos = get_position_from_db(symbol)
    if not pos:
        return jsonify({'error': f'Position {symbol} not found'}), 404
    
    # Cancel all orders
    for oid in [pos.get('sl_order_id'), pos.get('tp_order_id'), pos.get('partial_order_id')]:
        if oid:
            cancel_order(oid)
    
    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return jsonify({'error': 'Instrument key not found'}), 400
    
    opposite_action = "SELL" if pos['action'] == "BUY" else "BUY"
    remaining_qty = pos['filled_qty'] - (pos['partial_order_data']['quantity'] if pos.get('partial_filled') else 0)
    
    exit_order = {
        "quantity": remaining_qty,
        "product": "I",
        "validity": "DAY",
        "price": 0,
        "instrument_token": instrument_key,
        "order_type": "MARKET",
        "transaction_type": opposite_action,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False
    }
    
    result = place_order(exit_order, "MANUAL EXIT")
    
    if result["success"]:
        delete_position_from_db(symbol)
        send_telegram_message(f"✅ <b>Manual Exit</b>\n\nSymbol: {symbol}\nQty: {remaining_qty}")
        return jsonify({'success': True, 'message': f'Position {symbol} closed'})
    else:
        return jsonify({'error': 'Exit order failed'}), 500

@app.route('/close_all', methods=['POST'])
def close_all_positions():
    """Emergency close all positions"""
    closed = []
    failed = []
    
    active_positions = get_all_positions_from_db()
    
    for symbol, pos in list(active_positions.items()):
        for oid in [pos.get('sl_order_id'), pos.get('tp_order_id'), pos.get('partial_order_id')]:
            if oid:
                cancel_order(oid)
        
        instrument_key = get_instrument_key(symbol)
        if instrument_key:
            opposite_action = "SELL" if pos['action'] == "BUY" else "BUY"
            remaining_qty = pos['filled_qty'] - (pos['partial_order_data']['quantity'] if pos.get('partial_filled') else 0)
            
            exit_order = {
                "quantity": remaining_qty,
                "product": "I",
                "validity": "DAY",
                "price": 0,
                "instrument_token": instrument_key,
                "order_type": "MARKET",
                "transaction_type": opposite_action,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False
            }
            
            result = place_order(exit_order, f"EMERGENCY EXIT {symbol}")
            if result["success"]:
                closed.append(symbol)
                delete_position_from_db(symbol)
            else:
                failed.append(symbol)
    
    send_telegram_message(f"🚨 <b>Emergency Close All</b>\n\nClosed: {', '.join(closed)}\nFailed: {', '.join(failed)}")
    
    return jsonify({
        'closed': closed,
        'failed': failed,
        'remaining_positions': len(get_all_positions_from_db())
    })

# ═══════════════════════════════════════════════════════════════════════════
# LAMBDA HANDLER
# ═══════════════════════════════════════════════════════════════════════════
# Yeh line aisi hi honi chahiye (adapter="wsgi" bahut zaroori hai)
handler = Mangum(app, lifespan="off", adapter="wsgi")

if __name__ == "__main__":
    app.run(debug=True)