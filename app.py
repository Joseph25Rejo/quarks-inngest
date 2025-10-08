from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
from datetime import datetime
import time
import json
import logging
import os
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
is_production = os.environ.get('FLASK_ENV') == 'production' or os.environ.get('RENDER') == 'true'
if is_production:
    # More restrictive CORS for production
    CORS(app, origins=["https://quarks-nu.vercel.app", "http://localhost:3000", "http://localhost:5000", "http://localhost:3001"])  # Replace with your frontend domain
else:
    # Allow all origins in development
    CORS(app)

historical_cache = {}

@app.route('/api/historical')
def get_historical():
    symbol = request.args.get('symbol', '').strip()
    if not symbol:
        return jsonify({'error': 'Symbol parameter is required'}), 400
    
    # Auto-add .NS if needed
    if not any(suffix in symbol for suffix in ['.NS', '.BO']):
        symbol += '.NS'
    symbol = symbol.upper()

    # Return cached data if available
    if symbol in historical_cache:
        app.logger.info(f"✅ Returning cached data for {symbol}")
        return jsonify(historical_cache[symbol])

    app.logger.info(f"🔍 Fetching historical data for {symbol}...")

    result = {}
    try:
        # 1-minute data (7 days)
        app.logger.info("  → Fetching 1m data (7d)...")
        df_1m = yfinance_download(symbol, period='7d', interval='1m')
        result['1m'] = format_ohlc(df_1m)
        time.sleep(1)  # Rate limit delay

        # 5-minute data (60 days)
        app.logger.info("  → Fetching 5m data (60d)...")
        df_5m = yfinance_download(symbol, period='60d', interval='5m')
        result['5m'] = format_ohlc(df_5m)
        time.sleep(1)

        # 15-minute data (90 days)
        app.logger.info("  → Fetching 15m data (90d)...")
        df_15m = yfinance_download(symbol, period='90d', interval='15m')
        result['15m'] = format_ohlc(df_15m)
        time.sleep(1)

        # 1-hour data (6 months)
        app.logger.info("  → Fetching 1h data (6mo)...")
        df_1h = yfinance_download(symbol, period='6mo', interval='1h')
        result['1h'] = format_ohlc(df_1h)
        time.sleep(1)

        # 1-day data (1 year)
        app.logger.info("  → Fetching 1d data (1y)...")
        df_1d = yfinance_download(symbol, period='1y', interval='1d')
        result['1d'] = format_ohlc(df_1d)

        # Cache it
        historical_cache[symbol] = result
        app.logger.info(f"✅ Cached data for {symbol}")
        return jsonify(result)

    except Exception as e:
        app.logger.error(f"❌ Error for {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stream/<symbol>')
def stream_candle(symbol):
    """Server-Sent Events (SSE) endpoint for real-time price"""
    # Validate and normalize symbol
    symbol = symbol.strip().upper()
    if not symbol:
        return jsonify({'error': 'Symbol parameter is required'}), 400
    
    # Auto-add .NS if needed
    if not any(suffix in symbol for suffix in ['.NS', '.BO']):
        symbol += '.NS'

    def event_stream():
        # Keep track of connection state
        connected = True
        error_count = 0
        max_errors = 5
        
        while connected and error_count < max_errors:
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                
                # Try multiple sources for price data
                price = (
                    info.get('currentPrice') or 
                    info.get('regularMarketPrice') or 
                    info.get('previousClose')
                )
                
                # Get volume data
                volume = info.get('regularMarketVolume', 0)
                
                # Validate data before sending
                if price is not None and isinstance(price, (int, float)):
                    # Format time as HH:MM for better readability
                    now = datetime.now()
                    formatted_time = now.strftime("%H:%M")
                    
                    candle = {
                        'time': formatted_time,  # Human-readable time format
                        'timestamp': int(now.timestamp() * 1000),  # Keep Unix timestamp for precision
                        'open': round(float(price), 2),
                        'high': round(float(price), 2),
                        'low': round(float(price), 2),
                        'close': round(float(price), 2),
                        'volume': int(volume) if volume else 0,
                        'symbol': symbol
                    }
                    # Format as SSE
                    yield f"data: {json.dumps(candle)}\n\n"
                    # Reset error count on successful fetch
                    error_count = 0
                else:
                    app.logger.warning(f"No valid price data for {symbol}")
                
                # Use exponential backoff for polling interval
                # Start with 30 seconds, increase if errors occur
                base_interval = 30
                poll_interval = min(base_interval * (2 ** error_count), 300)  # Max 5 minutes
                time.sleep(poll_interval)
                
            except Exception as e:
                error_count += 1
                app.logger.error(f"⚠️ Stream error for {symbol} (attempt {error_count}/{max_errors}): {e}")
                if error_count >= max_errors:
                    # Send error event to client before closing
                    error_data = {
                        'error': 'Maximum error count reached',
                        'symbol': symbol,
                        'timestamp': int(datetime.now().timestamp() * 1000)
                    }
                    yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
                    connected = False
                else:
                    # Wait before retrying
                    time.sleep(min(30 * error_count, 120))  # Max 2 minutes

    # Set proper headers for SSE
    response = Response(event_stream(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['Access-Control-Allow-Origin'] = '*'  # Adjust for production
    
    return response

# Helper: Safe yfinance download with error handling
def yfinance_download(symbol, period, interval):
    try:
        app.logger.info(f"  → Downloading {symbol} data ({period}, {interval})...")
        df = yf.download(symbol, period=period, interval=interval, progress=False)
        app.logger.info(f"  → Download completed, df type: {type(df)}")
        if df is None:
            app.logger.info(f"  → df is None, returning None")
            return None
        app.logger.info(f"  → Checking if df is empty...")
        if df.empty:
            app.logger.info(f"  → df is empty, returning None")
            return None
        app.logger.info(f"  → df is valid, shape: {df.shape}")
        return df
    except Exception as e:
        app.logger.warning(f"⚠️ yfinance download failed for {symbol} ({period}, {interval}): {e}")
        return None

# Helper: Convert DataFrame to list of OHLC records
def format_ohlc(df):
    app.logger.info(f"  → Formatting OHLC data, df type: {type(df)}")
    if df is None:
        app.logger.info(f"  → df is None, returning empty list")
        return []
    app.logger.info(f"  → Checking if df is empty...")
    if df.empty:
        app.logger.info(f"  → df is empty, returning empty list")
        return []
    app.logger.info(f"  → df is valid, shape: {df.shape}")
    # Reset index to get 'Datetime' or 'Date' as column
    df = df.reset_index()
    records = []
    app.logger.info(f"  → Iterating through {len(df)} rows...")
    for i, row in df.iterrows():
        try:
            # Handle MultiIndex columns from yfinance
            dt = row.get('Datetime') or row.get('Date')
            if dt is None:
                app.logger.info(f"  → Row {i}: dt is None, skipping")
                continue
            
            # Extract scalar values from MultiIndex columns or Series
            def extract_scalar_value(val):
                if isinstance(val, (int, float)):
                    return val
                elif hasattr(val, 'iloc'):  # Series or similar
                    return val.iloc[0]
                elif hasattr(val, '__iter__') and not isinstance(val, str):  # Iterable but not string
                    try:
                        return next(iter(val))
                    except:
                        return 0
                else:
                    return val if val is not None else 0
            
            open_val = extract_scalar_value(row['Open'])
            high_val = extract_scalar_value(row['High'])
            low_val = extract_scalar_value(row['Low'])
            close_val = extract_scalar_value(row['Close'])
            volume_val = extract_scalar_value(row['Volume'])
            
            # Handle datetime properly
            dt_value = dt
            if hasattr(dt, 'iloc'):  # Series
                dt_value = dt.iloc[0]
            elif hasattr(dt, '__iter__') and not isinstance(dt, str):  # Iterable but not string
                try:
                    dt_value = next(iter(dt))
                except:
                    dt_value = datetime.now()
            
            # Ensure volume is a number before comparison
            volume_num = 0
            try:
                volume_num = int(volume_val) if not pd_isna(volume_val) else 0
            except (ValueError, TypeError):
                volume_num = 0
                
            records.append({
                'Datetime': dt_value.isoformat() if hasattr(dt_value, 'isoformat') else str(dt_value),
                'Open': float(open_val) if not pd_isna(open_val) else 0,
                'High': float(high_val) if not pd_isna(high_val) else 0,
                'Low': float(low_val) if not pd_isna(low_val) else 0,
                'Close': float(close_val) if not pd_isna(close_val) else 0,
                'Volume': volume_num
            })
            if i < 3:  # Log first 3 rows
                app.logger.info(f"  → Row {i}: {records[-1]}")
        except Exception as row_error:
            app.logger.error(f"  → Error processing row {i}: {row_error}")
            continue
    app.logger.info(f"  → Formatted {len(records)} records")
    return records

# Helper: Check for NaN (without requiring pandas import)
def pd_isna(val):
    return val is None or (isinstance(val, float) and str(val) == 'nan')

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'message': 'Indian Stock API is running!'})

@app.route('/stream-health')
def stream_health():
    """Health check endpoint for streaming service"""
    return jsonify({
        'status': 'ok', 
        'message': 'Stock data streaming service is running!',
        'timestamp': int(datetime.now().timestamp() * 1000)
    })

if __name__ == '__main__':
    # Check if running in production environment
    
    print("🚀 Starting Indian Stock API on http://localhost:5000")
    print("💡 Try: http://localhost:5000/api/historical?symbol=RELIANCE.NS")
    
    if is_production:
        # Production configuration for Render
        app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
    else:
        # Development configuration
        app.run(host='0.0.0.0', port=5000, debug=True)
