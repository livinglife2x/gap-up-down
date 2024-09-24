import pandas as pd
import requests
import json
import concurrent.futures
import math
import time

def place_order(symbol,side,quantity):
    global access_token
    url = 'https://api-hft.upstox.com/v2/order/place'
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}',
    }


    data = {
    'quantity': quantity,
    'product': 'I',
    'validity': 'DAY',
    'price': 0,
    'tag': 'string',
    'instrument_token': symbol,
    'order_type': 'MARKET',
    'transaction_type': side,
    'disclosed_quantity': 0,
    'trigger_price': 0,
    'is_amo': False,
    }

    try:
        # Send the POST request
        response = requests.post(url, json=data, headers=headers)

        # Print the response status code and body
        print('Response Code:', response.status_code)
        print('Response Body:', response.json())
        return True

    except Exception as e:
        # Handle exceptions
        print('Error:', str(e))
        return False
        
def get_market_status():
    global access_token
    url = 'https://api.upstox.com/v2/market/status/NSE'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)

    status = response.json()['data']['status']
    if status == "NORMAL_OPEN":
        return True
    return False
    
def get_ltp(symbol):
    global access_token
    url = f'https://api.upstox.com/v2/market-quote/ltp?instrument_key={symbol}'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    for i in response.json()['data']:
        return response.json()['data'][i]['last_price']

def get_balance():
    global access_token
    url = 'https://api.upstox.com/v2/user/get-funds-and-margin'

    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    print(response)
    return response.json()['data']['equity']['available_margin']
def generate_stock_list(stocks_to_trade):
    trade_list = []
    capital_per_stock = get_balance()/len(stocks_to_trade)
    for symbol in stocks_to_trade:
        temp_dict={}
        temp_dict['symbol'] = symbol
        ltp = get_ltp(symbol)
        prv_high = get_historical_data(symbol)[2].iloc[-1]
        temp_dict['quantity'] = math.floor(capital_per_stock/ltp)
        temp_dict['side'] = 'SELL'
        if ltp<prv_high:
            trade_list.append(temp_dict)
    return trade_list

def execute_orders(trade_list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(place_order, stock['symbol'], stock['side'], stock['quantity']) for stock in trade_list]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
    return results

def get_positions():
    global access_token
    positions = []
    url = 'https://api.upstox.com/v2/portfolio/short-term-positions'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    for i in response.json()['data']:
        temp_dict={}
        if i['quantity']:
            temp_dict['quantity'] = abs(i['quantity'])
            temp_dict['symbol'] = i['instrument_token']
            positions.append(temp_dict)
    return positions
            

def get_historical_data(symbol):
    global access_token
    url = f'https://api.upstox.com/v2/historical-candle/{symbol}/day/2099-12-30/2024-01-01'
    headers = {
    'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Do something with the response data (e.g., print it)
        return pd.DataFrame(response.json()['data']['candles']).sort_values(by=0)
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code} - {response.text}")
        return pd.DataFrame()

def check_prv_high_exit(symbol,quantity):
    hist_data = get_historical_data(symbol)
    if hist_data.empty:
        return False
    prv_high = hist_data[2].iloc[-1]
    ltp = get_ltp(symbol)
    if ltp>prv_high:
        place_order(symbol,'BUY',quantity)
    return True
        
def generate_exit_list(existing_positions):
    exit_trade_list = []
    for i in existing_positions:
        temp_dict = {}
        temp_dict['symbol'] = i['symbol']
        temp_dict['quantity'] = i['quantity']
        temp_dict['side'] = 'BUY'
        exit_trade_list.append(temp_dict)
    return exit_trade_list

def execute_exit_orders(exit_trade_list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(check_prv_high_exit, stock['symbol'], stock['quantity']) for stock in exit_trade_list]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]  
