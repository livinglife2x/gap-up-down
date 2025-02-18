import pandas as pd
import requests
import json
import concurrent.futures
import math
import time

def place_order(symbol,side,order_type,quantity,price,trigger_price,access_token):
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
    'price': price,
    'tag': 'string',
    'instrument_token': symbol,
    'order_type': order_type,
    'transaction_type': side,
    'disclosed_quantity': 0,
    'trigger_price': trigger_price,
    'is_amo': False,
    }

    try:
        # Send the POST request
        response = requests.post(url, json=data, headers=headers)

        # Print the response status code and body
        print('Response Code:', response.status_code)
        print('Response Body:', response.json())
        response_json = response.json()
        response_json["symbol"] = symbol
        return response_json

    except Exception as e:
        # Handle exceptions
        print('Error:', str(e))
        return {}

def cancel_order(order_id,access_token):
    url = f'https://api-hft.upstox.com/v2/order/cancel?order_id={order_id}'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.delete(url, headers=headers)
    return response.json()

def get_order_status(order_id,access_token):
    url = 'https://api.upstox.com/v2/order/details'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    params = {'order_id': order_id}

    response = requests.get(url, headers=headers, params=params)
    return response.json()


def get_market_status(access_token):
    url = 'https://api.upstox.com/v2/market/status/NSE'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)

    status = response.json()['data']['status']
    if "OPEN" in status:
        return True
    return False
    
def get_ltp(symbol,access_token):
    url = f'https://api.upstox.com/v2/market-quote/ltp?instrument_key={symbol}'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    for i in response.json()['data']:
        return response.json()['data'][i]['last_price']

def get_balance(access_token):
    url = 'https://api.upstox.com/v2/user/get-funds-and-margin'

    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    print(response)
    return response.json()['data']['equity']['available_margin']
def execute_stock_list(data):
    symbol = data[0]
    capital_per_stock = data[1]
    access_token = data[3]
    price = data[2]
    quantity = (math.floor(capital_per_stock/price))*3
    if quantity:
        place_order(symbol,"SELL","MARKET",quantity,0,0,access_token)
    return True

def execute_orders(trade_list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(place_order, stock['symbol'], stock['side'], stock['quantity'],0,stock['access_token']) for stock in trade_list]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
    return results

def get_positions(access_token):
    try:
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
                temp_dict['entered_price'] = i["day_sell_price"]
                positions.append(temp_dict)
        return positions
    except Exception as e:
        print(e)
        return []
            

def get_historical_data(symbol):
    url = f'https://api.upstox.com/v2/historical-candle/{symbol}/day/2099-12-30/2025-01-01'
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

def check_prv_high_exit(symbol,quantity,access_token,entered_price,existing_slm_orders):
    """
    hist_data = get_historical_data(symbol)
    if hist_data.empty:
        return False
    prv_high = hist_data[2].iloc[-1]
    """
    ltp = get_ltp(symbol,access_token)
    if ltp>=entered_price*1.02:
        for order in existing_slm_orders:
            if order['symbol']==symbol:
                response = cancel_order(order["order_id"], access_token)
                time.sleep(1)
                response = get_order_status(order["order_id"],access_token)
                if response["data"]["status"]=="cancelled":
                    place_order(symbol,'BUY',"MARKET",quantity,0,0,access_token)
    return True
        
def generate_exit_list(existing_positions,access_token,stocks_to_trade):
    exit_trade_list = []
    for i in existing_positions:
        temp_dict = {}
        temp_dict['symbol'] = i['symbol']
        temp_dict['quantity'] = i['quantity']
        temp_dict['side'] = 'BUY'
        temp_dict['access_token']=access_token
        temp_dict['entered_price'] = i["entered_price"]
        if i['symbol'] in stocks_to_trade.values and i['quantity']:
            exit_trade_list.append(temp_dict)
    return exit_trade_list

def execute_exit_orders(exit_trade_list,existing_slm_orders):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(check_prv_high_exit, stock['symbol'], stock['quantity'],stock['access_token'],stock['entered_price'],existing_slm_orders) for stock in exit_trade_list]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]  

def execute_stock_trade_list(stock_feed):
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(execute_stock_list, data): data for data in stock_feed}
        results = [f.result() for f in concurrent.futures.as_completed(futures)]  
    return results

def exit_all_positions(access_token):
    url = 'https://api.upstox.com/v2/order/positions/exit'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    
    data = {}
    
    try:
        # Send the POST request
        response = requests.post(url, json=data, headers=headers)
    
        # Print the response status code and body
        print('Response Code:', response.status_code)
        print('Response Body:', response.json())
    
    except Exception as e:
        # Handle exceptions
        print('Error:', str(e))

def create_slm_orders(existing_positions,stocks_to_trade,existing_slm_orders,access_token):
    slm_order_list = []
    if existing_positions:
        for position in existing_positions:
            temp_dict = {}
            if position["symbol"] in stocks_to_trade.values:
                if not any(d["symbol"] == position["symbol"] for d in existing_slm_orders):
                    temp_dict["symbol"] = position["symbol"]
                    temp_dict['quantity'] = position['quantity']
                    temp_dict['side'] = 'BUY'
                    temp_dict['access_token'] = access_token
                    temp_dict['trigger_price'] = position["entered_price"]*1.02
            if temp_dict:
                slm_order_list.append(temp_dict)
    return slm_order_list

def execute_slm_orders_list(slm_order_list):
    #symbol,side,order_type,quantity,price,trigger_price,access_tokenx
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(place_order, stock['symbol'],"BUY","SL-M", stock['quantity'],0,stock["trigger_price"],stock['access_token']) for stock in slm_order_list]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    return results




