from dep import *
import pandas as pd
import requests
import json
import concurrent.futures
import math
import time
import boto3
import io
import pytz
import datetime as dt

s3 = boto3.client('s3',
                 aws_access_key_id='abc',
                 aws_secret_access_key='abc',)
bucket_name = 'trade-artifacts'
json_file_key = 'config.json'
response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
content = response['Body'].read().decode('utf-8')
config = json.loads(content)
print(config)


key = 'stocks_to_trade.csv'
obj = s3.get_object(Bucket=bucket_name, Key=key)
try:
    stocks_to_trade = pd.read_csv(io.BytesIO(obj['Body'].read()))
except:
    stocks_to_trade = pd.DataFrame()
print(stocks_to_trade)
#config = json.load('.Downloads/config')
access_token = config['access_token']
trade_day = config['trade_day']
#stocks_to_trade = pd.read_csv('./Downloads/stocks_to_trade.csv')['1']
positions_taken = False
india = pytz.timezone('Asia/Calcutta')
today = dt.datetime.now(india)
existing_positions=None
try:
    capital_per_stock = (get_balance(access_token))/len(stocks_to_trade)
except:
    capital_per_stock = 0
print('capital per stock',capital_per_stock)
stock_feed = []
#scrip_list = ",".join(stocks_to_trade)
"""
while True:
    today = dt.datetime.now(india)
    if today.time()>=dt.datetime.strptime("9:13:00", '%H:%M:%S').time():
        url = f'https://api.upstox.com/v2/market-quote/ltp?instrument_key={scrip_list}'
        headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        ltp_list = []
        #print(data)

        for i in data['data']:
            ltp_list.append([data['data'][i]['instrument_token'],data['data'][i]['last_price']])
        print("data obtained today")
        break
"""
for i in range(len(stocks_to_trade)):
    stock_feed.append([stocks_to_trade['1'].iloc[i],capital_per_stock,stocks_to_trade['2'].iloc[i],access_token])
while True:
    try:
        today = dt.datetime.now(india)
        if today.time()>=dt.datetime.strptime("9:03", '%H:%M').time() and not positions_taken and trade_day:
            if get_market_status(access_token) and stock_feed:
                execute_stock_trade_list(stock_feed)
                positions_taken=True
                time.sleep(5)
        if positions_taken:
            #print("positions checked at ",today)
            existing_positions = get_positions(access_token)
            time.sleep(10)
        if existing_positions:
            exit_trade_list = generate_exit_list(existing_positions,access_token,stocks_to_trade['1'])
            execute_exit_orders(exit_trade_list)
        if positions_taken:
            existing_positions = get_positions(access_token)
            time.sleep(5)
        if today.time()>=dt.datetime.strptime("15:09", '%H:%M').time():
            if existing_positions:
                exit_trade_list = generate_exit_list(existing_positions,access_token,stocks_to_trade['1'])
                execute_orders(exit_trade_list)
                
            break
    except Exception as e:
        print(e)
