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
                 aws_access_key_id='AKIAYZZGSWEJG3SOXO5O',
                 aws_secret_access_key='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',)
bucket_name = 'trades-stock-list-access-token'
json_file_key = 'config.json'
response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
content = response['Body'].read().decode('utf-8')
config = json.loads(content)
print(config)


key = 'stocks_to_trade.csv'
obj = s3.get_object(Bucket=bucket_name, Key=key)
stocks_to_trade = pd.read_csv(io.BytesIO(obj['Body'].read()))['1']
#config = json.load('.Downloads/config')
access_token = config['access_token']
trade_day = config['trade_day']
#stocks_to_trade = pd.read_csv('./Downloads/stocks_to_trade.csv')['1']
positions_taken = False
india = pytz.timezone('Asia/Calcutta')
today = dt.datetime.now(india)
existing_positions=None
capital_per_stock = get_balance(access_token)/len(stocks_to_trade)
stock_feed = []
for stock in stocks_to_trade:
    stock_feed.append([stock,capital_per_stock,access_token])
while True:
    try:
        today = dt.datetime.now(india)
        if today.time()>=dt.datetime.strptime("9:15", '%H:%M').time() and not positions_taken and trade_day:
          if get_market_status(access_token) and trade_list:
            execute_stock_trade_list(stock_feed)
            positions_taken=True
            time.sleep(5)
        if positions_taken:
          existing_positions = get_positions(access_token)
          time.sleep(5)
        if existing_positions:
            exit_trade_list = generate_exit_list(existing_positions,access_token)
            execute_exit_orders(exit_trade_list)
        if today.time()>=dt.datetime.strptime("15:09", '%H:%M').time() and existing_positions:
            exit_trade_list = generate_exit_list(existing_positions,access_token)
            execute_orders(exit_trade_list)
            time.sleep(5)
    except Exception as e:
        print(e)
