from dep import *
import pandas as pd
import requests
import json
import concurrent.futures
import math
import time
import boto3

s3 = boto3.client('s3',
                 aws_access_key_id='AKIARSU7K2CMKQKJ3E75',
                 aws_secret_access_key='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',)
bucket_name = 'trades-stock-list-access-token'
json_file_key = 'config.json'
response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
content = response['Body'].read().decode('utf-8')
config = json.loads(content)
print(json_data)


key = 'stocks_to_trade.csv'
obj = s3.get_object(Bucket=bucket, Key=key)
stocks_to_trade = pd.read_csv(io.BytesIO(obj['Body'].read()))['1']
#config = json.load('.Downloads/config')
access_token = config['access_token']
trade_day = config['trades_day']
#stocks_to_trade = pd.read_csv('./Downloads/stocks_to_trade.csv')['1']
positions_taken = False
india = pytz.timezone('Asia/Calcutta')
today = dt.datetime.now(india)
trade_list = generate_stock_list(stocks_to_trade)
while True:
    try:
        today = dt.datetime.now(india)
        if today.time()>=dt.datetime.strptime("9:15", '%H:%M').time() and not positions_taken and trade_day:
            if get_market_status():
                execute_orders(trade_list)
                positions_taken=True
                time.sleep(5)
        existing_positions = get_positions()
        if existing_positions:
            exit_trade_list = generate_exit_list(existing_positions)
            execute_exit_orders(exit_trade_list)
        existing_positions = get_positions()
        if today.time()>=dt.datetime.strptime("15:10", '%H:%M').time() and existing_positions:
            exit_trade_list = generate_exit_list(existing_positions)
            execute_orders(exit_trade_list)
            time.sleep(5)
    except Exception as e:
        print(e)
