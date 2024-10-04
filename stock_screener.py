import json
import pandas as pd
import boto3
from io import StringIO
import requests
import os
import concurrent.futures

access_token = os.environ.get('access_token')

def get_balance(access_token):
    url = 'https://api.upstox.com/v2/user/get-funds-and-margin'

    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    print(response)
    return response.json()['data']['equity']['available_margin']



def get_upstox_data(ticker,start,end):
    url = f'https://api.upstox.com/v2/historical-candle/{ticker}/day/{end}/{start}'
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

def check_gapup_down(ticker):
    try:
        df =  get_upstox_data(ticker[3],'2024-01-01','2024-12-31')
        if df.empty:
            return ticker[2],ticker[3],0,False 
        if df[1].iloc[-1]>df[4].iloc[-2]*1.01:
            if df[4].iloc[-1]<df[1].iloc[-1]:
                return ticker[2],ticker[3],df[4].iloc[-1],True
        
        else:
            return ticker[2],ticker[3],0,False
        return ticker[2],ticker[3],0,False
    except:
        return ticker[2],ticker[3],0,False

def check_bearish_engulfing(tickers):
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(check_gapup_down, ticker): ticker for ticker in tickers}
        for future in concurrent.futures.as_completed(futures):
            ticker1,ticker2,close, is_engulfing = future.result()
            if is_engulfing:
                results.append([ticker1,ticker2,close])
    return results


def lambda_handler(event, context):
    s3 = boto3.client(
        's3'
    )
    csv_obj = s3.get_object(Bucket="trade-artifacts", Key='upstox_ticker_list.csv')
    body = csv_obj['Body'].read().decode('utf-8')
    data = StringIO(body)
    nifty_500_list = pd.read_csv(data)
    #print(nifty_500_list)
    #print(get_upstox_data("NSE_EQ|INE530B01024",'2024-01-01','2024-01-03'))
    ticker_list = list(nifty_500_list.to_records())
    #print(ticker_list[0])
    screened_list = check_bearish_engulfing(ticker_list)
    balance = get_balance(access_token)
    screened_list_df = pd.DataFrame(screened_list)
    print(screened_list_df)
    csv_buffer = StringIO()
    screened_list_df.to_csv(csv_buffer, index=False)  # index=False to avoid writing the index column
    s3.put_object(Bucket="trade-artifacts", Key="stocks_to_trade.csv", Body=csv_buffer.getvalue())
    
    sum_of_stocks=0
    trade_day=False
    if not screened_list_df.empty:
        sum_of_stocks = screened_list_df[2].sum()
    if sum_of_stocks>balance and sum_of_stocks:
        trade_day = False
    else:
        trade_day=True
    config = {}
    config['trade_day'] = trade_day
    config['access_token'] = access_token
    json_data = json.dumps(config)
    s3.put_object(Bucket="trade-artifacts", Key="config.json", Body=json_data)

    



    
    
