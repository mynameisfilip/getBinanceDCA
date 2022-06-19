import os 
import json
import logging
import time
import datetime
import sys
import requests
import hmac
import hashlib
import pandas as pd
from urllib.parse import urljoin, urlencode


#Get path to settings file
settingsFile = 'settings.json'
invocationDir = os.path.dirname(__file__)
settingsFilePath = os.path.join(invocationDir, settingsFile)

#Check if settings file exists, abort execution if not
if not os.path.isfile(settingsFilePath):
    print('Settings File does not exists.')
    time.sleep(5)
    sys.exit()

#Load settings.json to settings variable
with open (settingsFilePath, 'r') as file:
    settings = json.loads(file.read())

#Define variables using values from settings file
API_KEY = settings['API_KEY']
SECRET_KEY = settings['SECRET_KEY']
BASE_URL = settings['BASE_URL']
HEADERS = {'X-MBX-APIKEY': API_KEY}
orderHistoryFile = settings['orderHistoryFile']
cryptos = settings['cryptos']
stableCoins = settings['stableCoins']

startTime = settings['startTime']
dt_startTime = datetime.datetime.strptime(startTime, '%d.%m.%Y')
ts_startTime = int((datetime.datetime(dt_startTime.year, dt_startTime.month, dt_startTime.day).timestamp()) * 1000) 

#Define logging
logFile = settings['logFile']
level = settings['logLevel']
logFormat = '%(asctime)s - %(levelname)s - %(message)s'  

#Create list of symbols from cryptos & stableCoins ie. BTCUSDT, ETHBUSD.. you can only get order history for specific symbol 
symbols = []
for crypto in cryptos:
    for stableCoin in stableCoins:
        symbols.append(crypto + stableCoin)

#-------- Main function --------#
# # def getOrderHistory()
def getOrderHistory(symbol, startTime):
    endpoint = '/api/v3/allOrders'

    url = urljoin(BASE_URL, endpoint)
    timestamp = int((time.time()) * 1000)

    params = {'symbol' : symbol,
            'startTime': startTime,
            'timestamp' : timestamp}
    
    query_string = urlencode(params)
    params['signature'] = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    r = requests.get(url=url, headers=HEADERS, params=params)
    
    orders = r.json()
    df = pd.DataFrame()

    if len(orders) > 0:
        for order in orders:
            if order['status'] == 'FILLED':
                df = df.append({
                'time': datetime.datetime.fromtimestamp(order['time']/1000),
                'side':order['side'],
                'pair': order['symbol'],
                'price': order['price'],
                'executedQty': float(order['executedQty']),
                    'totalPrice': float(order['cummulativeQuoteQty'])}, ignore_index=True)
    return df



# Create dataframe with all symbols
symbol_df = pd.DataFrame()
for symbol in symbols:
    orderHistory = getOrderHistory(symbol=symbol, startTime=ts_startTime)
    if not orderHistory.empty:
        totalAmount = orderHistory['executedQty'].sum()
        totalPrice = orderHistory['totalPrice'].sum()
        dca = (totalPrice / totalAmount)
        symbol_df = symbol_df.append({
            'symbol': symbol,
            'totalAmount': totalAmount,
            'totalPrice': totalPrice,
            'dca': dca
        }, ignore_index=True)

# Create dataframe from symbol_df filtered by crypto ie BTCUSDT & BTCBUSD are calculate together to get dca for BTC
dca_df = pd.DataFrame()
for crypto in cryptos:

    totalAmount = symbol_df[symbol_df['symbol'].str.contains(crypto)]['totalAmount'].sum()
    totalPrice = symbol_df[symbol_df['symbol'].str.contains(crypto)]['totalPrice'].sum()
    totalDca = totalPrice / totalAmount

    dca_df = dca_df.append({
        'crypto': crypto,
        'totalAmount': round(totalAmount, 3),
        'totalPrice': round(totalPrice, 3),
        'avgEntry': round(totalDca,3)
    }, ignore_index=True)

totalSpent = dca_df['totalPrice'].sum()
print(datetime.datetime.today())
print(dca_df)
print("total spent: " + str(round(totalSpent,3)))
