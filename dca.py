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
orderHistoryFile = os.path.join(invocationDir, (settings['orderHistoryFile']))
cryptos = settings['cryptos']
stableCoins = settings['stableCoins']
startTime = settings['startTime']

#Define logging
logFile = os.path.join(invocationDir, (settings['logFile']))
logLevel = settings['logLevel']
logFormat = '%(asctime)s - %(levelname)s - %(message)s'  
logging.basicConfig(filename=logFile, encoding='utf-8', level=logLevel, format=logFormat)

#Create list of symbols from cryptos & stableCoins ie. BTCUSDT, ETHBUSD.. you can only get order history for specific symbol 
symbols = []
for crypto in cryptos:
    for stableCoin in stableCoins:
        symbols.append(crypto + stableCoin)

#-------- Main functions --------#
def getTimeStamp(s_datetime):
    dt_datetime = datetime.datetime.strptime(s_datetime, '%Y-%m-%d %H:%M:%S.%f')
    ts_datetime = int((datetime.datetime(dt_datetime.year, dt_datetime.month, dt_datetime.day, dt_datetime.hour, dt_datetime.minute, dt_datetime.second).timestamp()) * 1000)
    return ts_datetime 

def processResponseCode(response):
    if response.status_code != 200:
        logging.error(response.json())
        sys.exit()

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
    processResponseCode(r)
    orders = r.json()
    return orders

def processOrderHistory(orderHistory):
    df = pd.DataFrame()
    if len(orderHistory) > 0:
        for order in orderHistory:
            if order['status'] == 'FILLED':
                df = df.append({
                'orderId': int(order['orderId']),
                'time': datetime.datetime.fromtimestamp(order['time']/1000),
                'side':order['side'],
                'pair': order['symbol'],
                'price': order['price'],
                'executedQty': float(order['executedQty']),
                'totalPrice': float(order['cummulativeQuoteQty'])}, ignore_index=True)
    return df

#check if orderHistoryFile already exists
if os.path.isfile(orderHistoryFile):
    #load orderHistory from file to dataframe.
    df_orderHistory = pd.read_csv(orderHistoryFile)
    s_lastOrderDateTime = df_orderHistory['time'].max()
    ts_startTime = (getTimeStamp(s_lastOrderDateTime) + 1000)
    #Write mode = append, no header
    mode = 'a'
    header = False
else:
    df_orderHistory = pd.DataFrame()
    ts_startTime = getTimeStamp(startTime)
    mode = 'w'
    header = True

# Create orderHistory list
orderHistory = []
for symbol in symbols:
    orderHistory += getOrderHistory(symbol=symbol, startTime=ts_startTime)

if len(orderHistory) > 0: # some orders exists
    df_orderHistoryNew = processOrderHistory(orderHistory) #transform new orders to df
    df_orderHistoryNew.to_csv(orderHistoryFile, sep=',', index=False, mode=mode, header=header) # save new orders to file
    if df_orderHistory.empty: #No historic orders
        df_orderHistory = df_orderHistoryNew
    else:
        #some historic orders, append new orders df_orderHistory
        df_orderHistory.append(df_orderHistoryNew) ################# --------------------- NOT WORKING?
else:
    if df_orderHistory.empty:
        logging.info('No historic orders, no new orders')
        sys.exit()

#Calculate totalAmount, TotalPrice, avg entry etc and save to df_dca
df_dca = pd.DataFrame()
for crypto in cryptos:
    totalAmount = df_orderHistory[df_orderHistory['pair'].str.contains(crypto)]['executedQty'].sum()
    totalPrice = df_orderHistory[df_orderHistory['pair'].str.contains(crypto)]['totalPrice'].sum()
    totalDca = totalPrice / totalAmount
    
    df_dca = df_dca.append({
        'crypto': crypto,
        'totalAmount': round(totalAmount, 3),
        'totalPrice': round(totalPrice, 3),
        'avgEntry': round(totalDca,3)
    }, ignore_index=True)

totalSpent = df_dca['totalPrice'].sum()
print(datetime.datetime.today())
print(df_dca)
print("total spent: " + str(round(totalSpent,3)))
