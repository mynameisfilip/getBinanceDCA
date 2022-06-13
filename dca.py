import requests
import pandas as pd
import datetime
import time
import json
import hmac
import hashlib
from urllib.parse import urljoin, urlencode

#-------- Variables --------#
# Edit variables: API_KEY, SECRET_KEY, cryptos, stableCoins
API_KEY = '55cb3280f0d8273e3f59139808b56987'
SECRET_KEY = '44e58e460b33d7dd565922fbc59ac920'
BASE_URL = 'https://api.binance.com'
HEADERS = {'X-MBX-APIKEY': API_KEY}
cryptos = ['BTC', 'ETH', 'DOT']
stableCoins = ['USDT', 'BUSD']

symbols = []
for crypto in cryptos:
    for stableCoin in stableCoins:
        symbols.append(crypto + stableCoin)

#-------- Main function --------#
# def getOrderHistory()
def getOrderHistory(symbol):
    endpoint = '/api/v3/allOrders'

    url = urljoin(BASE_URL, endpoint)

    startTime = int((datetime.datetime(2022, 4, 15).timestamp()) * 1000)
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
    orderHistory = getOrderHistory(symbol)
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

#-------- Print output --------#
print(datetime.datetime.today())
print(dca_df)
print("total spent: " + str(round(totalSpent,3)))
