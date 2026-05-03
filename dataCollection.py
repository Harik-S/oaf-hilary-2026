import requests
import datetime as dt
import time
import pandas as pd
import numpy as np
from collections import deque
import os

#deribit API
clientID = "KQWpIu3V"
clientSecret = "0GPGBoEWRgGGC0gtIxhQl57RJaDOQ8BbgJuTALbTDyY"

FIRST_TIME = "2021-01-01"

#handle error codes
def coinbaseError(errorCode):
    if errorCode==200:
        return
    elif errorCode==400:
        raise Exception("Bad Request — Invalid request format")
    elif errorCode==401:
        raise Exception("Unauthorised - Invalid API Key")
    elif errorCode==403:
        raise Exception("Forbidden - You do not have access to the requested resource")
    elif errorCode==404:
        raise Exception("Not Found")
    elif errorCode==500:
        raise Exception("Internal Server Error - We had a problem with our server")
    elif errorCode==429:
        raise Exception("Too many requests (10 per second)")
    else:
        raise Exception("Status code not recognised")


#get API time so that the requests work if time is required
def getTime():
    urlTime="https://api.exchange.coinbase.com/time"
    timeResponse=requests.get(urlTime)
    coinbaseError(timeResponse.status_code)
    timeJSON=timeResponse.json()
    return timeJSON['epoch']

#get candles data
def getCandles(endTime, startTime, granularity=3600):
    urlCandles="https://api.exchange.coinbase.com/products/BTC-USD/candles"
    paramsCandles={"start": startTime.isoformat() + "Z", "end": endTime.isoformat() + "Z","granularity": granularity}
    candlesResponse=requests.get(urlCandles,params=paramsCandles)
    coinbaseError(candlesResponse.status_code)
    candlesJSON=candlesResponse.json()
    return list(candlesJSON)
    
def get_BTC_close_prices(firstTime):
    BTCtimes=[]
    BTCclosePrices=[]
    currentTime = dt.datetime.now(dt.timezone.utc)
    prevTime = currentTime.timestamp()
    while currentTime.timestamp() > firstTime:
        startTime = (currentTime - dt.timedelta(hours=300))
        if (startTime.timestamp()<firstTime):
            startTime = dt.datetime.fromtimestamp(firstTime)
        rawData=getCandles(endTime=currentTime,startTime=startTime)
        for i in rawData:
            if (prevTime-i[0] > 3600):
                print("Warning, gap to previous time is " + str(prevTime - i[0]))
                print(dt.datetime.fromtimestamp(prevTime))
                while prevTime > i[0]:
                    prevTime-=3600
                    BTCtimes.append(dt.datetime.fromtimestamp(prevTime))
                    BTCclosePrices.append(i[4])
            BTCtimes.append(dt.datetime.fromtimestamp(i[0]))
            BTCclosePrices.append(i[4])
            prevTime=i[0]
        currentTime = BTCtimes[-1]
        time.sleep(0.1)
    BTCtimes.reverse()
    BTCclosePrices.reverse()
    btc_prices = pd.DataFrame(data={"close": BTCclosePrices}, index=BTCtimes)
    btc_prices.index = pd.to_datetime(btc_prices.index, utc=True)
    return btc_prices

def calculate_returns(btc_prices):
    btc_prices["log_returns"] = np.log(btc_prices["close"] / btc_prices["close"].shift(1))
    return btc_prices

def main():
    first_time_ts = dt.datetime.timestamp(dt.datetime.fromisoformat(FIRST_TIME))
    btc_prices = get_BTC_close_prices(first_time_ts)
    btc_prices = calculate_returns(btc_prices)
    os.makedirs("data", exist_ok=True)
    btc_prices.to_csv("data/btc_prices.csv", index=True)
    print(f"Saved btc_prices.csv ({len(btc_prices)} rows)")

if __name__ == "__main__":
    main()