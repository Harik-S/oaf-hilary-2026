import requests
import datetime
import time
import pandas as pd
import numpy as np
from collections import deque
import os

#deribit API
clientID = "KQWpIu3V"
clientSecret = "0GPGBoEWRgGGC0gtIxhQl57RJaDOQ8BbgJuTALbTDyY"

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
    

BTCtimes=[]
BTCclosePrices=[]
firstTime = 1609459200 # 1-1-2021
currentTime = datetime.datetime.now(datetime.timezone.utc)
prevTime = currentTime.timestamp()
while currentTime.timestamp() > firstTime:
    startTime = (currentTime - datetime.timedelta(hours=300))
    if (startTime.timestamp()<firstTime):
        startTime = datetime.datetime.fromtimestamp(firstTime)
    rawData=getCandles(endTime=currentTime,startTime=startTime)
    for i in rawData:
        if (prevTime-i[0] > 3600):
            print("Warning, gap to previous time is " + str(prevTime - i[0]))
            print(datetime.datetime.fromtimestamp(prevTime))
            while prevTime > i[0]:
                prevTime-=3600
                BTCtimes.append(datetime.datetime.fromtimestamp(prevTime))
                BTCclosePrices.append(i[4])
        BTCtimes.append(datetime.datetime.fromtimestamp(i[0]))
        BTCclosePrices.append(i[4])
        prevTime=i[0]
    currentTime = BTCtimes[-1]
    time.sleep(0.1)
BTCtimes.reverse()
BTCclosePrices.reverse()

# Helper: classify each timestamp as 'weekend' or 'weekday'
# Weekend = Saturday 8am UTC to Monday 8am UTC
def is_weekend(dt):
    # Convert to UTC if not already timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    weekday = dt.weekday()  # Monday=0, Sunday=6
    hour = dt.hour

    # Saturday (5) at 08:00 onwards
    if weekday == 5 and hour >= 8:
        return True
    # All of Sunday (6)
    if weekday == 6:
        return True
    # Monday (0) before 08:00
    if weekday == 0 and hour < 8:
        return True
    return False

# Assign flags
day_flags = ['weekend' if is_weekend(dt) else 'weekday' for dt in BTCtimes]

# subtask 3 - separate RV buffers for weekday and weekend
rv_5d = [np.nan]
n = 120  # 5 * 24 hours

weekday_buffer = deque(maxlen=n)
weekend_buffer = deque(maxlen=n)
weekday_sum = 0.0
weekend_sum = 0.0

for i in range(1, len(BTCclosePrices)):
    x = np.log(BTCclosePrices[i] / BTCclosePrices[i-1])
    sq = x * x
    flag = day_flags[i]

    if flag == 'weekday':
        if len(weekday_buffer) == n:
            weekday_sum -= weekday_buffer[0]
        weekday_buffer.append(sq)
        weekday_sum += sq

        if len(weekday_buffer) == n:
            rv_5d.append(np.sqrt(weekday_sum / n) * np.sqrt(8760))
        else:
            rv_5d.append(np.nan)

    else:  # weekend
        if len(weekend_buffer) == n:
            weekend_sum -= weekend_buffer[0]
        weekend_buffer.append(sq)
        weekend_sum += sq

        if len(weekend_buffer) == n:
            rv_5d.append(np.sqrt(weekend_sum / n) * np.sqrt(8760))
        else:
            rv_5d.append(np.nan)


        

btc_prices = pd.DataFrame(data={"close": BTCclosePrices, "rv_5d": rv_5d}, index=BTCtimes)
btc_prices.index = pd.to_datetime(btc_prices.index, utc=True)


os.makedirs("data", exist_ok=True)
btc_prices.to_csv("data/btc_prices.csv", index=True)
print(f"Saved btc_prices.csv ({len(btc_prices)} rows)")