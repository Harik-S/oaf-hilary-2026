import requests
import datetime
import time
import pandas as pd

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
currentTime = datetime.datetime.now(datetime.UTC)
prevTime = currentTime.timestamp()
while currentTime.timestamp() > firstTime:
    startTime = (currentTime - datetime.timedelta(hours=300))
    if (startTime.timestamp()<firstTime):
        startTime = datetime.datetime.fromtimestamp(firstTime)
    rawData=getCandles(endTime=currentTime,startTime=startTime)
    for i in rawData:
        BTCtimes.append(datetime.datetime.fromtimestamp(i[0]))
        BTCclosePrices.append(i[4])
        if (i[0]-prevTime > 3600):
            print("Warning, gap to previous time is " + str(i[0]-prevTime))
        prevTime=i[0]
    currentTime = BTCtimes[-1]
    time.sleep(0.1)

btc_prices = pd.DataFrame(data={"close": BTCclosePrices}, index=BTCtimes)
print(btc_prices)