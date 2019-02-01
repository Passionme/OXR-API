# -*- coding: utf-8 -*-

"""

This module implements the End points to display all available
International-standard 3-letter ISO currency conversion rates for a requested currency or amount

#copyright# (c) 2019 by Esther

"""
from flask import Flask, request
import requests
import redis
import json
import mysql

app = Flask(__name__)

OXR_url = "https://openexchangerates.org/api/latest.json"
API_ID = "283667971248475384007bf39e921fc7"
global index
index = 0

try:
    # Connecting to currency database
    cur = mysql.connection.cursor()
except cur.DataError as e:
    print("Unable to connect to MYSQL",e)
try:
    # Create a redis client
    redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)
except cur.DataError as e:
    print("Unable to connect to REDIS",e)

# Endpoint to grab latest Exchange rates for available ISO3 codes
# and update the requested currency and amount value in USD

@app.route('/grab_and_save', methods=['POST'])
def grab_and_save():
    global index

    data = {}
    # Access Open Exchange Rate API for latest currency rates
    xRate = requests.get(OXR_url, app_id = API_ID )
    xRate = xRate.json()

    # Converting input currency and amount to USD as per current rate
    reqData = request.get_data()
    reqdFlds = ["currency", "amount"]
    for item in reqdFlds:
        if not reqData.get(item):
            return "Input valid data in " + item
    data.update({"currency": reqData.get("currency")})
    data.update({"amount": reqData.get("amount")})

    # Exchange rate of requested currency in USD
    currInUSD = 1 / xRate.get("rates")[data["currency"]]
    currInUSD = float("{0:.8f}".format(currInUSD))  # 8 decimals digits
    EqUSD = data["amount"] * currInUSD
    EqUSD = round(EqUSD, 2)  # rounded to precision 2
    data.update({"rate": reqData.get("currInUSD")})
    data.update({"timeT": reqData.get("timestamp")})
    data.update({"EqUSD": reqData.get("EqUSD")})

    try:
        # Update DB with currency, amount, exchange rate and final price in USD
        cur.execute('''INSERT INTO ExchangeRates (index, currency, amount, rate, EqUSD, timeT)
                        VALUES (%(index)s, %(currency)s, %(amount)s, %(rate)s, %(EqUSD)s, %(timeT)s''', data)
        cur.commit()
        redisClient.lpush("ExchangeRates", data)
    except:
        print("Error while updating the database")

    index += 1

# Endpoint to retrieve Exchange rates from the database and Cache
@app.route('/last', methods=['GET'])
def last():

    parse = 0
    retrvData = request.get_data()
    flds = ["currency", "count"]

    for item in flds:
        if not retrvData.get(item):
            parse += 1

    # Retrieve latest data updated db
    if parse == 0:
        cur.execute("SELECT * FROM ExchangeRates ORDER BY index DESC ")
        latestupdate = cur.fetchone()
        return latestupdate

    # Retrieve latest currency or number of updates of currency in db
    elif parse == 1:
        if retrvData.get("currency"):
            cur.execute("SELECT * FROM ExchangeRates ORDER BY index DESC WHERE currency " + retrvData.get("currency") )
            latestupdate = cur.fetchone()
            redisL = redisClient.sort("ExchangeRates", by='index', get='*->')
            return latestupdate, redisL[0]
        else:
            cur.execute("SELECT * FROM ExchangeRates ORDER BY index DESC" )
            latestupdate = cur.fetchmany( retrvData.get("count"))
            redisL = redisClient.sort("ExchangeRates", by='index', get='*->')

            return latestupdate,redisL[retrvData.get("count")]

    # Retrieve latest number of updates of currency in db
    else:
        cur.execute("SELECT * FROM ExchangeRates ORDER BY index DESC WHERE currency " + retrvData.get("currency"))
        latestupdate = cur.fetchmany(retrvData.get("count"))
        redisL = redisClient.sort("ExchangeRates", by='index-> currency', get='*->')

        return json.dump(latestupdate), redisL[retrvData.get("count")]



if __name__ == '__main__':
    app.run()
