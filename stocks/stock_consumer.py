import traceback
from enum import IntEnum

import jaydebeapi
import pandas as pd


class StockStatus(IntEnum):
    accepted = 1
    unprocessed = 0
    illegal_null = -1
    illegal_negative = -2


class Stock:
    def __init__(self, json_dict: dict):
        self.ticker = json_dict["ticker"]
        self.name = json_dict["name"]
        self.price = json_dict["price"]
        self.market_cap = json_dict["market_cap"]
        self.volume = json_dict["volume"]
        self.high = json_dict["high"]
        self.low = json_dict["low"]
        self.timestamp = json_dict["timestamp"]
        self.volatility = json_dict["volatility"]
        self.status = json_dict["status"]
        self.percent_change = json_dict["percent_change"]

    def print_stock(self):
        print(self.ticker, self.name, self.price, self.market_cap, self.volume, self.high, self.low, self.volatility,
              self.percent_change, self.timestamp, sep=" || ")


#  Transform stock data and persist it to the database
def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    try:
        stock = Stock(message)

        #  Properly format null market caps
        if pd.isna(stock.market_cap):
            stock.market_cap = 0

        # Make sure Stock hasn't already been processed
        if stock.status != 0:
            return

        # Check for illegal negatives
        if stock.price < 0 or stock.market_cap < 0 or stock.volume < 0 or stock.high < 0 or stock.low < 0:
            stock.status = StockStatus.illegal_negative
            return record_anomaly(stock, conn)

        # Check for illegal nulls
        if (not stock.ticker.strip() or not stock.ticker.strip or not isinstance(stock.price, (int, float))
                or not isinstance(stock.volume, (int, float)) or not isinstance(stock.high, (int, float))
                or not isinstance(stock.low, (int, float)) or not isinstance(stock.status, (int, float))):
            stock.status = StockStatus.illegal_null

        # Fix the high and low
        if stock.high < stock.price:
            stock.high = stock.price
        if stock.low > stock.price:
            stock.low = stock.price
        record_stock(stock, conn)
    except:
        traceback.print_exc()


# Used to record a successful stock price addition
def record_stock(stock: Stock, conn: jaydebeapi.Connection):
    try:
        curs = conn.cursor()
        query = 'INSERT INTO stocks(ticker, name, price, market_cap, volume, high, low, timestamp, status) VALUES ' \
                '(?,?,?,?, ?, ?, ?, ?, ?) '
        stock.status = StockStatus.accepted
        vals = (
            stock.ticker, stock.name, stock.price, stock.market_cap, stock.volume, stock.high, stock.low,
            stock.timestamp, stock.status
        )
        curs.execute(query, vals)
    except:
        print("could not write transaction")
        traceback.print_exc()
        conn.rollback()


# Used to record a successful stock price addition
def record_anomaly(stock: Stock, conn: jaydebeapi.Connection):
    try:
        curs = conn.cursor()
        query = 'INSERT INTO stocks(ticker, name, price, market_cap, volume, high, low, timestamp, status) VALUES ' \
                '(?,?,?,?, ?, ?, ?, ?, ?) '
        vals = (
            stock.ticker, stock.name, stock.price, stock.market_cap, stock.volume, stock.high, stock.low,
            stock.timestamp, stock.status
        )
        curs.execute(query, vals)
        print("Anomaly Recorded!")
    except:
        print("could not write transaction")
        traceback.print_exc()
        conn.rollback()
