import datetime
import traceback
from enum import IntEnum

import jaydebeapi


class StockStatus(IntEnum):
    accepted = 1
    unprocessed = 0
    insufficient_funds = -1
    inactive_dependent = -2
    invalid_account_type = -5
    payment_too_large = -6


class Stock:
    def __init__(self, json_dict: dict):
        self.percent_change = json_dict["percent_change"]
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

    def print_stock(self):
        print(self.ticker, self.name, self.price, self.market_cap, self.volume, self.high, self.low, self.volatility,
              self.percent_change, self.timestamp, sep=" || ")


def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    try:
        stock = Stock(message)
        stock.print_stock()
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
        print("Successful stock recorded!")
    except:
        print("could not write transaction")
        traceback.print_exc()
        conn.rollback()
