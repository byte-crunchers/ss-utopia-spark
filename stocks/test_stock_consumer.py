import os

import jaydebeapi
import pytest
import stock_consumer as sc


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    conn = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL;database_to_upper=false"
                                               "", ["sa", ""],
                              os.environ.get("H2"))
    conn.cursor().execute("set schema bytecrunchers")
    conn.jconn.setAutoCommit(False)
    return conn


def test_h2_insert(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM stocks")
    curs.execute("INSERT INTO stocks VALUES (8001, 'AAPL', 'Apple Inc',-900, 1000, 1000, 1000, 1000,"
                 " '2021-10-25 00:00:00',0)")
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][0] == 8001)


def test_illegal_negative(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM stocks")
    stock = {
        "ticker": "AAPL", "name": "Apple Inc", "price": -900, "market_cap": 1000, "volume": 1000, "high": 1000,
        "low": 1000, "timestamp": "2021-10-06 11:28:47.209401", "volatility": 0.002, "percent_change": 0.001,
        "status": 0
    }
    sc.consume(stock, connect_h2)
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][9] == -2)  # Confirm that the stock has been given illegal negative status
    connect_h2.rollback()


def test_good_low(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM stocks")
    stock = {
        "ticker": "AAPL", "name": "Apple Inc", "price": 900, "market_cap": 1000, "volume": 1000, "high": 1000,
        "low": 1000, "timestamp": "2021-10-06 11:28:47.209401", "volatility": 0.002, "percent_change": 0.001,
        "status": 0
    }
    sc.consume(stock, connect_h2)
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][9] == 1)  # Confirm that the stock has been accepted status
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][7] == 900)  # Confirm that the stock low has been set
    connect_h2.rollback()


def test_good_high(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM stocks")
    stock = {
        "ticker": "AAPL", "name": "Apple Inc", "price": 1100, "market_cap": 1000, "volume": 1000, "high": 1000,
        "low": 1000, "timestamp": "2021-10-06 11:28:47.209401", "volatility": 0.002, "percent_change": 0.001,
        "status": 0
    }
    sc.consume(stock, connect_h2)
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][9] == 1)  # Confirm that the stock has been accepted status
    curs.execute("SELECT * FROM stocks")
    assert(curs.fetchall()[0][6] == 1100)  # Confirm that the stock low has been set
    connect_h2.rollback()


def test_already_processed(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM stocks")
    stock = {
        "ticker": "AAPL", "name": "Apple Inc", "price": 1100, "market_cap": 1000, "volume": 1000, "high": 1000,
        "low": 1000, "timestamp": "2021-10-06 11:28:47.209401", "volatility": 0.002, "percent_change": 0.001,
        "status": 1
    }
    sc.consume(stock, connect_h2)
    curs.execute("SELECT * FROM stocks")
    assert(not curs.fetchall())  # Confirm that nothing has been added
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
