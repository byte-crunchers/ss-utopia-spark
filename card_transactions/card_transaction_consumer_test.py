import pytest
import jaydebeapi
import os
import card_transactions.card_transaction_consumer as ctc


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
    con.jconn.setAutoCommit(False)
    return con

def test_bad_dict(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    trans = {"origin_accounts_id": 9001,"memo": "Batter my heart, three personed god, for you", "status": 0, "type": "transaction"}
    ctc.consume(trans, connect_h2)
    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456") #make sure the transaction didn't write
    assert not curs.fetchall()
    connect_h2.rollback()

def test_already_processed(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 12, "memo": "As yet but knock, breathe, shine and seek to mend", "transfer_value": 121.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 1, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)
    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert not curs.fetchall()
    connect_h2.rollback()
    
def test_inactive(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 0, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9002, 1234567890123456, 1234, 567, 890, '2023-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9001, "memo": "That I may rise and stand, o'erthrow me, and bend", "transfer_value": 21.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.inactive_account
    connect_h2.rollback()




def test_not_enough(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9002, 1234567890123456, 1234, 123, 456, '2023-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9001, "memo": "Thy force to break, blow, burn and make me new", "transfer_value": 521.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.insufficient_funds
    connect_h2.rollback()


def test_expired(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9002, 1234567890123456, 1234, 123, 456, '2020-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9001, "memo": "I, like an usurped town to another due", "transfer_value": 21.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.expired
    connect_h2.rollback()

def test_invalid(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9002, 1234567890123456, 1234, 123, 456, '2023-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9001, "memo": "Labor to admit you, but oh to no end", "transfer_value": 21.1, \
        "pin": None, "cvc1": None, "cvc2": 457, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.invalid
    connect_h2.rollback()

def test_good(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9002, 1234567890123456, 1234, 123, 456, '2023-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9001, "memo": "Reason, your viceroy in me, me should defend", "transfer_value": 420.00, \
        "pin": None, "cvc1": None, "cvc2": 456, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.accepted
    
    curs.execute("SELECT balance FROM accounts WHERE id = 9002")
    assert curs.fetchall()[0][0] == 80.00

    connect_h2.rollback()


def test_good_credit(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9003")
    assert not curs.fetchall()
    
    curs.execute("INSERT INTO accounts VALUES (9003, 1, 'Plus Credit', -2600, 200.00, '2021-10-23', -4204, 0.10435, 1, 1, 1), \
                                              (9004, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO cards VALUES (9003, 1234567890123456, null, 123, 456, '2023-10-30')")
    trans = {"card_num": 1234567890123456, "merchant_account_id": 9004, "memo": "But is captured, and proves weak or untrue", "transfer_value": 400.00, \
        "pin": None, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "card_transaction"}
    ctc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM card_transactions WHERE card_num = 1234567890123456")
    assert curs.fetchall()[0][10] == ctc.PaymentStatus.accepted
    
    curs.execute("SELECT balance FROM accounts WHERE id = 9003")
    assert curs.fetchall()[0][0] == -3000.00

    connect_h2.rollback()