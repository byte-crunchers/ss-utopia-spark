import pytest
import jaydebeapi
import os
import transactions.transaction_consumer as tc


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
    con.jconn.setAutoCommit(False)
    return con

def test_bad_dict(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001")
    assert not curs.fetchall()
    trans = {"origin_accounts_id": 9001,"memo": "Batter my heart, three personed god, for you", "status": 0, "type": "transaction"}
    tc.consume(trans, connect_h2)
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001") #make sure the transaction didn't write
    assert not curs.fetchall()
    connect_h2.rollback()

def test_already_processed(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001")
    assert not curs.fetchall()
    trans = {"origin_accounts_id": 9001, "destination_accounts_id": 12, "memo": "As yet but knock, breathe, shine and seek to mend", "transfer_value": 121.1, "time_stamp": "2021-10-06 11:28:47.209401", "status": 1, "type": "transaction"}
    tc.consume(trans, connect_h2)
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001")
    assert not curs.fetchall()
    connect_h2.rollback()
    
def test_inactive(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 0, 1, 1)")
    trans = {"origin_accounts_id": 9001, "destination_accounts_id": 9002, "memo": "That I may rise and stand, o'erthrow me, and bend", "transfer_value": 80.12, "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "transaction"}
    tc.consume(trans, connect_h2)

    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001")
    assert curs.fetchall()[0][6] == -2 #status code for inactive
    connect_h2.rollback()

def test_good(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM transactions")
    
    curs.execute("INSERT INTO accounts VALUES (9001, 1, 'Savings', 100.00, 0, null, null, 0, 1, 1, 1), \
                                              (9002, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    trans = {"origin_accounts_id": 9001, "destination_accounts_id": 9002, "memo": "Your force to break, blow, burn and make me new", "transfer_value": 20.00, "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "transaction"}
    tc.consume(trans, connect_h2)
    
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9001")
    assert curs.fetchall()
    
    curs.execute("SELECT balance FROM accounts WHERE id = 9001")
    assert curs.fetchall()[0][0] == 80

    connect_h2.rollback()


def test_good_credit(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9003")
    assert not curs.fetchall()
    
    curs.execute("INSERT INTO accounts VALUES (9003, 1, 'Plus Credit', -3295.42, 329.54, '2021-10-23', -4204, 0.10435, 1, 1, 1), \
                                              (9004, 2, 'Checking', 500.00, 0, null, null, 0, 1, 1, 1)")
    trans = {"origin_accounts_id": 9003, "destination_accounts_id": 9004, "memo": "I, like an usurp'd town to another due", "transfer_value": 100, "time_stamp": "2021-10-06 11:28:47.209401", "status": 0, "type": "transaction"}
    tc.consume(trans, connect_h2)
    
    curs.execute("SELECT * FROM transactions WHERE origin_account = 9003")
    assert curs.fetchall()

    curs.execute("SELECT balance FROM accounts WHERE id = 9003")
    assert curs.fetchall()[0][0] == -3395.42

    connect_h2.rollback()
