import os

import jaydebeapi
import pytest
import lp_consumer as lpc


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    conn = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL;database_to_upper=false"
                                               "", ["sa", ""],
                              os.environ.get("H2"))
    conn.cursor().execute("set schema bytecrunchers")
    conn.jconn.setAutoCommit(False)
    return conn


def test_bad(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM accounts")
    curs.execute("DELETE FROM loans")
    curs.execute("DELETE FROM loan_payments")
    curs.execute("INSERT INTO loans VALUES (8001,1,-900, 0.07270,'2021-10-25 00:00:00',12809.31,'Morgage',12198.74,1,"
                 "1,1)")
    loan_payment = {
        "loan_id": 8001, "account_id": 8001, "amount": 900, "time_stamp": "2021-10-06 11:28:47.209401", "status": 0
    }
    # Ensure the account and loan have been added with the correct balance
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    # Process the Loan Payment
    lpc.consume(loan_payment, connect_h2)
    curs.execute("SELECT * FROM loan_payments")
    assert not curs.fetchall() # Check that nothing has been added
    connect_h2.rollback()


def test_already_processed(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM accounts")
    curs.execute("DELETE FROM loans")
    curs.execute("DELETE FROM loan_payments")
    curs.execute("INSERT INTO accounts VALUES (8001, 1, 'Savings', 900.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO loans VALUES (8001,1,-900, 0.07270,'2021-10-25 00:00:00',12809.31,'Morgage',12198.74,1,"
                 "1,1)")
    loan_payment = {
        "loan_id": 8001,
        "account_id": 8001,
        "amount": 900,
        "time_stamp": "2021-10-06 11:28:47.209401",
        "status": 1

    }
    # Ensure the account and loan have been added with the correct balance
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 900)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    # Try to process the already processed loan payment
    lpc.consume(loan_payment, connect_h2)
    curs.execute("SELECT * FROM loan_payments")
    assert (curs.fetchall() == [])
    # Check that the balances have not been updated
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 900)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    connect_h2.rollback()


def test_payment_too_large(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM accounts")
    curs.execute("DELETE FROM loans")
    curs.execute("DELETE FROM loan_payments")
    curs.execute("INSERT INTO accounts VALUES (8001, 1, 'Savings', 9900.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO loans VALUES (8001,1,-900, 0.07270,'2021-10-25 00:00:00',12809.31,'Morgage',12198.74,1,"
                 "1,1)")
    loan_payment = {
        "loan_id": 8001,
        "account_id": 8001,
        "amount": 1900,
        "time_stamp": "2021-10-06 11:28:47.209401",
        "status": 0

    }
    # Ensure the account and loan have been added with the correct balance
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 9900)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    # Process the Loan Payment
    lpc.consume(loan_payment, connect_h2)
    # Check that the balances have not been updated and the loan has the correct bad status
    curs.execute("SELECT * FROM loan_payments")
    assert (curs.fetchall()[0][5] == -6)
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 9900)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    connect_h2.rollback()


def test_good(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM accounts")
    curs.execute("DELETE FROM loans")
    curs.execute("DELETE FROM loan_payments")
    curs.execute("INSERT INTO accounts VALUES (8001, 1, 'Savings', 900.00, 0, null, null, 0, 1, 1, 1)")
    curs.execute("INSERT INTO loans VALUES (8001,1,-900, 0.07270,'2021-10-25 00:00:00',12809.31,'Morgage',12198.74,1,"
                 "1,1)")
    loan_payment = {
        "loan_id": 8001,
        "account_id": 8001,
        "amount": 900,
        "time_stamp": "2021-10-06 11:28:47.209401",
        "status": 0

    }
    # Ensure the account and loan have been added with the correct balance
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 900)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == -900)
    # Process the Loan Payment
    lpc.consume(loan_payment, connect_h2)
    curs.execute("SELECT * FROM loan_payments")
    assert (curs.fetchall()[0][5] == 1)
    # Check that the address has been updated
    curs.execute("SELECT * FROM accounts")
    assert (curs.fetchall()[0][3] == 0)
    curs.execute("SELECT * FROM loans")
    assert (curs.fetchall()[0][2] == 0)
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
