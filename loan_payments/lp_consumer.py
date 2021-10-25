import traceback
from enum import IntEnum

import jaydebeapi



class PaymentStatus(IntEnum):
    accepted = 1
    unprocessed = 0
    insufficient_funds = -1
    inactive_dependent = -2
    invalid_account_type = -5
    payment_too_large = -6


def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    account = None
    loan = None
    try:
        loan_payment = LoanPayment(message)

        # Check that the loan payment hasn't already been processed
        if loan_payment.status != 0:
            print("Loan Payment already processed")
            return
        curs = conn.cursor()

        # Get corresponding account
        try:
            curs.execute("select * from accounts where id = ?", (loan_payment.account_id,))
            account = Account(curs.fetchall()[0])
        except:
            print("Could not find associated account")

        # Get corresponding loan
        try:
            curs.execute("select * from loans where id = ?", (loan_payment.loan_id,))
            loan = Loan(curs.fetchall()[0])
        except:
            print("Could not find associated loan")

        # Check if the Account type is invalid for loan payments
        if account.account_type != "Savings" and account.account_type != "Checking":
            loan_payment.status = PaymentStatus.invalid_account_type
            print("Invalid account type")
            return record_anomaly(loan_payment, conn)

        # Check there is enough money in the account for the payment
        if account.balance < loan_payment.amount:
            loan_payment.status = PaymentStatus.insufficient_funds
            print("Insufficient funds, loan payment rejected")
            return record_anomaly(loan_payment, conn)

        # Check if account or loan is inactive
        if not account.active or not loan.active:
            loan_payment.status = PaymentStatus.inactive_dependent
            print("Either Loan or Account is inactive, loan payment rejected")
            return record_anomaly(loan_payment, conn)

        # Check if the loan payment is too large (greater than loan balance)
        if loan_payment.amount > -loan.balance:
            loan_payment.status = PaymentStatus.payment_too_large
            print("The payment is larger than the loan balance, loan payment rejected")
            return record_anomaly(loan_payment, conn)

        # If all the above condition checks pass, consume the valid loan payment
        loan_payment.status = PaymentStatus.accepted
        record_loan_payment(loan_payment, account, loan, conn)
    except:
        print("failed to process transaction")


class LoanPayment:
    def __init__(self, json_dict: dict) -> None:
        self.loan_id = json_dict["loan_id"]
        self.account_id = json_dict["account_id"]
        self.amount = json_dict["amount"]
        self.time_stamp = json_dict["time_stamp"]
        self.status = json_dict["status"]


class Loan:
    def __init__(self, row) -> None:
        self.id = row[0]
        self.users_id = row[1]
        self.balance = row[2]
        self.interest_rate = row[3]
        self.due_date = row[4]
        self.payment_due = row[5]
        self.loan_type = row[6]
        self.monthly_payment = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]


class Account:
    def __init__(self, row) -> None:
        self.id = row[0]
        self.users_id = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.credit_limit = row[6]
        self.debt_interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]


def date_to_string(date):  # differs from str(date) in that it accepts none
    if date:
        return str(date)
    return None


# Used to record a successful transaction.
def record_loan_payment(loan_payment: LoanPayment, account: Account, loan: Loan, conn: jaydebeapi.Connection):
    try:
        curs = conn.cursor()
        query = 'UPDATE accounts SET balance = balance - ? WHERE id = ?'
        curs.execute(query, (loan_payment.amount, account.id))
        query = 'UPDATE loans SET balance = balance + ? WHERE id = ?'
        curs.execute(query, (loan_payment.amount, loan.id))

        query = 'INSERT INTO loan_payments(loan_id, account_id, amount, time_stamp, status) VALUES (?,?,?,?,?)'
        vals = (
            loan_payment.loan_id, loan_payment.account_id, loan_payment.amount,
            date_to_string(loan_payment.time_stamp),
            loan_payment.status)
        curs.execute(query, vals)
        loan_payment.status = PaymentStatus.accepted
        print("Successful payment recorded!")
    except:
        print("could not write transaction")
        traceback.print_exc()
        conn.rollback()


# Used to record an unsuccessful transaction. Should be followed by a return so the transaction is not recorded twice
def record_anomaly(loan_payment: LoanPayment, conn: jaydebeapi.Connection):
    try:
        curs = conn.cursor()
        query = 'INSERT INTO loan_payments(loan_id, account_id, amount, time_stamp, status) VALUES (?,?,?,?,?)'
        vals = (
            loan_payment.loan_id, loan_payment.account_id, loan_payment.amount,
            date_to_string(loan_payment.time_stamp),
            loan_payment.status)
        curs.execute(query, vals)
        print("Anomally recorded: " + str(loan_payment.status))
    except:
        traceback.print_exc()
        print("could not write transaction")
        conn.rollback()
    return
