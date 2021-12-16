import pytest
import jaydebeapi
import os
from card_transactions.fraud_analysis import *

@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
    con.jconn.setAutoCommit(False)
    return con

def test_cvc(connect_h2):
    trans_dict = {
        "card_num":1234567890123456,
        "merchant_account_id":8086,
        "memo":"But the kitten sitting lonely",
        "transfer_value":500.52,
        "pin":None,
        "cvc1":None,
        "cvc2":None,
        "location":"TX",
        "time_stamp": "1863-07-03T14:27:42",
        "status":0,
        }

    trans = Card_Transaction(trans_dict)

    acc_row = (None, 15, "debit", 52, 500, 0, None, None, None, 1, 1, 1)

    """self.user = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.limit = row[6]
        self.interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]"""

    acc = Account(acc_row)


    anal = Analyzer(trans, acc, connect_h2)

    assert anal.fraud_value == 0
    anal.anal_cvc()
    assert anal.fraud_value == anal.weighting_cvc
    assert anal.fraud_value > 0

def test_location(connect_h2):
    trans_dict = {
        "card_num":1234567890123456,
        "merchant_account_id":8086,
        "memo":"But the kitten sitting lonely",
        "transfer_value":500.52,
        "pin":None,
        "cvc1":None,
        "cvc2":None,
        "location":"UK",
        "time_stamp": "1863-07-03T14:27:42",
        "status":0,
        }

    trans = Card_Transaction(trans_dict)

    acc_row = (None, 15, "debit", 52, 500, 0, None, None, None, 1, 1, 1)

    """self.user = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.limit = row[6]
        self.interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]"""

    acc = Account(acc_row)


    anal = Analyzer(trans, acc, connect_h2)

    assert anal.fraud_value == 0
    anal.anal_location()
    assert anal.fraud_value == anal.weighting_location
    assert anal.fraud_value > 0

def test_amount(connect_h2):
    trans_dict = {
        "card_num":1234567890123456,
        "merchant_account_id":8086,
        "memo":"But the kitten sitting lonely",
        "transfer_value":20,
        "pin":None,
        "cvc1":None,
        "cvc2":None,
        "location":"UK",
        "time_stamp": "1863-07-03T14:27:42",
        "status":0,
        }

    trans = Card_Transaction(trans_dict)

    acc_row = (None, 15, "debit", 100, 500, 0, None, None, None, 1, 1, 1)

    """self.user = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.limit = row[6]
        self.interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]"""

    acc = Account(acc_row)


    anal = Analyzer(trans, acc, connect_h2)

    

    assert anal.fraud_value == 0
    anal.anal_amount()
    assert anal.fraud_value == anal.weighting_amount*0.2
    assert anal.fraud_value > 0

    # def anal_amount(this): #adds to fraud_value based on ratio of transaction value vs available funds
    #     if (this.acc.limit):
    #         av_funds = float(this.acc.balance) - float(this.acc.limit)
    #     else:
    #         av_funds = float(this.acc.balance)
    #     this.fraud_value += this.weighting_amount * this.trans.value / av_funds
    #     return


def test_velocity(connect_h2):
    curs = connect_h2.cursor()

    #To test the velocity analysis, we must add in data to analyze
    #In this case it is transaction history over the past month
    #The analyzer works with two windows - one over the past three days and one over the previous 30 days
    #We make it so that the average of the previous month is far lower than the past 3 days ($50 per day vs $166 per day) 
    curs.execute("INSERT INTO card_transactions values(9000, 1234567890123456, 12345, 'mine', 500, null, null, null, 'US', '" 
                     + str(datetime.datetime.now() - datetime.timedelta(days = 1)) +"', 1)")
    curs.execute("INSERT INTO card_transactions values(9001, 1234567890123456, 12345, 'mine', 1000, null, null, null, 'US', '" 
                     + str(datetime.datetime.now() - datetime.timedelta(days = 5)) +"', 1)")
                     
    curs.execute("INSERT INTO card_transactions values(9002, 1234567890123456, 12345, 'mine', 500, null, null, null, 'US', '" 
                     + str(datetime.datetime.now() - datetime.timedelta(days = 10)) +"', 1)")

    trans_dict = {
        "card_num":1234567890123456,
        "merchant_account_id":8086,
        "memo":"But the kitten sitting lonely",
        "transfer_value":20,
        "pin":None,
        "cvc1":None,
        "cvc2":None,
        "location":"UK",
        "time_stamp": "1863-07-03T14:27:42",
        "status":0,
        }

    trans = Card_Transaction(trans_dict)

    acc_row = (None, 15, "debit", 100, 500, 0, None, None, None, 1, 1, 1)

    """self.user = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.limit = row[6]
        self.interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]"""

    acc = Account(acc_row)


    anal = Analyzer(trans, acc, connect_h2)

    

    assert anal.fraud_value == 0
    anal.anal_velocity()
    assert anal.fraud_value > anal.weighting_velocity*2 #166/50 - 1 > 2
    assert anal.fraud_value > 0

    connect_h2.rollback()
