import pytest
from card_transactions.alert_email import *

def test_date():
    transaction = Card_Transaction({"card_num": 1234567890123456, "merchant_account_id": 12, "memo": "Batter my heart, three personed God, for you", "transfer_value": 121.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 1, "type": "card_transaction"})
    al = Alerter("test@example.com", "Test User", 9001, transaction)
    al.stamp = "2022-01-14T03:43:12.26241"
    assert al.init_date() == "Friday, 14 January 2022 at 03:43 AM"

def test_body():
    transaction = Card_Transaction({"card_num": 1234567890123456, "merchant_account_id": 12, "memo": "As yet but knock, breathe, shine and seek to mend", "transfer_value": 121.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 1, "type": "card_transaction"})
    al = Alerter("test@example.com", "Test User", 9001, transaction)
    print(al.body)
    assert(al.body ==   "Dear Test User,\n\nWe have detected a potentially fraudulent transaction from your account and your account has been frozen. \n" \
                        "The transaction is from account number 9001 in the amount of $121.1 on Wednesday, 06 October 2021 at 11:28 AM.\n" \
                        "The memo for this transaction is: As yet but knock, breathe, shine and seek to mend.\nIf you do not recognize this transaction, please contact us immediately at 1-800-555-4862 or online at utopia-financial.com.\n" \
                        "If you do recognize this transaction, you best hope we implement account reactivation soon.\n\n" \
                        "Best, \nThe Utopia Financial Fraud Response Team\n\n\n" \
                        "Please do not reply to this email.")
def test_send():
    transaction = Card_Transaction({"card_num": 1234567890123456, "merchant_account_id": 12, "memo": "That I may rise and stand, o'erthrow me and bend", "transfer_value": 121.1, \
        "pin": 1234, "cvc1": 123, "cvc2": None, "location": "VA", "time_stamp": "2021-10-06 11:28:47.209401", "status": 1, "type": "card_transaction"})
    al = Alerter("test@example.com", "Test User", 9001, transaction)
    assert(al.send_alert())