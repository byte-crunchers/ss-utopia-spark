import random
import boto3
import os
import jaydebeapi
import traceback
import json
from enum import IntEnum
import datetime
from dateutil import parser as date_parse
import sys
from boto3.dynamodb.types import TypeSerializer
from card_transactions.fraud_analysis import Analyzer as FraudAnalyzer
from card_transactions.classes import Card_Transaction, Account, Card
from decimal import Decimal
from card_transactions.alert_email import Alerter

class TransactionStatus(IntEnum):
    accepted = 1
    insufficient_funds = -1
    inactive_account = -2
    expired = -3
    invalid = -4
    no_card = -7
    potential_fraud = -8


def date_to_string(date): #differs from str(date) in that it accepts none
    if date:
        return str(date)
    return None

def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    try:
        trans = Card_Transaction(message)
        if (trans.status != 0): #make sure we haven't already processed it
            #print('transaction already processed')
            return
        curs = conn.cursor()
        curs.execute("select * from cards where card_num = ?", (trans.card,))
        try:
            card = Card(curs.fetchall()[0])
        except:
            #print("transaction rejected - no such card")
            trans.status = TransactionStatus.no_card
            return record_anomoly(trans, conn, message)
        curs.execute("select * from accounts where id = ?", (card.acc,))
        origin = Account(curs.fetchall()[0])
        curs.execute("select * from accounts where id = ?", (trans.acc,))
        dest = Account(curs.fetchall()[0])

        #Make sure both accounts and the card are active
        if not origin.active or not dest.active:
            #print("transaction rejected - inactive account")
            trans.status = TransactionStatus.inactive_account
            return record_anomoly(trans, conn, message)
        #Make sure there's enough money
        if (origin.limit):
            av_funds = float(origin.balance) - float(origin.limit)
        else:
            av_funds = float(origin.balance)
        if av_funds < trans.value:
            #print("transaction rejected - not enough funds")
            trans.status = TransactionStatus.insufficient_funds
            return record_anomoly(trans, conn, message)

        if datetime.datetime.now() > date_parse.parse(card.exp):
            #print ("transaction rejected - expired card")
            trans.status = TransactionStatus.expired
            return record_anomoly(trans, conn, message)

        if trans.cvc1: #in person
            if trans.cvc1 != card.cvc1:
                #print ("invalid credentials")
                trans.status = TransactionStatus.invalid
                return record_anomoly(trans, conn, message)
            if card.pin and card.pin != trans.pin:                    
                #print ("invalid credentials")
                trans.status = TransactionStatus.invalid
                return record_anomoly(trans, conn, message)
        elif trans.cvc2: #online not amazon
            if trans.cvc2 != card.cvc2:
                #print ("invalid credentials")
                trans.status = TransactionStatus.invalid
                return record_anomoly(trans, conn, message)

        analyzer = FraudAnalyzer(trans, origin, conn)
        analyzer.analyze()
        if analyzer.fraud_value > analyzer.threshold_fraud:
            trans.status = TransactionStatus.potential_fraud
            #deactivate account
            #query = 'UPDATE accounts SET active = 0 WHERE id = ?'
            #curs.execute(query, (origin.id,))
            #get user details
            query =  '''SELECT users.first_name, users.last_name, users.email FROM accounts
                        INNER JOIN users ON users.id = accounts.users_id 
                        WHERE accounts.id = ?'''
            curs.execute(query, (origin.id,))
            user_ret = curs.fetchone() #first_name, last_name, email
            alert = Alerter(user_ret[2], (user_ret[0] + " " + user_ret[1]), origin.id, trans)
            alert.send_alert()
            return record_anomoly(trans, conn, message)

        
        try:
            query = 'UPDATE accounts SET balance = balance - ? WHERE id = ?'
            curs.execute(query, (trans.value, card.acc))
            query = 'UPDATE accounts SET balance = balance + ? WHERE id = ?'
            curs.execute(query, (trans.value, trans.acc))
            
            query = 'INSERT INTO card_transactions(card_num, merchant_account_id, memo, transfer_value, pin, cvc1, cvc2, location, time_stamp, status) VALUES (?,?,?,?,?,?,?,?,?,?)'
            vals = (trans.card, trans.acc, trans.memo, trans.value, trans.pin, trans.cvc1, trans.cvc2, trans.location, date_to_string(trans.time_stamp), 1)
            curs.execute(query, vals)
            #print("submitted transaction")
        except:
            print("could not write transaction", file=sys.stderr)
            
            conn.rollback()
    except:
        print("failed to process transaction", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failover(message)



#used to record an unsuccessful transaction. Should be followed by a return so the transaction is not recorded twice
def record_anomoly(trans: Card_Transaction, conn: jaydebeapi.Connection, message: dict):
    try:
            curs = conn.cursor()
            query = 'INSERT INTO card_transactions(card_num, merchant_account_id, memo, transfer_value, pin, cvc1, cvc2, location, time_stamp, status) VALUES (?,?,?,?,?,?,?,?,?,?)'
            vals = (trans.card, trans.acc, trans.memo, trans.value, trans.pin, trans.cvc1, trans.cvc2, trans.location, date_to_string(trans.time_stamp), trans.status)
            curs.execute(query, vals)
    except:
            print("could not write transaction", file=sys.stderr)
            conn.rollback()
            failover(message)
    return

def failover(message: dict):
    try:
        message['key']=random.randrange(0, 32768) #random 16 bit number to uniquify the entry
        message_dec = json.loads(json.dumps(message), parse_float=Decimal) # "Float types are not supported. Use Decimal types instead." - Boto3, 2021
        serializer = TypeSerializer() #Dynamo/boto doesn't accept raw jsons
        dyn = boto3.client('dynamodb', region_name='us-east-1',
            aws_access_key_id=os.environ.get("ACCESS_KEY"), aws_secret_access_key=os.environ.get("SECRET_KEY"))
        dyn.put_item(TableName='utopia-failover-HA-DynamoDB', Item={k: serializer.serialize(v) for k, v in message_dec.items()}) #I stole this code
    except:
        print('Failed to write to DynamoDB!:\n', file=sys.stderr)
        traceback.print_exc()