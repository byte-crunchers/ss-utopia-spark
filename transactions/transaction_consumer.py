import jaydebeapi
import traceback
import sys
import random
import boto3
import os

class Transaction:
    
    def __init__(self, json_dict: dict) -> None:
        self.origin = json_dict["origin_accounts_id"]
        self.destination = json_dict["destination_accounts_id"]
        self.memo = json_dict["memo"]
        self.value = json_dict["transfer_value"]
        self.time_stamp = json_dict["time_stamp"]
        self.status = json_dict["status"]


class Account:
    
    def __init__(self, row) -> None:
        self
        self.user = row[1]
        self.account_type = row[2]
        self.balance = row[3]
        self.payment_due = row[4]
        self.due_date = row[5]
        self.limit = row[6]
        self.interest = row[7]
        self.active = row[8]
        self.approved = row[9]
        self.confirmed = row[10]

def failover(message: dict):
    try:
        message['key']=random.randrange(0, 32768) #random 16 bit number to uniquify the entry
        dyn = boto3.client('dynamodb', region_name='us-east-1',
            aws_access_key_id=os.environ.get("ACCESS_KEY"), aws_secret_access_key=os.environ.get("SECRET_KEY"))
        dyn.put_item(TableName='utopia-failover-HA-DynamoDB', Item=message)
    except:
        print('Failed to write to DynamoDB!:\n {}\n'.format(message), file=sys.stderr)

def date_to_string(date): #differs from str(date) in that it accepts none
    if date:
        return str(date)
    return None

def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    try:
        trans = Transaction(message)
        if (trans.status != 0): #make sure we haven't already processed it
            #print('transaction already processed')
            return
        curs = conn.cursor()
        curs.execute("select * from accounts where id = ?", (trans.origin,))
        origin = Account(curs.fetchall()[0])
        curs.execute("select * from accounts where id = ?", (trans.destination,))
        dest = Account(curs.fetchall()[0])

        #Make sure both accounts are active
        if not origin.active or not dest.active:
            #print("transaction rejected - inactive account")
            trans.status = -2
            return record_anomoly(trans, conn, message)

        #Make sure there's enough money
        if (origin.limit):
            av_funds = float(origin.balance) - float(origin.limit)
        else:
            av_funds = float(origin.balance)
        if av_funds < trans.value:
            #print("transaction rejected - not enough funds")
            trans.status = -1
            return record_anomoly(trans, conn, message)

        try:
            query = 'UPDATE accounts SET balance = balance - ? WHERE id = ?'
            curs.execute(query, (trans.value, trans.origin))
            query = 'UPDATE accounts SET balance = balance + ? WHERE id = ?'
            curs.execute(query, (trans.value, trans.destination))
            query = 'INSERT INTO transactions(origin_account, destination_account, memo, transfer_value, time_stamp, status) VALUES (?,?,?,?,?,?)'
            vals = (trans.origin, trans.destination, trans.memo, trans.value, date_to_string(trans.time_stamp), 1) #trans status is one for successful transactions
            curs.execute(query, vals)
            #print("submitted transaction")
        except:
            print("could not write transaction", file=sys.stderr)
            conn.rollback()
    except:
        print("failed to process transaction", file=sys.stderr)
        traceback.print_exc()

#used to record an unsuccessful transaction. Should be followed by a return so the transaction is not recorded twice
def record_anomoly(trans: Transaction, conn: jaydebeapi.Connection, message: dict):
    try:
            curs = conn.cursor()
            query = 'INSERT INTO transactions(origin_account, destination_account, memo, transfer_value, time_stamp, status) VALUES (?,?,?,?,?,?)'
            vals = (trans.origin, trans.destination, trans.memo, trans.value, date_to_string(trans.time_stamp), trans.status)
            curs.execute(query, vals)
    except:
            print("could not write transaction", file=sys.stderr)
            conn.rollback()
            failover(message)
    return

