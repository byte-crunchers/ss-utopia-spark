import jaydebeapi
import os
import traceback
import time



def date_to_string(date): #differs from str(date) in that it accepts none
    if date:
        return str(date)
    return None

def consume(message: dict, conn: jaydebeapi.Connection) -> None:
    try:
        #tic = time.perf_counter()
        #toc = time.perf_counter()
        #print ("connection took {:5.f} ms".format((toc-tic)*1000))
        trans: Transaction = parse_trans(message)
        if (trans.status != 0): #make sure we haven't already processed it
            print('transaction already processed')
            return
        curs = conn.cursor()
        curs.execute("select * from accounts where id = ?", (trans.origin,))
        origin = Account(curs.fetchall()[0])
        curs.execute("select * from accounts where id = ?", (trans.destination,))
        dest = Account(curs.fetchall()[0])
        
        if (origin.limit):
            av_funds = float(origin.balance) - float(origin.limit)
        else:
            av_funds = float(origin.balance)

        if av_funds < trans.value:
            print("transaction rejected - not enough funds")
            trans.status = -1
            return
        try:
            query = 'UPDATE accounts SET balance = balance - ? WHERE id = ?'
            curs.execute(query, (trans.value, trans.origin))
            query = 'UPDATE accounts SET balance = balance + ? WHERE id = ?'
            curs.execute(query, (trans.value, trans.destination))
            query = 'INSERT INTO transactions(origin_account, destination_account, memo, transfer_value, time_stamp, status) VALUES (?,?,?,?,?,?)'
            vals = (trans.origin, trans.destination, trans.memo, trans.value, date_to_string(trans.time_stamp), trans.status)
            curs.execute(query, vals)
            print("submitted transaction")
        except:
            print("could not write transaction")
            conn.rollback()
    except:
        print("failed to process transaction")
        traceback.print_exc()

class Transaction:
    def __init__(self):
        self.origin = None
        self.destination = None
        self.memo = None
        self.value = None
        self.time_stamp = None
        self.status = None

def parse_trans(json_dict: dict) -> Transaction:

    trans = Transaction()
    trans.origin = json_dict["origin_accounts_id"]
    trans.destination = json_dict["destination_accounts_id"]
    trans.memo = json_dict["memo"]
    trans.value = json_dict["transfer_value"]
    trans.time_stamp = json_dict["time_stamp"]
    trans.status = json_dict["status"]
    return trans


class Account:
    def __init__(self):
            self.user = None
            self.account_type = None
            self.balance = None
            self.payment_due = None
            self.due_date = None
            self.limit = None
            self.interest = None
            self.active = None
            self.approved = None
            self.confirmed = None

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