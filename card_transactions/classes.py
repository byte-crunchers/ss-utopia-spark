#Holds data classes for common reference

def check_null(string: str) -> str:
    if string == '\\N' or string == '':
        return None
    return string


class Card_Transaction:
    
    def __init__(self, json_dict: dict) -> None:
        self.card = json_dict["card_num"]
        self.acc = json_dict["merchant_account_id"]
        self.memo = json_dict["memo"]
        self.value = json_dict["transfer_value"]
        self.pin = check_null(json_dict["pin"])
        self.cvc1 = check_null(json_dict["cvc1"])
        self.cvc2 = check_null(json_dict["cvc2"])
        self.location = json_dict["location"]
        self.time_stamp = json_dict["time_stamp"]
        self.status = json_dict["status"]

class Card:
    def __init__(self, row) -> None:
        self.acc = row[0]
        self.num = row[1]
        self.pin = check_null(row[2])
        self.cvc1 = row[3]
        self.cvc2 = row[4]
        self.exp = row[5]



class Account:
    
    def __init__(self, row) -> None:
        self
        self.id = row[0]
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