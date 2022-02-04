import boto3
import os
from botocore.exceptions import ClientError
from card_transactions.classes import Card_Transaction
from datetime import datetime
from dateutil.parser import isoparse

#This class sends a fraud alert email 
#It is a class and not a method so I can test it easier
#Please only call init and run - everything else is only broken up for testing
class Alerter:
    def __init__(self, address: str, name: str, acc_num: int, transaction:Card_Transaction) -> None:
        self.address = address
        self.name    = name
        self.acc_num = acc_num
        self.stamp   = transaction.time_stamp
        self.value   = transaction.value
        self.memo    = transaction.memo
        self.date    = self.init_date()
        self.body    = self.init_body()
    

    #format from ISO 8601 to human readable
    def init_date(self) -> str:
        date = isoparse(self.stamp)
        return date.strftime("%A, %d %B %Y at %I:%M %p")

    def init_body(self) -> str:
        return  "Dear " + self.name + ",\n\nWe have detected a potentially fraudulent transaction from your account and your account has been frozen. \n" \
                "The transaction is from account number "+str(self.acc_num)+" in the amount of $"+str(self.value)+" on "+self.date+".\n" \
                "The memo for this transaction is: "+self.memo+".\nIf you do not recognize this transaction, please contact us immediately at 1-800-555-4862 or online at utopia-financial.com.\n" \
                "If you do recognize this transaction, you best hope we implement account reactivation soon.\n\n" \
                "Best, \nThe Utopia Financial Fraud Response Team\n\n\n" \
                "Please do not reply to this email." \
                .format(name=self.name, num=self.acc_num, amount=self.value, date=self.date, mem=self.memo)
    
    def send_alert(self):
        
        sender = "noreply@utopia-financial.com"
        
        sub = "Notification of Potential Fraud"

        charset = "UTF-8"

        client = boto3.client('ses', region_name='us-east-1',
            aws_access_key_id=os.environ.get("ACCESS_KEY"), aws_secret_access_key=os.environ.get("SECRET_KEY"))
        response = None
        try:
            response = client.send_email(
                Destination={
                    'ToAddresses': [self.address]
                },
                Message={
                    'Body':{
                        'Text':{
                            'Charset': charset,
                            'Data': self.body
                        }
                    },
                    'Subject':{
                        'Charset': charset,
                        'Data': sub
                    }
                },
                Source=sender
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        return response