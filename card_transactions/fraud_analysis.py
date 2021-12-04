import datetime
from card_transactions.card_transaction_consumer import Card_Transaction, Account
import jaydebeapi
import os

class Analyzer:
    def __init__(this, trans: Card_Transaction, acc: Account, conn: jaydebeapi.Connection) -> None:
        this.trans=trans
        this.acc=acc
        this.conn=conn
        this.fraud_value=0.0 #analytical value of how likely this transafction is fraudulent
        this.threshold_fraud=os.environ.get("THRESHOLD_FRAUD") #a fraud_value above this threshold will trigger fraud prevention
        this.threshold_velocity=os.environ.get("THRESHOLD_VELOCITY") #a fraud value above this will trigger a velocity analysis
        this.weighting_cvc=os.environ.get("WEIGHTING_CVC")
        this.weighting_amount=os.environ.get("WEIGHTING_AMOUNT")
        this.weighting_location=os.environ.get("WEIGHTING_LOCATION")
        this.weighting_velocity=os.environ.get("WEIGHTING_VELOCITY")
        this.sample_chance=os.environ.get("FRAUD_SAMPLE_CHANCE")
        

    def anal_cvc(this): #checks to see if there is a cvc on this transaction (amazon has none)
        if (not this.trans.cvc1) and (not this.trans.cvc2):
            this.fraud_value += this.weighting_cvc
        return
    
    def anal_location(this): #checks if the card is being used outside the US
        if this.trans.location != 'US':
            this.fraud_value += this.weighting_location
        return

    def anal_amount(this): #adds to fraud_value based on ratio of transaction value vs available funds
        if (this.acc.limit):
            av_funds = float(this.acc.balance) - float(this.acc.limit)
        else:
            av_funds = float(this.acc.balance)
        this.fraud_value += this.weighting_amount * this.trans.value / av_funds
        return

    #checks how the spending patterns of the last few days compare to the average of the previous months
    #This is DB heavy so it should only be used if other values indecate possible fraud
    def anal_velocity(this):
        curs = this.conn.cursor()
        current_date = datetime.datetime.now()
        window = current_date - datetime.timedelta(days = 3)
        comparison_date = current_date - datetime.timedelta(days = 33)
        recent = curs.execute("SELECT SUM(transfer_value) FROM card_transactions \
                               WHERE card_num = 4319231021984280 AND time_stamp > '" + str(window) +  "'")[0]
        history = curs.execute("SELECT SUM(transfer_value) FROM card_transactions \
                                WHERE card_num = 4319231021984280 \
                                AND time_stamp BTWEEN '" + str(window) +  "' AND '" + str(comparison_date) + "'")[0]
        this.fraud_value += (10 * recent / history - 1) * this.weighting_velocity

        



        


        
    
    

     