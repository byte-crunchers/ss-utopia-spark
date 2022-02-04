import datetime
from card_transactions.classes import Card_Transaction, Account
import jaydebeapi
import os
import random

class Analyzer:
    def __init__(this, trans: Card_Transaction, acc: Account, conn: jaydebeapi.Connection) -> None:
        this.trans=trans
        this.acc=acc
        this.conn=conn
        this.fraud_value=0.0 #analytical value of how likely this transafction is fraudulent
        this.threshold_fraud=float(os.environ.get("THRESHOLD_FRAUD")) #a fraud_value above this threshold will trigger fraud prevention
        this.threshold_velocity=float(os.environ.get("THRESHOLD_VELOCITY")) #a fraud value above this will trigger a velocity analysis
        this.weighting_cvc=float(os.environ.get("WEIGHTING_CVC"))
        this.weighting_amount=float(os.environ.get("WEIGHTING_AMOUNT"))
        this.weighting_location=float(os.environ.get("WEIGHTING_LOCATION"))
        this.weighting_velocity=float(os.environ.get("WEIGHTING_VELOCITY"))
        this.sample_chance=float(os.environ.get("FRAUD_SAMPLE_CHANCE"))
        

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
        curs.execute("SELECT SUM(transfer_value) FROM card_transactions \
                      WHERE card_num = " + str(this.trans.card) + " AND time_stamp > '" + str(window) +  "'")
        recent = curs.fetchone()[0]
        #if the user has no recent activity, skip this check
        if recent == None or recent < 10:
            return
        print("recent: " + str(recent))
        curs.execute("SELECT sum(transfer_value) FROM card_transactions \
                      WHERE card_num = " + str(this.trans.card) + " \
                      AND time_stamp BETWEEN '" + str(comparison_date) +  "' AND '" + str(window) + "'")
        history = curs.fetchone()[0]
        print ("history: " + str(history))
        #if the user has no history, add half the velocity weighting
        if history == None or history < 100:
            this.fraud_value += this.weighting_velocity * 0.5
            return
        this.fraud_value += (10 * recent / history - 1) * this.weighting_velocity

    #entrypoint function
    def analyze(this):
        this.anal_cvc()
        cv = this.fraud_value
        print("cvc: "+ str(cv))
        this.anal_amount()
        av = this.fraud_value
        print("amount: " + str(av - cv))
        this.anal_location()
        lv = this.fraud_value
        print("location: " + str(lv - av))

        #only analyze velocity (two extra db querries) if we have a reason to or by random chance
        if this.fraud_value > this.threshold_velocity or random.random() < this.sample_chance:
            this.anal_velocity()
            
            vv = this.fraud_value
            print("velocity: " + str(vv - lv))
        
        
    

        



        


        
    
    

     