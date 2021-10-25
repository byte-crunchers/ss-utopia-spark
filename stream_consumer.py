import json
import os
import traceback

import jaydebeapi
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import card_transactions.card_transaction_consumer as card_c
import loan_payments.lp_consumer as loan_payment_c
import transactions.transaction_consumer as trans_c

import sys

def connect():
    mysql_pass = os.environ.get("MYSQL_PASS")
    mysql_user = os.environ.get("MYSQL_USER")
    mysql_jar = os.environ.get("MYSQL_JAR")
    mysql_loc = os.environ.get("MYSQL_LOC")
    con_try = None
    try:
        con_try = jaydebeapi.connect("com.mysql.cj.jdbc.Driver", mysql_loc,
                                     [mysql_user, mysql_pass], mysql_jar)
        con_try.jconn.setAutoCommit(False)
    except:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!", file=sys.stderr)
    return con_try


def process_message(message: str) -> None:
    conn = connect()
    if conn:
        try:
        
            mdict = json.loads(message)
            if mdict['type'] == 'transaction':
                trans_c.consume(mdict, conn)
            if mdict['type'] == 'card_transaction':
                card_c.consume(mdict, conn)
            if mdict['type'] == 'loan_payment':
                loan_payment_c.consume(mdict, conn)
            else:
                print("unrecognized type")
            conn.commit()
          except:
              print('unable to parse message:\n {}\n'.format(message), file=sys.stderr)

def consume():
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.2 pyspark-shell'

    sc = SparkContext(appName="TransactionConsumer")
    ssc = StreamingContext(sc, 5)
    stream = KinesisUtils.createStream(ssc, os.environ.get("CONSUMER_NAME"), "byte-henry", \
                                        "https://kinesis.us-east-1.amazonaws.com", 'us-east-1',
                                        InitialPositionInStream.LATEST, 2, \
                                        awsAccessKeyId=os.environ.get("ACCESS_KEY"),
                                        awsSecretKey=os.environ.get("SECRET_KEY"))    


    stream.foreachRDD(lambda x: x.foreach(process_message))
    print("submitting")
    ssc.start()
    print("done")
    ssc.awaitTermination()
    print("end of script")


if __name__ == "__main__":
    consume()
