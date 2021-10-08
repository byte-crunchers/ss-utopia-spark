from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import jaydebeapi
import traceback

import transactions.transaction_consumer as trans_c
import transactions.card_transaction_consumer as card_c

import os
import json

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
    except jaydebeapi.Error:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    return con_try

def processMessage(message: str) -> None:
    conn = connect()
    try:
    
        mdict = json.loads(message)
        if mdict['type'] == 'transaction':
            trans_c.consume(mdict, conn)
        if mdict['type'] == 'card_transaction':
            card_c.consume(mdict, conn)
        else:
            print("unrecognized type")
        conn.commit()
    except:
        print('unable to parse message:\n {}\n'.format(message))

def consume():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.2 pyspark-shell'


    sc = SparkContext(appName="TransactionConsumer")
    ssc = StreamingContext(sc, 5)
    f = open('awskey.json', 'r')
    key = json.load(f)
    stream = KinesisUtils.createStream(ssc, "TransactionConsumer", "byte-henry", \
        "https://kinesis.us-east-1.amazonaws.com", 'us-east-1', InitialPositionInStream.LATEST, 2, \
        awsAccessKeyId=key['access_key'], awsSecretKey=key['secret_key'])

    

    stream.foreachRDD(lambda x: x.foreach(processMessage))
    
    print("submitting")
    ssc.start()
    ssc.awaitTermination()
    print("end of script")

if __name__ == "__main__":
    consume()