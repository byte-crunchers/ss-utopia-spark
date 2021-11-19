import time
import json
import os
import sys
import traceback
import threading
import random

import jaydebeapi
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import boto3

import card_transactions.card_transaction_consumer as card_c
import loan_payments.lp_consumer as loan_payment_c
import stocks.stock_consumer as stock_c
import transactions.transaction_consumer as trans_c


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
        print("There was a problem connecting to the database, please make sure the database information is correct!",
              file=sys.stderr)
    return con_try


def process_message(message: str, lock: threading.Lock, threadPool: int) -> None:
    #make sure not to excede the max threads
    while (threadPool[0] <= 0):
        time.sleep(0.1) #rechecks every 100ms
    lock.acquire()
    threadPool[0] -= 1
    lock.release()
    conn = connect()

    if conn:
        try:
            mdict = json.loads(message)
            try:    
                if mdict['type'] == 'transaction':
                    trans_c.consume(mdict, conn)
                elif mdict['type'] == 'card_transaction':
                    card_c.consume(mdict, conn)
                elif mdict['type'] == 'loan_payment':
                    loan_payment_c.consume(mdict, conn)
                elif mdict['type'] == 'stock':
                    stock_c.consume(mdict, conn)
                else:
                    print("unrecognized type")
                conn.commit()

            except:
                print('unable to process message:\n {}\n'.format(message), file=sys.stderr)
                failover(mdict)
                
            finally:
                conn.close()
                lock.acquire()
                threadPool[0] += 1
                lock.release()
        except: #jsonify message failec
            print('unable to parse message into dictionary:\n {}\n'.format(message), file=sys.stderr)
            
def failover(message: dict):
    try:
        message['key']=random.randrange(0, 32768) #random 16 bit number to uniquify the entry
        dyn = boto3.client('dynamodb', region_name='us-east-1',
            aws_access_key_id=os.environ.get("ACCESS_KEY"), aws_secret_access_key=os.environ.get("SECRET_KEY"))
        dyn.put_item(TableName='utopia-failover-HA-DynamoDB', Item=message)
    except:
        print('Failed to write to DynamoDB!:\n {}\n'.format(message), file=sys.stderr)
        


#Helper function to multithread this workload
#This workload is bound by network delays, and so more threads is better
#However, spark will only allow one thread per core, so we use native Python multithreading
def consumePartition(partition) -> None:
    conn = connect() #I have to do this so jaydebeapi starts the JVM ahead of time
    #jaydebeapi is very much not threadsafe

    max_threads = os.environ.get("MAX_THREADS")
    if(not max_threads):
        max_threads = 50
    threadPool = [int(max_threads)] #just a counter. It's a list because I need to pass by reference
    lock = threading.Lock()
    threads = []

    for message in partition:
        t = threading.Thread(target=process_message, args=(message,lock, threadPool))
        while(threadPool[0] <= 0): #This soft lock should prevent Python from spawning too many threads
            time.sleep(0.1)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    if conn:
        conn.close()


def consume():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.2 pyspark-shell' #only used for running localy
    sc = SparkContext(appName="TransactionConsumer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, int(os.environ.get("BATCH_LENGTH")))
    stream = KinesisUtils.createStream(ssc, os.environ.get("CONSUMER_NAME"), "byte-henry", \
                                        "https://kinesis.us-east-1.amazonaws.com", 'us-east-1',
                                        InitialPositionInStream.LATEST, 2, \
                                        awsAccessKeyId=os.environ.get("ACCESS_KEY"),
                                        awsSecretKey=os.environ.get("SECRET_KEY"))    
    partitions = int(os.environ.get("PARTITIONS"))                                
    print("splitting into {:d} paritions".format(partitions))
    partitionedStream = stream.repartition(partitions) #allows us to process the stream across multiple tasks/cores/executors
    partitionedStream.foreachRDD(lambda rdd: rdd.foreachPartition(consumePartition)) #splits the stream into tasks based on partition
    print("submitting")
    ssc.start()
    print("done")
    ssc.awaitTermination()
    print("end of script")
    ssc.stop()
    sc.stop()


if __name__ == "__main__":
    consume()