import time
import json
import os
import sys
import traceback
import threading

import jaydebeapi
from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

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
            print('unable to parse message:\n {}\n'.format(message), file=sys.stderr)
        finally:
            conn.close()
            lock.acquire()
            threadPool[0] += 1
            lock.release()
    



#Helper function to multithread this workload
#This workload is bound by network delays, and so more threads is better
#However, spark will only allow one thread per core, so we use native Python multithreading
def consumeRDD(rdd: RDD) -> None:
    conn = connect() #I have to do this so jaydebeapi starts the JVM ahead of time
    #jaydebeapi is very much not threadsafe

    max_threads = os.environ.get("MAX_THREADS")
    if(not max_threads):
        max_threads = 50
    threadPool = [max_threads]
    lock = threading.Lock()
    threads = []

    for message in rdd.toLocalIterator():
        t = threading.Thread(target=process_message, args=(message,lock, threadPool))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    conn.close()


def consume():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.2 pyspark-shell'
    sc = SparkContext(appName="TransactionConsumer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    stream = KinesisUtils.createStream(ssc, os.environ.get("CONSUMER_NAME"), "byte-henry", \
                                        "https://kinesis.us-east-1.amazonaws.com", 'us-east-1',
                                        InitialPositionInStream.LATEST, 2, \
                                        awsAccessKeyId=os.environ.get("ACCESS_KEY"),
                                        awsSecretKey=os.environ.get("SECRET_KEY"))    
    #stream.foreachRDD(lambda x: x.foreach(process_message))
    stream.foreachRDD(consumeRDD)
    print("submitting")
    ssc.start()
    print("done")
    ssc.awaitTermination()
    print("end of script")
    ssc.stop()
    sc.stop()

if __name__ == "__main__":
    consume()