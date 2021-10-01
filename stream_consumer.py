from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import transactions.transaction_consumer as trans_c

import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:2.4.4,com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.12.5 pyspark-shell'



sc = SparkContext(appName="TransactionConsumer")
ssc = StreamingContext(sc, 5)
f = open('awskey.json', 'r')
key = json.load(f)
stream = KinesisUtils.createStream(ssc, "TransactionConsumer", "byte-henry", \
    "https://kinesis.us-east-1.amazonaws.com", 'us-east-1', InitialPositionInStream.LATEST, 2, \
    awsAccessKeyId=key['access_key'], awsSecretKey=key['secret_key'])

def processMessage(message: str) -> None:
    try:
        mdict = json.loads(message)
        if mdict['type'] == 'transaction':
            trans_c.consume(mdict)
    except:
        print('unable to parse message:\n {}\n'.format(message))

stream.foreachRDD(lambda x: x.foreach(processMessage))

print("submitting")
ssc.start()
ssc.awaitTermination()
print("end of script")