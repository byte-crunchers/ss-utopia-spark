from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import os
import json
os.environ['JAVA_HOME'] = "C:\Program Files\Java\jdk-11.0.12"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:2.4.4,com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.12.5 pyspark-shell'

sc = SparkContext(appName="TransactionConsumer")
ssc = StreamingContext(sc, 5)
f = open('awskey.json', 'r')
key = json.load(f)
#kin = boto3.client('kinesis', aws_access_key_id=key['access_key'], aws_secret_access_key=key['secret_key'])
trans = KinesisUtils.createStream(ssc, "TransactionConsumer", "byte-henry", \
    "https://kinesis.us-east-1.amazonaws.com", 'us-east-1', InitialPositionInStream.LATEST, 2, \
    awsAccessKeyId=key['access_key'], awsSecretKey=key['secret_key'])
trans.pprint()
print("about to submit")
ssc.start()
ssc.awaitTermination()
print("end of script")