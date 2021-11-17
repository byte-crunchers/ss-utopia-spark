
# Welcome to ss-utopia-spark!
This repo contains a python script controlling a Spark instance that will read messages from Kinesis streams and act on them, mainly by altering the database.
This instance will run constantly, pulling data from a Kinesis stream and processing it into a database.
All of the needed resources are created using the Jenkinsfile contained in the top level of the repo.
The top level script stream_consumer.py is the main script. It reads messages from kinesis via spark and delegates them to one of the lower level scripts in the sub-directories e.g. if the message is a transaction then stream_consumer.py will call transaction_consumer.py.


## Resources:
This repo is built using Jenkins on an EC2 instance. The Jenkins instance should be a t3a.medium using the utopia-spark_CI/CD_image-HA-AMI image. The Jenkins file is found in the same directory as this readme.

Execution of the Jenkins pipeline creates all further resources using eksctl and Spark itself. This includes the EKS cluster, the VPC said cluster runs on, the Fargate profiles, and the Fargate pods themselves. If the cluster is not already created, the process of creating it will take about 25 minutes. If it has already been created, the entire build process should take about one minute.

The Jenkins pipeline does not handle destruction of resources. For that, run the command `eksctl delete cluster --name Spark` on the AWS CLI. This takes about two minutes and will delete all resources except for the Jenkins instance. The Jenkins instance may be deleted or stopped through the AWS GUI or CLI.


## Interactions:

This service mainly interacts with the main RDS. It reads from the accounts, cards, and loans tables, and writes to the transactions, card_transactions, loan_payments, and stocks tables.

This service of course also pulls events from the Kinesis stream and indirectly interacts with whichever services write messages to it.

This service also uses DynamoDB both for checkpointing (keeping track of where in the stream the consumer is), and for failover. Specifically it uses the cloud-consumer and utopia-failover-HA-DynamoDB tables. 

In the future this service will notify users of potential fraud,  but the method by which this works is TBD.

## Jenkins Parameters
### Setup Stage
**SPARK_VERSION** - '3.x.x' - The version of Spark to be downloaded
**ASSEMBLY_SPARK_VERSION** - '3.x.x' - The version of Spark used in the spark-streaming-kinesis-asl JAR that has been pre-assembled and uploaded onto S3. Idealy this should be the same as SPARK_VERSION, but life is far from ideal.
**HADOOP_VERSION** - '3.x' - The version of Hadoop used with Spark
**MYSQL_JAR_VERSION** - '8.x.x' - The version of the MySQL JDBC JAR to download
**SCALA_VERSION** - '2.x' - The version of Scala used with Spark
### Deploy Stage
**NUM_EXECUTORS** - 'x' - Base number of executor containers - deprecated
**MAX_EXECUTORS** - 'x' - Maximum number of executors that may be created with auto-scaling
**NUM_CORES** - 'x' - Number of cores per executor
**THREADS** - 'x'  - Number of Python threads and therefore DB connections per task (at one task per core)
**EXECUTOR_MEMORY** - "xm" - Amount of memory allocated to the executor pod
**DRIVER_MEMORY** - "xg" - Amount of memory allocated to the driver pod
**SUSTAINED_TIMEOUT** - "xm" -  Time after requesting new executors to request more if need be - deprecated
**PARTITIONS** - 'x' - Number of partitions to break each batch into. Should be around one per task or MAX_EXECUTORS * NUM_CORES
**BATCH_LENGTH** - 'x' - Window of time in which spark collects messaged from Kinesis before sending them to the executors as a single batch.
**SCALING_INTERVAL** - 'x'  - Every interval, the autoscaler checks to see if more or fewer pods are needed

## URL:
https://github.com/byte-crunchers/ss-utopia-spark
