# Welcome to ss-utopia-spark!
This repo contains a python script that will read messages from kinesis streams and act on them, mainly by altering the database
This script will run constantly, pulling data from a kinesis stream and processing it into a database.
The top level script stream_consumer.py is the main script. It reads messages from kinesis via spark and delegates them to one of the 
lower level scripts in the sub-directories e.g. if the message is a transaction then stream_consumer.py will call transaction_consumer.py.


## Resources:
This will be a python module running a spark instance. It would likely be best to run it on a single docker container i.e. no separate worker nodes.

## Interactions:
At this point the script will interact purely with Kinesis and the database. No plans right now to communicate directly with any microservice.

## URL:
https://github.com/byte-crunchers/ss-utopia-spark