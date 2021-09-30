# Welcome to ss-utopia-spark!
This repo contains python scripts that will read from kinesis streams and act on them, mainly by altering the database

This script will run constantly, pulling data from a kinesis stream and processing it into a database.

## Resources:
This will be a python scripts running spark instances. It would likely be best to run it on a single docker container i.e. no separate worker nodes.

## Interactions:
At this point the script will interact purely with Kinesis and the database. No plans right now to communicate directly with any microservice.

## URL:
https://github.com/byte-crunchers/ss-utopia-spark