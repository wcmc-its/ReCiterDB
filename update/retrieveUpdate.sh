#!/bin/bash
now=$(date +"%T")
echo "$now"
/usr/bin/python3 retrieveS3.py
echo "Retrieval of data from s3 is complete"
now=$(date +"%T")
echo "$now"
/usr/bin/python3 retrieveDynamoDB.py
echo "Retrieval of data from DynamoDB is complete"
now=$(date +"%T")
echo "$now"
/usr/bin/python3 updateReciterDB.py
echo "ReciterDB has been successfully updated"
now=$(date +"%T")
echo "$now"
/usr/bin/python3 retrieveNIH.py
echo "NIH RCR script complete"
now=$(date +"%T")
echo "$now"
