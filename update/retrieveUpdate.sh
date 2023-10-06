#!/bin/bash

## This shell script executes all the Python scripts required to update ReCiterDB in succession.
## Of these, executeFeatureGenerator.py, abstractImport.py, and conflictsImport.py are optional, and will not 
## cause chaos if they are not present.

now=$(date +"%T")
echo "$now"

if [[ -f "executeFeatureGenerator.py" ]]; then
    /usr/bin/python3 executeFeatureGenerator.py
    echo "ReCiter Feature Generator successfully run for individuals in reporting_ad_hoc_feature_generator_execution table"
else
    echo "executeFeatureGenerator.py not found. Proceeding..."
fi

if [[ -f "retrieveS3.py" ]]; then
    /usr/bin/python3 retrieveS3.py
    echo "Retrieval of data from s3 is complete"
else
    echo "retrieveS3.py not found. Proceeding..."
fi

now=$(date +"%T")
echo "$now"

if [[ -f "retrieveDynamoDB.py" ]]; then
    /usr/bin/python3 retrieveDynamoDB.py
    echo "Retrieval of data from DynamoDB is complete"
else
    echo "retrieveDynamoDB.py not found. Proceeding..."
fi

now=$(date +"%T")
echo "$now"

if [[ -f "updateReciterDB.py" ]]; then
    /usr/bin/python3 updateReciterDB.py
    echo "ReciterDB has been successfully updated"
else
    echo "updateReciterDB.py not found. Proceeding..."
fi

now=$(date +"%T")
echo "$now"

if [[ -f "retrieveNIH.py" ]]; then
    /usr/bin/python3 retrieveNIH.py
    echo "NIH RCR script complete"
else
    echo "retrieveNIH.py not found. Proceeding..."
fi

if [[ -f "abstractImport.py" ]]; then
    /usr/bin/python3 abstractImport.py
    echo "Abstracts successfully imported into ReciterDB"
else
    echo "abstractImport.py not found. Proceeding..."
fi

if [[ -f "conflictsImport.py" ]]; then
    /usr/bin/python3 conflictsImport.py
    echo "Conflicts of Interest successfully imported into ReciterDB"
else
    echo "conflictsImport.py not found. Proceeding..."
fi

now=$(date +"%T")
echo "$now"
