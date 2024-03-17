#!/bin/bash

# Parse the environment variable containing the service account key
if [ -n "$SERVICE_ACCOUNT_KEY" ]; then
    echo "$SERVICE_ACCOUNT_KEY" > /tmp/service-account-key.json
    gcloud auth activate-service-account --key-file=/tmp/service-account-key.json
    rm /tmp/service-account-key.json
fi

# Execute the master script
python -m master
