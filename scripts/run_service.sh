#!/bin/bash

service_name="$1"
instance_name="$2"
zone="$3"
github_repository="$4"

# Define the command template with a placeholder for the repository URL
command_template="mkdir -p app && \
cd app && \
git clone REPOSITORY_URL . && \
pip install -r requirements.txt && \
python -m \"$service_name\""

# Replace the placeholder with the actual repository URL
command="${command_template/REPOSITORY_URL/$github_repository}"

# SSH into the instance and execute the command
gcloud compute ssh "$instance_name" --zone="$zone" --command="$command"
