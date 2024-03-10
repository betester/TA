#!/bin/bash

# this script is for initializing 'local' instances deployment process from your local machine, it will deploy 4 things.
# 1. kafka component, for relaying data between services
# 2. master component, this needs to run after the kafka component for observability
# 3. crawler components and analyzer can run independently
# 
# Even though we deploy it on google cloud, for the sake of research, we will consider these machines as local.

github_repository="$1"
project="$2"
zone="$3"
service_account="$4"

kafka_service_name="$5"

master_service_name="$6"
master_envs="$7"

crawler_service_name="$8"
crawler_envs="$9"

analyzer_service_name="$10"
analyzer_envs="$11"


# # Creating kafka server with the following config 
# echo "Creating kafka broker with name: ${kafka_service_name}"
#
# gcloud compute instances create-with-container "$kafka_service_name" \
#     --project="$project" \
#     --zone="$zone" \
#     --machine-type=e2-medium \
#     --network-interface=network-tier=PREMIUM,subnet=default \
#     --maintenance-policy=MIGRATE \
#     --provisioning-model=STANDARD \
#     --service-account="$service_account" \
#     --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
#     --tags=http-server,https-server \
#     --image=projects/cos-cloud/global/images/cos-stable-109-17800-147-28 \
#     --boot-disk-size=10GB \
#     --boot-disk-type=pd-balanced \
#     --boot-disk-device-name="$kafka_service_name" \
#     --container-image=docker.io/bitnami/kafka:3.6 \
#     --container-restart-policy=always \
#     --container-env=^,@^KAFKA_ENABLE_KRAFT=yes,@KAFKA_CFG_NODE_ID=0,@KAFKA_CFG_PROCESS_ROLES=controller,broker,@KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@127.0.0.1:9093,@KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,@KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,@KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,@KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER,@KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT,@KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS=7000 \
#     --container-mount-host-path=host-path=/tmp,mode=rw,mount-path=/bitnami \
#     --no-shielded-secure-boot \
#     --shielded-vtpm \
#     --shielded-integrity-monitoring \
#     --metadata="hostname=${kafka_service_name}.moke"\
#     --labels=goog-ec-src=vm_add-gcloud,container-vm=cos-stable-109-17800-147-28
#
# # we will sleep the next commands, making sure that the kafka already is set, you can change this value as you see fit.
# echo "Waiting for 30 seconds, making sure the kafka configs are set."
# sleep 30
#
#  deploying the master component.

# Creating kafka server with the following config 
 echo "Creating master component with name: ${master_service_name}"
./scripts/create_local_instance.sh "$master_service_name" "$project" "$zone" "$service_account" "$master_envs"

# #  deploying the crawler component.
# ./create_local_instance.sh "$crawler_service_name" "$project" "$zone" "$service_account" "$crawler_envs"
#
# # Running the crawler component
# ./run_service.sh  "crawler" "$crawler_service_name" "$zone" "$github_repository"
#
# #  deploying the analyzer component.
# ./create_local_instance.sh "$analyzer_service_name" "$project" "$zone" "$service_account" "$analyzer_envs"
#
# # Running the analyzer component
# ./run_service.sh  "analyzer" "$analyzer_service_name" "$zone" "$github_repository"
#
