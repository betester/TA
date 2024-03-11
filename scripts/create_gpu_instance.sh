#!/bin/bash

instance_name="$1"
cloud_project="$2"
zone_instance="$3"
service_account="$4"
envs="$5"

# create the instance first
gcloud compute instances create "$instance_name" \
    --project="$cloud_project" \
    --zone="$zone_instance" \
    --machine-type=n1-standard-2 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=TERMINATE \
    --provisioning-model=STANDARD \
    --service-account="$service_account" \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --accelerator=count=1,type=nvidia-tesla-t4 \
    --tags=http-server,https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name="$instance_name",image=projects/ml-images/global/images/c2-deeplearning-pytorch-2-2-cu121-v20240306-debian-11,mode=rw,size=50,type=projects/"$cloud_project"/zones/"$zone_instance"/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --metadata="$envs" \
    --reservation-affinity=any

# upon creating, you first need to install nvidia driver again, this step will do that for you automatically
echo y | gcloud compute ssh "$instance_name" --zone="$zone_instance"
