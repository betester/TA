#!/bin/bash

instance_name="$1"
project="$2"
zone="$3"
service_account="$4"
startup_script="$5"

gcloud compute instances create "$instance_name" \
    --project="$project" \
    --zone="$zone" \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account="$service_account" \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --create-disk=auto-delete=yes,boot=yes,device-name="$instance_name",image=projects/debian-cloud/global/images/debian-12-bookworm-v20240213,mode=rw,size=10,type=projects/"$project"/zones/"$zone"/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --metadata="startup-script=${startup_script}" \
    --reservation-affinity=any
