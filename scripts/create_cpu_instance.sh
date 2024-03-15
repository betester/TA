#!/bin/bash

service_name="$1"
project_name="$2"
zone="$3"
service_account="$4"
container_env="$5"
image_name="$6"


gcloud compute instances create-with-container ${service_name} \
    --project=${project_name} \
    --zone=${zone} \
    --container-image=${image_name} \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=${service_account} \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --tags=http-server,https-server \
    --image=projects/cos-cloud/global/images/cos-stable-109-17800-147-28 \
    --boot-disk-size=25GB \
    --boot-disk-type=pd-balanced \
    --boot-disk-device-name=${service_name} \
    --container-restart-policy=always \
    --container-env-file=${container_env} \
    --container-mount-host-path=host-path=/tmp,mode=rw,mount-path=/bitnami \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud,container-vm=cos-stable-109-17800-147-28
