#!/bin/bash

instance_name="$1"
cloud_project="$2"
zone_instance="$3"
service_account="$4"
docker_env="$5"
image="$6"
#
# gcloud compute instances create "$instance_name" \
#     --project="$cloud_project" \
#     --zone="$zone_instance" \
#     --machine-type=n1-standard-2 \
#     --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
#     --maintenance-policy=TERMINATE \
#     --provisioning-model=STANDARD \
#     --service-account="$service_account" \
#     --scopes=https://www.googleapis.com/auth/cloud-platform \
#     --accelerator=count=1,type=nvidia-tesla-t4 \
#     --tags=http-server,https-server \
#     --create-disk=auto-delete=yes,boot=yes,device-name="$instance_name",image=projects/ml-images/global/images/c2-deeplearning-pytorch-2-2-cu121-v20240306-debian-11,mode=rw,size=50,type=projects/"$cloud_project"/zones/"$zone_instance"/diskTypes/pd-balanced \
#     --no-shielded-secure-boot \
#     --shielded-vtpm \
#     --shielded-integrity-monitoring \
#     --labels=goog-ec-src=vm_add-gcloud \
#     --metadata=startup-script='sudo /opt/deeplearning/install-driver.sh' \
#     --reservation-affinity=any


echo "Running docker instance"
gcloud compute ssh "$instance_name" --project="$cloud_project" --zone="$zone_instance" --command="docker run --gpus all --restart unless-stopped $docker_env -d $image"
