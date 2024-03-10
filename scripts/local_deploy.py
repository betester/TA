# contains script for local deployment on the cloud

import os
import json
import asyncio
from asyncio.subprocess import PIPE, STDOUT 

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# this can be used for deploying instance that uses docker image
async def deploy_local_instance(
    project_name: str,
    service_name: str,
    image_name: str,
    zone: str,
    service_account: str,
    container_envs: dict[str, str]
    ):

    cmd = (
        f"gcloud compute instances create-with-container {service_name} "
            f"--project={project_name} "
            f"--zone={zone} "
            "--machine-type=e2-medium "
            "--network-interface=network-tier=PREMIUM,subnet=default "
            "--maintenance-policy=MIGRATE "
            "--provisioning-model=STANDARD "
            f"--service-account={service_account} "
            "--scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append "
            "--tags=http-server,https-server "
            "--image=projects/cos-cloud/global/images/cos-stable-109-17800-147-28 "
            "--boot-disk-size=10GB "
            "--boot-disk-type=pd-balanced "
            f"--boot-disk-device-name={service_name} "
            f"--container-image={image_name} "
            "--container-restart-policy=always "
    )

    for key, value in container_envs.items():
        cmd += f"--container-env={key}={value} "

    cmd += (
        "--container-mount-host-path=host-path=/tmp,mode=rw,mount-path=/bitnami "
            "--no-shielded-secure-boot "
            "--shielded-vtpm "
            "--shielded-integrity-monitoring "
            "--labels=goog-ec-src=vm_add-gcloud,container-vm=cos-stable-109-17800-147-28"
    )

    process = await asyncio.create_subprocess_shell(
        cmd,
        stdin=PIPE,
        stdout=PIPE,
        stderr=STDOUT
    )

    if process.stdout:
        async for line in process.stdout:
            print(line)


async def main():
    # relative towards the project change this if you run it from not from the root project 
    config_path = "configs" 

    PROJECT = os.environ.get("PROJECT")
    assert PROJECT is not None
    ZONE = os.environ.get("ZONE")
    assert ZONE is not None
    SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
    assert SERVICE_ACCOUNT is not None
    DOCKER_USERNAME = os.environ.get("DOCKER_USERNAME") 
    assert DOCKER_USERNAME is not None
    
    service_configs_path = [os.path.join(config_path, file) for file in os.listdir(config_path) if file.endswith('.json')]

    for service_config_path in service_configs_path:
        with open(service_config_path) as f:
            if f.name != "kafka.json":
                continue
            service_name = f.name.replace('.json', '')
            service_envs = json.load(f)
            await deploy_local_instance(
                PROJECT,
                service_name,
                f"{DOCKER_USERNAME}/service_name",
                ZONE,
                SERVICE_ACCOUNT,
                service_envs
            )

if __name__ == "__main__":
    asyncio.run(main())
