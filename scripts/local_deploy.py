# contains script for local deployment on the cloud

import os
import time
import json
import asyncio

from asyncio.subprocess import PIPE, STDOUT 


async def deploy_instance(
    project_name: str,
    service_name: str,
    image_name: str,
    zone: str,
    service_account: str,
    container_env: str,
    machine_type: str = "CPU"
    ):
    
    deploy_script_resource = "create_cpu_instance.sh" if machine_type == "CPU" else "create_gpu_instance.sh"
    cmd = f"./scripts/{deploy_script_resource} {service_name} {project_name} {zone} {service_account} '{container_env}' {image_name}"

    process = await asyncio.create_subprocess_shell(
        cmd,
        stdin=PIPE,
        stdout=PIPE,
        stderr=STDOUT
    )

    if process.stdout:
        async for line in process.stdout:
            print(line.decode('utf-8'))


# this can be used for deploying instance that uses docker image

def parse_txt(source_file: str):
    cmd = ""

    with open(source_file) as f:
        for content in f:
            no_newline = content.rstrip('\n')
            cmd += f" -e {no_newline}"
    
    return cmd

async def main():
    # relative towards the project change this if you run it from not from the root project 


    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

    config_path = "configs" 
    PROJECT = os.environ.get("PROJECT")
    assert PROJECT is not None
    ZONE = os.environ.get("ZONE")
    assert ZONE is not None
    SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
    assert SERVICE_ACCOUNT is not None

    configs = {
        "kafka" : {
            "next_config": ["master"],
            "wait_time": 10
        },
        "master" : {
            "next_config": ["crawler", "analyzer"],
            "wait_time": 5
        },
        "analyzer": {
            "machine_type" : "GPU",
            "after_deploy_script": "./scripts/run_docker_gpu.sh"

        }
    }

    current_config = ["kafka"]

    while len(current_config) != 0:

        config_name = current_config.pop()

        print(f"Deploying {config_name} service")

        config_source = f"{os.path.join(config_path, config_name)}.txt"
        config_metadata = f"{os.path.join(config_path, config_name)}.json"

        with open(config_metadata) as f:
            metadata = json.load(f) 
            image_name = metadata['IMAGE']
            extra_config = configs.get(config_name, {})

            machine_type = extra_config.get("machine_type", "CPU")
            env = config_source
            zone = ZONE

            if machine_type == "GPU":
                env = parse_txt(config_source)
                zone = "us-west4-a"

                env += f" -e PROJECT_NAME={PROJECT}"
                env += f" -e SERVICE_NAME={config_name}"
                env += f" -e IMAGE_NAME={image_name}"
                env += f" -e CLOUD_ZONE={zone}"
                env += f" -e SERVICE_ACCOUNT={SERVICE_ACCOUNT}"

            await deploy_instance(
                PROJECT,
                config_name,
                image_name,
                zone,
                SERVICE_ACCOUNT,
                env,
                machine_type
            )
        
        wait_time = extra_config.get("wait_time", 0)

        if wait_time != 0:
            print(f"Waiting for {wait_time}, making sure {config_name} service is configured")
            time.sleep(wait_time)

        next_config = extra_config.get("next_config", [])

        for config in next_config:
            print(f"Adding {config} for the next service to deploy")
            current_config.append(config)

if __name__ == "__main__":
    asyncio.run(main())
