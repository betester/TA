# contains script for local deployment on the cloud

import os
import time
import json
import asyncio

from dotenv import load_dotenv, find_dotenv

from scripts import deploy_instance


load_dotenv(find_dotenv())

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

    current_config = ["analyzer"]

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
