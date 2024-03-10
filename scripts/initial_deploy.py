# this will use the bash script ./initial_deploy.sh if you are lazy setting up the 
# args, use this python script.

# you need to create .env file to set up the args with the same name convention as .example .

import os
import json
import subprocess
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def initial_script(envs):

    github_repo = os.environ.get("GITHUB_REPOSITORY")
    bash_script = "#!/bin/bash\n\n"
    bash_script += "sudo apt-get update\n"
    bash_script += "echo Y | sudo apt-get install git\n"
    bash_script += "echo Y | sudo apt install python3-pip\n"

    for key, value in envs.items():
        bash_script += f"export {key}='{value}'\n"

    bash_script += "mkdir -p app\n"
    bash_script += "cd app\n"
    bash_script += f"git clone -b cloud-deployment {github_repo} .\n"
    bash_script += "pip install -r ./requirements/$instance_name.txt\n"
    bash_script += "python -m \"$instance_name\"\n"
    
    return bash_script

args_params = [
    "GITHUB_REPOSITORY",
    "PROJECT",
    "ZONE",
    "SERVICE_ACCOUNT",
    "KAFKA_SERVICE_NAME",
    "MASTER_SERVICE_NAME",
    "MASTER_ENVS",
    "CRAWLER_SERVICE_NAME",
    "CRAWLER_ENVS",
    "ANALYZER_SERVICE_NAME",
    "ANALYZER_ENVS",
]

args = []

for param in args_params:
    val = os.environ.get(param)
    assert val is not None
    if "ENVS" in param:
        with open(val) as f:
            envs = json.load(f)
            args.append(initial_script(envs))
    else:
        args.append(val)

result = subprocess.run(["./scripts/initial_deploy.sh"] + args, shell=False)
