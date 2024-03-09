# this will use the bash script ./initial_deploy.sh if you are lazy setting up the 
# args, use this python script.

# you need to create .env file to set up the args with the same name convention as .example .

import os
import subprocess
from dotenv import load_dotenv

load_dotenv()

args = [
    os.environ.get("GITHUB_REPOSITORY"),
    os.environ.get("PROJECT"),
    os.environ.get("ZONE"),
    os.environ.get("SERVICE_ACCOUNT"),
    os.environ.get("KAFKA_SERVICE_NAME"),
    os.environ.get("MASTER_SERVICE_NAME"),
    os.environ.get("MASTER_ENVS"),
    os.environ.get("CRAWLER_SERVICE_NAME"),
    os.environ.get("CRAWLER_ENVS"),
    os.environ.get("ANALYZER_SERVICE_NAME"),
    os.environ.get("ANALYZER_ENVS")
]

result = subprocess.run(["./initial_deploy.sh"] + args, shell=True)
