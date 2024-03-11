
import os

# Get Docker Hub username from environment variable
dockerhub_username = os.environ.get("DOCKERHUB_USERNAME")

# Iterate over Dockerfiles
for file in os.listdir("dockerfiles"):
    if file.startswith("Dockerfile."):
        # Get the extension name from the file
        extension_name = file.split(".")[1]
        # Build the Docker image
        os.system(f"docker build -t {extension_name} -f {file} .")
        # Tag the Docker image
        os.system(f"docker tag {extension_name}:latest {dockerhub_username}/{extension_name}:latest")
        # Push the Docker image to Docker Hub
        os.system(f"docker push {dockerhub_username}/{extension_name}:latest")

        os.system(f"docker image rm {extension_name}")
