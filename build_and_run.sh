#!/bin/bash
set -e

# Build the Docker image with the latest code
IMAGE_NAME="web2music:latest"
docker build -t $IMAGE_NAME .

# Run the Docker container interactively
exec docker run -it $IMAGE_NAME
