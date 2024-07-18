#!/bin/bash

if [ $# -ne 3 ]; then
  echo "Usage: $0 <project_id> <image_name> <path_to_dockerfile>"
  exit 1
fi

project_id=$1
image_name=$2
path_to_dockerfile=$3

if [ ! -d "$path_to_dockerfile" ]; then
  echo "Error: Dockerfile path '$path_to_dockerfile' does not exist."
  exit 1
fi

docker build -t $image_name:v1 $path_to_dockerfile

docker tag $image_name:v1 asia-south1-docker.pkg.dev/$project_id/kserve/$image_name:v1

docker push asia-south1-docker.pkg.dev/$project_id/kserve/$image_name:v1

docker tag $image_name:v1 us-central1-docker.pkg.dev/$project_id/kserve/$image_name:v1

docker push us-central1-docker.pkg.dev/$project_id/kserve/$image_name:v1
