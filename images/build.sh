#!/usr/bin/env bash

build_image=$1
git_token=$2

docker build -t ${build_image} --build-arg GIT_TOKEN=${git_token} -f Dockerfile_build . --no-cache
if [ $? -eq 0 ]; then 
    container_id=`docker run -d ${build_image}`
    if [ $? -eq 0 ]; then 
        docker cp ${container_id}:/ape-dts/target/release/ape-dts ./ape-dts
        docker cp ${container_id}:/ape-dts/target/release/dt-struct ./dt-struct
        docker cp ${container_id}:/ape-dts/target/release/dt-precheck ./dt-precheck
        docker rm -f ${container_id}
        exit 0
    fi
    echo "run image failed"
fi
echo "build image failed"