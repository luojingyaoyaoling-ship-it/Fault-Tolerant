#!/bin/bash

docker-compose -f ../docker-compose-kafka.yml up -d
sleep 5
docker-compose -f ../docker-compose-simple-minio.yml up -d
sleep 5
docker-compose build --build-arg protocol="UNC" --build-arg interval="5" \
                    --build-arg scale_factor="3"
docker-compose up --scale worker="3" -d
