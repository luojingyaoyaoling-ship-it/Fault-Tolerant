#!/bin/bash

docker-compose down --volumes
docker-compose -f ../docker-compose-kafka.yml down --volumes
docker-compose -f ../docker-compose-simple-minio.yml down --volumes
