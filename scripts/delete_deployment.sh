#!/bin/bash

experiment=$1
saving_dir=$2

experiment_folder=$saving_dir/"$experiment"

docker-compose logs &> "$experiment_folder"/"$experiment"-styx-logs.log
docker-compose -f ../docker-compose-simple-minio.yml logs &> "$experiment_folder"/"$experiment"-minio-logs.log
docker-compose -f ../docker-compose-kafka.yml logs &> "$experiment_folder"/"$experiment"-kafka-logs.log

docker-compose down --volumes > /dev/null 2>&1
docker-compose -f ../docker-compose-kafka.yml down --volumes > /dev/null 2>&1
docker-compose -f ../docker-compose-simple-minio.yml down --volumes > /dev/null 2>&1

echo "容器清除完毕"
