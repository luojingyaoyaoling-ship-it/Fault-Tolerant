#!/bin/bash

protocol=$1
interval=$2
failure=$3
scale_factor=$4
enable_Cascadefailure=$5
AF_Tolerance=$AF_Tolerance

shift 5
array=("$@")

docker-compose -f ../docker-compose-kafka.yml up -d >/dev/null 2>&1
sleep 5
docker-compose -f ../docker-compose-simple-minio.yml up -d >/dev/null 2>&1
sleep 5

if [[ $failure = "true" || $failure = "True" ]]; then
  echo "Failure is $failure"
  echo "enable_Cascadefailure is $enable_Cascadefailure"
  if [[ $enable_Cascadefailure = "true" || $enable_Cascadefailure = "True" ]]; then
    echo "111"
    docker-compose build --quiet --build-arg protocol="$protocol" --build-arg interval="$interval" --build-arg failure="-f" \
                      --build-arg scale_factor="$scale_factor" --build-arg encf="-cf" >/dev/null 2>&1
    echo "111>>>>>>"
  else
    docker-compose build --quiet --build-arg protocol="$protocol" --build-arg interval="$interval" --build-arg failure="-f" \
                      --build-arg scale_factor="$scale_factor" >/dev/null 2>&1
  fi
else
  docker-compose build --quiet --build-arg protocol="$protocol" --build-arg interval="$interval" \
                    --build-arg scale_factor="$scale_factor" >/dev/null 2>&1
fi

docker-compose up --scale worker="0" -d >/dev/null 2>&1
half=$((${#array[@]} / 2))
for ((i=0; i < half; i++)); do
    container_index=$((i + 1))
    current_element="${array[$i]}"
    paired_element="${array[$((i + half))]}"
    docker-compose run --name "checkmate_worker_$container_index" -e CPUS="${current_element},${paired_element}" -d worker >/dev/null 2>&1
done