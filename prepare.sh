#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

#mvn clean install -f ${SCRIPT_DIR}/pom.xml -q
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up -d
NAME=$(docker ps --filter "name=kafka" --format='{{.Names}}')
docker exec -it $NAME /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --topic output --partitions 1 --bootstrap-server localhost:9092