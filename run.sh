#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ "$#" -ne 2 ]; then
  echo "Usage: run.sh <user agent> <path to data folder>"
  exit 1
fi

java -cp ${SCRIPT_DIR}/target/adform-homework-1.0-SNAPSHOT-jar-with-dependencies.jar Application "$@"