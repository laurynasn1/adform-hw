#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

java -cp ${SCRIPT_DIR}/target/adform-homework-1.0-SNAPSHOT-jar-with-dependencies.jar data.ResultReader