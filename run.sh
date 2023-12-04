#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ "$#" -ne 2 ]; then
  echo "Usage: run.sh <user agent> <path to data folder>"
  exit 1
fi

JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [ $JAVA_VER -ge 17 ]; then
  VM_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
fi

java ${VM_OPTS} -cp ${SCRIPT_DIR}/target/adform-homework-1.0-SNAPSHOT-jar-with-dependencies.jar Application "$@"