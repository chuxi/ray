#!/bin/bash
set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

$TP_DIR/thirdparty/scripts/setup.sh $PYTHON_EXECUTABLE

LANGUAGE="python"
if [[ -n  "$2" ]]; then
  LANGUAGE=$2
fi
if [[ "$LANGUAGE" == "java" ]]; then
    pushd thirdparty/build/arrow/java/plasma
    mvn clean install -Dmaven.test.skip
    popd
fi
