#!/bin/bash

# This script stops Flink, Redis, and RMI registry. Requirements:
#
#   REDIS_HOME should point to the Redis directory
#   FLINK_HOME should point to the Flink directory

PID="$(pidof redis-server)"
if [[ "${PID}" -ne "" ]]; then
  echo "Stopping Redis"
  kill "${PID}"
fi

echo "Stopping Flink"

${FLINK_HOME}/bin/stop-cluster.sh
${FLINK_HOME}/bin/taskmanager.sh stop-all

PID="$(pidof rmiregistry)"
if [[ "${PID}" -ne "" ]]; then
  echo "Stopping RMI registry"
  kill "${PID}"
fi
