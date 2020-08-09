#!/bin/bash

# This script starts Flink, Redis, and RMI registry, which have to be running
# for the Topic Count experiments to work. Requirements:
#
#   REDIS_HOME should point to the Redis directory
#   FLINK_HOME should point to the Flink directory

echo "Restarting Redis"

PID="$(pidof redis-server)"
if [[ "${PID}" -ne "" ]]; then
  kill "${PID}"
  sleep 1
fi

${REDIS_HOME}/src/redis-server &
sleep 1

# Check if the words and topics need to be repopulated

EXISTS="$(${REDIS_HOME}/src/redis-cli exists topic-count:topic:intelligence)"
if [[ "${EXISTS}" -eq "0" ]]; then
  java -jar ./target/diffstream-topiccount-1.0-SNAPSHOT.jar
fi

echo "Restarting Flink"

${FLINK_HOME}/bin/stop-cluster.sh
${FLINK_HOME}/bin/taskmanager.sh stop-all
${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/taskmanager.sh start
${FLINK_HOME}/bin/taskmanager.sh start
sleep 1

echo "Restarting RMI registry"

PID="$(pidof rmiregistry)"
if [[ "${PID}" -ne "" ]]; then
  kill "${PID}"
  sleep 1
fi

rmiregistry -J-Djava.rmi.server.codebase=file:${HOME}/diffstream/target/classes/ &
sleep 1
