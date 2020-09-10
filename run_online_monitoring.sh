#!/bin/bash

##
## Requires that:
##  (i) everything is installed,
##  (ii) kafka unknown JVM parameters are deleted, and
##  (iii) RMI is running
##

## Change those if you want to run the test with different load
## and for a longer period of time
LOAD=5000
TEST_TIME=600

helpFunction()
{
   echo ""
   echo "Usage: $0 [-l LOAD] [-t DURATION]"
   echo -e "\t-l Load: Input events per second. Default: 5000"
   echo -e "\t-t Duration: Experiment duration in seconds. Default: 600"
   exit 1 # Exit script after printing help
}

while getopts "l:t:" opt
do
   case "$opt" in
      l ) LOAD="$OPTARG" ;;
      t ) TEST_TIME="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

## The directory that will contain the results
RESULTS_PARENT_DIR="data/online-monitoring/"
RESULTS_DIR_NAME="server_load_${LOAD}_time_${TEST_TIME}_leftpar_1_right_par_2"
RESULTS_DIR="${RESULTS_PARENT_DIR}/${RESULTS_DIR_NAME}"

echo "Starting RMI registry"
rmiregistry -J-Djava.rmi.server.codebase=file:${HOME}/diffstream/target/classes/ &
sleep 1

cd streaming-benchmarks/

## A word of warning
echo "Please don't interrupt this script mid-execution or all hell could break loose :'("

echo "Stopping anything that could be running... be patient, this could take a couple minutes"
./flink-bench.sh STOP_ALL > /dev/null 2>&1

## Run the test
STDOUT_LOG=online-monitoring-stdout.log
STDERR_LOG=online-monitoring-stderr.log
## TODO: Figure out a good load and test time
echo "Running the test with load: ${LOAD} for duration: ${TEST_TIME} seconds."
echo "|-- stdout can be checked out here: streaming-benchmarks/${STDOUT_LOG}"
echo "|-- and stderr here: streaming-benchmarks/${STDERR_LOG}"
LOAD=${LOAD} TEST_TIME=${TEST_TIME} ./flink-bench.sh FLINK_TEST 1> ${STDOUT_LOG} 2> ${STDERR_LOG}
## When this ends there is an exception but that is fine.
## TODO: Figure out if this could affect anything.

## Stop everything
echo "Stopping everything... be patient, this could take a couple minutes"
./flink-bench.sh STOP_ALL > /dev/null 2>&1

## Make a directory to store the results
echo "Storing the results in ${RESULTS_DIR}"
mkdir -p "../${RESULTS_DIR}"
cp memory-log.txt "../${RESULTS_DIR}/"
cp unmatched-items.txt "../${RESULTS_DIR}/"
mv durations-matcher-id-1.bin "../${RESULTS_DIR}/"

## Plot the results
echo "Producing plots..."
cd "../${RESULTS_PARENT_DIR}"
python3 collect_and_plot.py "${RESULTS_DIR_NAME}"
cd ../../

echo "Plots and results are available in: ${RESULTS_DIR}"
echo "File: ${RESULTS_DIR}/unmatched_histogram.pdf contains a histogram of the unmatched items."
echo "File: ${RESULTS_DIR}/used_memory_in_time.pdf contains the used memory in time."

PID="$(pidof rmiregistry)"
if [[ "${PID}" -ne "" ]]; then
  echo "Stopping RMI registry"
  kill "${PID}"
fi
