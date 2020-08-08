#!/bin/bash

##
## Requires that:
##  (i) everything is installed,
##  (ii) kafka unknown JVM parameters are deleted, and
##  (iii) RMI is running
##

## Change those if you want to run the test with different load
## and for a longer period of time
LOAD=1000
TEST_TIME=120

## The directory that will contain the results
RESULTS_PARENT_DIR="data/online-monitoring/"
RESULTS_DIR_NAME="server_load_${LOAD}_time_${TEST_TIME}_leftpar_2_right_par_2"
RESULTS_DIR="${RESULTS_PARENT_DIR}/${RESULTS_DIR_NAME}"

cd streaming-benchmarks/

echo "Stopping anything that could be running"
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
echo "Stopping everything"
./flink-bench.sh STOP_ALL > /dev/null 2>&1

## Make a directory to store the results
echo "Store the results in ${RESULTS_DIR}"
mkdir -p "../${RESULTS_DIR}"
cp memory-log.txt "../${RESULTS_DIR}/"
cp unmatched-items.txt "../${RESULTS_DIR}/"

## Plot the results
cd "../${RESULTS_PARENT_DIR}"
python3 collect_and_plot.py "${RESULTS_DIR_NAME}"
cd ../../

echo "Results are available in: ${RESULTS_DIR}"
echo "File: ${RESULTS_DIR}/unmatched_histogram.pdf contains a histogram of the unmatched items."
echo "File: ${RESULTS_DIR}/used_memory_in_time.pdf contains the used memory in time."
