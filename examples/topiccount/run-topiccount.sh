#!/bin/bash

# Prerequisits:
#   REDIS_HOME should point to the Redis directory
#   FLINK_HOME should point to the Flink directory

function exit_if_not_running {
  local NAME="${1}"
  local MATCH="${2}"
  local RUNNING="$(ps ef | grep ${MATCH} | grep -v grep)"
  if [[ "${RUNNING}" == "" ]]; then
    echo "${NAME} is not running. Please run ./prepare-topiccount.sh"
    exit 1
  fi
}

exit_if_not_running Redis redis-server
exit_if_not_running Flink org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
exit_if_not_running "RMI registry" rmiregistry

function usage {
  echo "Usage: ./run-topiccount.sh CMD [<additional arguments>]"
  echo
  echo "CMD is one of the following:"
  echo "  sequential     -- Runs the sequential version of the Topic Count"
  echo "  parallel       -- Runs the parallel version of the Topic Count"
  echo "  seq-diff-test  -- Runs a differential test where the sequential Topic Count"
  echo "                    is tested against itself with parallelism more than 1"
  echo "  par-diff-test  -- Runs a differential test where the parallel Topic Count"
  echo "                    is tested against the sequential Topic Count"
  echo "  par-scaleup    -- Runs the parallel Topic Count with parallelism from 1 to 8"
  echo
  echo "Additional arguments can be the following:"
  echo "  --wordsPerDocument <num>  -- Sets the number of words per document to <num>"
  echo "                               (default: 100000)"
  echo "  --totalDocuments <num>    -- Sets the number of documents to <num>"
  echo "                               (default: 10)"
  echo "  --parallelism <num>       -- Sets the parallelism of the parallel TopicCount, or"
  echo "                               the sequential TopicCount in seq-diff-test, to <num>"
  echo "                               (default: 4)"
}

CMD="${1}"
shift

case ${CMD} in
  sequential)
    echo "Starting the sequential Topic Count"
    echo

    ${FLINK_HOME}/bin/flink run \
      -c edu.upenn.diffstream.examples.topiccount.TopicCountSequentialExperiment \
      ./target/diffstream-topiccount-1.0-SNAPSHOT.jar "${@}"
    ;;

  parallel)
    echo "Starting the parallel Topic Count"
    echo

    ${FLINK_HOME}/bin/flink run \
      -c edu.upenn.diffstream.examples.topiccount.TopicCountParallelExperiment \
      ./target/diffstream-topiccount-1.0-SNAPSHOT.jar "${@}"
    ;;

  seq-diff-test)
    echo "Starting the sequential differential test"
    echo

    ${FLINK_HOME}/bin/flink run \
      -c edu.upenn.diffstream.examples.topiccount.TopicCountSequentialDiffTesting \
      ./target/diffstream-topiccount-1.0-SNAPSHOT.jar "${@}"
    ;;

  par-diff-test)
    echo "Starting the parallel differential test"
    echo

    ${FLINK_HOME}/bin/flink run \
      -c edu.upenn.diffstream.examples.topiccount.TopicCountParallelDiffTesting \
      ./target/diffstream-topiccount-1.0-SNAPSHOT.jar "${@}"
    ;;

  par-scaleup)
    if [[ -f "temp-scaleup-times" ]]; then
      rm temp-scaleup-times
    fi

    for p in {1..8}; do
      echo "Starting the parallel Topic Count with parallelism ${p}"
      echo

      ${FLINK_HOME}/bin/flink run \
        -c edu.upenn.diffstream.examples.topiccount.TopicCountParallelExperiment \
        ./target/diffstream-topiccount-1.0-SNAPSHOT.jar "${@}" \
        --parallelism "${p}" | tee -a temp-scaleup-times
      echo "---"
      echo
    done

    TIMES=( $(grep "Total time" temp-scaleup-times | awk '{print $3}') )
    echo "Summary of the results:"
    echo
    echo -e "parallelism\ttime (ms)\tspeedup"
    for p in {1..8}; do
      echo -e "${p}\t\t${TIMES[$(( p - 1 ))]}\t\t$(echo scale=2\; ${TIMES[0]} / ${TIMES[$(( p - 1 ))]} | bc)"
    done
    rm temp-scaleup-times
    ;;

  *)
    usage
    ;;
esac
