#!/bin/bash

cd examples/topiccount
./prepare-topiccount.sh
./run-topiccount.sh sequential
./run-topiccount.sh parallel
./run-topiccount.sh seq-diff-test --totalDocuments 1 --wordsPerDocument 1
./run-topiccount.sh par-diff-test
./run-topiccount.sh par-scaleup
./teardown-topiccount.sh
cd ../..
