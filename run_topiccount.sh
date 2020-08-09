#!/bin/bash

cd examples/topiccount
./prepare-topiccount.sh
./run-topiccount.sh par-scaleup --totalDocuments 2 --wordsPerDocument 10000
./teardown-topiccount.sh
cd ../..
