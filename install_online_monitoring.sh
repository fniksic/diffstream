#!/bin/bash

cd streaming-benchmarks/

echo "Installing Streaming benchmarks..."
./flink-bench.sh SETUP_FILIP

sed -i 's/-XX:+UseParNewGC//g' kafka_2.11-0.8.2.2/bin/kafka-run-class.sh
sed -i 's/-XX:+PrintGCDateStamps//g' kafka_2.11-0.8.2.2/bin/kafka-run-class.sh
sed -i 's/-XX:+PrintGCTimeStamps//g' kafka_2.11-0.8.2.2/bin/kafka-run-class.sh

cd ../
