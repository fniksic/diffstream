package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.datastream.DataStream;

@FunctionalInterface
public interface FlinkProgram<IN, OUT> {

    DataStream<OUT> apply(DataStream<IN> inputStream);

}
