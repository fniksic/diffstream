package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * KK: Is it possible to parametrize this by a variable-size set of
 * input and output streams instead of just one?
 **/
@FunctionalInterface
public interface FlinkProgram<IN, OUT> {

    DataStream<OUT> apply(DataStream<IN> inputStream);

}
