package edu.upenn.diffstream;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * A Flink computation that takes an input stream of events of type {@link IN} and
 * produces an output stream of events of type {@link OUT}.
 **/
@FunctionalInterface
public interface FlinkProgram<IN, OUT> {

    DataStream<OUT> apply(DataStream<IN> inputStream);

}
