package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * 5. StrConcatReducer
 * Requires that the consumer of the output does not care about order.
 */
public class StrConcatReducerV1 implements
        AggregateFunction<ReducerExamplesItem, String, String> {
    public String createAccumulator() {
        return "";
    }
    public String add(ReducerExamplesItem newItem, String state) {
        return state + "@" + newItem.x;
    }
    public String getResult(String state) {
        return state;
    }
    public String merge(String ignore1, String ignore2) {
        throw new RuntimeException("'merge' should not be called");
    }
}
