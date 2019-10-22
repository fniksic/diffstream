package edu.upenn.streamstesting.examples.mapreduce.reducers;

import edu.upenn.streamstesting.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 5. StrConcatReducer
 * Requires that the consumer of the output does not care about order.
 */
public class StrConcatReducer implements
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
