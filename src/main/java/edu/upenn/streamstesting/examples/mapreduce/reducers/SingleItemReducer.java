package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 1. SingleItem reducer
 * Requires the functional dependency key -> x.
 */
public class SingleItemReducer implements
        AggregateFunction<ReducerExamplesItem, Integer, Integer>
{
    public Integer createAccumulator() {
        return null;
    }
    public Integer add(ReducerExamplesItem newItem, Integer saved) {
        return newItem.x;
    }
    public Integer getResult(Integer saved) {
        return saved;
    }
    public Integer merge(Integer ignore1, Integer ignore2) {
        throw new RuntimeException("'merge' should not be called");
    }
}
