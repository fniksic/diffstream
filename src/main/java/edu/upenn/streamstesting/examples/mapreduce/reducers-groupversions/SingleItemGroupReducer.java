package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * 1. SingleItem reducer
 * Requires the functional dependency key -> x.
 */
public class SingleItemGroupReducer implements
        GroupReduceFunction<ReducerExamplesItem, Integer>
{
    @Override
    public void reduce(Iterable<ReducerExamplesItem> in,
                       Collector<Integer> out) {
        Integer x = null;
        for (ReducerExamplesItem i: in) {
            x = i.x;
        }
        if (x != null) {
            out.collect(x);
        }
    }
}
