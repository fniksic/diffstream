package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * 5. StrConcatReducer
 * Requires that the consumer of the output does not care about order.
 */
public class StrConcatReducer implements
    GroupReduceFunction<ReducerExamplesItem, ReducerExamplesItem>
{
    @Override
    public void reduce(Iterable<ReducerExamplesItem> in,
                       Collector<ReducerExamplesItem> out) {
        for (ReducerExamplesItem i: in) {
            out.collect(i);
        }
    }
}
