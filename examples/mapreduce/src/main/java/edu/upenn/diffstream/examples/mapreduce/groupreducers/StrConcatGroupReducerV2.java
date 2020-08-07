package edu.upenn.diffstream.examples.mapreduce.groupreducers;

import edu.upenn.diffstream.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * 5. StrConcatReducer
 * Requires that the consumer of the output does not care about order.
 */
public class StrConcatGroupReducerV2 implements
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
