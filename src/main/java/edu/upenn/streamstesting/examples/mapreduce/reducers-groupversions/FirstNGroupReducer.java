package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * 4. FirstNReducer
 * Requires that there are at most 100 items to process.
 * Otherwise, nondeterministic for most inputs.
 */
public class FirstNGroupReducer implements
    GroupReduceFunction<ReducerExamplesItem, Set<ReducerExamplesItem>>
{
    @Override
    public void reduce(Iterable<ReducerExamplesItem> in,
                       Collector<Set<ReducerExamplesItem>> out) {
        Set<ReducerExamplesItem> items = new HashSet<ReducerExamplesItem>();
        Integer count = 0;
        for (ReducerExamplesItem i: in) {
            count++;
            if (count > 100) {
                break;
            }
            items.add(i);
        }
        out.collect(items);
    }
}
