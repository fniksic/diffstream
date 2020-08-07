package edu.upenn.diffstream.examples.mapreduce.groupreducers;

import edu.upenn.diffstream.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 2. IndexValuePair reducer
 * Requires the functional dependency (key, x) -> y.
 */
public class IndexValuePairGroupReducer implements
    GroupReduceFunction<ReducerExamplesItem, Map<Integer, Integer>>
{
    @Override
    public void reduce(Iterable<ReducerExamplesItem> in,
                       Collector<Map<Integer, Integer>> out) {
        Map<Integer, Integer> outmap = new HashMap<>();
        for (ReducerExamplesItem i: in) {
            Integer x = i.x;
            Integer y = i.y;
            outmap.put(x, y);
        }
        out.collect(outmap);
    }
}
