package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 3. MaxRowReducer
 * Requires either:
 *   - the functional dependency (key, x) -> y
 *   - OR there is at most one item with the max value of x
 */
public class MaxRowReducer implements
    GroupReduceFunction<ReducerExamplesItem, Tuple2<Integer, Integer>>
{
    @Override
    public void reduce(Iterable<ReducerExamplesItem> in,
                       Collector<Tuple2<Integer, Integer>> out) {
        Integer max_x = 0;
        Integer corresponding_y = null;
        for (ReducerExamplesItem i: in) {
            Integer x = i.x;
            if (x > max_x) {
                max_x = x;
                corresponding_y = i.y;
            }
        }
        out.collect(new Tuple2<Integer, Integer> (max_x, corresponding_y));
    }
}
