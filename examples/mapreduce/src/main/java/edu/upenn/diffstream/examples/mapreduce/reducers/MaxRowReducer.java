package edu.upenn.diffstream.examples.mapreduce.reducers;

import edu.upenn.diffstream.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 3. MaxRowReducer
 * Requires either:
 *   - the functional dependency (key, x) -> y
 *   - OR there is at most one item with the max value of x
 */
public class MaxRowReducer implements
        AggregateFunction<ReducerExamplesItem,
                          Tuple2<Integer, Integer>,
                          Tuple2<Integer, Integer>> {
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<>(0, null);
    }
    public Tuple2<Integer, Integer> add(ReducerExamplesItem newItem,
                                        Tuple2<Integer, Integer> prev) {
        Tuple2<Integer, Integer> curr = new Tuple2<>(newItem.x, newItem.y);
        if (curr.f0 > prev.f0) {
            return curr;
        } else {
            return prev;
        }
    }
    public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> curr) {
        return curr;
    }
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> ignore1,
                                          Tuple2<Integer, Integer> ignore2) {
        throw new RuntimeException("'merge' should not be called");
    }
}
