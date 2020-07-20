package edu.upenn.streamstesting.examples.mapreduce.reducers;

import edu.upenn.streamstesting.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * 2. IndexValuePair reducer
 * Requires the functional dependency (key, x) -> y.
 */
public class IndexValuePairReducer implements
        AggregateFunction<ReducerExamplesItem, Map<Integer, Integer>, Map<Integer, Integer>> {
    public Map<Integer, Integer> createAccumulator() {
        return new HashMap<Integer, Integer>();
    }
    public Map<Integer, Integer> add(ReducerExamplesItem in, Map<Integer, Integer> outmap) {
        outmap.put(in.x, in.y);
        return outmap;
    }
    public Map<Integer, Integer> getResult(Map<Integer, Integer> outmap) {
        return outmap;
    }
    public Map<Integer, Integer> merge(Map<Integer, Integer> ignore1, Map<Integer, Integer> ignore2) {
        throw new RuntimeException("'merge' should not be called");
    }
}
