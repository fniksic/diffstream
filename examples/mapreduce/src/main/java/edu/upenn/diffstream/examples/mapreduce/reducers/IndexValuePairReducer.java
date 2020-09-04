package edu.upenn.diffstream.examples.mapreduce.reducers;

import edu.upenn.diffstream.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

/**
 * 2. IndexValuePair reducer
 * Requires the functional dependency (key, x) -> y.
 */
public class IndexValuePairReducer implements
        AggregateFunction<ReducerExamplesItem,
                HashMap<Integer, Integer>,
                HashMap<Integer, Integer>> {

    public HashMap<Integer, Integer> createAccumulator() {
        return new HashMap<>();
    }

    public HashMap<Integer, Integer> add(ReducerExamplesItem in,
                                         HashMap<Integer, Integer> outmap) {
        outmap.put(in.x, in.y);
        return outmap;
    }

    public HashMap<Integer, Integer> getResult(HashMap<Integer, Integer> outmap) {
        return outmap;
    }

    public HashMap<Integer, Integer> merge(HashMap<Integer, Integer> ignore1,
                                           HashMap<Integer, Integer> ignore2) {
        throw new RuntimeException("'merge' should not be called");
    }

}
