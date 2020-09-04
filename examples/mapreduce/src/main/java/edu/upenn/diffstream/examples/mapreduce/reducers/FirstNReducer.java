package edu.upenn.diffstream.examples.mapreduce.reducers;

import edu.upenn.diffstream.examples.mapreduce.ReducerExamplesItem;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

/**
 * 4. FirstNReducer
 * Requires that there are at most 100 items to process.
 * Otherwise, nondeterministic for most inputs.
 */
public class FirstNReducer implements
        AggregateFunction<ReducerExamplesItem,
                Tuple2<HashSet<ReducerExamplesItem>, Integer>,
                HashSet<ReducerExamplesItem>> {

    public Tuple2<HashSet<ReducerExamplesItem>, Integer> createAccumulator() {
        HashSet<ReducerExamplesItem> s = new HashSet<>();
        Integer count = 0;
        return new Tuple2<>(s, count);
    }

    public Tuple2<HashSet<ReducerExamplesItem>, Integer> add(
            ReducerExamplesItem newItem,
            Tuple2<HashSet<ReducerExamplesItem>, Integer> state
    ) {
        HashSet<ReducerExamplesItem> items = state.f0;
        Integer count = state.f1;
        count++;
        if (count <= 5) {
            items.add(newItem);
        }
        return new Tuple2<>(items, count);
    }

    public HashSet<ReducerExamplesItem> getResult(Tuple2<HashSet<ReducerExamplesItem>,
            Integer> state) {
        return state.f0;
    }

    public Tuple2<HashSet<ReducerExamplesItem>, Integer> merge(
            Tuple2<HashSet<ReducerExamplesItem>, Integer> ignore1,
            Tuple2<HashSet<ReducerExamplesItem>, Integer> ignore2
    ) {
        throw new RuntimeException("'merge' should not be called");
    }

}
