package edu.upenn.streamstesting.examples.flinktraining;

import edu.upenn.streamstesting.Dependence;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyByParallelismDependence implements Dependence<Tuple2<Long, Tuple2<Long, Long>>> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean test(Tuple2<Long, Tuple2<Long, Long>> fst, Tuple2<Long, Tuple2<Long, Long>> snd) {
        return (fst.f0 == snd.f0);
    }
}
