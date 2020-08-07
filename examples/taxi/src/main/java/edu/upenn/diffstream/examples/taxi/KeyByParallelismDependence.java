package edu.upenn.diffstream.examples.taxi;

import edu.upenn.diffstream.Dependence;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyByParallelismDependence implements Dependence<Tuple2<Long, Tuple2<Long, Long>>> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean test(Tuple2<Long, Tuple2<Long, Long>> fst, Tuple2<Long, Tuple2<Long, Long>> snd) {
        return (fst.f0.equals(snd.f0));
    }
}
