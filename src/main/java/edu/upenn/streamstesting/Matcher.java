package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Matcher<IN> extends RichCoFlatMapFunction<IN, IN, Boolean> {

    private final Dependence<IN> dependence;

    public Matcher(Dependence<IN> dependence) {
        this.dependence = dependence;
    }

    @Override
    public void flatMap1(IN item, Collector<Boolean> collector) throws Exception {

    }

    @Override
    public void flatMap2(IN item, Collector<Boolean> collector) throws Exception {

    }
}
