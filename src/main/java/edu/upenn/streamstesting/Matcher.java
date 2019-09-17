package edu.upenn.streamstesting;

import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Matcher<IN> extends RichCoFlatMapFunction<IN, IN, Boolean> {

    private final Poset<IN> unmatchedItems1;

    private final Poset<IN> unmatchedItems2;

    public Matcher(Dependence<IN> dependence) {
        this.unmatchedItems1 = new Poset<>(dependence);
        this.unmatchedItems2 = new Poset<>(dependence);
    }

    @Override
    public void flatMap1(IN item, Collector<Boolean> collector) throws Exception {
        Element<IN> element = unmatchedItems1.allocateElement(item);
        if (element.isMinimal() && unmatchedItems2.matchAndRemoveMinimal(item)) {
            return;
        }
        else if (unmatchedItems2.isDependent(item)) {
            collector.collect(false);
        }
        else {
            unmatchedItems1.addAllocatedElement(element);
        }
    }

    @Override
    public void flatMap2(IN item, Collector<Boolean> collector) throws Exception {
        Element<IN> element = unmatchedItems2.allocateElement(item);
        if (element.isMinimal() && unmatchedItems1.matchAndRemoveMinimal(item)) {
            return;
        }
        else if (unmatchedItems1.isDependent(item)) {
            collector.collect(false);
        }
        else {
            unmatchedItems2.addAllocatedElement(element);
        }
    }
}
