package edu.upenn.streamstesting;

import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Matcher<IN> extends CoProcessFunction<IN, IN, Boolean> {

    private final Poset<IN> unmatchedItems1;

    private final Poset<IN> unmatchedItems2;

    public Matcher(Dependence<IN> dependence) {
        unmatchedItems1 = new Poset<>(dependence);
        unmatchedItems2 = new Poset<>(dependence);
    }

    @Override
    public void processElement1(IN item, Context context, Collector<Boolean> collector) throws Exception {
        Element<IN> element = unmatchedItems1.allocateElement(item);
        if (element.isMinimal() && unmatchedItems2.matchAndRemoveMinimal(item)) {
            return;
        } else if (unmatchedItems2.isDependent(item)) {
            collector.collect(false);
        } else {
            unmatchedItems1.addAllocatedElement(element);
        }
        context.timerService().registerProcessingTimeTimer(Long.MAX_VALUE);
    }

    @Override
    public void processElement2(IN item, Context context, Collector<Boolean> collector) throws Exception {
        Element<IN> element = unmatchedItems2.allocateElement(item);
        if (element.isMinimal() && unmatchedItems1.matchAndRemoveMinimal(item)) {
            return;
        } else if (unmatchedItems1.isDependent(item)) {
            collector.collect(false);
        } else {
            unmatchedItems2.addAllocatedElement(element);
        }
        context.timerService().registerProcessingTimeTimer(Long.MAX_VALUE);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Boolean> out) throws Exception {
        if (timestamp == Long.MAX_VALUE && !(unmatchedItems1.isEmpty() && unmatchedItems2.isEmpty())) {
            out.collect(false);
        }
    }
}
