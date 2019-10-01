package edu.upenn.streamstesting;

import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class SinkBasedMatcherSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = -7225809892155674827L;

    private long matcherId;

    /* Are we the left or the right sink? */
    private boolean left;

    private transient SinkBasedMatcher<IN> sinkBasedMatcher;
    private transient Poset<IN> myUnmatchedItems;
    private transient Poset<IN> otherUnmatchedItems;

    public SinkBasedMatcherSink() {

    }

    public SinkBasedMatcherSink(long matcherId, boolean left) {
        this.matcherId = matcherId;
        this.left = left;
    }

    public long getMatcherId() {
        return matcherId;
    }

    public void setMatcherId(long matcherId) {
        this.matcherId = matcherId;
    }

    public boolean isLeft() {
        return left;
    }

    public void setLeft(boolean left) {
        this.left = left;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sinkBasedMatcher = SinkBasedMatcher.getMatcherById(matcherId);
        myUnmatchedItems = (left ? sinkBasedMatcher.getUnmatchedItemsLeft() : sinkBasedMatcher.getUnmatchedItemsRight());
        otherUnmatchedItems = (left ? sinkBasedMatcher.getUnmatchedItemsRight() : sinkBasedMatcher.getUnmatchedItemsLeft());
    }

    @Override
    public void close() throws Exception {
        sinkBasedMatcher = null;
        myUnmatchedItems = null;
        otherUnmatchedItems = null;
    }

    @Override
    public void invoke(IN item, Context context) throws Exception {
        synchronized (sinkBasedMatcher) {
            if (sinkBasedMatcher.getDetectedNonEquivalence()) {
                // Simply ignore further items
                return;
            }

            Element<IN> element = myUnmatchedItems.allocateElement(item);
            if (element.isMinimal() && otherUnmatchedItems.matchAndRemoveMinimal(item)) {
                return;
            } else if (otherUnmatchedItems.isDependent(item)) {
                sinkBasedMatcher.setDetectedNonEquivalence();
            } else {
                myUnmatchedItems.addAllocatedElement(element);
            }
        }
    }
}
