package edu.upenn.diffstream;

import edu.upenn.diffstream.poset.Element;
import edu.upenn.diffstream.poset.Poset;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class StreamEquivalenceSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = -7225809892155674827L;

    private long matcherId;

    /* Are we the left or the right sink? */
    private boolean left;

    private transient StreamEquivalenceMatcher<IN> matcher;
    private transient Poset<IN> myUnmatchedItems;
    private transient Poset<IN> otherUnmatchedItems;

    public StreamEquivalenceSink() {

    }

    public StreamEquivalenceSink(long matcherId, boolean left) {
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
    public void open(Configuration parameters) {
        matcher = StreamEquivalenceMatcher.getMatcherById(matcherId);
        myUnmatchedItems = (left ? matcher.getUnmatchedItemsLeft() : matcher.getUnmatchedItemsRight());
        otherUnmatchedItems = (left ? matcher.getUnmatchedItemsRight() : matcher.getUnmatchedItemsLeft());
    }

    @Override
    public void close() {
        matcher = null;
        myUnmatchedItems = null;
        otherUnmatchedItems = null;
    }

    @Override
    public void invoke(IN item, Context context) throws Exception {
        synchronized (matcher) {
            if (matcher.getDetectedNonEquivalence()) {
                // Simply ignore further items
                return;
            }

            Element<IN> element = myUnmatchedItems.allocateElement(item);
            if (element.isMinimal() && otherUnmatchedItems.matchAndRemoveMinimal(item)) {
                return;
            } else if (otherUnmatchedItems.isDependent(item)) {
                matcher.setDetectedNonEquivalence();
            } else {
                myUnmatchedItems.addAllocatedElement(element);
            }
        }
    }
}
