package edu.upenn.diffstream.matcher;

import edu.upenn.diffstream.Dependence;
import edu.upenn.diffstream.poset.Element;
import edu.upenn.diffstream.poset.Poset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class StreamEquivalenceMatcher<IN extends Serializable> implements RemoteMatcher<IN>, AutoCloseable {

    private final static Logger LOG = LoggerFactory.getLogger(StreamEquivalenceMatcher.class);

    private final long id;
    private final boolean isRemote;
    private final Poset<IN> unmatchedItemsLeft;
    private final Poset<IN> unmatchedItemsRight;
    private boolean detectedNonEquivalence = false;

    // Statistics
    private int processedItems = 0;
    private long totalProcessingDuration = 0;
    private long maxProcessingDuration = 0;

    StreamEquivalenceMatcher(long id, Dependence<IN> dependence, boolean isRemote) {
        this.id = id;
        this.isRemote = isRemote;
        this.unmatchedItemsLeft = new Poset<>(dependence);
        this.unmatchedItemsRight = new Poset<>(dependence);
    }

    public long getId() {
        return id;
    }

    public boolean isRemote() {
        return isRemote;
    }

    @Override
    public synchronized void processItem(IN item, boolean left) throws StreamsNotEquivalentException {
        final Instant start = Instant.now();
        try {
            if (left) {
                processItem(item, unmatchedItemsLeft, unmatchedItemsRight);
            } else {
                processItem(item, unmatchedItemsRight, unmatchedItemsLeft);
            }
        } finally {
            final long duration = start.until(Instant.now(), ChronoUnit.NANOS);
            processedItems++;
            totalProcessingDuration += duration;
            maxProcessingDuration = Math.max(maxProcessingDuration, duration);
        }
    }

    private void processItem(IN item, Poset<IN> myUnmatchedItems, Poset<IN> otherUnmatchedItems) throws StreamsNotEquivalentException {
        if (detectedNonEquivalence) {
            // Simply ignore further items
            return;
        }

        Element<IN> element = myUnmatchedItems.allocateElement(item);
        if (element.isMinimal() && otherUnmatchedItems.matchAndRemoveMinimal(item)) {
            return;
        } else if (otherUnmatchedItems.isDependent(item)) {
            detectedNonEquivalence = true;
            throw new StreamsNotEquivalentException();
        } else {
            myUnmatchedItems.addAllocatedElement(element);
        }
    }

    /**
     * Succeeds if the equivalence of the two streams has not already been disproved and
     * there are no remaining unmatched items. Otherwise throws {@link StreamsNotEquivalentException}.
     *
     * @throws StreamsNotEquivalentException If the streams are not equivalent
     */
    public void assertStreamsAreEquivalent() throws StreamsNotEquivalentException {
        if (detectedNonEquivalence || !unmatchedItemsLeft.isEmpty() || !unmatchedItemsRight.isEmpty()) {
            throw new StreamsNotEquivalentException();
        }
    }

    public synchronized String getStats() {
        final long avgDuration = processedItems == 0 ? 0 : totalProcessingDuration / processedItems;
        return "Matcher{id=" + id + "}:" +
                " left: " + unmatchedItemsLeft.size() +
                " right: " + unmatchedItemsRight.size() +
                " totalProcessed: " + processedItems +
                " totalDuration (ns): " + totalProcessingDuration +
                " avgDuration (ns): " + avgDuration +
                " maxDuration (ns): " + maxProcessingDuration;
    }

    @Override
    public void close() {
        MatcherFactory.destroyMatcher(this);
    }

}
