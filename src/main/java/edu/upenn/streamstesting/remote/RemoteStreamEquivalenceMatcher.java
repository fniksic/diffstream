package edu.upenn.streamstesting.remote;

import edu.upenn.streamstesting.Dependence;
import edu.upenn.streamstesting.StreamsNotEquivalentException;
import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteStreamEquivalenceMatcher<IN extends Serializable> implements RemoteMatcher<IN> {

    private static final AtomicLong matcherCount = new AtomicLong(0L);

    private final long id;
    private final Poset<IN> unmatchedItemsLeft;
    private final Poset<IN> unmatchedItemsRight;
    private boolean detectedNonEquivalence = false;

    public RemoteStreamEquivalenceMatcher(Dependence<IN> dependence) {
        this.id = matcherCount.incrementAndGet();
        this.unmatchedItemsLeft = new Poset<>(dependence);
        this.unmatchedItemsRight = new Poset<>(dependence);
    }

    public long getId() {
        return id;
    }

    public RemoteStreamEquivalenceSink<IN> getSinkLeft() {
        return new RemoteStreamEquivalenceSink<>(this.id, true);
    }

    public RemoteStreamEquivalenceSink<IN> getSinkRight() {
        return new RemoteStreamEquivalenceSink<>(this.id, false);
    }

    @Override
    public synchronized void processItem(IN item, boolean left) throws RemoteException, StreamsNotEquivalentException {
        if (left) {
            processItem(item, unmatchedItemsLeft, unmatchedItemsRight);
        } else {
            processItem(item, unmatchedItemsRight, unmatchedItemsLeft);
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

    public void assertStreamsAreEquivalent() throws StreamsNotEquivalentException {
        RemoteMatcherFactory.getInstance().destroyMatcher(id);
        if (detectedNonEquivalence || !unmatchedItemsLeft.isEmpty() || !unmatchedItemsRight.isEmpty()) {
            throw new StreamsNotEquivalentException();
        }
    }
}
