package edu.upenn.streamstesting.remote;

import edu.upenn.streamstesting.Dependence;
import edu.upenn.streamstesting.StreamsNotEquivalentException;
import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.rmi.RemoteException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteStreamEquivalenceMatcher<IN extends Serializable> implements RemoteMatcher<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStreamEquivalenceMatcher.class);

    private static final AtomicLong matcherCount = new AtomicLong(0L);

    private final long id;
    private final Poset<IN> unmatchedItemsLeft;
    private final Poset<IN> unmatchedItemsRight;
    private boolean detectedNonEquivalence = false;

    // Statistics
    private int processedItems = 0;
    private long totalProcessingDuration = 0;
    private long maxProcessingDuration = 0;

    public RemoteStreamEquivalenceMatcher(Dependence<IN> dependence) {
        this.id = matcherCount.incrementAndGet();
        this.unmatchedItemsLeft = new Poset<>(dependence);
        this.unmatchedItemsRight = new Poset<>(dependence);
    }

    public RemoteStreamEquivalenceMatcher(Dependence<IN> dependence, boolean matcherLogItems) {
        this.id = matcherCount.incrementAndGet();
        this.unmatchedItemsLeft = new Poset<>(dependence);
        this.unmatchedItemsRight = new Poset<>(dependence);
        if(matcherLogItems) {
            Thread t = new Thread() {

                public void run() {
                    logItems();
                }
            };
            t.start();
        }
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
        final Instant start = Instant.now();
        try {
            if (left) {
                processItem(item, unmatchedItemsLeft, unmatchedItemsRight);
            } else {
                processItem(item, unmatchedItemsRight, unmatchedItemsLeft);
            }
        }
        finally {
            final long duration = start.until(Instant.now(), ChronoUnit.NANOS);
            processedItems++;
            totalProcessingDuration += duration;
            maxProcessingDuration = Math.max(maxProcessingDuration, duration);
        }
    }

    private synchronized String getStats() {
        final long avgDuration = processedItems == 0 ? 0 : totalProcessingDuration / processedItems;
        return "Unmatched items:" +
                " left: " + unmatchedItemsLeft.size() +
                " right: " + unmatchedItemsRight.size() +
                " totalProcessed: " + processedItems +
                " totalDuration (ns): " + totalProcessingDuration +
                " avgDuration (ns): " + avgDuration +
                " maxDuration (ns): " + maxProcessingDuration;
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

    // If we have issues with sleep drift, then we can use this method.
    // https://stackoverflow.com/questions/24104313/how-do-i-make-a-delay-in-java
    public void logItems() {
        PrintWriter pw = null;

        LOG.info(" -- -- -- Matcher Thread -- -- -- ");
        try {
            File file = new File("unmatched-items.txt");
            LOG.info("File writable: " + file.canWrite());
            file.setWritable(true);
            FileWriter fw = new FileWriter(file);
            pw = new PrintWriter(fw);
            while(true) {
                final String stats = getStats();
                pw.println(stats);
                pw.flush();
                LOG.info(stats);
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (IOException e) {
            LOG.error("IOException", e);
        } catch (InterruptedException e) {
            LOG.error(" !! !! [ERROR] Thread got interrupted!", e);
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }
}
