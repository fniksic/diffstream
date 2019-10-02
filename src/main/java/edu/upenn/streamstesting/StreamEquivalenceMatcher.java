package edu.upenn.streamstesting;

import edu.upenn.streamstesting.poset.Poset;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamEquivalenceMatcher<IN> {

    /* We maintain a static pool of matchers to deal with the serialization issues in Flink.
       Namely, sinks in Flink need to be serializable, so they cannot have an explicit, baked in
       reference to the corresponding matcher. Instead, a sink retrieves its corresponding
       matcher from the static pool during initialization.
     */

    private static final AtomicLong matcherCount = new AtomicLong(0L);
    private static final ConcurrentMap<Long, StreamEquivalenceMatcher<?>> matcherPool = new ConcurrentHashMap<>();

    public static <IN> StreamEquivalenceMatcher<IN> createMatcher(Dependence<IN> dependence) {
        StreamEquivalenceMatcher<IN> matcher = new StreamEquivalenceMatcher<>(dependence);
        matcherPool.put(matcher.getId(), matcher);
        return matcher;
    }

    public static <IN> StreamEquivalenceMatcher<IN> getMatcherById(long matcherId) {
        return (StreamEquivalenceMatcher<IN>) matcherPool.get(matcherId);
    }

    public static void destroyMatcher(long matcherId) {
        matcherPool.remove(matcherId);
    }

    private final long id;

    private final StreamEquivalenceSink sinkLeft;
    private final Poset<IN> unmatchedItemsLeft;

    private final StreamEquivalenceSink sinkRight;
    private final Poset<IN> unmatchedItemsRight;

    private boolean detectedNonEquivalence = false;

    private StreamEquivalenceMatcher(Dependence<IN> dependence) {
        this.id = matcherCount.incrementAndGet();
        this.unmatchedItemsLeft = new Poset<>(dependence);
        this.unmatchedItemsRight = new Poset<>(dependence);
        this.sinkLeft = new StreamEquivalenceSink(this.id, true);
        this.sinkRight = new StreamEquivalenceSink(this.id, false);
    }

    public long getId() {
        return id;
    }

    public StreamEquivalenceSink getSinkLeft() {
        return sinkLeft;
    }

    public StreamEquivalenceSink getSinkRight() {
        return sinkRight;
    }

    public Poset<IN> getUnmatchedItemsLeft() {
        return unmatchedItemsLeft;
    }

    public Poset<IN> getUnmatchedItemsRight() {
        return unmatchedItemsRight;
    }

    public boolean getDetectedNonEquivalence() {
        return detectedNonEquivalence;
    }

    public void setDetectedNonEquivalence() {
        this.detectedNonEquivalence = true;
    }

    /**
     * Returns {@code true} if the equivalence of the two streams has not already been disproved and
     * there are no remaining unmatched items.
     *
     * <p>As a side effect, this method removes the current matcher from the static pool of matchers.</p>
     *
     * @return
     */
    public boolean streamsAreEquivalent() {
        destroyMatcher(id);
        return !detectedNonEquivalence && unmatchedItemsLeft.isEmpty() && unmatchedItemsRight.isEmpty();
    }
}
