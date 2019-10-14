package edu.upenn.streamstesting;

import edu.upenn.streamstesting.poset.Poset;
import org.apache.flink.streaming.api.datastream.DataStream;

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

    public static <IN> StreamEquivalenceMatcher<IN> createMatcher(DataStream<IN> out1,
                                                                  DataStream<IN> out2,
                                                                  Dependence<IN> dependence) {
        StreamEquivalenceMatcher<IN> matcher = new StreamEquivalenceMatcher<>(dependence);
        matcherPool.put(matcher.getId(), matcher);
        out1.addSink(matcher.getSinkLeft()).setParallelism(1);
        out2.addSink(matcher.getSinkRight()).setParallelism(1);
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

    public void setDetectedNonEquivalence() throws StreamsNotEquivalentException {
        this.detectedNonEquivalence = true;
        throw new StreamsNotEquivalentException();
    }

    /**
     * Succeeds if the equivalence of the two streams has not already been disproved and
     * there are no remaining unmatched items. Otherwise throws {@link StreamsNotEquivalentException}.
     *
     * <p>As a side effect, this method removes the current matcher from the static pool of matchers.</p>
     *
     * @throws StreamsNotEquivalentException If the streams are not equivalent
     */
    public void assertStreamsAreEquivalent() throws StreamsNotEquivalentException {
        destroyMatcher(id);
        if (detectedNonEquivalence || !unmatchedItemsLeft.isEmpty() || !unmatchedItemsRight.isEmpty()) {
            throw new StreamsNotEquivalentException();
        }
    }
}
