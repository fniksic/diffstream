package edu.upenn.diffstream.matcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;

class StreamEquivalenceSink<IN extends Serializable> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = -7225809892155674827L;

    private long matcherId;

    /* Are we the left or the right sink? */
    private boolean left;

    private transient StreamEquivalenceMatcher<IN> matcher;

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
        matcher = MatcherFactory.getMatcher(matcherId);
    }

    @Override
    public void close() {
        matcher = null;
    }

    @Override
    public void invoke(IN item, Context context) throws Exception {
        matcher.processItem(item, left);
    }

}
