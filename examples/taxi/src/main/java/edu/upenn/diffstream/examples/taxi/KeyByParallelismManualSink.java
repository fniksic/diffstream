package edu.upenn.diffstream.examples.taxi;

import edu.upenn.diffstream.StreamsNotEquivalentException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class KeyByParallelismManualSink extends RichSinkFunction<Tuple2<Long, Tuple2<Long, Long>>> {

    private static final long serialVersionUID = 1L;

    /* Are we the left or the right sink? */
    private boolean left;

    // If the sinks are initialized to be online, they call the online methods of the matcher,
    // which clean up common prefixes while working
    private boolean online;

    public KeyByParallelismManualSink() {

    }

    public KeyByParallelismManualSink(boolean left, boolean online) {
        this.left = left;
        this.online = online;
    }

    public boolean isLeft() {
        return left;
    }

    public void setLeft(boolean left) {
        this.left = left;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    @Override
    public void invoke(Tuple2<Long, Tuple2<Long, Long>> item, Context context) throws Exception {
        if(isLeft()) {
            if(isOnline()) {
                if (KeyByParallelismManualMatcher.newLeftOutputOnline(item)) {
                    throw new StreamsNotEquivalentException();
                }
            } else {
                KeyByParallelismManualMatcher.newLeftOutput(item);
            }
        } else {
            if(isOnline()) {
                if (KeyByParallelismManualMatcher.newRightOutputOnline(item)) {
                    throw new StreamsNotEquivalentException();
                }
            } else {
                KeyByParallelismManualMatcher.newRightOutput(item);
            }
        }
    }
}
