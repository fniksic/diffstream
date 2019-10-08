package edu.upenn.streamstesting.examples.flinktraining;

import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import edu.upenn.streamstesting.poset.Element;
import edu.upenn.streamstesting.poset.Poset;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class KeyByParallelismManualSink extends RichSinkFunction {

    private static final long serialVersionUID = 1L;

    /* Are we the left or the right sink? */
    private boolean left;

    public KeyByParallelismManualSink() {

    }

    public KeyByParallelismManualSink(boolean left) {
        this.left = left;
    }

    public boolean isLeft() {
        return left;
    }

    public void setLeft(boolean left) {
        this.left = left;
    }

    public void invoke(Tuple2<Long, Tuple2<Long, Long>> item, Context context) throws Exception {
        if(isLeft()) {
            KeyByParallelismManualMatcher.newLeftOutput(item);
        } else {
            KeyByParallelismManualMatcher.newRightOutput(item);
        }
    }
}
