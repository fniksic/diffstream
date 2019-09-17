package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MatcherSink implements SinkFunction<Boolean> {

    private static final List<Boolean> EXPECTED_RESULT = Collections.EMPTY_LIST;

    private static final List<Boolean> result = Collections.synchronizedList(new ArrayList<>());

    public MatcherSink() {
        result.clear();
    }

    @Override
    public void invoke(Boolean value, Context context) throws Exception {
        result.add(value);
    }

    public boolean equalsTrue() {
        return EXPECTED_RESULT.equals(result);
    }
}
