package edu.upenn.diffstream.examples.topiccount;

import java.util.function.Function;

public class EndOfFile implements TopicCountItem {

    private static final long serialVersionUID = -3912679215458677916L;

    @Override
    public <T> T match(Function<Word, T> wordCase, Function<EndOfFile, T> endOfFileCase) {
        return endOfFileCase.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
