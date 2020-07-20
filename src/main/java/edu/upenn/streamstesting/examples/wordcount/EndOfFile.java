package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.utils.Case;

public class EndOfFile implements WordCountItem {

    private static final long serialVersionUID = -3912679215458677916L;

    @Override
    public <T> T match(Case<Word, T> wordCase, Case<EndOfFile, T> endOfFileCase) {
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
