package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.utils.Case;

public class Hash implements IncDecItem {

    private static final long serialVersionUID = 4455327691370736984L;

    @Override
    public <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase) {
        return hashCase.apply(this);
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
