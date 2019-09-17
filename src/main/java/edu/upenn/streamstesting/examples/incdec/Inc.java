package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.utils.Case;

public class Inc implements IncDecItem {

    private static final long serialVersionUID = 4387835873116520724L;

    private int value;

    public Inc() {
    }

    public Inc(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase) {
        return incCase.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Inc inc = (Inc) o;
        return value == inc.getValue();
    }

    @Override
    public int hashCode() {
        return value;
    }
}
