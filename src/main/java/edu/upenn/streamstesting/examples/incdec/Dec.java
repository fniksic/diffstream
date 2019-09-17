package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.utils.Case;

public class Dec implements IncDecItem {

    private static final long serialVersionUID = 1725847752528684253L;

    private int value;

    public Dec() {
    }

    public Dec(int value) {
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
        return decCase.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dec dec = (Dec) o;
        return value == dec.getValue();
    }

    @Override
    public int hashCode() {
        return value;
    }
}
