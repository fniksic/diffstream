package edu.upenn.diffstream.tutorial.incdec;

import java.util.function.Function;

/**
 * {@link Dec} is a class representing integer decrements.
 */
public class Dec implements IncDecItem {

    private static final long serialVersionUID = 1725847752528684253L;

    private int value = 1;

    /**
     * Constructs {@link Dec} with an underlying value of 1
     */
    public Dec() {
    }

    /**
     * Constructs {@link Dec} with an underlying non-negative value
     *
     * @param value A non-negative integer
     */
    public Dec(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Sets the underlying value of the decrement
     *
     * @param value A non-negative integer
     */
    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public <T> T match(Function<Inc, T> incCase, Function<Dec, T> decCase, Function<Barrier, T> hashCase) {
        return decCase.apply(this);
    }

    /**
     * An object of type {@link Dec} is equal to another {@link Dec} object if they have the same value.
     *
     * @param o The object tested for equality
     * @return {@code true} if and only if {@code o} is of type {@link Dec} and it has the same value
     */
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
