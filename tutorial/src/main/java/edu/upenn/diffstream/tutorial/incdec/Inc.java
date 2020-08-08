package edu.upenn.diffstream.tutorial.incdec;

import java.util.function.Function;

/**
 * {@link Inc} is a class representing an integer increment.
 */
public class Inc implements IncDecItem {

    private static final long serialVersionUID = 4387835873116520724L;

    private int value = 1;

    /**
     * Constructs {@link Inc} with an underlying value of 1
     */
    public Inc() {
    }

    /**
     * Constructs {@link Inc} with an underlying non-negative value
     *
     * @param value A non-negative integer
     */
    public Inc(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Sets the underlying value of the increment
     *
     * @param value A non-negative integer
     */
    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public <T> T match(Function<Inc, T> incCase, Function<Dec, T> decCase, Function<Barrier, T> hashCase) {
        return incCase.apply(this);
    }

    /**
     * An object of type {@link Inc} is equal to another {@link Inc} object if they have the same value.
     *
     * @param o The object tested for equality
     * @return {@code true} if and only if {@code o} is of type {@link Inc} and it has the same value
     */
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
