package edu.upenn.diffstream.tutorial.incdec;

import java.util.function.Function;

/**
 * {@link Barrier} is a class representing an event that delineates sets of
 * increments and decrements in a data stream.
 */
public class Barrier implements IncDecItem {

    private static final long serialVersionUID = 4455327691370736984L;

    @Override
    public <T> T match(Function<Inc, T> incCase, Function<Dec, T> decCase, Function<Barrier, T> hashCase) {
        return hashCase.apply(this);
    }

    /**
     * Any two objects of type {@link Barrier} are equal.
     *
     * @param o The other object tested for equality
     * @return {@code true} if and only if {@code o} is of type {@link Barrier}
     */
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
