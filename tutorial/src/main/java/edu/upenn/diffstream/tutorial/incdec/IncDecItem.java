package edu.upenn.diffstream.tutorial.incdec;

import java.io.Serializable;
import java.util.function.Function;

/**
 * The base data type in the IncDec example. It is implemented by three classes:
 * {@link Inc}, representing increments, {@link Dec}, representing decrements,
 * and {@link Barrier}, representing events that delineate sets of increments and
 * decrements in a data stream.
 */
public interface IncDecItem extends Serializable {

    /**
     * A method that allows pattern matching on the concrete type of {@link IncDecItem}. For each
     * of the three possible cases ({@link Inc}, {@link Dec}, and {@link Barrier}), a function is provided
     * that takes care of that case. All provided functions need to agree on the return type, which is
     * given as a type parameter {@link T}. An implementation on a concrete object will call the
     * corresponding case function, pass the concrete object as an argument, and return the function's
     * return value. For example, {@link Inc#match(Function, Function, Function)} will call {@code incCase}, etc.
     *
     * @param incCase A function that takes {@link Inc} as input and returns {@link T}
     * @param decCase A function that takes {@link Dec} as input and returns {@link T}
     * @param hashCase A function that takes {@link Barrier} as input and returns {@link T}
     * @param <T> A common return type of the case functions
     * @return The return value of the case function
     */
    <T> T match(Function<Inc, T> incCase, Function<Dec, T> decCase, Function<Barrier, T> hashCase);

}
