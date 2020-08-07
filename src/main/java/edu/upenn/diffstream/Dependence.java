package edu.upenn.diffstream;

/**
 * Binary predicate that expresses dependence of data items of type {@code T}.
 * Should be symmetric.
 *
 * @param <T> The type of the data item
 */
@FunctionalInterface
public interface Dependence<T> {

    /**
     * Returns true if and only if {@code fst} and {@code snd} are dependent. The implementation
     * should make sure that <code>test(fst, snd) == test(snd, fst)</code>.
     *
     * @param fst
     * @param snd
     * @return
     */
    boolean test(T fst, T snd);

}
