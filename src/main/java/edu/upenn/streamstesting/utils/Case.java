package edu.upenn.streamstesting.utils;

@FunctionalInterface
public interface Case<M, T> {

    T apply(M m);

    static <M, T> Case<M, T> constant(T t) {
        return (M m) -> t;
    }
}
