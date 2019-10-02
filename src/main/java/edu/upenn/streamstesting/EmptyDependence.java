package edu.upenn.streamstesting;

public class EmptyDependence<T> implements Dependence<T> {

    @Override
    public boolean test(T fst, T snd) {
        return false;
    }
}
