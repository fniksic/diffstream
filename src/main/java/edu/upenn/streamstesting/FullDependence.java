package edu.upenn.streamstesting;

public class FullDependence<T> implements Dependence<T> {

    @Override
    public boolean test(T fst, T snd) {
        return true;
    }
}
