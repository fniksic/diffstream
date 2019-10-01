package edu.upenn.streamstesting;

public class EmptyDependence<T> implements Dependence<T> {

    private static final long serialVersionUID = -5745833530725753838L;

    @Override
    public boolean test(T fst, T snd) {
        return false;
    }
}
