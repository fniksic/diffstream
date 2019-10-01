package edu.upenn.streamstesting;

import edu.upenn.streamstesting.Dependence;
import edu.upenn.streamstesting.utils.Case;

public class FullDependence<T> implements Dependence<T> {

    // Does this UID have anything to do with this?
    // private static final long serialVersionUID = 950028911730833743L;
    private static final long serialVersionUID = 950028911730833744L;
    
    @Override
    public boolean test(T fst, T snd) {
        return true;
    }
}
