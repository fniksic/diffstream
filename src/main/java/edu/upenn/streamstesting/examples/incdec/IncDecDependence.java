package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.Dependence;
import edu.upenn.streamstesting.utils.Case;

public class IncDecDependence implements Dependence<IncDecItem> {

    @Override
    public boolean test(IncDecItem fst, IncDecItem snd) {
        return fst.match(
                inc -> snd.match(Case.constant(false), Case.constant(true), Case.constant(true)),
                dec -> snd.match(Case.constant(true), Case.constant(false), Case.constant(true)),
                hash -> snd.match(Case.constant(true), Case.constant(true), Case.constant(true))
        );
    }
}
