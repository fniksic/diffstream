package edu.upenn.diffstream.tutorial.incdec;

import edu.upenn.diffstream.Dependence;
import edu.upenn.diffstream.utils.Case;

public class IncDecDependence implements Dependence<IncDecItem> {

    private static final long serialVersionUID = 950028911730833743L;

    @Override
    public boolean test(IncDecItem fst, IncDecItem snd) {
        return fst.match(
                inc -> snd.match(Case.constant(false), Case.constant(true), Case.constant(true)),
                dec -> snd.match(Case.constant(true), Case.constant(false), Case.constant(true)),
                hash -> snd.match(Case.constant(true), Case.constant(true), Case.constant(true))
        );
    }
}
