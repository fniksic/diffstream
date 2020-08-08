package edu.upenn.diffstream.tutorial.incdec;

import edu.upenn.diffstream.Dependence;

/**
 * The dependence relation in the IncDec example: two increments ({@link Inc}) are
 * independent, two decrements ({@link Dec}) are independent, and everything else is
 * dependent.
 *
 * One can imagine the following stream processing computation where this dependence
 * relation would be relevant. Suppose that for each sequence of increments and decrements delineated
 * with barriers in a data stream we want to compute partial sums. On each barrier we want to output
 * the maximal partial sum attained in the sequence up to that barrier. For example, if the input
 * stream is
 *
 *   Inc(2), Inc(5), Dec(1), Barrier, Dec(2), Inc(3), Dec(3), Barrier
 *
 * the output stream would be 7, 1.
 *
 * Note that the output doesn't change if we reorder adjacent increments or adjacent
 * decrements, but it does change if we reorder an increment and a decrement. For example, if Inc(5)
 * Dec(1) above are reordered, the first number in the output would be 6.
 *
 * The ordering sensitivity in this computation is precisely captured by {@link IncDecDependence}.
 */
public class IncDecDependence implements Dependence<IncDecItem> {

    private static final long serialVersionUID = 950028911730833743L;

    @Override
    public boolean test(IncDecItem fst, IncDecItem snd) {
        return fst.match(
                fstInc -> snd.match(sndInc -> false, sndDec -> true, sndHash -> true),
                fstDec -> snd.match(sndInc -> true, sndDec -> false, sndHash -> true),
                fstHash -> true
        );
    }

}
