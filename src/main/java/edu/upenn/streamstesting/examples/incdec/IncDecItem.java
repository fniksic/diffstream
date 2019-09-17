package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.utils.Case;

import java.io.Serializable;

public interface IncDecItem extends Serializable {

    <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase);

}
