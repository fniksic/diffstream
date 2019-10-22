package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.utils.Case;

import java.io.Serializable;

public interface WordCountItem extends Serializable {

    <T> T match(Case<Word, T> wordCase, Case<EndOfFile, T> endOfFileCase);
}
