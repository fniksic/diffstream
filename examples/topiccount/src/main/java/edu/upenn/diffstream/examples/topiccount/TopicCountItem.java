package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.utils.Case;

import java.io.Serializable;

public interface TopicCountItem extends Serializable {

    <T> T match(Case<Word, T> wordCase, Case<EndOfFile, T> endOfFileCase);
}
