package edu.upenn.diffstream.examples.topiccount;

import java.io.Serializable;
import java.util.function.Function;

public interface TopicCountItem extends Serializable {

    <T> T match(Function<Word, T> wordCase, Function<EndOfFile, T> endOfFileCase);

}
