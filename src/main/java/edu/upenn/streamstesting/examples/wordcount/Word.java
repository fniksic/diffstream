package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.utils.Case;

import java.util.Objects;

public class Word implements WordCountItem {

    private static final long serialVersionUID = -2648778314677794008L;

    private final String word;

    public Word(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    @Override
    public <T> T match(Case<Word, T> wordCase, Case<EndOfFile, T> endOfFileCase) {
        return wordCase.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Word word1 = (Word) o;
        return word.equals(word1.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word);
    }
}
