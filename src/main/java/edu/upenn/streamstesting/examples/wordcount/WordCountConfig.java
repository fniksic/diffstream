package edu.upenn.streamstesting.examples.wordcount;

import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountConfig {

    private static final int WORDS_PER_DOCUMENT = 100000;
    private static final int TOTAL_DOCUMENTS = 10;
    private static final int PARALLELISM = 4;

    private final int wordsPerDocument;
    private final int totalDocuments;
    private final int parallelism;

    private WordCountConfig(int wordsPerDocument, int totalDocuments, int parallelism) {
        this.wordsPerDocument = wordsPerDocument;
        this.totalDocuments = totalDocuments;
        this.parallelism = parallelism;
    }

    public int getWordsPerDocument() {
        return wordsPerDocument;
    }

    public int getTotalDocuments() {
        return totalDocuments;
    }

    public int getParallelism() {
        return parallelism;
    }

    public static WordCountConfig fromArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return new WordCountConfig(
                parameterTool.getInt("wordsPerDocument", WORDS_PER_DOCUMENT),
                parameterTool.getInt("totalDocuments", TOTAL_DOCUMENTS),
                parameterTool.getInt("parallelism", PARALLELISM)
        );
    }
}
