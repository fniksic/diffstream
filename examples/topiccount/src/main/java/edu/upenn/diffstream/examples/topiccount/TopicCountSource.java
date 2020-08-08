package edu.upenn.diffstream.examples.topiccount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Random;

public class TopicCountSource implements SourceFunction<TopicCountItem> {

    private volatile boolean isRunning = true;

    private final Random random;
    private final List<String> words;
    private final int wordsPerDocument;
    private final int totalDocuments;

    public TopicCountSource(int wordsPerDocument, int totalDocuments) {
        this.random = new Random();
        this.words = WordRepository.getWordList();
        this.wordsPerDocument = wordsPerDocument;
        this.totalDocuments = totalDocuments;
    }

    public Word generateWord() {
        return new Word(words.get(random.nextInt(words.size())));
    }

    @Override
    public void run(SourceContext<TopicCountItem> sourceContext) {
        for (int i = 0; i < totalDocuments; i++) {
            for (int j = 0; j < wordsPerDocument; j++) {
                sourceContext.collect(generateWord());
            }
            sourceContext.collect(new EndOfFile());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
