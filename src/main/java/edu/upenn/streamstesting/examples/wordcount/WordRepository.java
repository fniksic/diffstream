package edu.upenn.streamstesting.examples.wordcount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordRepository {

    public static final String REDIS_HOST = "localhost";
    public static final String REDIS_KEY_PREFIX = "word-count:topic:";
    public static final List<String> TOPICS = Arrays.asList(
            "sport", "science", "politics", "environment", "medicine", "religion", "feminism", "sex", "film",
            "literature", "geography", "history", "music", "art", "travel", "food & drinks", "chess", "crime",
            "comedy", "theater"
    );
    private static final String WORDS_FILE = "/google-10000-english.txt";
    private static final Logger LOG = LoggerFactory.getLogger(WordRepository.class);

    private static FileSystem fs = null;
    private static Path wordsFile = null;

    public static void initFileSystem() {
        URL wordsURL = WordRepository.class.getResource(WORDS_FILE);
        try {
            URI wordsURI = wordsURL.toURI();
            if (wordsURI.toString().startsWith("jar")) {
                String[] wordsURIParts = wordsURI.toString().split("!");
                fs = FileSystems.newFileSystem(URI.create(wordsURIParts[0]), new HashMap<>());
                wordsFile = fs.getPath(wordsURIParts[1]);
            } else {
                wordsFile = Paths.get(wordsURI);
            }
        } catch (URISyntaxException | IOException e) {
            LOG.error("Exception while initializing file system: {}", e.getMessage());
        }
    }

    public static void closeFileSystem() {
        if (null != fs) {
            try {
                fs.close();
            } catch (IOException e) {
                LOG.error("Exception while trying to close file system: {}", e.getMessage());
            }
        }
    }

    public static Stream<String> getWordStream() {
        if (wordsFile == null) {
            return Stream.empty();
        }
        try {
            return Files.lines(wordsFile);
        } catch (IOException e) {
            LOG.error("Exception while opening {}: {}", WORDS_FILE, e.getMessage());
        }
        return Stream.empty();
    }

    public static List<String> getWordList() {
        initFileSystem();
        List<String> wordList = WordRepository.getWordStream().collect(Collectors.toList());
        closeFileSystem();
        return wordList;
    }

    public static String prependKeyPrefix(String word) {
        return REDIS_KEY_PREFIX + word;
    }

    public static String lookupTopic(Jedis jedis, String word) {
        return jedis.get(prependKeyPrefix(word));
    }

    /**
     * Populates a Redis instance with words read from {@link WordRepository#WORDS_FILE}.
     * Each word is mapped to a random topic from {@link WordRepository#TOPICS}.
     *
     * @param args
     */
    public static void main(String[] args) {
        Random random = new Random();
        initFileSystem();
        try (Jedis jedis = new Jedis(REDIS_HOST)) {
            getWordStream()
                    .map(WordRepository::prependKeyPrefix)
                    .forEach(word -> {
                        int nextTopic = random.nextInt(TOPICS.size());
                        jedis.set(word, TOPICS.get(nextTopic));
                    });
        }
        closeFileSystem();
    }
}
