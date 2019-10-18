package edu.upenn.streamstesting.examples.wordcount;

import com.pholser.junit.quickcheck.generator.GenerationStatus.Key;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.java.lang.IntegerGenerator;
import com.pholser.junit.quickcheck.internal.GeometricDistribution;
import com.pholser.junit.quickcheck.internal.ParameterSampler;
import com.pholser.junit.quickcheck.internal.ParameterTypeContext;
import com.pholser.junit.quickcheck.internal.generator.GeneratorRepository;
import com.pholser.junit.quickcheck.internal.generator.ServiceLoaderGeneratorSource;
import com.pholser.junit.quickcheck.internal.generator.SimpleGenerationStatus;
import com.pholser.junit.quickcheck.internal.sampling.TupleParameterSampler;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import edu.upenn.streamstesting.StreamGenerationStatus;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import javax.management.timer.Timer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

// Reads and outputs words from the most common 10000 english words
// https://github.com/first20hours/google-10000-english
public class WordCountSource implements SourceFunction<String> {

	private Integer fuel;
	// KK: From what I understand isRunning must be volatile
	//     because the `run` function runs indefinitely until
	//     cancel is called by another thread.
	private volatile boolean isRunning = true;

	private ArrayList<String> words;
	private Random random;

	public WordCountSource() {
		this.fuel = 10;
	}

	public WordCountSource(Integer fuel) {
		this.fuel = fuel;
	}

	@Override
	public void run(SourceContext<String> sourceContext) throws InterruptedException, IOException {

		random = new Random();
		prepareSetOfWords("google-10000-english.txt");

		while (isRunning && fuel > 0) {
			fuel -= 1;
			outputWords(sourceContext, 10);
			TimeUnit.MILLISECONDS.sleep(100);
		}
	}

	private void outputWords(SourceContext<String> sourceContext, int n) {
		for (int i = 0; i < n; i++) {
			sourceContext.collect(generateWord());
		}
	}

	// TODO: I am not sure if this should be called in the constructor or run.
	public void prepareSetOfWords(String filename) throws IOException {
		words = new ArrayList<>(Files.readAllLines(Paths.get(filename)));
	}

	public String generateWord() {
		return words.get(random.nextInt(words.size()));
	}
		
	@Override
	public void cancel() {
		isRunning = false;
	}
}
