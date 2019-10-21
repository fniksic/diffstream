package edu.upenn.streamstesting.examples.mapreduce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BadDataSource implements SourceFunction<ReducerExamplesItem> {
    private Integer fuel;
    private volatile boolean isRunning = true;
    private Random random;

    private static final boolean DEBUG = false;

    public BadDataSource() {this.fuel = 10; }
    public BadDataSource(Integer fuel) {this.fuel = fuel; }

    private static final int TIMEOUT_EVERY = 10;
    private static final int TIMEOUT_MILLISECONDS = 5;

    private static final int RANDOM_INT_MIN = 1;
    private static final int RANDOM_INT_BOUND = 3;

    @Override
    public void run(SourceContext<ReducerExamplesItem> sourceContext)
            throws InterruptedException {

        random = new Random();

        while (isRunning && fuel > 0) {
            fuel -= 1;
            ReducerExamplesItem newItem = generateItem();
            if (DEBUG) { System.out.println("ITEM GENERATED: " + newItem); }
            sourceContext.collect(newItem);
            if (fuel % TIMEOUT_EVERY == 0) {
                TimeUnit.MILLISECONDS.sleep(TIMEOUT_MILLISECONDS);
            }
        }
    }

    public ReducerExamplesItem generateItem() {
        return new ReducerExamplesItem(
                generateInteger(),
                generateInteger(),
                generateInteger()
        );
    }

    public Integer generateInteger() {
        return RANDOM_INT_MIN + random.nextInt(RANDOM_INT_BOUND - RANDOM_INT_MIN);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
