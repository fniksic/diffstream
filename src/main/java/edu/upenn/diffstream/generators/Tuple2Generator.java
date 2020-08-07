package edu.upenn.diffstream.generators;


import com.pholser.junit.quickcheck.generator.ComponentizedGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Note: In order for the generator to be discovered by the framework,
 * we need to add in in the resources/META-INF/services folder, in the
 * Generator file.
 */
public class Tuple2Generator extends ComponentizedGenerator<Tuple2> {


    public Tuple2Generator() {
        super(Tuple2.class);
    }

    @Override public Tuple2<?, ?> generate(SourceOfRandomness random,
                                           GenerationStatus status) {

        // TODO: Allow configuring the Tuple2Generator

        Generator<?> leftGenerator = componentGenerators().get(0);
        Generator<?> rightGenerator = componentGenerators().get(1);

        Tuple2<?,?> item = new Tuple2<>(leftGenerator.generate(random, status),
                                        rightGenerator.generate(random, status));

        return (item);
    }

    @Override public int numberOfNeededComponents() {
        return 2;
    }

}

