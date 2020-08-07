package edu.upenn.diffstream.generators;


import com.pholser.junit.quickcheck.generator.ComponentizedGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Note: In order for the generator to be discovered by the framework,
 * we need to add in in the resources/META-INF/services folder, in the
 * Generator file.
 */
public class Tuple3Generator extends ComponentizedGenerator<Tuple3> {


    public Tuple3Generator() {
        super(Tuple3.class);
    }

    @Override public Tuple3<?,?,?> generate(SourceOfRandomness random,
                                            GenerationStatus status) {

        // TODO: Allow configuring the Tuple2Generator

        Generator<?> leftGenerator = componentGenerators().get(0);
        Generator<?> middleGenerator = componentGenerators().get(1);
        Generator<?> rightGenerator = componentGenerators().get(2);

        Tuple3<?,?,?> item = new Tuple3<>(leftGenerator.generate(random, status),
                                          middleGenerator.generate(random, status),
                                          rightGenerator.generate(random, status));

        return (item);
    }

    @Override public int numberOfNeededComponents() {
        return 3;
    }

}

