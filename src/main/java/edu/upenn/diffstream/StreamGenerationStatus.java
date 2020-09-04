package edu.upenn.diffstream;

import com.pholser.junit.quickcheck.internal.GeometricDistribution;
import com.pholser.junit.quickcheck.internal.generator.SimpleGenerationStatus;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

public class StreamGenerationStatus extends SimpleGenerationStatus {

    public StreamGenerationStatus(
            GeometricDistribution distro,
            SourceOfRandomness random,
            int attempts) {
        super(distro, random, attempts);
    }


    // The size needs to be overriden so that the stream has always at
    // least one element
    //
    // Have something better than just hardcoded 100 in the default case
    @Override
    public int size() {
        int attempts = attempts();
        if (attempts > 0)
            return attempts;
        else
            return 100;
    }

}
