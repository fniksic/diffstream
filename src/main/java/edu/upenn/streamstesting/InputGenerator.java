package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

// Input generation
import com.pholser.junit.quickcheck.generator.java.lang.IntegerGenerator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.internal.generator.SimpleGenerationStatus;
import com.pholser.junit.quickcheck.internal.GeometricDistribution;
import java.util.Random;
// import org.quicktheories.generators.IntegersDSL;
// import org.quicktheories.core.Gen;
// import org.quicktheories.core.RandomnessSource;
// import org.quicktheories.core.XOrShiftPRNG;

// KK: We should probably extend this to be a checkpointable source
//
// KK: This might have to be parametrized by some type??
public class InputGenerator implements SourceFunction<Integer> {

	// KK: From what I understand isRunning must be volatile
	//     because the `run` function runs indefinitely until
	//     cancel is called by another thread.
	private volatile boolean isRunning = true;
	
	public InputGenerator() { }

	@Override
	public void run(SourceFunction.SourceContext<Integer> sourceContext) {

		// TODO: In order to parametrize this, I could follow the tree from JUnitQuickcheck.java
		
		// Instantiate a new integer generator
		// TODO: Make this all parametrizable
		IntegerGenerator gen = new IntegerGenerator();

		// Instantiate source of randomness and generation status to call the generator
		Random randGen = new Random();
		SourceOfRandomness rand = new SourceOfRandomness(randGen);
		GeometricDistribution geo = new GeometricDistribution();
	        SimpleGenerationStatus status = new SimpleGenerationStatus(geo, rand, 0);

		// Quicktheories
		// IntegersDSL intGen = new IntegersDSL();
		// Gen<Integer> gen = intGen.all();
		// // TODO: Have a random see maybe?
		// XOrShiftPRNG prng = new XOrShiftPRGN(1);

		while (isRunning) {
			// Integer curr = gen.generate(prng);
			Integer curr = gen.generate(rand, status);
			sourceContext.collect(curr);
		}
	}
 
	@Override
	public void cancel() {
		isRunning = false;
	}
}
