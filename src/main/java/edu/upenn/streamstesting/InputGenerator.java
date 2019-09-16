package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

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

		// TODO:
		//
		// - Instantiate an integer generator from the junit-quickcheck library
		//
		// - Add junit-quickcheck as a Maven repo? As it seems
		//   that we might not need to play with source?
		//
		// - Call the generate method of the integer generator with a sourceofRandomness
		while (isRunning) {
			sourceContext.collect(1);
		}
	}
 
	@Override
	public void cancel() {
		isRunning = false;
	}
}
