package edu.upenn.streamstesting.examples.flinktraining;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

public class KeyByParallelismManualMatcher {
    // TODO: This is the matcher that is called by the two sinks, whenever they have a new item. An easy way to do it,
    //       is to having it static, and accessing its class' methods by the sinks whenever new items appear. Though this
    //       requires that tests are sequential. THis will do for our use case I think.

    // HUGE WARNING: Only one test should be run at a time (since output recorders are static!!!)

    // Note: We could also parametrize the Key and value of items, as long as these fields weren't static.
    //       Is it worth doing that?
    public volatile static HashMap<Long, ArrayList<Tuple2<Long, Tuple2<Long, Long>>>> leftOutput = new HashMap<>();
    public volatile static HashMap<Long, ArrayList<Tuple2<Long, Tuple2<Long, Long>>>> rightOutput = new HashMap<>();

    private static final Object lock = new Object();

    public static void newLeftOutputOnline(Tuple2<Long, Tuple2<Long, Long>> item) {
        newLeftOutput(item);

        // Check and remove the prefix of output items for this key
        Long key = item.f0;
        cleanUpPrefixes(key);
    }

    public static void newLeftOutput(Tuple2<Long, Tuple2<Long, Long>> item) {
        Long key = item.f0;
        // Add the new item in the map
        synchronized (lock) {
            ArrayList oldItems = leftOutput.getOrDefault(key, new ArrayList<>());
            oldItems.add(item);
            leftOutput.put(key, oldItems);
        }
    }

    public static void newRightOutputOnline(Tuple2<Long, Tuple2<Long, Long>> item) {
        newRightOutput(item);

        // Check and remove the prefix of output items for this key
        Long key = item.f0;
        cleanUpPrefixes(key);
    }

    public static void newRightOutput(Tuple2<Long, Tuple2<Long, Long>> item) {
        Long key = item.f0;
        // Add the new item in the map
        synchronized (lock) {
            ArrayList oldItems = rightOutput.getOrDefault(key, new ArrayList<>());
            oldItems.add(item);
            rightOutput.put(key, oldItems);
        }
    }

    public static void cleanUpPrefixes(Long key) {
        synchronized (lock) {
            ArrayList oldLeft = leftOutput.getOrDefault(key, new ArrayList<>());
            ArrayList oldRight = rightOutput.getOrDefault(key, new ArrayList<>());

            // Find the longest common prefix of the two lists

            // WARNING: This is disgusting code :)
            while(oldLeft.size() > 0 && oldRight.size() > 0) {
                if(oldLeft.get(0) == oldRight.get(0)) {
                    oldLeft.remove(0);
                    oldRight.remove(0);
                } else {
                    break;
                }
            }

            // Update the sequences in the hashmaps
            leftOutput.put(key, oldLeft);
            rightOutput.put(key, oldRight);
        }
    }

    // TODO: This function checks if there are any unmatched and then reinitializes the hashes.
    public static boolean allMatched() {
        boolean areAllMatched = false;
        synchronized (lock) {
//            System.out.println(leftOutput);
//            System.out.println(rightOutput);
            areAllMatched = leftOutput.equals(rightOutput);
            leftOutput.clear();
            rightOutput.clear();
        }
        return areAllMatched;
    }
}
