package edu.upenn.streamstesting.examples.mapreduce;

import java.io.Serializable;

/**
 * Common class of data items for the examples.
 */

public class ReducerExamplesItem implements Serializable {
    public Integer key;
    public Integer x;
    public Integer y;
}
