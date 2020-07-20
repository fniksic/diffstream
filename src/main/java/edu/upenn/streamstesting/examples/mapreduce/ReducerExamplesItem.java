package edu.upenn.streamstesting.examples.mapreduce;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Common class of data items for the examples.
 */

public class ReducerExamplesItem implements Serializable {
    public Integer key;
    public Integer x;
    public Integer y;
    public Timestamp timestamp;

    public ReducerExamplesItem(Integer key, Integer x, Integer y) {
        this.key = key;
        this.x = x;
        this.y = y;
        this.timestamp = new Timestamp(System.currentTimeMillis());

    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null || getClass() != other.getClass())
            return false;
        ReducerExamplesItem o = (ReducerExamplesItem) other;
        return (this.key.equals(o.key) && this.x.equals(o.x) && this.y.equals(o.y));
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + 17 * x.hashCode() + y.hashCode();
    }

    @Override
    public String toString() {
        return "(" + key + ", " + x + ", " + y + ") [ts: " + timestamp + "]";
    }
}
