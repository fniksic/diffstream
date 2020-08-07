package edu.upenn.diffstream.tutorial.sum;

public class Value implements DataItem {
    private final int val;

    public Value(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }
}
