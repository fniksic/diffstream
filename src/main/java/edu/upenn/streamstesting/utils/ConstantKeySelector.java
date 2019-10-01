package edu.upenn.streamstesting.utils;

import org.apache.flink.api.java.functions.KeySelector;

public class ConstantKeySelector<IN> implements KeySelector<IN, Integer> {
    
    @Override
    public Integer getKey(IN in) throws Exception {
        return 0;
    }
}
