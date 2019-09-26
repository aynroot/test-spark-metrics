package io.relayr;


import io.relayr.helpers.Lazy;
import io.relayr.metrics.SerializableCounter;

import java.io.Serializable;

public class SummingStateWithMetric implements Serializable {
    private Integer accumulatedValue;
    private Lazy<SerializableCounter> singleWordCounter;

    public SummingStateWithMetric(String key, Integer value) {
        this.accumulatedValue = value;

        singleWordCounter = new Lazy<>(() -> new SerializableCounter("SingleWordCounter." + key));
    }

    public void update(Integer value) {
        singleWordCounter.get().inc(value);
        this.accumulatedValue += value;
    }


    public Integer getAccumulatedValue() {
        return accumulatedValue;
    }
}
