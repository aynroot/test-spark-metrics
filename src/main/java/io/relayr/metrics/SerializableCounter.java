package io.relayr.metrics;

import org.apache.spark.groupon.metrics.SparkCounter;
import org.apache.spark.groupon.metrics.UserMetricsSystem;

import java.io.Serializable;

public class SerializableCounter implements Serializable {
    private SparkCounter counter;

    public SerializableCounter(String name) {
        counter = UserMetricsSystem.counter(name);
    }

    public void inc(long n) {
        counter.inc(n);
    }

    public void dec(long n) {
        counter.dec(n);
    }
}
