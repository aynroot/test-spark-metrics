package io.relayr;

import io.relayr.helpers.Lazy;
import io.relayr.metrics.SerializableCounter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class TestSparkMetrics implements Serializable {

    private static JavaStreamingContext initSpark(int duration) {
        SparkContext sparkContext = SparkSessionSingleton.getInstance().sparkContext();
        sparkContext.setLogLevel("ERROR");

        UserMetricsSystem.initialize(sparkContext, "MyMetricNamespace");

        JavaStreamingContext context = new JavaStreamingContext(new JavaSparkContext(sparkContext), Durations.seconds(duration));
        context.checkpoint("checkpoints");


        String applicationId = sparkContext.applicationId();
        System.out.println(String.format("Spark context is initialized. Application ID = %s", applicationId));
        return context;
    }

    public static void main(String[] args) throws Exception {
        int duration = 10;
        JavaStreamingContext context = initSpark(duration);

        JavaReceiverInputDStream<String> lines = context.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        Lazy<SerializableCounter> totalWordCounter = new Lazy<>(() -> new SerializableCounter("TotalWordCounter"));

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> {
            totalWordCounter.get().inc(1);
            // TODO: the counter often gets stuck
            // the lower is the polling interval, the more often this happens
            return new Tuple2<>(s, 1);
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<SummingStateWithMetric>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String key, Optional<Integer> value, State<SummingStateWithMetric> sparkState) {
                if (sparkState.isTimingOut() || !value.isPresent()) {
                    return null;
                }

                SummingStateWithMetric state;
                if (!sparkState.exists()) {
                    state = new SummingStateWithMetric(key, 0);
                } else {
                    state = sparkState.get();
                }
                state.update(value.get());
                sparkState.update(state);
                return new Tuple2<>(key, state.getAccumulatedValue());
            }
        })).mapToPair(t -> t);

        wordCounts.print();

        context.start();
        context.awaitTermination();
    }

    // TODO: implement all other metric types

}
