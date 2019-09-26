package io.relayr;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;

import java.nio.file.Paths;

/**
 * Spark configuration wrapper
 */
public class SparkService {

    private static Logger logger = LogManager.getFormatterLogger(SparkService.class);

    @Getter
    @Setter
    private static boolean testPhase = false;
    @Getter
    private static int testPhaseAPIPort = 0;

    /**
     * @return spark configuration with predefined custom properties.
     * Depends on testPhase flag.
     */
    public static SparkConf getConfig() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("test-spark-metrics")
                .setMaster("local[*]")

                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.kryo.unsafe", "false")
                .set("spark.sql.warehouse.dir", Paths.get(System.getProperty("user.dir"), "logs/spark-warehouse").toString());

        sparkConf.set("spark.metrics.conf", "/Users/valeria/Documents/work/test-spark-metrics/src/main/resources/metrics.properties");
        return sparkConf;
    }
}
