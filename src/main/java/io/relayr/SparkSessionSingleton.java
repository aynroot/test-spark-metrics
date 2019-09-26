package io.relayr;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionSingleton {
    /**
     * @return spark session instance
     */
    public static SparkSession getInstance() {

        SparkConf sparkConf = SparkService.getConfig();
        return SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
    }
}
