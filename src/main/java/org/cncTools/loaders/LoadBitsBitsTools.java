package org.cncTools.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class LoadBitsBitsTools {

    private SparkSession sparkSession;
    public LoadBitsBitsTools(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> loadBitsBits() {
        return sparkSession.read()
                .option("multiline","true")
                .json("src/main/resources/BitsBits Fusion Tool Library.V1.3.json")
                .withColumn("data", explode(col("data")));
    }
}
