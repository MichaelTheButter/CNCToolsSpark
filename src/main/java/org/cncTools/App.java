package org.cncTools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.source.image.*;
import org.cncTools.loaders.LoadBitsBitsTools;
import org.cncTools.loaders.LoadSandvikTools;

import static org.apache.spark.sql.functions.*;

public class App {
    public static void main( String[] args ) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("AppName")
                .master("local[*]")
                .getOrCreate();

        LoadSandvikTools loader = new LoadSandvikTools(sparkSession);
        Dataset<Row> df = loader.loadJoinedSandvikCatalog();
        df.show();
        df.printSchema();

        LoadBitsBitsTools bbloader = new LoadBitsBitsTools(sparkSession);
        Dataset<Row> df2 = bbloader.loadBitsBits();

        df2.show();
        df2.printSchema();


    }
}
