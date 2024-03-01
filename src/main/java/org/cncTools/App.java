package org.cncTools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main( String[] args ) {
        System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        SparkSession sparkSession = SparkSession.builder()
                .appName("AppName")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .format("xml")
                .option("rowTag", "cd")
                .load("src/main/resources/cd_catalog.xml");

        df.show();
        df.printSchema();
    }
}
