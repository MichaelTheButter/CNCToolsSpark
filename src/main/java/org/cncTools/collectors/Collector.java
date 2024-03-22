package org.cncTools.collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Collector {
    public static void saveAsCsv(Dataset<Row> df, String filePath) {
        df.write()
          .format("csv")
          .save(filePath);
    }

    public static void saveAsParquet(Dataset<Row> df, String filePath) {
        df.write()
          .format("parquet")
          .save(filePath);
    }
}
