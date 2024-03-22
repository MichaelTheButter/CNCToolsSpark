package org.cncTools.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UnionLoader {

    public Dataset<Row> loadUnion(Dataset<Row> df1, Dataset<Row> df2) {
        return df1.unionByName(df2);
    }
}
