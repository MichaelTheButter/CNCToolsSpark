package org.cncTools.analysers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.UnionSchema;

import static org.apache.spark.sql.functions.*;


public class ToolAnalyser {
    private SparkSession sparkSession;

    public ToolAnalyser(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * AGGREGATION
     * @param df
     * @return Dataframe count by description with min and max body diameter
     */
    public Dataset<Row> calculateDiamRangeByDescriptions(Dataset<Row> df) {
        return df.groupBy(UnionSchema.DESCRIPTION)
                .agg(max(UnionSchema.BODY_DIAMETER), min(UnionSchema.BODY_DIAMETER), count(UnionSchema.DESCRIPTION))
                .na().drop();
    }

    /**
     * AGGREGATION
     * @param df
     * @return Dataframe wirth columns body_diameter, count, unit_system
     */
    public Dataset<Row> calculateByDiameter(Dataset<Row> df) {
        return df.groupBy(UnionSchema.BODY_DIAMETER).count()
                .join(
                        df.select(df.col(UnionSchema.BODY_DIAMETER).as(UnionSchema.DIAMETER_ALIAS), df.col(UnionSchema.UNIT_SYSTEM)),
                        col(UnionSchema.BODY_DIAMETER).equalTo(col(UnionSchema.DIAMETER_ALIAS)),
                "inner")
                .drop(col(UnionSchema.DIAMETER_ALIAS))
                .dropDuplicates();
    }
}
