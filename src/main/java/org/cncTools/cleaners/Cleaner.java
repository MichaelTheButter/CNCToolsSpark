package org.cncTools.cleaners;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.cncTools.SandvikSchema;
import org.cncTools.UnionSchema;
import scala.collection.immutable.Seq;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Cleaner {
    static final int MILLISEC_TO_SEC_DIVISOR = 1000;

    public Dataset<Row> getUnionTools(Dataset<Row> df, Dataset<Row> df2){
        return df.unionByName(df2);
    }

    public Dataset<Row> cleanSandvik(Dataset<Row> df) {
        return df.drop(col(SandvikSchema.DOCUMENT_LIST), col(SandvikSchema.MAPPING_RULE), col(SandvikSchema.P21_STRUCTURE_CHANGE_TIMEsTAMP), col(SandvikSchema.P21_VALUE_CHANGE_TIMESTAMP))
                .withColumn(UnionSchema.DESCRIPTION, col(UnionSchema.DESCRIPTION).cast(DataTypes.StringType))
                .withColumn(UnionSchema.DESCRIPTION, regexp_replace(col(UnionSchema.DESCRIPTION), "[\\[\\]]", ""))
                .withColumn(UnionSchema.MODIFIED_DATE, col(UnionSchema.MODIFIED_DATE).cast(DataTypes.TimestampType))
                .withColumn(UnionSchema.PRODUCT_ID, col(UnionSchema.PRODUCT_ID).cast(DataTypes.StringType))
                .withColumn(UnionSchema.PARENT_CLASS, col(UnionSchema.PARENT_CLASS).cast(DataTypes.StringType))
                .withColumn(UnionSchema.VERSION, col(SandvikSchema.GTC_GENERIC_VERSION))
                .withColumnRenamed(SandvikSchema.P21_FILE_URL, UnionSchema.FILE_URL)
                .withColumn(UnionSchema.BODY_DIAMETER, regexp_extract(col(UnionSchema.BODY_DIAMETER), "'[0-9.]+'", 0))
                .withColumn(UnionSchema.BODY_DIAMETER, regexp_replace(col(UnionSchema.BODY_DIAMETER), "[']", "")
                        .cast(DataTypes.DoubleType))
                .withColumn(UnionSchema.BODY_LENGTH, regexp_extract(col(UnionSchema.BODY_LENGTH), "'[0-9.]+'", 0))
                .withColumn(UnionSchema.BODY_LENGTH, regexp_replace(col(UnionSchema.BODY_LENGTH), "[']", "")
                        .cast(DataTypes.DoubleType))
                .withColumn(UnionSchema.PRODUCER, lit(SandvikSchema.PRODUCER_NAME))
                .withColumn(UnionSchema.ADDITIONAL_INFO, to_json(struct(col(SandvikSchema.GTC_GENERIC_CLASS_ID), col(SandvikSchema.GTC_VENDOR_CLASS_ID), col(SandvikSchema.P21_FILE_NAME), col(SandvikSchema.ID), col(SandvikSchema.PARENT_ID))))
                .drop(SandvikSchema.GTC_GENERIC_CLASS_ID, SandvikSchema.PREFERRED_NAME, SandvikSchema.GTC_VENDOR_CLASS_ID, SandvikSchema.P21_FILE_NAME, SandvikSchema.ID, SandvikSchema.NODE_NAME, SandvikSchema.PARENT_ID, SandvikSchema.GTC_GENERIC_VERSION);
    }

    public Dataset<Row> cleanBitsBits(Dataset<Row> df) {
        List<String> fieldsToDrop = List.of("guid", "type", "unit", "GRADE", "holder", "`product-id`", "`product-link`", "`post-process`", "`start-values`", "vendor");
        Seq<String> stringSeq = scala.collection.JavaConverters
                .asScalaIteratorConverter(
                        fieldsToDrop.iterator())
                .asScala()
                .toSeq();

        return df.withColumn(UnionSchema.PRODUCT_ID, col("data.product-id"))
                .withColumn(UnionSchema.MODIFIED_DATE, from_unixtime(col("data.holder.last_modified")
                        .divide(MILLISEC_TO_SEC_DIVISOR))
                        .cast(DataTypes.TimestampType))
                .withColumn(UnionSchema.FILE_URL, col("data.product-link"))
                .withColumn(UnionSchema.UNIT_SYSTEM, col("data.unit"))
                .withColumn(UnionSchema.DESCRIPTION, col("data.type"))
                .withColumn(UnionSchema.PARENT_CLASS, col("data.GRADE"))
                .withColumn(UnionSchema.BODY_DIAMETER, col("data.geometry.DC"))
                .withColumn(UnionSchema.BODY_LENGTH, col("data.geometry.LCF"))
                .withColumn("data", col("data").dropFields(stringSeq))
                .withColumn(UnionSchema.ADDITIONAL_INFO, to_json(col("data")))
                .withColumn(UnionSchema.PRODUCER, lit("bitsBits"))
                .drop(col("data"));
    }
}
