package org.cncTools.cleaners;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.cncTools.UnionSchema;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Cleaner {

    public Dataset<Row> getUnionTools(Dataset<Row> df, Dataset<Row> df2){
        return df.unionByName(df2);
    }

    public Dataset<Row> cleanSandvik(Dataset<Row> df) {
        return df.drop(col("document_list"), col("mapping_rule"), col("p21_structure_change_timestamp"), col("p21_value_change_timestamp"))
                .withColumn(UnionSchema.DESCRIPTION, col(UnionSchema.DESCRIPTION).cast(DataTypes.StringType))
                .withColumn(UnionSchema.DESCRIPTION, regexp_replace(col(UnionSchema.DESCRIPTION), "[\\[\\]]", ""))
                .withColumn(UnionSchema.MODIFIED_DATE, col(UnionSchema.MODIFIED_DATE).cast(DataTypes.TimestampType))
                .withColumn(UnionSchema.PRODUCT_ID, col(UnionSchema.PRODUCT_ID).cast(DataTypes.StringType))
                .withColumn(UnionSchema.PARENT_CLASS, col(UnionSchema.PARENT_CLASS).cast(DataTypes.StringType))
                .withColumn(UnionSchema.VERSION, col("gtc_generic_version"))
                .withColumnRenamed("p21_file_url", UnionSchema.FILE_URL)
                .withColumn(UnionSchema.BODY_DIAMETER, regexp_extract(col(UnionSchema.BODY_DIAMETER), "'[0-9.]+'", 0))
                .withColumn(UnionSchema.BODY_DIAMETER, regexp_replace(col(UnionSchema.BODY_DIAMETER), "[']", "")
                        .cast(DataTypes.DoubleType))
                .withColumn(UnionSchema.BODY_LENGTH, regexp_extract(col(UnionSchema.BODY_LENGTH), "'[0-9.]+'", 0))
                .withColumn(UnionSchema.BODY_LENGTH, regexp_replace(col(UnionSchema.BODY_LENGTH), "[']", "")
                        .cast(DataTypes.DoubleType))
                .withColumn(UnionSchema.PRODUCER, lit("sandvik"))
                .withColumn(UnionSchema.ADDITIONAL_INFO, to_json(struct(col("gtc_generic_class_id"), col("gtc_vendor_class_id"), col("p21_file_name"), col("id"), col("parent_id"))))
                .drop("gtc_generic_class_id", "preferred_name", "gtc_vendor_class_id", "p21_file_name", "id", "node_name", "parent_id", "gtc_generic_version");
    }

    public Dataset<Row> cleanBitsBits(Dataset<Row> df) {
        List<String> fieldsToDrop = List.of("guid", "type", "unit", "GRADE", "holder", "`product-id`", "`product-link`", "`post-process`", "`start-values`", "vendor");
        Seq<String> stringSeq = scala.collection.JavaConverters
                .asScalaIteratorConverter(
                        fieldsToDrop.iterator())
                .asScala()
                .toSeq();
        int milliSecToSecDivisor = 1000;

        return df.withColumn(UnionSchema.PRODUCT_ID, col("data.product-id"))
                .withColumn(UnionSchema.MODIFIED_DATE, from_unixtime(col("data.holder.last_modified")
                        .divide(milliSecToSecDivisor))
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
