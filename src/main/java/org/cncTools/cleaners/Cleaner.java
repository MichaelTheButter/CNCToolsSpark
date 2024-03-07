package org.cncTools.cleaners;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
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
                .withColumn("description", col("preferred_name").cast(DataTypes.StringType))
                .drop(col("preferred_name"))
                .withColumn("description", regexp_replace(col("description"), "[\\[\\]]", ""))
                .withColumn("modified_date", col("modified_date").cast(DataTypes.TimestampType))
                .withColumn("product_id", col("product_id").cast(DataTypes.StringType))
                .withColumn("parent_class", col("node_name").cast(DataTypes.StringType))
                .withColumnRenamed("p21_file_url", "file_url")
                .withColumn("additional_info", to_json(struct(col("gtc_generic_class_id"), col("gtc_generic_version"), col("gtc_vendor_class_id"), col("p21_file_name"), col("id"), col("parent_id"))))
                .drop("gtc_generic_class_id", "preferred_name", "gtc_generic_version", "gtc_vendor_class_id", "p21_file_name", "id", "node_name", "parent_id");
    }

    public Dataset<Row> cleanBitsBits(Dataset<Row> df) {
        List<String> list = List.of("guid", "type", "unit", "GRADE", "holder", "`product-id`", "`product-link`", "`post-process`", "`start-values`", "vendor");
        Seq<String> stringSeq = scala.collection.JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        int milliSecToSecDivisor = 1000;
        return df.withColumn("product_id", col("data.product-id"))
                .withColumn("modified_date", from_unixtime(col("data.holder.last_modified")
                        .divide(milliSecToSecDivisor))
                        .cast(DataTypes.TimestampType))
                .withColumn("file_url", col("data.product-link"))
                .withColumn("unit_system", col("data.unit"))
                .withColumn("description", col("data.type"))
                .withColumn("parent_class", col("data.GRADE"))
                .withColumn("body_diameter", col("data.geometry.DC"))
                .withColumn("tool_length", col("data.geometry.LCF"))
                .withColumn("data", col("data").dropFields(stringSeq))
                .withColumn("additional_info", to_json(col("data")))
                .drop(col("data"));
    }
}
