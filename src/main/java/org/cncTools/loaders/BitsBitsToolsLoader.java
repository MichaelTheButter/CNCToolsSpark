package org.cncTools.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.UnionSchema;

import static org.apache.spark.sql.functions.*;

public class BitsBitsToolsLoader {

    private SparkSession sparkSession;
    public BitsBitsToolsLoader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> loadJoinedBitsCatalog() {
        Dataset<Row> dataDF = loadBitsBits();
        Dataset<Row> imagesDF = loadBitsImages();
        return dataDF.join(imagesDF, imagesDF.col(UnionSchema.IMAGE_NAME).contains(dataDF.col("data.type")), "left");
    }

    private Dataset<Row> loadBitsBits() {
        return sparkSession.read()
                .option("multiline","true")
                .json("src/main/resources/BitsBits Fusion Tool Library.V1.3.json")
                .withColumn("data", explode(col("data")));
    }

    private Dataset<Row> loadBitsImages() {
        final String REGEX_IMAGE_ORIGIN_TO_REMOVE = "file[$,:/;#=()'~a-zA-Z0-9._%+-]+Images/";
        return sparkSession.read()
                .format("image")
                .option("dropInvalid", true)
                .load("src/main/resources/bitsbitsImages")
                .withColumn(UnionSchema.IMAGE_NAME, regexp_replace(col("image.origin"), REGEX_IMAGE_ORIGIN_TO_REMOVE, ""))
                .withColumn(UnionSchema.IMAGE_NAME, regexp_replace(col(UnionSchema.IMAGE_NAME), "[_]", " "));
    }


}
