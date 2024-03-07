package org.cncTools.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class LoadSandvikTools {

    private SparkSession sparkSession;

    public LoadSandvikTools(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> loadJoinedSandvikCatalog() {
        Dataset<Row> packageDF = loadSandvikPackage();
        Dataset<Row> gtcClassDF = getOnlyEngDescriptions(
                loadSandvikGtcClasss()
        );
        Dataset<Row> productDataDF = loadProductDataFiles();
        Dataset<Row> images = loadSandvikImages();

        return packageDF.join(gtcClassDF,
                        packageDF.col("gtc_generic_class_id").equalTo(gtcClassDF.col("id")),
                        "left")
                .join(productDataDF,
                        productDataDF.col("name").contains(packageDF.col("p21_file_name")),
                        "left")
                .join(images,
                        productDataDF.col("picture").contains(images.col("image_name"))
                        , "left")
                .drop(col("name"), col("picture"));
    }

    private Dataset<Row> loadSandvikPackage() {
        return sparkSession.read()
                .format("xml")
                .option("rowTag", "item")
                .load("src/main/resources/sandvikTools/package_assortment.xml")
                .na().drop();
    }

    private Dataset<Row> loadSandvikGtcClasss() {
        return sparkSession.read()
                .format("xml")
                .option("rowTag", "gtc_class")
                .load("src/main/resources/sandvikTools/gtc_class_hierarchy_vendor.xml");
    }

    private Dataset<Row> getOnlyEngDescriptions(Dataset<Row> df) {
        return df.withColumn("node_name",
                        expr("filter(node_name.string_with_language, x -> x.language = \"eng\")")
                                .getField("string_value"))
                .withColumn("preferred_name",
                        expr("filter(preferred_name.string_with_language, x -> x.language = \"eng\")")
                                .getField("string_value"));
    }

    private Dataset<Row> loadSandvikImages() {
        return sparkSession.read()
                .format("image")
                .option("dropInvalid", true)
                .load("src/main/resources/sandvikTools/product_pictures")
                .withColumn("image_name", regexp_replace(col("image.origin"), "file[$,:/;#=()'~a-zA-Z0-9._%+-]+pictures/", ""));
    }

    private Dataset <Row> loadProductDataFiles() {
        return sparkSession.read().
                option("wholetext", "true")
                .text("src/main/resources/sandvikTools/product_data_files")
                .withColumn("name", regexp_extract(col("value"), "/\\*\\sname\\s\\*/\\s'[0-9]+.p21'", 0))
                .withColumn("picture", regexp_extract(col("value"), "product_picture[(\\s+|[$,:/;#=()'~a-zA-Z0-9._%+-]+)]+jpg", 0))
                .withColumn("body_diameter", regexp_extract(col("value"), "body\\sdiameter[(\\s|[$,:/#='~a-zA-Z0-9._%+-]+)]+'\\);", 0))
                .withColumn("body_length", regexp_extract(col("value"), "body\\slength[(\\s|[$,:/#='~a-zA-Z0-9._%+-]+)]+'\\);", 0))
                .drop(col("value"));
    }
}
