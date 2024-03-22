package org.cncTools.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.SandvikSchema;
import org.cncTools.UnionSchema;

import static org.apache.spark.sql.functions.*;

public class SandvikToolsLoader {

    private SparkSession sparkSession;

    public SandvikToolsLoader(SparkSession sparkSession) {
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
                        packageDF.col(SandvikSchema.GTC_GENERIC_CLASS_ID).equalTo(gtcClassDF.col(SandvikSchema.ID)),
                        "left")
                .join(productDataDF,
                        productDataDF.col(SandvikSchema.TEMPORARY_NAME_COLUMN).contains(packageDF.col(SandvikSchema.P21_FILE_NAME)),
                        "left")
                .join(images,
                        productDataDF.col(SandvikSchema.TEMPORARY_PICTURE_COLUMN).contains(images.col(UnionSchema.IMAGE_NAME))
                        , "left")
                .drop(col(SandvikSchema.TEMPORARY_NAME_COLUMN), col(SandvikSchema.TEMPORARY_PICTURE_COLUMN));
    }

    private Dataset<Row> loadSandvikPackage() {
        return sparkSession.read()
                .format("xml")
                .option("rowTag", SandvikSchema.ITEM)
                .load("src/main/resources/sandvikTools/package_assortment.xml")
                .na().drop();
    }

    private Dataset<Row> loadSandvikGtcClasss() {
        return sparkSession.read()
                .format("xml")
                .option("rowTag", SandvikSchema.GTC_CLASS)
                .load("src/main/resources/sandvikTools/gtc_class_hierarchy_vendor.xml");
    }

    private Dataset<Row> getOnlyEngDescriptions(Dataset<Row> df) {
        final String FILTER_ENG_NODE_NAME = "filter(node_name.string_with_language, x -> x.language = \"eng\")";
        final String FILTER_ENG_PREFERRED_NAME = "filter(preferred_name.string_with_language, x -> x.language = \"eng\")";
        final String STRING_VALUE = "string_value";
        return df.withColumn(UnionSchema.PARENT_CLASS,
                        expr(FILTER_ENG_NODE_NAME)
                                .getField(STRING_VALUE))
                .withColumn(UnionSchema.DESCRIPTION,
                        expr(FILTER_ENG_PREFERRED_NAME)
                                .getField(STRING_VALUE));
    }

    private Dataset<Row> loadSandvikImages() {
        final String REGEX_IMAGE_ORIGIN_TO_REMOVE = "file[$,:/;#=()'~a-zA-Z0-9._%+-]+pictures/";

        return sparkSession.read()
                .format("image")
                .option("dropInvalid", true)
                .load("src/main/resources/sandvikTools/product_pictures")
                .withColumn(UnionSchema.IMAGE_NAME, regexp_replace(col(SandvikSchema.IMAGE_ORIGIN), REGEX_IMAGE_ORIGIN_TO_REMOVE, ""));
    }

    private Dataset <Row> loadProductDataFiles() {
        final String VALUE_COLUMN  = "value";
        final String REGEX_EXTRACT_NAME = "/\\*\\sname\\s\\*/\\s'[0-9]+.p21'";
        final String REGEX_EXTRACT_PICTURE = "product_picture[(\\s+|[$,:/;#=()'~a-zA-Z0-9._%+-]+)]+jpg";
        final String REGEX_EXTRACT_BODY_DIAMETER = "body\\sdiameter[(\\s|[$,:/#='~a-zA-Z0-9._%+-]+)]+'\\);";
        final String REGEX_EXTRACT_BODY_LENGTH = "body\\slength[(\\s|[$,:/#='~a-zA-Z0-9._%+-]+)]+'\\);";

        return sparkSession.read()
                .option("wholetext", "true")
                .text("src/main/resources/sandvikTools/product_data_files")
                .withColumn(SandvikSchema.TEMPORARY_NAME_COLUMN, regexp_extract(col(VALUE_COLUMN), REGEX_EXTRACT_NAME, 0))
                .withColumn(SandvikSchema.TEMPORARY_PICTURE_COLUMN, regexp_extract(col(VALUE_COLUMN), REGEX_EXTRACT_PICTURE, 0))
                .withColumn(UnionSchema.BODY_DIAMETER, regexp_extract(col(VALUE_COLUMN), REGEX_EXTRACT_BODY_DIAMETER, 0))
                .withColumn(UnionSchema.BODY_LENGTH, regexp_extract(col(VALUE_COLUMN), REGEX_EXTRACT_BODY_LENGTH, 0))
                .drop(col(VALUE_COLUMN));
    }

}
