package org.cncTools.analysers;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.UnionSchema;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ToolSearch {

    private SparkSession sparkSession;

    public ToolSearch(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> searchByKeyWord(Dataset<Row> df, String keyWord) {
        return df.filter(col(UnionSchema.DESCRIPTION).contains(keyWord));
    }

    public Dataset<Row> searchByKeyWords(Dataset<Row> df, String[]keyWords) {
        String keyWordsString = arrayToString(keyWords);
        return df.filter(array_size(
                array_intersect(split(col(UnionSchema.DESCRIPTION), " "), split(lit(keyWordsString), ", ")))
                        .geq(1));
    }

    public Dataset<Row> searchBodyDiameterLeq(Dataset<Row> df, double diameter) {
        return df.withColumn(UnionSchema.DIAMETER_ALIAS, convertToMillimeters(col(UnionSchema.BODY_DIAMETER)))
                .filter(col(UnionSchema.DIAMETER_ALIAS).leq(diameter));
    }

    public Dataset<Row> searchBodyLengthGeq(Dataset<Row> df, double body_length) {
        return df.withColumn(UnionSchema.LENGTH_ALIAS, convertToMillimeters(col(UnionSchema.BODY_LENGTH)))
                .filter(col(UnionSchema.LENGTH_ALIAS).geq(body_length));
    }

    private Column convertToMillimeters(Column value) {
        final String INCHES = "inches";
        final double INCHES_TO_MM = 25.4;
        return when(col(UnionSchema.UNIT_SYSTEM).equalTo(INCHES), value.multiply(INCHES_TO_MM)).otherwise(value);
    }
    private String arrayToString(String[] keyWords) {
        return Arrays.toString(keyWords)
                .replace("[", "")
                .replace("]", "");
    }

}
