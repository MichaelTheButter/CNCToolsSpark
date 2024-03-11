package org.cncTools.analysers;

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.UnionSchema;

import java.util.Arrays;
import java.util.Date;

import static org.apache.spark.sql.functions.col;

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
        return df;
    }

    public Dataset<Row> searchBodyDiameterLeq(Dataset<Row> df, double diameter) {
        return df;
    }

    public Dataset<Row> searchBodyLengthGeq(Dataset<Row> df, double body_length) {
        return df;
    }

    public Dataset<Row> searchUpdatedLaterThan(Dataset<Row> df, Date date) {
        return df;
    }

    private String arrayToString(String[] keyWords) {
        return Arrays.toString(keyWords)
                .replace("[", "")
                .replace("]", "");
    }

}
