package org.cncTools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.analysers.ToolAnalyser;
import org.cncTools.analysers.ToolSearch;
import org.cncTools.cleaners.Cleaner;
import org.cncTools.collectors.Collector;
import org.cncTools.loaders.BitsBitsToolsLoader;
import org.cncTools.loaders.SandvikToolsLoader;
import org.cncTools.loaders.UnionLoader;

import java.util.List;

public class App {
    public static void main( String[] args ) {
        System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        SparkSession sparkSession = SparkSession.builder()
                .appName("AppName")
                .master("local[*]")
                .getOrCreate();

        SandvikToolsLoader loader = new SandvikToolsLoader(sparkSession);
        Dataset<Row> sandvikRaw = loader.loadJoinedSandvikCatalog();

        BitsBitsToolsLoader bitsLoader = new BitsBitsToolsLoader(sparkSession);
        Dataset<Row> bitsRaw = bitsLoader.loadJoinedBitsCatalog();

        Cleaner cleaner = new Cleaner();
        Dataset<Row> sandvikDF = cleaner.cleanSandvik(sandvikRaw);
        Dataset<Row> bitsDF = cleaner.cleanBitsBits(bitsRaw);

        UnionLoader unionLoader = new UnionLoader();
        Dataset<Row> cncToolsDF = unionLoader.loadUnion(sandvikDF, bitsDF);
        Collector.saveAsParquet(cncToolsDF, "cncTools.parquet");


        ToolAnalyser analyser = new ToolAnalyser(sparkSession);
        analyser.calculateDiamRangeByDescriptions(cncToolsDF).show();
        Collector.saveAsCsv(
                analyser.calculateDiamRangeByDescriptions(cncToolsDF),
        "calcDiamRangeByDesc.csv"
        );
        Collector.saveAsCsv(
                analyser.calculateByDiameter(sandvikDF),
        "calcByDiameter.csv"
        );

        ToolSearch searcher = new ToolSearch(sparkSession);
        String[] keyWords = {"drill", "flat"};
        Collector.saveAsCsv(
                searcher.searchByKeyWords(cncToolsDF, keyWords),
        "searchByKeyWords.csv"
        );

        Collector.saveAsCsv(
                searcher.searchBodyDiameterLeq(cncToolsDF, 50),
                "searchByDiamGeq50.csv"
        );

    }
}
