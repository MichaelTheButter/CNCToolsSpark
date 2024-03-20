package org.cncTools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.analysers.ToolAnalyser;
import org.cncTools.analysers.ToolSearch;
import org.cncTools.cleaners.Cleaner;
import org.cncTools.loaders.BitsBitsToolsLoader;
import org.cncTools.loaders.SandvikToolsLoader;
import org.cncTools.loaders.UnionLoader;

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
//        cncToolsDF.show();
        cncToolsDF.printSchema();

        ToolAnalyser analyser = new ToolAnalyser(sparkSession);
//        analyser.calculateDiamRangeByDescriptions(cncToolsDF).show();
//        analyser.calculateByDiameter(sandvikDF).show();

        ToolSearch searcher = new ToolSearch(sparkSession);
        String[] keyWords = {"drill", "flat"};
//        searcher.searchByKeyWords(cncToolsDF, keyWords).show();
//        searcher.searchBodyDiameterLeq(cncToolsDF, 50).show(60);
//        searcher.searchBodyLengthGeq(bitsDF, 40).show();
        //cncToolsDF.drop("image").write().format("csv").save("C:\\winutil\\sample.csv");
    }
}
