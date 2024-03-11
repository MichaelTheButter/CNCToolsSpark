package org.cncTools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cncTools.analysers.ToolAnalyser;
import org.cncTools.cleaners.Cleaner;
import org.cncTools.loaders.BitsBitsToolsLoader;
import org.cncTools.loaders.SandvikToolsLoader;
import org.cncTools.loaders.UnionLoader;

public class App {
    public static void main( String[] args ) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("AppName")
                .master("local[*]")
                .getOrCreate();

        SandvikToolsLoader loader = new SandvikToolsLoader(sparkSession);
        Dataset<Row> df = loader.loadJoinedSandvikCatalog();

        BitsBitsToolsLoader bbloader = new BitsBitsToolsLoader(sparkSession);
        Dataset<Row> df2 = bbloader.loadJoinedBitsCatalog();

        Cleaner cleaner = new Cleaner();
        Dataset<Row> df3 = cleaner.cleanSandvik(df);
        Dataset<Row> df4 = cleaner.cleanBitsBits(df2);

        UnionLoader unionLoader = new UnionLoader();
        Dataset<Row> df5 = unionLoader.loadUnion(df3, df4);
        df5.show();
        df5.printSchema();

        ToolAnalyser analyser = new ToolAnalyser(sparkSession);
        analyser.calculateDiamRangeByDescriptions(df5).show();
        analyser.calculateByDiameter(df5).show();
    }
}
