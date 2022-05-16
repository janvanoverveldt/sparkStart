package kdg.bigdata;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class sparkStructuredStreaming {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
       SparkConf conf = new SparkConf().setAppName("SparkTutorial").setMaster("local[*]").set("spark.sql.shuffle.partitions", "4");
       SparkContext sc = new SparkContext(conf);
       // Sparksession is startpunt voor de Dataset Dataframe API
       SparkSession spark = SparkSession.builder().sparkContext(sc).getOrCreate();
       //Data inlezen kan uit een hele reeks datasources http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        // Het is ook gewoon mogelijk om een server op te starten waar je sql statements naar kan sturen: http://spark.apache.org/docs/latest/sql-distributed-sql-engine.html
        lines.printSchema();
        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

// Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
