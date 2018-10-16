package kdg.bigdata;

import kdg.bigdata.domain.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.plans.logical.View;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructuredStreaming {


    public static void main(String[] args) throws StreamingQueryException {
        SparkConf conf = new SparkConf().setAppName("SparkTutorial");
        if (args.length==4 && args[3].equals("forceLocal")) {
            conf.setMaster("local[*]");
        }

        // Default 200 waardoor heel veel tasks aangemaakt worden
        //TODO shuffle configureerbaar maken.
        conf.set("spark.sql.shuffle.partitions","2");
        conf.set("spark.sql.streaming.checkpointLocation",args[2]);
        SparkContext sc = new SparkContext(conf);

        //Startpunt bij de Dataset en Dataframe API
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sc)
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();


        // Beschrijft de structuur van het inputbestand
        StructType personSchema = new StructType(new StructField[] {
                new StructField("name",
                        DataTypes.StringType, false,
                        Metadata.empty()) ,
                new StructField("salary",
                        DataTypes.IntegerType, false,
                        Metadata.empty())
        });



        //Lees een stream readStream() in plaats van read() bij batch
        Dataset<Row> lines = spark.readStream().schema(personSchema).json(args[0]);

        // Mogelijkheid 1: gebruik ingebouwde groupBy en sum functionaliteit.
        Dataset<Row> totals = lines.groupBy("name").sum("salary");

        //Mogelijkheid 2: gebruik sql
        //lines.createOrReplaceTempView("people");
        //Dataset<Row> totals2 = spark.sql("select name, sum(salary) from people group by name");

        // DataStreamWriter schrijft de data op de stroom naar een externe opslag weg.
        // Ter illustriatie is dit naar de console
        // outputMode(complete) wil zeggen dat de volledige output (van alle voorgaande gegevens) wordt weggeschreven. Zie documentatie voor andere mogelijkheden.
            StreamingQuery query = totals.writeStream().outputMode("complete")
                .format("console")
                .start(args[1]);



        query.awaitTermination();
    }


    public StructuredStreaming() throws StreamingQueryException {

    }
}
