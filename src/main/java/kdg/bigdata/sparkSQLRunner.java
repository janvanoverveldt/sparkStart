package kdg.bigdata;


import kdg.bigdata.domain.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class sparkSQLRunner {
    public static void main(String[] args) {
       SparkConf conf = new SparkConf().setAppName("SparkTutorial").setMaster("local[*]");
       SparkContext sc = new SparkContext(conf);
       // Sparksession is startpunt voor de Dataset Dataframe API
       SparkSession spark = SparkSession.builder().sparkContext(sc).getOrCreate();
       //Data inlezen kan uit een hele reeks datasources http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
       Dataset<Row> df = spark.read().json("input/people.json");
       //Laat de inhoud van de tabel zien
       df.show();
       //Laat het schema zien (Spark heeft deze automatisch afegeleid uit het json bestand)
       df.printSchema();
       //Maak een view op de dataset zodat je er queries op kan uitvoeren.
       df.createOrReplaceTempView("people");
       // Voer query uit en sla het resultaat op
       spark.sql("select * from people where salary > 3500").show();


        Dataset<Row> usersDF = spark.read().load("input/users.parquet");
        usersDF.printSchema();
        usersDF.show();

        // Het is ook gewoon mogelijk om een server op te starten waar je sql statements naar kan sturen: http://spark.apache.org/docs/latest/sql-distributed-sql-engine.html
    }
}
