package kdg.bigdata;

import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * Created by overvelj on 11/07/2016.
 */
public class TutorialFunctions {
    private static final Logger LOGGER = Logger.getLogger(sparkRunner.class);
    private SparkConf conf;
    private JavaSparkContext sc;

    public TutorialFunctions() {
        conf = new SparkConf().setAppName("SparkTutorial");
        // Als je op een Hadoop cluster een job draait dan krijgt spark de master mee.
        // Dat is de cluster waarop je de job uitvoert.
        // Als je lokaal uitvoert met een "run configuration" in intelliJ wordt er geen master info meegegeven.
        // In dit geval zetten we de master zelf. We testen of er een spark.master is. Zo nee dan maken we een.
        // local[*] master aan.
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[*]");
            LOGGER.log(Level.WARN, "No spark.master detected: Running in local mode");
        }
        sc = new JavaSparkContext(conf);

    }



    public void WordCount(String input, String output) {

    }

    //Closure principle
    // Spark maakt taken aan om delen van RDDs parallel te verwerken. Elke taak heeft een closure. Dat zijn de variabelen en methoden waar die taak zicht op heeft.
    // In het onderstaande voorbeeld wordt er telkens een kopie (dus geen pointer naar) van de counter doorgegeven. Dat kan ook moeilijk anders want de verschillende nodes zijn afzonderlijke omgevingen.
    // Elke  executor (node) krijgt dus een eigen kopie en als we het op de driver node achteraf willen afdrukken dan zal de waarde van de lokale kopie worden afgedrukt (en daar is nog niets mee gebeurd).
    public void ClosureDemo() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        final int[] counter = {0};
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);
        // Wrong: Don't do this!!
        rdd.foreach(x -> counter[0] += x);
        System.out.println("Counter value: " + counter[0]);
    }

    public void PrintValues() {
        List<Integer> data = Arrays.asList(5, 3, 8, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);
        //Printen doe je niet zo...
        rdd.foreach(x -> System.out.println("Niet zo" + x));
        // Het probleem: Dit werkt wel als je alles lokaal draait. Draai je de job op een cluster met meerdere nodes, dan zal
        // de println worden gestuurd naar de consoleoutput van de node die de deeltaak aan het uitvoeren is.

        // Wel correcct is...
        // Hier zal de driver alle gegevens verzamelen en dan lokaal afdrukken.
        rdd.collect().forEach(x -> System.out.println("Maar zo" + x));
        // Als het te veel data betreft doe je beter het volgende:
        rdd.take(100).forEach(x -> System.out.println("of zo" + x));
    }

    public void ElementsPerPartition() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        // We maken een RDD aan met 5 elementen in twee partities
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        //distData.cache();
        // Als test voeren we een count actie uit (we tellen gewoon hoeveel elementen er in de RDD zijn)
        System.out.println("Aantal data-elementen: " + distData.count());

        //Om te weten te komen hoeveel elemenenten er per partitie zijn voeren we een mapPartitions uit. mapPartitions heeft als inputparameter een ieterator van alle elementen binnen de partitie.
        JavaRDD<Integer> countElementsPerPartition = distData.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) (Iterator<Integer> input) -> {
            Integer count = 0;
            while (input.hasNext()) {
                //We tellen het aantal elementen via de iterator.
                count++;
                input.next();
            }
            // een mapPartitions functie heeft 0 of meerdere outputrecords. Daarom moeten we het ene resultaat dat we per partitie hebben toch wegschrijven naar een ArrayList.
            ArrayList<Integer> ret = new ArrayList<>();
            ret.add(count);
            return ret.iterator();
        });
        // Per partitie wordt het aantal afgedrukt.
        int index = 0;
        countElementsPerPartition.collect().forEach(x -> System.out.println("Aantal elementen in partitie: " + x));


    }


    public void keepRunning() throws InterruptedException {
        //Onderstaande loop zorgt ervoor dat de job blijft draaien, zodat je de SparkUI kan inspecteren.
        // Zet deze in comments als je hem niet nodig hebt.
        sleep(360000);

    }

}



