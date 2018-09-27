package kdg.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamTutorialFunctions {

    SparkConf conf;
    JavaStreamingContext ssc;

    @SuppressWarnings("unchecked")
    Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
            (word, one, state) -> {
                int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                state.update(sum);
                return output;
            };

    public StreamTutorialFunctions(boolean forceLocal, int batchDuration) {
        conf = new SparkConf().setAppName("SparkStreamingTutorial");
        // De code is er op voorzien dat als je lokaal werkt een pseudocluster gebruikt. Daarvoor moet je de master op local[2] instellen.
        // Als je de gecompileerde code  op een productieve omgeving zoals AWS of clouderaVM wil uitvoeren wordt de master default ingesteld door die omgeving.
        // Daarom geven we geen setMaster mee als we niet lokaal draaien.
        if (forceLocal) {
            conf.setMaster("local[*]");
        }
        ssc = new JavaStreamingContext(conf, new Duration(batchDuration));
        ssc.checkpoint(".");
    }

    public void streamWordCount(String host, String port, String output) {
        JavaDStream<String> lines = ssc.socketTextStream("127.0.0.1", 4444);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        JavaDStream<String> unpairedWords = wordCounts.map(i -> i._2 + " : " + i._1);
        unpairedWords.dstream().saveAsTextFiles(output, "txt");
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void streamWordCountWithState(String host, int port, String op) throws Exception {

        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")
        List<Tuple2<String, Integer>> tuples =
                Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaDStream<String> lines = ssc.socketTextStream(host, port, StorageLevels.MEMORY_AND_DISK_SER_2);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));


        //stateDstream.dstream().print(10);

        stateDstream.dstream().saveAsTextFiles(op,"");
        ssc.start();
        ssc.awaitTermination();

    }


}
