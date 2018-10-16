package kdg.bigdata;

import org.apache.spark.SparkConf;

public class sparkStreamingRunner {
    static SparkConf conf;

    public static void main(String[] args) {
        conf = new SparkConf().setAppName("SparkStreamingTutorial");
        // De code is er op voorzien dat als je lokaal werkt een pseudocluster gebruikt. Daarvoor moet je de master op local[2] instellen.
        // Als je de gecompileerde code  op een productieve omgeving zoals AWS of clouderaVM wil uitvoeren wordt de master default ingesteld door die omgeving.
        // Daarom geven we geen setMaster mee als we niet lokaal draaien.
        if (args.length==5 && args[4].equals("forceLocal")){
            conf.setMaster("local[*]");
        }
        if (args.length < 2) {
            System.err.println("Usage: host port output_location batchduration (forceLocal) ");
            System.exit(1);
        }

        int batchDuration = Integer.parseInt(args[3]);
        StreamTutorialFunctions st = new StreamTutorialFunctions(conf,  batchDuration);
        st.streamWordCount(args[0], args[1], args[2]);
        //st.streamWordCountWithState(args[0], Integer.parseInt(args[1]),args[2]);
        //st.streamWebSocket(args[0],Integer.parseInt(args[1]),args[2]);
    }
}


