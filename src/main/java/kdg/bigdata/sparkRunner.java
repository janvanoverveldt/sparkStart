package kdg.bigdata; /**
 * Created by overvelj on 5/07/2016.
 */

public class sparkRunner {


    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: kdg.bigdata.sparkRunner.java input_location {output_location}");
            System.exit(1);
        } else {

        }

        TutorialFunctions tf = new TutorialFunctions();

        tf.WordCount(args[0],args[1]);
        //tf.ClosureDemo();
        //tf.PrintValues();
        //tf.ElementsPerPartition();
        tf.keepRunning();
        }
    }

