package kdg.bigdata;

public class sparkStreamingRunner {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: host port output_location batchduration (forceLocal) ");
            System.exit(1);
        }

        int batchDuration = Integer.parseInt(args[3]);

        StreamTutorialFunctions st = null;
        if (args.length==5 && args[4].equals("forceLocal")){
            st = new StreamTutorialFunctions(true,batchDuration);
        } else {
            st = new StreamTutorialFunctions(false,batchDuration);
        }

        st.streamWordCount(args[0], args[1], args[2]);
        //st.streamWordCountWithState(args[0], Integer.parseInt(args[1]),args[2]);
        //st.streamWebSocket(args[0],Integer.parseInt(args[1]),args[2]);
    }
}


