import data.DataReader;
import data.ResultWriter;
import processors.RecordProcessor;

import java.util.Arrays;

public class Application {

    public static void main(String[] args) {
        System.out.println(Arrays.toString(args));
        if (args.length < 2) {
            System.out.println("Usage: Application <user agent> <data folder path>");
            System.exit(0);
        }

        String userAgent = args[0];
        String dataPath = args[1];

        try (DataReader reader = new DataReader(dataPath, userAgent);
             ResultWriter writer = new ResultWriter()) {
            RecordProcessor recordProcessor = new RecordProcessor();

            var clicks = reader.readClicks();
            if (clicks != null) {
                var clickResults = recordProcessor.processRecords(clicks, userAgent, "clicks");
                writer.writeResults(clickResults);
            }

            var impressions = reader.readImpressions();
            if (impressions != null) {
                var impressionResults = recordProcessor.processRecords(impressions, userAgent, "impressions");
                writer.writeResults(impressionResults);
            }
        }
    }


}