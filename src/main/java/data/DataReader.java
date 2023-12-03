package data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataReader implements AutoCloseable {

    private Path dataPath;
    private String userAgent;
    private SparkSession spark;

    private static final Path METADATA_FILE = Paths.get("processed.metadata");
    private Multimap<String, String> processedFiles = HashMultimap.create();

    public DataReader(String dataPath, String userAgent) {
        this.dataPath = Paths.get(dataPath);
        this.userAgent = userAgent;
        this.spark = SparkSession.builder()
                .appName("Example")
                .master("local")
                .getOrCreate();

        readMetadata();
    }

    private void readMetadata() {
        if (Files.exists(METADATA_FILE)) {
            try {
                for (var line : Files.readAllLines(METADATA_FILE)) {
                    var strings = line.split("\\|");
                    processedFiles.put(strings[0], strings[1]);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public Dataset<Row> readClicks() {
        return readRecords("clicks_processed_dk_");
    }

    public Dataset<Row> readImpressions() {
        return readRecords("impressions_processed_dk_");
    }

    public Dataset<Row> readRecords(String prefix) {
        try (var paths = Files.walk(dataPath, 1)) {
            var records = paths
                    .filter(path -> path.getFileName().toString().startsWith(prefix))
                    .filter(path -> !processedFiles.containsEntry(userAgent, path.getFileName().toString()))
                    .peek(path -> processedFiles.put(userAgent, path.getFileName().toString()))
                    .map(path -> spark.read().parquet(path.toString()).withColumn("date_time", functions.lit(getDatetimeFromFileName(path.getFileName(), prefix))))
                    .reduce(Dataset::union);

            if (records.isEmpty()) {
                return null;
            }
            var dataset = records.get();
            return dataset.where(dataset.col("device_settings.user_agent").equalTo(userAgent));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getDatetimeFromFileName(Path file, String prefix) {
        var s = file.toString().substring(prefix.length());
        return s.substring(0, s.indexOf("_")).substring(0, 10);
    }

    @Override
    public void close() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(METADATA_FILE.toFile()))) {
            for (var entry : processedFiles.entries()) {
                writer.write(entry.getKey() + "|" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
