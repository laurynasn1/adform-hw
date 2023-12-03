import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import processors.RecordProcessor;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordProcessorTest {

    private SparkSession spark;
    private StructType schema;
    private RecordProcessor recordProcessor;

    @BeforeAll
    public void beforeAll() {
        this.spark = SparkSession.builder()
                .appName("Example")
                .master("local")
                .getOrCreate();

        this.schema = new StructType(new StructField[]{
                new StructField("device_settings", DataTypes.createStructType(new StructField[]{
                        new StructField("user_agent", DataTypes.StringType, false, Metadata.empty())
                }), false, Metadata.empty()),
                new StructField("date_time", DataTypes.StringType, false, Metadata.empty()),
                new StructField("creation_time_local", DataTypes.StringType, false, Metadata.empty())
        });

        this.recordProcessor = new RecordProcessor();
    }

    @Test
    public void shouldCountForDifferentHours() {
        var rows = Arrays.asList(
                createRow("some user agent", "2023120319", "0"),
                createRow("some user agent", "2023120320", "0")
        );
        var df = spark.createDataFrame(rows, schema);

        var results = recordProcessor.processRecords(df, "some user agent", "");

        assertEquals(2, results.size());
        assertEquals("2023120319", results.get(0).getDateTime());
        assertEquals(1, results.get(0).getRecordCount());
        assertEquals("2023120320", results.get(1).getDateTime());
        assertEquals(1, results.get(1).getRecordCount());
    }

    @Test
    public void shouldFillMissingHoursWithZeros() {
        var rows = Arrays.asList(
                createRow("some user agent", "2023120319", "0"),
                createRow("some user agent", "2023120322", "0")
        );
        var df = spark.createDataFrame(rows, schema);

        var results = recordProcessor.processRecords(df, "some user agent", "");

        assertEquals(4, results.size());
        assertEquals("2023120319", results.get(0).getDateTime());
        assertEquals(1, results.get(0).getRecordCount());
        assertEquals("2023120320", results.get(1).getDateTime());
        assertEquals(0, results.get(1).getRecordCount());
        assertEquals("2023120321", results.get(2).getDateTime());
        assertEquals(0, results.get(2).getRecordCount());
        assertEquals("2023120322", results.get(3).getDateTime());
        assertEquals(1, results.get(3).getRecordCount());
    }

    @Test
    public void shouldCalculateAverageDifferenceBetweenSubsequent() {
        var rows = Arrays.asList(
                createRow("some user agent", "2023120319", "0"),
                createRow("some user agent", "2023120319", "50"),
                createRow("some user agent", "2023120319", "150"),
                createRow("some user agent", "2023120320", "10000000000000")
        );
        var df = spark.createDataFrame(rows, schema);

        var results = recordProcessor.processRecords(df, "some user agent", "");

        assertEquals(2, results.size());
        assertEquals("2023120319", results.get(0).getDateTime());
        assertEquals((100 + 50) / 2, results.get(0).getAverageBetweenRecords());
    }

    private Row createRow(String userAgent, String dateTime, String creationTime) {
        return RowFactory.create(RowFactory.create(userAgent), dateTime, creationTime);
    }
}
