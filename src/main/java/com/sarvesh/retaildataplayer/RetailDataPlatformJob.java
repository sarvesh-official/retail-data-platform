package com.sarvesh.retaildataplayer;

import com.sarvesh.retaildataplayer.functions.CsvLineParser;
import com.sarvesh.retaildataplayer.functions.SalesOrderEnricher;
import com.sarvesh.retaildataplayer.functions.SalesOrderValidator;
import com.sarvesh.retaildataplayer.sink.PostgresSinkFactory;
import com.sarvesh.retaildataplayer.source.MinioCsvSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Main pipeline: reads CSV from MinIO, parses, validates, enriches, writes to PostgreSQL
public class RetailDataPlatformJob {

    private static final Logger LOG = LoggerFactory.getLogger(RetailDataPlatformJob.class);

    // Config
    private static final String MINIO_ENDPOINT    = "http://minio:9000";
    private static final String MINIO_BUCKET      = "retail-data";
    private static final String MINIO_OBJECT      = "sales_orders_100.csv";
    private static final String MINIO_ACCESS_KEY  = "minioadmin";
    private static final String MINIO_SECRET_KEY  = "minioadmin";

    private static final String POSTGRES_URL      = "jdbc:postgresql://postgres:5432/retaildb";
    private static final String POSTGRES_USER     = "retailuser";
    private static final String POSTGRES_PASSWORD = "retailpassword";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        LOG.info("Starting Retail Data Platform job: bucket={}, object={}", MINIO_BUCKET, MINIO_OBJECT);

        // Read CSV lines from MinIO
        var rawLines = env.addSource(new MinioCsvSource(
                        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_OBJECT,
                        MINIO_ACCESS_KEY, MINIO_SECRET_KEY))
                .name("minio-csv-source");

        // Parse lines into SalesOrder objects
        var parsed = rawLines.map(new CsvLineParser()).name("csv-parser");

        // Filter out invalid records
        var validated = parsed.filter(new SalesOrderValidator()).name("validator");

        // Add computed fields: total amount and order tier
        var enriched = validated.map(new SalesOrderEnricher()).name("enricher");

        // Write to PostgreSQL (UPSERT to handle reruns)
        PostgresSinkFactory.addSink(enriched, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);

        // Run the pipeline
        env.execute("Retail Data Platform");
    }
}
