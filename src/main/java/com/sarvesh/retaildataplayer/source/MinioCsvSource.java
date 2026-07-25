package com.sarvesh.retaildataplayer.source;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


public class MinioCsvSource implements SourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MinioCsvSource.class);

    private final String endpoint;
    private final String bucket;
    private final String object;
    private final String accessKey;
    private final String secretKey;

    private volatile boolean isRunning = true;

    public MinioCsvSource(String endpoint, String bucket, String object, String accessKey, String secretKey) {
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.object = object;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        LOG.info("Reading from MinIO: bucket={}, object={}", bucket, object);

        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        try (InputStream is = client.getObject(GetObjectArgs.builder()
                .bucket(bucket)
                .object(object)
                .build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            String line;
            boolean headerSkipped = false;

            while (isRunning && (line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }
                ctx.collect(line);
            }
        }

        LOG.info("Finished reading from MinIO");
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
