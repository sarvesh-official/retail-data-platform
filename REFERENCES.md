# References

Documentation, articles, and resources used while building this pipeline.

## Apache Flink

### Official Documentation
- **Project Configuration (Maven setup, dependency scopes, fat jar packaging)**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/project-configuration/
  Used for: `pom.xml` structure, `provided` vs `compile` scope for Flink deps, `maven-shade-plugin` configuration, signature file filtering.

- **DataStream API — Sources**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/sources/
  Used for: understanding `SourceFunction` (legacy) vs FLIP-27 `Source` (modern). Chose `SourceFunction` for simplicity in the custom MinIO source.

- **DataStream API — Operators (map, filter, sink)**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/
  Used for: `MapFunction`, `FilterFunction`, `addSink()` patterns in the pipeline.

- **JDBC Connector**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/jdbc/
  Used for: `JdbcSink.sink()`, `JdbcExecutionOptions` (batching, retries), `JdbcConnectionOptions`.

- **Execution Configuration (parallelism, checkpointing)**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution/execution_config/
  Used for: `env.setParallelism()`, understanding operator chaining.

- **Flink Docker images (Java 17 tag)**
  https://hub.docker.com/_/flink
  Used for: selecting `flink:1.20.1-java17` image to match our Java 17 compilation target.

### Articles / Blog Posts
- **Apache Flink: Java Version Support Matrix**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/installing_flink/#java-version
  Used for: verifying Java 17 is supported in Flink 1.20 (it is, officially).

## MinIO

### Official Documentation
- **MinIO Java SDK — GetObject**
  https://min.io/docs/minio/linux/developers/java/minio-java.html#read-and-write-objects
  Used for: `MinioClient.builder()`, `GetObjectArgs`, reading an object as an `InputStream`.

- **MinIO Client (mc) — bucket operations**
  https://min.io/docs/minio/linux/reference/minio-mc.html
  Used for: the `create-bucket` sidecar in `docker-compose.yml` (`mc mb`, `mc cp`).

## PostgreSQL

### Official Documentation
- **INSERT ... ON CONFLICT (UPSERT)**
  https://www.postgresql.org/docs/current/sql-insert.html
  Used for: the `ON CONFLICT (order_id, sub_category) DO UPDATE SET ...` clause in the sink SQL. `EXCLUDED` refers to the proposed-but-conflicting row.

- **docker-entrypoint-initdb.d (init scripts)**
  https://hub.docker.com/_/postgres
  Used for: mounting `./sql/init.sql` so the `sales_orders` table is created automatically on first start.

## Maven

### Official Documentation
- **Maven Shade Plugin**
  https://maven.apache.org/plugins/maven-shade-plugin/
  Used for: building the fat jar, `ManifestResourceTransformer` (sets Main-Class), `ServicesResourceTransformer` (merges META-INF/services for JDBC drivers).

- **Maven Compiler Plugin — --release flag**
  https://maven.apache.org/plugins/maven-compiler-plugin/compile-mojo.html#release
  Used for: `<release>17</release>` which sets source + target + API surface in one property (safer than separate source/target).

## Docker Compose

### Official Documentation
- **Compose file reference — healthcheck, depends_on, volumes**
  https://docs.docker.com/compose/compose-file/
  Used for: `condition: service_healthy` to order the bucket-creation sidecar after MinIO is ready.

## Java

### Official Documentation
- **Java Records (JEP 395)**
  https://openjdk.org/jeps/395
  Used for: `SalesOrder` — a record gives immutability, constructors, accessors, equals/hashCode/toString for free.

- **Java Volatile keyword (Java Memory Model)**
  https://docs.oracle.com/javase/specs/jls/se17/html/jls-17.html#jls-17.7
  Used for: `volatile boolean isRunning` in `MinioCsvSource` — `cancel()` may be called from a different thread than `run()`.

## General Data Engineering Concepts
- **Idempotent writes / exactly-once semantics**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/stateful-stream-processing/
  Applied in: the UPSERT sink design — re-running the pipeline produces the same result as running it once.

- **Dead-letter queue pattern**
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/side_output/
  Note: not implemented in this version (invalid records are logged, not routed to a DLQ). Listed as a next step.
