# Flink CDC Output to Google Cloud Storage

## Overview
- The existing proof of concept project uses Debezium to ingest MongoDB wager change streams, buffers in Kafka, and distributes records through Apache Flink SQL jobs.
- By adding Google Cloud Storage (GCS) filesystem connector and sink table, the same SQL job can persist curated wager analytics to buckets instead of (or in addition to) the local `flink/output` directory.
- Flink SQL already supports rich ETL transformations for wager data (filtering, platform revenue calculation, aggregation, type conversion). Extending the current job only requires new SQL statements—no additional application code.

## Environment Requirements
- Google Cloud project with Cloud Storage API enabled.
- Target bucket (e.g., `gs://cdc-wagers-analytics`). Create folders in the bucket as needed (e.g., `cdc/wagers/`).
- Dedicated service account with **Storage Object Admin** role (read/write). Download its JSON key. Treat it as secret.
- Local tools: Docker Desktop, Docker Compose v2, and the same dependencies listed in `README.md`.

## Step 1 — Prepare secrets and ignore rules
1. Save the service account JSON key as `flink/secrets/gcs-writer.json`. **Do not** commit it to source control.
2. If not already included in `.gitignore`, add the following pattern:
   ```
   flink/secrets/
   ```

## Step 2 — Install GCS connector in Flink image
1. Edit `flink/Dockerfile` and append download of shaded GCS Hadoop connector:
   ```Dockerfile
   ARG GCS_CONNECTOR_VERSION=2.2.19
   RUN curl -fsSL "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-${GCS_CONNECTOR_VERSION}/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}-shaded.jar" \
         -o /opt/flink/lib/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}-shaded.jar
   ```
   - The shaded artifact packages required Hadoop classes for Java 11 and Flink 1.17.
   - Rebuild the image after changing the Dockerfile: `docker compose build flink-jobmanager --no-cache`.

## Step 3 — Provide Hadoop-style configuration to Flink
1. Create `flink/conf/core-site.xml` with properties the connector expects:
   ```xml
   <?xml version="1.0"?>
   <configuration>
     <property>
       <name>fs.gs.impl</name>
       <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
     </property>
     <property>
       <name>fs.AbstractFileSystem.gs.impl</name>
       <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
     </property>
     <property>
       <name>google.cloud.auth.service.account.enable</name>
       <value>true</value>
     </property>
     <property>
       <name>google.cloud.auth.service.account.json.keyfile</name>
       <value>/opt/flink/secrets/gcs-writer.json</value>
     </property>
     <property>
       <name>fs.gs.project.id</name>
       <value>YOUR_PROJECT_ID</value>
     </property>
   </configuration>
   ```
2. Mount configuration and secrets in `docker-compose.yml`:
   ```yaml
   flink-jobmanager:
     volumes:
       - ./flink/sql:/opt/flink/sql:ro
       - ./flink/output:/opt/flink/output
       - ./flink/conf:/opt/flink/conf:ro
       - ./flink/secrets:/opt/flink/secrets:ro
   flink-taskmanager:
     volumes:
       - ./flink/output:/opt/flink/output
       - ./flink/conf:/opt/flink/conf:ro
       - ./flink/secrets:/opt/flink/secrets:ro
   ```
   The `core-site.xml` file is automatically discovered by Flink at startup.

## Step 4 — Describe GCS sink and ETL transformations in SQL
1. Extend `flink/sql/change_stream_job.sql` with new sink table and insert statement:
   ```sql
   CREATE TABLE wagers_gcs (
     wagerId STRING,
     gameId STRING,
     gameSessionId STRING,
     userId STRING,
     version BIGINT,
     totalBet DECIMAL(18,2),
     totalWin DECIMAL(18,2),
     totalLose DECIMAL(18,2),
     beginTime TIMESTAMP_LTZ(3),
     endTime TIMESTAMP_LTZ(3),
     lastUpdateTime TIMESTAMP_LTZ(3),
     isCompleted BOOLEAN,
     eventCount INT,
     netAmount DECIMAL(18,2),
     processedAt TIMESTAMP_LTZ(3)
   ) WITH (
     'connector' = 'filesystem',
     'path' = 'gs://cdc-wagers-analytics/cdc/wagers',
     'format' = 'json',
     'sink.rolling-policy.rollover-interval' = '5 min'
   );

   INSERT INTO wagers_gcs
   SELECT
     JSON_VALUE(message, '$.payload._id') AS wagerId,
     JSON_VALUE(message, '$.payload.gameId') AS gameId,
     JSON_VALUE(message, '$.payload.gameSessionId') AS gameSessionId,
     JSON_VALUE(message, '$.payload.userId') AS userId,
     CAST(JSON_VALUE(message, '$.payload.version') AS BIGINT) AS version,
     CAST(JSON_VALUE(message, '$.payload.totalBet') AS DECIMAL(18,2)) AS totalBet,
     CAST(JSON_VALUE(message, '$.payload.totalWin') AS DECIMAL(18,2)) AS totalWin,
     CAST(JSON_VALUE(message, '$.payload.totalLose') AS DECIMAL(18,2)) AS totalLose,
     CASE
       WHEN JSON_VALUE(message, '$.payload.beginTime') IS NOT NULL
       THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.payload.beginTime') AS BIGINT), 3)
       ELSE NULL
     END AS beginTime,
     CASE
       WHEN JSON_VALUE(message, '$.payload.endTime') IS NOT NULL
       THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.payload.endTime') AS BIGINT), 3)
       ELSE NULL
     END AS endTime,
     CASE
       WHEN JSON_VALUE(message, '$.payload.lastUpdateTime') IS NOT NULL
       THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.payload.lastUpdateTime') AS BIGINT), 3)
       ELSE NULL
     END AS lastUpdateTime,
     CASE
       WHEN JSON_VALUE(message, '$.payload.endTime') IS NOT NULL THEN true
       ELSE false
     END AS isCompleted,
     CAST(JSON_VALUE(message, '$.payload.version') AS INT) AS eventCount,
     CAST(JSON_VALUE(message, '$.payload.totalBet') AS DECIMAL(18,2)) -
     CAST(JSON_VALUE(message, '$.payload.totalWin') AS DECIMAL(18,2)) AS netAmount,
     CURRENT_TIMESTAMP AS processedAt
   FROM mongo_wagers_raw
   WHERE CAST(JSON_VALUE(message, '$.payload.version') AS INT) >= 1;
   ```
   - The projection demonstrates realistic wager analytics ETL workflows:
     - Extract core wager identifiers and financial data.
     - Calculate platform revenue (`netAmount = totalBet - totalWin`).
     - Convert timestamps from Debezium format.
     - Determine completion status and event count.
     - Filter for valid wagers with at least one event.
     - Add processing metadata for audit trails.
   - Adjust bucket paths and business rules based on actual requirements.

## Step 5 — Run and verify
1. Rebuild and restart the stack:
   ```sh
   docker compose down -v
   docker compose build
   docker compose up -d
   ```
2. Re-register MongoDB connector and re-submit Flink SQL job:
   ```sh
   ./scripts/register-mongo-connector.sh
   ./scripts/submit-flink-sql.sh
   ```
3. Generate test wagers (optional): `dotnet run --project src/MongoChangeStreamSeeder GCSDEMO`.
4. Confirm files appear in bucket (`gsutil ls gs://cdc-wagers-analytics/cdc/wagers/`).

## Troubleshooting checklist
- `docker compose logs flink-jobmanager` will show connector classpath errors. Missing JARs usually mean the GCS connector was not copied to `/opt/flink/lib`.
- Ensure the service account key path inside containers (`/opt/flink/secrets/gcs-writer.json`) matches `core-site.xml`.
- Bucket path must exist and the service account needs permission to write objects.
- If you see authentication errors, rotate the key and confirm the project ID in `core-site.xml`.

## Next steps
- Implement time-windowed aggregations for real-time platform revenue monitoring through Flink SQL window functions.
- Add temporal lookup joins with game metadata tables to enrich wager analytics with RTP calculations.
- If you plan to stream to Dataproc or integrate with BigQuery, replace the filesystem sink with unified BigQuery connector for real-time analytics dashboards.
- Implement complex event processing (CEP) patterns to detect suspicious betting behavior.
- Add integration tests that start a local GCS emulator (e.g., `fs-gcs` or `fake-gcs-server`) before promoting changes to production.