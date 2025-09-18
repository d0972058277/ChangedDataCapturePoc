# Loading StarRocks using Flink CDC

## Overview
- The existing proof of concept project uses Debezium to ingest MongoDB wager change streams, buffers in Kafka, and drives Apache Flink SQL for downstream processing.
- By including the StarRocks Flink connector, the same SQL job can stream curated wager analytics to StarRocks OLAP tables for low-latency gaming analytics and real-time platform revenue monitoring.
- All ETL logic—wager filtering, platform revenue calculation, event enrichment, derived financial metrics—remains in SQL scripts; no additional application code needed.

## Environment Requirements
- Running StarRocks cluster (at least one FE and one BE), accessible from Docker network (hostname or IP resolvable from Flink containers).
- Pre-created target database and table (e.g., database `wager_analytics`, table `ods_wagers`), matching your primary key or duplicate key model for wager data.
- StarRocks account with `LOAD_PRIV` permissions. Create a dedicated user with a strong password.
- Local tools: Docker Desktop, Docker Compose v2, and dependencies already listed in `README.md`.

## Step 1 — Prepare StarRocks table
1. Connect to FE MySQL port (default `9030`) and create database/table:
   ```sql
   CREATE DATABASE IF NOT EXISTS wager_analytics;
   USE wager_analytics;
   CREATE TABLE IF NOT EXISTS ods_wagers (
     wager_id STRING,
     game_id STRING,
     game_session_id STRING,
     user_id STRING,
     version BIGINT,
     total_bet DECIMAL(18,2),
     total_win DECIMAL(18,2),
     total_lose DECIMAL(18,2),
     begin_time DATETIME,
     end_time DATETIME,
     last_update_time DATETIME,
     is_completed BOOLEAN,
     event_count INT,
     net_amount DECIMAL(18,2),
     processed_at DATETIME
   ) ENGINE=OLAP
   DUPLICATE KEY(wager_id)
   DISTRIBUTED BY HASH(wager_id) BUCKETS 8
   PROPERTIES (
     "replication_allocation" = "tag.location.default: 1"
   );
   ```
2. Adjust schema for production requirements (primary key model, partitioning by date, aggregation tables for real-time dashboards, etc.).

## Step 2 — Manage credentials and ignore rules
1. Store secrets under `flink/secrets/starrocks.env`, for example:
   ```
   STARROCKS_JDBC_URL=jdbc:mysql://starrocks-fe:9030/wager_analytics
   STARROCKS_LOAD_URL=starrocks-fe:8030
   STARROCKS_USER=flink_writer
   STARROCKS_PASSWORD=REPLACE_WITH_REAL_PASSWORD
   ```
2. Ensure `.gitignore` already includes `flink/secrets/` to avoid committing private data.
3. Mount the secrets directory in `docker-compose.yml` so both Flink services can read:
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

## Step 3 — Add StarRocks connector to Flink image
1. Edit `flink/Dockerfile` and download connector matching Flink 1.17:
   ```Dockerfile
   ARG STARROCKS_CONNECTOR_VERSION=1.2.8_flink-1.17
   RUN curl -fsSL "https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/${STARROCKS_CONNECTOR_VERSION}/flink-connector-starrocks-${STARROCKS_CONNECTOR_VERSION}.jar" \
         -o /opt/flink/lib/flink-connector-starrocks-${STARROCKS_CONNECTOR_VERSION}.jar
   ```
2. Rebuild image to package the new JAR:
   ```sh
   docker compose build flink-jobmanager --no-cache
   docker compose build flink-taskmanager --no-cache
   ```

## Step 4 — Describe StarRocks sink and ETL logic in SQL
1. Extend `flink/sql/change_stream_job.sql` with sink table and insert statement:
   ```sql
   CREATE TABLE wagers_starrocks (
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
     'connector' = 'starrocks',
     'jdbc-url' = '${STARROCKS_JDBC_URL}',
     'load-url' = '${STARROCKS_LOAD_URL}',
     'database-name' = 'wager_analytics',
     'table-name' = 'ods_wagers',
     'username' = '${STARROCKS_USER}',
     'password' = '${STARROCKS_PASSWORD}',
     'sink.buffer-flush.interval-ms' = '2000',
     'sink.buffer-flush.max-rows' = '5000',
     'sink.properties.format' = 'json',
     'sink.properties.strip_outer_array' = 'true'
   );

   INSERT INTO wagers_starrocks
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
   - Replace `${STARROCKS_*}` with actual values, or pass through SQL Client (`sql-client.sh -Dkey=value`) or environment variables.
   - The `netAmount` calculation represents platform revenue (totalBet - totalWin) from the platform's perspective.
   - The query demonstrates wager analytics ETL patterns: financial calculations, timestamp conversions, completion status, and filtering for valid wagers.
   - This enables real-time platform revenue monitoring and gaming analytics dashboards in StarRocks.

## Step 5 — Run and verify
1. Restart stack to apply new connector and mounts:
   ```sh
   docker compose down -v
   docker compose build
   docker compose up -d
   ```
2. Re-register Debezium connector and re-submit Flink SQL job:
   ```sh
   ./scripts/register-mongo-connector.sh
   ./scripts/submit-flink-sql.sh
   ```
3. (Optional) Generate sample wager data: `dotnet run --project src/MongoChangeStreamSeeder STARROCKS`.
4. Verify target table:
   ```sql
   SELECT * FROM wager_analytics.ods_wagers ORDER BY processed_at DESC LIMIT 20;

   -- Query platform revenue metrics
   SELECT
     game_id,
     COUNT(*) as wager_count,
     SUM(total_bet) as total_revenue,
     SUM(total_win) as total_payout,
     SUM(net_amount) as platform_profit,
     AVG(net_amount) as avg_profit_per_wager
   FROM wager_analytics.ods_wagers
   WHERE is_completed = true
   GROUP BY game_id
   ORDER BY platform_profit DESC;
   ```

## Troubleshooting
- Use `docker compose logs flink-jobmanager` to confirm connector JAR loads successfully; `ClassNotFoundException` usually means missing files in `/opt/flink/lib`.
- Authentication or permission failures in StarRocks can be fixed by checking credentials or granting `LOAD_PRIV` (`GRANT LOAD_PRIV ON wager_analytics.* TO 'flink_writer'@'%'`).
- For higher throughput, adjust `sink.buffer-flush.*`, `sink.parallelism`, or StarRocks table distribution buckets.
- When Stream Load rejects rows due to schema mismatch or JSON issues, check FE logs (e.g., `fe/log/fe.warn.log`).

## Next steps
- Externalize sensitive parameters through SQL Client `SET` statements or configuration services rather than hardcoding.
- Implement time-windowed aggregations for real-time platform revenue monitoring dashboards.
- Enrich wager streams with lookup joins to game metadata tables, calculating RTP (Return to Player) and other gaming metrics.
- Add complex event processing (CEP) patterns to detect suspicious betting behavior in real-time.
- Create materialized views in StarRocks for pre-aggregated gaming analytics (hourly/daily revenue, player segments, etc.).
- Add end-to-end integration tests that start StarRocks containers in CI to validate wager analytics logic and ingestion performance.