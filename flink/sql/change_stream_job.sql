SET 'execution.shutdown-on-application-finish' = 'false';

DROP TABLE IF EXISTS mongo_wagers_raw;
DROP TABLE IF EXISTS wagers_print;
DROP TABLE IF EXISTS wagers_fs;

CREATE TABLE mongo_wagers_raw (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'mongo.inventory.wagers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-mongo-wagers',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'raw',
  'raw.charset' = 'UTF-8'
);

CREATE TABLE wagers_print (
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
  netAmount DECIMAL(18,2)
) WITH (
  'connector' = 'print'
);

CREATE TABLE wagers_fs (
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
  netAmount DECIMAL(18,2)
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/wagers',
  'format' = 'json',
  'sink.rolling-policy.rollover-interval' = '1 min',
  'sink.rolling-policy.file-size' = '64MB'
);

EXECUTE STATEMENT SET BEGIN
  INSERT INTO wagers_print
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
    CAST(JSON_VALUE(message, '$.payload.totalWin') AS DECIMAL(18,2)) AS netAmount
  FROM mongo_wagers_raw;

  INSERT INTO wagers_fs
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
    CAST(JSON_VALUE(message, '$.payload.totalWin') AS DECIMAL(18,2)) AS netAmount
  FROM mongo_wagers_raw;
END;
