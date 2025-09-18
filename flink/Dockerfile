FROM flink:1.17.2-scala_2.12-java11

ARG CONNECTOR_VERSION=3.0.0-1.17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${CONNECTOR_VERSION}/flink-sql-connector-kafka-${CONNECTOR_VERSION}.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka-${CONNECTOR_VERSION}.jar

USER flink
