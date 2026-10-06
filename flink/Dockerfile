FROM flink:2.2.1-scala_2.12

ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.2

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential python3 python3-venv python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m venv /opt/pyflink
RUN /opt/pyflink/bin/pip install --no-cache-dir apache-flink==2.2.1
ENV PATH="/opt/pyflink/bin:${PATH}"
USER flink

RUN wget -q \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
    -O /opt/flink/lib/flink-sql-connector-kafka.jar
