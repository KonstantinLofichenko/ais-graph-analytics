FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
# Pin Iceberg to the REST fixture version and S3A to Spark's Hadoop 3.3.4.
RUN python3 -m pip install --no-cache-dir pyarrow==21.0.0 pandas==2.2.3
RUN set -eu; \
    base=https://repo.maven.apache.org/maven2; \
    curl -fsSL "$base/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar" -o /opt/spark/jars/iceberg-spark-runtime.jar; \
    curl -fsSL "$base/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" -o /opt/spark/jars/iceberg-aws-bundle.jar; \
    curl -fsSL "$base/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o /opt/spark/jars/hadoop-aws.jar; \
    curl -fsSL "$base/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o /opt/spark/jars/aws-java-sdk-bundle.jar
# Spark's Parquet 1.13 reader cannot parse the native GEOMETRY footer annotation.
RUN set -eu; rm /opt/spark/jars/parquet-*-1.13.1.jar; \
    for module in column common encoding format-structures hadoop jackson; do \
      curl -fsSL "https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-$module/1.16.0/parquet-$module-1.16.0.jar" \
        -o "/opt/spark/jars/parquet-$module-1.16.0.jar"; \
    done
USER 185

# DockerOperator launches this image without host-path mounts.
COPY --chown=185:0 spark/jobs /opt/ais/jobs
