# Image Spark du projet : Spark 3.5 + Python + connecteurs Kafka et Iceberg.
# Les jars sont téléchargés à la construction de l'image, pas au lancement du job :
# démarrage plus rapide, pas besoin d'Internet ensuite, et versions figées.
FROM apache/spark:3.5.6-python3

ARG SPARK_VERSION=3.5.6
ARG SCALA_VERSION=2.12
ARG ICEBERG_VERSION=1.9.2
# Versions imposées par spark-sql-kafka 3.5.6 (voir son fichier pom)
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Lire Kafka depuis Spark Structured Streaming
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# Écrire des tables Iceberg, et accéder à MinIO avec le client S3
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

# Dossier des checkpoints du streaming, accessible en écriture par l'utilisateur spark
RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints

USER spark
