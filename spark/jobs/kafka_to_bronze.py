"""Ingestion en streaming : topics Kafka -> tables Bronze (Iceberg).

La couche Bronze garde chaque message tel qu'il est arrivé (JSON brut), avec ses métadonnées
Kafka. Aucun nettoyage ici : un message invalide ne doit jamais bloquer l'ingestion.
Le nettoyage (doublons, retards, valeurs aberrantes) est le rôle de la couche Silver.

Tourne dans le conteneur Spark (Python 3.8) : pas de syntaxe Python plus récente.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/spark/checkpoints")
TRIGGER_INTERVAL = os.getenv("BRONZE_TRIGGER_INTERVAL", "10 seconds")

# topic Kafka -> table Bronze
STREAMS = {
    "cnc.telemetry": "lakehouse.bronze.telemetry",
    "cnc.machine-events": "lakehouse.bronze.machine_events",
}


def create_bronze_table(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            message_key     STRING    COMMENT 'clé Kafka (machine_id)',
            payload         STRING    COMMENT 'message JSON brut, non modifié',
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT    COMMENT '(topic, partition, offset) identifie un message',
            kafka_timestamp TIMESTAMP COMMENT 'heure d arrivée dans Kafka',
            ingested_at     TIMESTAMP COMMENT 'heure d écriture dans Bronze'
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
        """
    )


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # au tout premier lancement : tout l'historique
        .load()
    )
    # Kafka fournit key/value en octets : on les convertit en texte, sans interpréter le JSON.
    return raw.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def write_to_bronze(df: DataFrame, table: str) -> StreamingQuery:
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)  # un micro-batch toutes les 10 secondes
        # Le checkpoint mémorise les offsets déjà écrits : après un redémarrage,
        # Spark reprend exactement là où il s'était arrêté (ni perte, ni doublon).
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
        .option("fanout-enabled", "true")  # écrit plusieurs partitions sans tri préalable
        .queryName(table.split(".")[-1])
        .toTable(table)
    )


def main() -> None:
    spark = SparkSession.builder.appName("kafka_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    for topic, table in STREAMS.items():
        create_bronze_table(spark, table)
        write_to_bronze(read_topic(spark, topic), table)
        print(f"Streaming démarré : {topic} -> {table}", flush=True)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
