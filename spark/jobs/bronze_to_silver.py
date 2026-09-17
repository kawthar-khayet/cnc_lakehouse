"""Couche Silver : Bronze (JSON brut) -> tables propres et fiables.

Pour chaque micro-batch de nouvelles lignes Bronze :
1. lire le JSON avec un schéma explicite ;
2. valider chaque ligne : les lignes invalides partent en quarantaine (table *_rejects)
   avec la raison du rejet, au lieu d'être supprimées sans trace ;
3. retirer les doublons (même event_id) ;
4. repérer les événements en retard (event_time très antérieur à l'arrivée dans Kafka) ;
5. écrire avec MERGE INTO : rejouer un batch ne crée jamais de doublon (idempotence).

Tourne dans le conteneur Spark (Python 3.8) : pas de syntaxe Python plus récente.
"""

from __future__ import annotations

import os
from typing import Callable, NamedTuple, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/spark/checkpoints")
TRIGGER_INTERVAL = os.getenv("SILVER_TRIGGER_INTERVAL", "30 seconds")
LATE_THRESHOLD_S = int(os.getenv("LATE_THRESHOLD_S", "120"))

STATUSES = ["RUNNING", "IDLE", "DOWN", "MAINTENANCE"]

Rule = Tuple[str, Column]  # (raison du rejet, condition vraie quand la ligne est invalide)


class Dataset(NamedTuple):
    name: str
    schema: StructType
    rules: Callable[[], list[Rule]]
    silver_ddl: str

    @property
    def bronze_table(self) -> str:
        return f"lakehouse.bronze.{self.name}"

    @property
    def silver_table(self) -> str:
        return f"lakehouse.silver.{self.name}"

    @property
    def rejects_table(self) -> str:
        return f"lakehouse.silver.{self.name}_rejects"


# ------------------------------------------------------------------ télémétrie

TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("machine_id", StringType()),
        StructField("machine_type", StringType()),
        StructField("event_time", StringType()),
        StructField("status", StringType()),
        StructField("spindle_rpm", DoubleType()),
        StructField("spindle_temp_c", DoubleType()),
        StructField("vibration_mm_s", DoubleType()),
        StructField("power_kw", DoubleType()),
        StructField("tool_wear_pct", DoubleType()),
    ]
)


def telemetry_rules() -> list[Rule]:
    # Une mesure nulle (capteur muet) n'est PAS rejetée : les autres mesures restent utiles.
    # Une comparaison avec null donne null, que F.when traite comme "faux".
    return [
        ("missing_event_id", F.col("event_id").isNull()),
        ("missing_machine_id", F.col("machine_id").isNull()),
        ("invalid_event_time", F.col("event_time").isNull()),
        ("unknown_status", F.col("status").isNull() | ~F.col("status").isin(STATUSES)),
        ("negative_rpm", F.col("spindle_rpm") < 0),
        ("temperature_out_of_range", ~F.col("spindle_temp_c").between(-20, 200)),
        ("vibration_out_of_range", ~F.col("vibration_mm_s").between(0, 50)),
        ("negative_power", F.col("power_kw") < 0),
        ("tool_wear_out_of_range", ~F.col("tool_wear_pct").between(0, 100)),
    ]


TELEMETRY_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        event_id          STRING,
        schema_version    INT,
        machine_id        STRING,
        machine_type      STRING,
        event_time        TIMESTAMP COMMENT 'heure de la mesure sur la machine (UTC)',
        status            STRING,
        spindle_rpm       DOUBLE,
        spindle_temp_c    DOUBLE,
        vibration_mm_s    DOUBLE,
        power_kw          DOUBLE,
        tool_wear_pct     DOUBLE,
        kafka_timestamp   TIMESTAMP COMMENT 'heure d arrivée dans Kafka',
        ingestion_delay_s DOUBLE    COMMENT 'kafka_timestamp - event_time',
        is_late           BOOLEAN,
        processed_at      TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
"""

# ------------------------------------------------------------------ événements machine

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("machine_id", StringType()),
        StructField("event_time", StringType()),
        StructField("previous_status", StringType()),
        StructField("new_status", StringType()),
        StructField("failure_code", StringType()),
        StructField("maintenance_type", StringType()),
        StructField("tool_wear_pct", DoubleType()),
    ]
)


def events_rules() -> list[Rule]:
    return [
        ("missing_event_id", F.col("event_id").isNull()),
        ("missing_machine_id", F.col("machine_id").isNull()),
        ("invalid_event_time", F.col("event_time").isNull()),
        ("unknown_status", ~F.col("previous_status").isin(STATUSES)),
        ("unknown_status", ~F.col("new_status").isin(STATUSES)),
        ("failure_without_code", (F.col("new_status") == "DOWN") & F.col("failure_code").isNull()),
    ]


EVENTS_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        event_id          STRING,
        schema_version    INT,
        machine_id        STRING,
        event_time        TIMESTAMP,
        previous_status   STRING,
        new_status        STRING,
        failure_code      STRING,
        maintenance_type  STRING,
        tool_wear_pct     DOUBLE,
        kafka_timestamp   TIMESTAMP,
        ingestion_delay_s DOUBLE,
        is_late           BOOLEAN,
        processed_at      TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
"""

REJECTS_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        reject_reason   STRING,
        payload         STRING COMMENT 'message brut, pour pouvoir analyser ou rejouer',
        kafka_topic     STRING,
        kafka_partition INT,
        kafka_offset    BIGINT,
        kafka_timestamp TIMESTAMP,
        rejected_at     TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(kafka_timestamp))
"""

DATASETS = [
    Dataset("telemetry", TELEMETRY_SCHEMA, telemetry_rules, TELEMETRY_DDL),
    Dataset("machine_events", EVENTS_SCHEMA, events_rules, EVENTS_DDL),
]

# ------------------------------------------------------------------ transformations


def parse(bronze: DataFrame, schema: StructType) -> DataFrame:
    """JSON brut -> une colonne par champ. Un JSON illisible donne des champs nuls."""
    return (
        bronze.withColumn("data", F.from_json("payload", schema))
        .select(
            "payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            *[F.col("data." + field.name).alias(field.name) for field in schema.fields],
        )
        .withColumn("event_time", F.to_timestamp("event_time"))
    )


def first_failed_rule(rules: list[Rule]) -> Column:
    """Raison de la première règle non respectée, ou null si la ligne est valide."""
    reason = F.lit(None).cast("string")
    for name, is_invalid in reversed(rules):
        reason = F.when(is_invalid, F.lit(name)).otherwise(reason)
    return reason


def merge_new_rows(source: DataFrame, table: str, match_condition: str) -> None:
    """Insère seulement les lignes absentes de la table : relancer ne duplique rien."""
    spark = source.sparkSession
    view = "source_" + table.replace(".", "_")
    source.select(*spark.table(table).columns).createOrReplaceTempView(view)
    spark.sql(
        f"MERGE INTO {table} t USING {view} s ON {match_condition} WHEN NOT MATCHED THEN INSERT *"
    )


def process_batch(ds: Dataset) -> Callable[[DataFrame, int], None]:
    def _process(bronze_batch: DataFrame, batch_id: int) -> None:
        checked = (
            parse(bronze_batch, ds.schema)
            .withColumn("reject_reason", first_failed_rule(ds.rules()))
            .withColumn("processed_at", F.current_timestamp())
            .persist()  # utilisé plusieurs fois ci-dessous : on évite de tout recalculer
        )
        total = checked.count()
        if total == 0:
            checked.unpersist()
            return

        rejects = checked.filter(F.col("reject_reason").isNotNull()).withColumnRenamed(
            "processed_at", "rejected_at"
        )
        valid = (
            checked.filter(F.col("reject_reason").isNull())
            # Doublons dans le même batch : MERGE ne les verrait pas, il faut les retirer avant.
            .dropDuplicates(["event_id"])
            .withColumn(
                "ingestion_delay_s",
                F.col("kafka_timestamp").cast("double") - F.col("event_time").cast("double"),
            )
            .withColumn("is_late", F.col("ingestion_delay_s") > LATE_THRESHOLD_S)
            .persist()
        )

        # Doublons entre batches : MERGE n'insère pas un event_id déjà présent.
        merge_new_rows(valid, ds.silver_table, "t.event_id = s.event_id")
        merge_new_rows(
            rejects,
            ds.rejects_table,
            "t.kafka_topic = s.kafka_topic AND t.kafka_partition = s.kafka_partition "
            "AND t.kafka_offset = s.kafka_offset",
        )

        n_valid = valid.count()
        n_rejects = rejects.count()
        print(
            f"[silver.{ds.name}] batch {batch_id} : lus={total} valides={n_valid} "
            f"doublons_retirés={total - n_valid - n_rejects} rejetés={n_rejects} "
            f"en_retard={valid.filter('is_late').count()}",
            flush=True,
        )
        valid.unpersist()
        checked.unpersist()

    return _process


def main() -> None:
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    for ds in DATASETS:
        spark.sql(ds.silver_ddl.format(table=ds.silver_table))
        spark.sql(REJECTS_DDL.format(table=ds.rejects_table))
        (
            # Lecture en streaming d'une table Iceberg : seules les nouvelles lignes sont lues.
            spark.readStream.format("iceberg")
            .load(ds.bronze_table)
            .writeStream.foreachBatch(process_batch(ds))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver/{ds.name}")
            .queryName(f"silver_{ds.name}")
            .start()
        )
        print(f"Streaming démarré : {ds.bronze_table} -> {ds.silver_table}", flush=True)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
