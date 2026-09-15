"""Envoie les données du simulateur dans Kafka.

Lancement :
    uv run python -m simulator.producer --max-ticks 60
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer

from simulator.machines import FactorySimulator

TELEMETRY_TOPIC = "cnc.telemetry"
EVENTS_TOPIC = "cnc.machine-events"

log = logging.getLogger("simulator")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulateur d'atelier CNC -> Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    )
    parser.add_argument("--interval", type=float, default=1.0, help="secondes entre deux ticks")
    parser.add_argument("--dirty-rate", type=float, default=0.02, help="part de mesures sales")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--max-ticks", type=int, default=0, help="0 = tourne indéfiniment")
    return parser.parse_args()


class DeliveryStats:
    """Compte les messages confirmés ou en échec par le broker."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def callback(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("Échec d'envoi vers %s : %s", msg.topic(), err)
        else:
            self.delivered += 1


def send(producer: Producer, topic: str, record: dict, stats: DeliveryStats) -> None:
    # La clé = machine_id : tous les messages d'une machine vont dans la même partition,
    # donc leur ordre est conservé.
    payload = json.dumps(record).encode("utf-8")
    while True:
        try:
            producer.produce(
                topic, key=record["machine_id"], value=payload, on_delivery=stats.callback
            )
            return
        except BufferError:  # file d'attente locale pleine : on laisse partir des messages
            producer.poll(0.5)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "cnc-simulator",
            "acks": "all",  # le broker confirme seulement quand le message est bien écrit
            "enable.idempotence": True,  # pas de doublon créé par les retries du producteur
            "linger.ms": 50,  # regroupe les messages en petits lots
            "compression.type": "lz4",
        }
    )
    simulator = FactorySimulator(seed=args.seed, dirty_rate=args.dirty_rate)
    stats = DeliveryStats()

    log.info("Envoi vers %s (Ctrl+C pour arrêter)", args.bootstrap_servers)
    tick = 0
    try:
        while args.max_ticks == 0 or tick < args.max_ticks:
            telemetry, events = simulator.tick(datetime.now(UTC))
            for record in telemetry:
                send(producer, TELEMETRY_TOPIC, record, stats)
            for event in events:
                send(producer, EVENTS_TOPIC, event, stats)
                log.info(
                    "%s : %s -> %s %s",
                    event["machine_id"],
                    event["previous_status"],
                    event["new_status"],
                    event["failure_code"] or event["maintenance_type"] or "",
                )
            producer.poll(0)
            tick += 1
            if tick % 10 == 0:
                log.info(
                    "tick=%d livrés=%d échecs=%d défauts injectés=%s",
                    tick,
                    stats.delivered,
                    stats.failed,
                    dict(simulator.quality_issues),
                )
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Arrêt demandé")
    finally:
        remaining = producer.flush(10)
        log.info(
            "Terminé : livrés=%d échecs=%d non envoyés=%d",
            stats.delivered,
            stats.failed,
            remaining,
        )


if __name__ == "__main__":
    main()
