"""Point d'entrée : relie les réglages, le générateur et le producteur Kafka.

Lancement :
    uv run python -m data_simulator --max-ticks 60
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from data_simulator.config import EVENTS_TOPIC, TELEMETRY_TOPIC, SimulatorSettings, load_settings
from data_simulator.generator import FactorySimulator
from data_simulator.producer import EventProducer

log = logging.getLogger("data_simulator")


def run(settings: SimulatorSettings) -> None:
    simulator = FactorySimulator(seed=settings.seed, dirty_rate=settings.dirty_rate)
    producer = EventProducer(settings.bootstrap_servers)

    log.info("Envoi vers %s (Ctrl+C pour arrêter)", settings.bootstrap_servers)
    tick = 0
    try:
        while settings.max_ticks == 0 or tick < settings.max_ticks:
            telemetry, events = simulator.tick(datetime.now(UTC))
            for record in telemetry:
                producer.send(TELEMETRY_TOPIC, record["machine_id"], record)
            for event in events:
                producer.send(EVENTS_TOPIC, event["machine_id"], event)
                log.info(
                    "%s : %s -> %s %s",
                    event["machine_id"],
                    event["previous_status"],
                    event["new_status"],
                    event["failure_code"] or event["maintenance_type"] or "",
                )
            tick += 1
            if tick % 10 == 0:
                log.info(
                    "tick=%d livrés=%d échecs=%d défauts injectés=%s",
                    tick,
                    producer.delivered,
                    producer.failed,
                    dict(simulator.quality_issues),
                )
            time.sleep(settings.interval_s)
    except KeyboardInterrupt:
        log.info("Arrêt demandé")
    finally:
        remaining = producer.flush()
        log.info(
            "Terminé : livrés=%d échecs=%d non envoyés=%d",
            producer.delivered,
            producer.failed,
            remaining,
        )


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(load_settings(argv))


if __name__ == "__main__":
    main()
