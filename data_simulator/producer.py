"""Envoi des messages vers Kafka.

Ce module ne connaît rien à la simulation : il reçoit des dictionnaires et les publie.
"""

from __future__ import annotations

import json
import logging

from confluent_kafka import KafkaError, Message, Producer

log = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "cnc-simulator",
                "acks": "all",  # le broker confirme seulement quand le message est bien écrit
                "enable.idempotence": True,  # pas de doublon créé par les retries du producteur
                "linger.ms": 50,  # regroupe les messages en petits lots
                "compression.type": "lz4",
            }
        )
        self.delivered = 0
        self.failed = 0

    def send(self, topic: str, key: str, record: dict) -> None:
        # La clé (machine_id) envoie tous les messages d'une machine dans la même partition,
        # donc leur ordre est conservé.
        payload = json.dumps(record).encode("utf-8")
        while True:
            try:
                self._producer.produce(topic, key=key, value=payload, on_delivery=self._on_delivery)
                break
            except BufferError:  # file d'attente locale pleine : on laisse partir des messages
                self._producer.poll(0.5)
        self._producer.poll(0)  # déclenche les callbacks des messages déjà livrés

    def flush(self, timeout_s: float = 10) -> int:
        """Attend l'envoi des messages en attente. Retourne le nombre de messages non envoyés."""
        return self._producer.flush(timeout_s)

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("Échec d'envoi vers %s : %s", msg.topic(), err)
        else:
            self.delivered += 1
