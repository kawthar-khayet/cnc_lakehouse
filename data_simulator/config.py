"""Réglages du simulateur.

Ordre de priorité : ligne de commande > variables d'environnement > valeurs par défaut.
Les variables d'environnement serviront quand le simulateur tournera dans Docker.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

TELEMETRY_TOPIC = "cnc.telemetry"
EVENTS_TOPIC = "cnc.machine-events"


@dataclass(frozen=True)
class SimulatorSettings:
    bootstrap_servers: str
    interval_s: float  # secondes entre deux ticks
    dirty_rate: float  # part de mesures volontairement sales
    seed: int | None
    max_ticks: int  # 0 = tourne indéfiniment

    @classmethod
    def from_env(cls) -> SimulatorSettings:
        seed = os.getenv("SIMULATOR_SEED")
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            interval_s=float(os.getenv("SIMULATOR_INTERVAL_S", "1.0")),
            dirty_rate=float(os.getenv("SIMULATOR_DIRTY_RATE", "0.02")),
            seed=int(seed) if seed else None,
            max_ticks=int(os.getenv("SIMULATOR_MAX_TICKS", "0")),
        )


def load_settings(argv: list[str] | None = None) -> SimulatorSettings:
    """Lit l'environnement, puis laisse la ligne de commande écraser ces valeurs."""
    env = SimulatorSettings.from_env()

    parser = argparse.ArgumentParser(
        prog="data_simulator", description="Simulateur d'atelier CNC -> Kafka"
    )
    parser.add_argument("--bootstrap-servers", default=env.bootstrap_servers)
    parser.add_argument("--interval", type=float, default=env.interval_s)
    parser.add_argument("--dirty-rate", type=float, default=env.dirty_rate)
    parser.add_argument("--seed", type=int, default=env.seed)
    parser.add_argument("--max-ticks", type=int, default=env.max_ticks)
    args = parser.parse_args(argv)

    return SimulatorSettings(
        bootstrap_servers=args.bootstrap_servers,
        interval_s=args.interval,
        dirty_rate=args.dirty_rate,
        seed=args.seed,
        max_ticks=args.max_ticks,
    )
