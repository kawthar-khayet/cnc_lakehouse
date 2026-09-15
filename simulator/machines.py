"""Simulation d'un atelier de 10 machines CNC.

Ce module ne connaît pas Kafka : il produit seulement des dictionnaires.
Séparer la logique métier de l'envoi permet de la tester sans infrastructure.

Modèle simplifié mais cohérent :
- une machine en marche use son outil (tool_wear_pct augmente) ;
- plus l'usure est forte, plus la température et les vibrations montent,
  et plus la probabilité de panne augmente (signal utile pour la maintenance prédictive) ;
- panne -> arrêt (DOWN) -> maintenance -> outil neuf -> reprise.
"""

from __future__ import annotations

import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum

SCHEMA_VERSION = 1


class Status(str, Enum):
    RUNNING = "RUNNING"
    IDLE = "IDLE"
    DOWN = "DOWN"
    MAINTENANCE = "MAINTENANCE"


@dataclass(frozen=True)
class MachineSpec:
    machine_id: str
    machine_type: str
    nominal_rpm: float
    nominal_temp_c: float
    nominal_vibration_mm_s: float
    nominal_power_kw: float


FLEET: list[MachineSpec] = [
    MachineSpec("LATHE-01", "lathe", 2500, 45, 1.2, 11.0),
    MachineSpec("LATHE-02", "lathe", 2500, 45, 1.2, 11.0),
    MachineSpec("MILL-01", "mill", 8000, 50, 1.8, 15.0),
    MachineSpec("MILL-02", "mill", 8000, 50, 1.8, 15.0),
    MachineSpec("MILL-03", "mill", 12000, 55, 2.0, 18.0),
    MachineSpec("MILL-04", "mill", 12000, 55, 2.0, 18.0),
    MachineSpec("ROUTER-01", "router", 18000, 40, 1.5, 7.5),
    MachineSpec("GRINDER-01", "grinder", 3000, 42, 0.8, 5.5),
    MachineSpec("DRILL-01", "drill", 4000, 40, 1.0, 4.0),
    MachineSpec("DRILL-02", "drill", 4000, 40, 1.0, 4.0),
]

FAILURE_CODES = ["SPINDLE_OVERHEAT", "TOOL_BREAKAGE", "COOLANT_LOW", "AXIS_SERVO_FAULT"]
MEASURE_FIELDS = ["spindle_rpm", "spindle_temp_c", "vibration_mm_s", "power_kw"]


@dataclass
class MachineState:
    spec: MachineSpec
    status: Status = Status.RUNNING
    tool_wear_pct: float = 0.0
    remaining_ticks: int = 0  # durée restante dans IDLE / DOWN / MAINTENANCE


def to_iso(ts: datetime) -> str:
    """Horodatage ISO 8601 en UTC, ex. 2026-09-15T08:30:00.123Z."""
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class FactorySimulator:
    fleet: list[MachineSpec] = field(default_factory=lambda: FLEET)
    seed: int | None = None
    dirty_rate: float = 0.0  # proportion de mesures volontairement "sales"

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.machines = [
            MachineState(spec, tool_wear_pct=self.rng.uniform(0, 60)) for spec in self.fleet
        ]
        self.quality_issues: Counter[str] = Counter()

    # ------------------------------------------------------------------ API publique

    def tick(self, now: datetime) -> tuple[list[dict], list[dict]]:
        """Avance d'un pas de temps. Retourne (mesures de télémétrie, événements machine)."""
        telemetry: list[dict] = []
        events: list[dict] = []
        for machine in self.machines:
            events.extend(self._update_status(machine, now))
            telemetry.append(self._measure(machine, now))
        return self._inject_quality_issues(telemetry), events

    # ------------------------------------------------------------------ états

    def _update_status(self, m: MachineState, now: datetime) -> list[dict]:
        rng = self.rng

        if m.status is Status.RUNNING:
            m.tool_wear_pct = min(100.0, m.tool_wear_pct + rng.uniform(0.05, 0.15))
            failure_prob = 0.0002 + (m.tool_wear_pct / 100) ** 4 * 0.02
            if m.tool_wear_pct >= 100 or rng.random() < failure_prob:
                m.remaining_ticks = rng.randint(30, 120)
                return [
                    self._transition(m, Status.DOWN, now, failure_code=rng.choice(FAILURE_CODES))
                ]
            if m.tool_wear_pct > 85 and rng.random() < 0.01:
                m.remaining_ticks = rng.randint(20, 60)
                return [self._transition(m, Status.MAINTENANCE, now, maintenance_type="PREVENTIVE")]
            if rng.random() < 0.002:
                m.remaining_ticks = rng.randint(20, 60)
                return [self._transition(m, Status.IDLE, now)]
            return []

        m.remaining_ticks -= 1
        if m.remaining_ticks > 0:
            return []

        if m.status is Status.DOWN:
            m.remaining_ticks = rng.randint(20, 60)
            return [self._transition(m, Status.MAINTENANCE, now, maintenance_type="CORRECTIVE")]
        if m.status is Status.MAINTENANCE:
            m.tool_wear_pct = 0.0
        return [self._transition(m, Status.RUNNING, now)]

    def _transition(
        self,
        m: MachineState,
        new_status: Status,
        now: datetime,
        failure_code: str | None = None,
        maintenance_type: str | None = None,
    ) -> dict:
        event = {
            "event_id": self._new_id(),
            "schema_version": SCHEMA_VERSION,
            "machine_id": m.spec.machine_id,
            "event_time": to_iso(now),
            "previous_status": m.status.value,
            "new_status": new_status.value,
            "failure_code": failure_code,
            "maintenance_type": maintenance_type,
            "tool_wear_pct": round(m.tool_wear_pct, 2),
        }
        m.status = new_status
        return event

    # ------------------------------------------------------------------ mesures

    def _measure(self, m: MachineState, now: datetime) -> dict:
        rng, spec, wear = self.rng, m.spec, m.tool_wear_pct / 100

        if m.status is Status.RUNNING:
            rpm = spec.nominal_rpm * rng.gauss(1, 0.02)
            temp = spec.nominal_temp_c + wear * 25 + rng.gauss(0, 1)
            vibration = spec.nominal_vibration_mm_s * (1 + wear**2 * 1.5) + rng.gauss(0, 0.1)
            power = spec.nominal_power_kw * rng.gauss(1, 0.05) * (1 + wear / 4)
        else:
            rpm = 0.0
            temp = 25 + rng.gauss(0, 1)
            vibration = abs(rng.gauss(0.05, 0.02))
            power = 0.5 if m.status is Status.IDLE else 0.2

        return {
            "event_id": self._new_id(),
            "schema_version": SCHEMA_VERSION,
            "machine_id": spec.machine_id,
            "machine_type": spec.machine_type,
            "event_time": to_iso(now),
            "status": m.status.value,
            "spindle_rpm": round(rpm, 1),
            "spindle_temp_c": round(temp, 2),
            "vibration_mm_s": round(max(vibration, 0.0), 3),
            "power_kw": round(power, 2),
            "tool_wear_pct": round(m.tool_wear_pct, 2),
        }

    # ------------------------------------------------------------------ données sales

    def _inject_quality_issues(self, records: list[dict]) -> list[dict]:
        """Ajoute les défauts qu'on rencontre en vrai, pour les traiter plus tard en Silver."""
        out: list[dict] = []
        for record in records:
            out.append(record)
            if self.rng.random() >= self.dirty_rate:
                continue
            issue = self.rng.choice(["duplicate", "late", "null", "out_of_range"])
            self.quality_issues[issue] += 1
            if issue == "duplicate":  # même event_id envoyé deux fois (retry réseau)
                out.append(dict(record))
            elif issue == "late":  # événement qui arrive avec 5 à 30 min de retard
                ts = datetime.fromisoformat(record["event_time"])  # accepte le "Z" depuis 3.11
                record["event_time"] = to_iso(ts - timedelta(minutes=self.rng.uniform(5, 30)))
            elif issue == "null":  # capteur qui ne répond pas
                record[self.rng.choice(MEASURE_FIELDS)] = None
            else:  # capteur défaillant
                if self.rng.random() < 0.5:
                    record["spindle_rpm"] = -abs(record["spindle_rpm"] or 1.0)
                else:
                    record["spindle_temp_c"] = 999.0
        return out

    def _new_id(self) -> str:
        # Dérivé du générateur aléatoire : même seed -> mêmes identifiants (reproductible).
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
