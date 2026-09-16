"""Structures de données du simulateur.

- Status       : les états possibles d'une machine
- MachineSpec  : la fiche technique (ne change jamais)
- MachineState : l'état vivant (change à chaque seconde)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


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


@dataclass
class MachineState:
    spec: MachineSpec
    status: Status = Status.RUNNING
    tool_wear_pct: float = 0.0
    remaining_ticks: int = 0  # durée restante dans IDLE / DOWN / MAINTENANCE
