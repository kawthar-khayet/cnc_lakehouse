"""Catalogue de l'atelier : les machines et les causes de panne possibles.

Dans une vraie usine, ces données viendraient d'un référentiel (base de données, fichier de
configuration). Elles deviendront la table de dimension dim_machine dans la couche Gold.
"""

from __future__ import annotations

from data_simulator.models import MachineSpec

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
