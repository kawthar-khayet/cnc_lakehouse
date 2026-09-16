from datetime import UTC, datetime, timedelta

from data_simulator.generator import FactorySimulator
from data_simulator.machines import FLEET
from data_simulator.models import Status

START = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)


def run(sim: FactorySimulator, ticks: int) -> tuple[list[dict], list[dict]]:
    telemetry, events = [], []
    for i in range(ticks):
        t, e = sim.tick(START + timedelta(seconds=i))
        telemetry += t
        events += e
    return telemetry, events


def test_same_seed_gives_same_data():
    assert run(FactorySimulator(seed=42), 50) == run(FactorySimulator(seed=42), 50)


def test_one_clean_measure_per_machine_per_tick():
    telemetry, _ = FactorySimulator(seed=1, dirty_rate=0).tick(START)
    assert [r["machine_id"] for r in telemetry] == [s.machine_id for s in FLEET]
    for record in telemetry:
        assert record["event_time"] == "2026-09-15T08:00:00.000Z"
        assert all(record[f] is not None for f in ("spindle_rpm", "spindle_temp_c"))
        assert record["spindle_rpm"] >= 0


def test_worn_tool_fails_then_is_repaired():
    sim = FactorySimulator(seed=7, dirty_rate=0)
    machine = sim.machines[0]
    machine.tool_wear_pct = 100

    _, events = run(sim, 300)
    mine = [e for e in events if e["machine_id"] == machine.spec.machine_id]

    assert mine[0]["new_status"] == Status.DOWN.value
    assert mine[0]["failure_code"] is not None
    assert [e["new_status"] for e in mine[1:3]] == ["MAINTENANCE", "RUNNING"]
    assert mine[1]["maintenance_type"] == "CORRECTIVE"
    assert machine.tool_wear_pct < 100


def test_dirty_data_is_injected():
    sim = FactorySimulator(seed=3, dirty_rate=1.0)
    telemetry, _ = run(sim, 20)

    ids = [r["event_id"] for r in telemetry]
    assert len(ids) > len(set(ids))  # doublons
    assert any(
        r[f] is None
        for r in telemetry
        for f in ("spindle_rpm", "power_kw", "spindle_temp_c", "vibration_mm_s")
    )
    assert set(sim.quality_issues) == {"duplicate", "late", "null", "out_of_range"}
