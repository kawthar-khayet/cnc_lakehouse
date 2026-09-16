from data_simulator.machines import FAILURE_CODES, FLEET


def test_fleet_has_ten_machines_with_unique_ids():
    ids = [spec.machine_id for spec in FLEET]
    assert len(ids) == 10
    assert len(set(ids)) == len(ids)


def test_machine_id_starts_with_its_type():
    for spec in FLEET:
        assert spec.machine_id.lower().startswith(spec.machine_type)


def test_nominal_values_are_positive():
    for spec in FLEET:
        assert spec.nominal_rpm > 0
        assert spec.nominal_temp_c > 0
        assert spec.nominal_vibration_mm_s > 0
        assert spec.nominal_power_kw > 0


def test_failure_codes_are_unique():
    assert FAILURE_CODES
    assert len(set(FAILURE_CODES)) == len(FAILURE_CODES)
