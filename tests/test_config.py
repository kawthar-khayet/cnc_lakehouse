import pytest
from data_simulator.config import load_settings

ENV_VARS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "SIMULATOR_INTERVAL_S",
    "SIMULATOR_DIRTY_RATE",
    "SIMULATOR_SEED",
    "SIMULATOR_MAX_TICKS",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def test_defaults():
    settings = load_settings([])
    assert settings.bootstrap_servers == "localhost:29092"
    assert settings.dirty_rate == 0.02
    assert settings.seed is None
    assert settings.max_ticks == 0


def test_command_line_overrides_environment(monkeypatch):
    monkeypatch.setenv("SIMULATOR_DIRTY_RATE", "0.1")
    monkeypatch.setenv("SIMULATOR_SEED", "7")

    settings = load_settings(["--seed", "42"])

    assert settings.dirty_rate == 0.1  # vient de l'environnement
    assert settings.seed == 42  # la ligne de commande gagne
