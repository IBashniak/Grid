"""
Тесты вычислительного движка симуляции (engine.py).

Проверяют:
1. Детерминизм симуляции при фиксированном seed.
2. Стохастическое разнообразие траекторий при разных seed.
3. Границы метрик (dns_proxy в [0.0, 1.0], cascade_trips >= 0).
4. Корректность и согласованность работы воркера simulate_trials_chunk.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Подключение пути к исходному коду
SOURCE_DIR = Path(__file__).resolve().parents[1] / "source" / "grid-mimd"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from engine import run_single_trial, simulate_trials_chunk
from topology import build_topology_from_data


@pytest.fixture(scope="module")
def shared_topology():
    """Фикстура: предзагруженная топология сети для ускорения тестов."""
    return build_topology_from_data()


def test_trial_determinism(shared_topology) -> None:
    """
    Проверка детерминизма: два последовательных запуска с одним и тем же seed
    должны давать на 100% идентичный результат (одинаковые отключенные ветви и метрики).
    """
    fixed_seed = 98765
    k_fault = 2

    run_first = run_single_trial(
        topo_data=shared_topology,
        k_fault=k_fault,
        seed=fixed_seed,
    )
    run_second = run_single_trial(
        topo_data=shared_topology,
        k_fault=k_fault,
        seed=fixed_seed,
    )

    assert run_first["seed"] == run_second["seed"]
    assert run_first["blackout"] == run_second["blackout"]
    assert run_first["cascade_trips"] == run_second["cascade_trips"]
    assert run_first["dns_proxy"] == pytest.approx(run_second["dns_proxy"])
    assert run_first["failed_branches"] == run_second["failed_branches"]


def test_trial_diversity(shared_topology) -> None:
    """
    Проверка стохастического разнообразия: испытания с разными seed
    должны генерировать различные сценарии первичных ударов и траекторий.
    """
    trajectories = []
    seeds = [101, 202, 303, 404, 505]
    for seed in seeds:
        result = run_single_trial(
            topo_data=shared_topology,
            k_fault=2,
            seed=seed,
        )
        trajectories.append(tuple(result["failed_branches"]))

    # Множество уникальных наборов отключений должно содержать более 1 элемента
    unique_trajectories = set(trajectories)
    assert len(unique_trajectories) > 1, "Разные seed должны давать разные сценарии аварий"


def test_metrics_ranges(shared_topology) -> None:
    """
    Проверка диапазонов значений метрик:
    - dns_proxy лежит строго в отрезке [0.0, 1.0];
    - cascade_trips неотрицательно;
    - blackout корректно соотносится с порогом 0.05.
    """
    for seed in range(50):
        result = run_single_trial(
            topo_data=shared_topology,
            k_fault=3,
            seed=seed,
        )

        dns = result["dns_proxy"]
        trips = result["cascade_trips"]
        blackout = result["blackout"]

        assert 0.0 <= dns <= 1.0, f"dns_proxy {dns} вышел за границы [0.0, 1.0]"
        assert trips >= 0, f"cascade_trips {trips} не может быть отрицательным"
        assert isinstance(blackout, bool), "blackout должен быть типом bool"

        # Проверка порога блэкаута (> 5% потерянной мощности)
        if dns > 0.05:
            assert blackout is True
        else:
            assert blackout is False


def test_simulate_trials_chunk(shared_topology) -> None:
    """
    Проверка воркера simulate_trials_chunk:
    - возвращает согласованный словарь с нужными ключами;
    - число выполненных испытаний совпадает с запрошенным;
    - сумма блэкаутов не превышает число испытаний;
    - размер массива branch_trip_counts равен числу линий в сети.
    """
    num_trials = 20
    chunk_id = 7
    base_seed = 1000
    k_fault = 1

    chunk_result = simulate_trials_chunk(
        (chunk_id, num_trials, base_seed, k_fault, shared_topology)
    )

    assert isinstance(chunk_result, dict)
    assert chunk_result["chunk_id"] == chunk_id
    assert chunk_result["num_trials"] == num_trials
    assert 0 <= chunk_result["blackouts_count"] <= num_trials
    assert chunk_result["total_cascade_trips"] >= 0
    assert 0.0 <= chunk_result["dns_sum"] <= float(num_trials)
    assert 0.0 <= chunk_result["dns_max"] <= 1.0

    # Проверка размерности массива частот отключений линий
    branch_trips = chunk_result["branch_trip_counts"]
    assert len(branch_trips) == shared_topology.num_branches
    assert all(count >= 0 for count in branch_trips)
