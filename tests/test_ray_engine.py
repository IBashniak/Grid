"""
Тесты распределенных воркеров (@ray.remote) вычислительного движка (test_ray_engine.py).

Проверяют:
1. Детерминизм и идемпотентность удаленной задачи simulate_trials_chunk_remote при одинаковом seed.
2. Корректную работу с ссылкой topology_ref (ray.ObjectRef) из разделяемой памяти без повторной сериализации.
3. Диапазоны физических метрик (blackouts_count, dns_sum, cascade_trips_sum, branch_trips).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "source"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import grid_ray  # noqa: F401, E402
import ray  # noqa: E402
from grid_ray.ray_engine import (  # noqa: E402
    run_single_trial,
    simulate_trials_chunk_remote,
)
from grid_ray.topology import (  # noqa: E402
    build_topology_from_data,
    prepare_cluster_topology,
)


@pytest.fixture(scope="module")
def cluster_topology_and_ref():
    """Фикстура: инициализирует локальный Ray и помещает топологию в Object Store."""
    ray.init(local_mode=False, num_cpus=2, ignore_reinit_error=True)
    topo = build_topology_from_data()
    topo_ref = prepare_cluster_topology(topo)
    return topo, topo_ref


def test_remote_task_determinism_and_idempotency(cluster_topology_and_ref) -> None:
    """
    Проверка детерминизма и идемпотентности удаленной задачи:
    Два вызова simulate_trials_chunk_remote.remote(...) с одинаковым base_seed
    возвращают идентичные результаты (необходимое условие для безопасных retries).
    """
    _, topo_ref = cluster_topology_and_ref

    future_a = simulate_trials_chunk_remote.remote(
        chunk_id=1,
        num_trials=25,
        base_seed=12345,
        k_fault=2,
        topology_ref=topo_ref,
    )
    future_b = simulate_trials_chunk_remote.remote(
        chunk_id=1,
        num_trials=25,
        base_seed=12345,
        k_fault=2,
        topology_ref=topo_ref,
    )

    result_a, result_b = ray.get([future_a, future_b])

    assert result_a["chunk_id"] == result_b["chunk_id"] == 1
    assert result_a["num_trials"] == result_b["num_trials"] == 25
    assert result_a["blackouts_count"] == result_b["blackouts_count"]
    assert result_a["cascade_trips_sum"] == result_b["cascade_trips_sum"]
    assert result_a["dns_sum"] == pytest.approx(result_b["dns_sum"])
    assert result_a["branch_trips"] == result_b["branch_trips"]


def test_topology_ref_shared_across_multiple_remote_tasks(cluster_topology_and_ref) -> None:
    """
    Проверка корректной работы с общей ссылкой topology_ref:
    Несколько параллельных воркеров читают одну и ту же ссылку ray.ObjectRef
    без мутации исходного графа и возвращают различные стохастические результаты
    при разных base_seed.
    """
    topo, topo_ref = cluster_topology_and_ref
    assert isinstance(topo_ref, ray.ObjectRef)

    futures = [
        simulate_trials_chunk_remote.remote(
            chunk_id=idx,
            num_trials=20,
            base_seed=1000 + idx * 100,
            k_fault=2,
            topology_ref=topo_ref,
        )
        for idx in range(4)
    ]

    results = ray.get(futures)
    assert len(results) == 4

    # Проверяем, что чанки с разными seed дают разнообразные распределения отключений
    branch_trip_tuples = {tuple(r["branch_trips"]) for r in results}
    assert len(branch_trip_tuples) > 1

    # Проверяем, что объект в Object Store остался неизменным
    after_topo = ray.get(topo_ref)
    assert after_topo.num_branches == topo.num_branches == 186
    assert all(b.active is True for b in after_topo.branches)


def test_remote_metrics_ranges(cluster_topology_and_ref) -> None:
    """
    Валидация диапазонов возвращаемых метрик удаленного воркера:
    - blackouts_count в [0, num_trials]
    - dns_sum в [0.0, num_trials]
    - cascade_trips_sum >= 0
    - длина branch_trips равна 186 (числу ветвей сети IEEE-118)
    """
    topo, topo_ref = cluster_topology_and_ref
    num_trials = 30

    ref = simulate_trials_chunk_remote.remote(
        chunk_id=5,
        num_trials=num_trials,
        base_seed=777,
        k_fault=3,
        topology_ref=topo_ref,
    )
    res = ray.get(ref)

    assert res["chunk_id"] == 5
    assert res["num_trials"] == num_trials
    assert 0 <= res["blackouts_count"] <= num_trials
    assert res["cascade_trips_sum"] >= 0
    assert 0.0 <= res["dns_sum"] <= float(num_trials)
    assert len(res["branch_trips"]) == topo.num_branches == 186
    assert all(c >= 0 for c in res["branch_trips"])
    assert "worker_ip" in res
