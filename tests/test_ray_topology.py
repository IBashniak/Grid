"""
Тесты сериализации и размещения топологии сети в Ray Object Store (test_ray_topology.py).

Проверяют:
1. Помещение объекта GridTopology в Ray Object Store через prepare_cluster_topology / ray.put
   и корректное извлечение через ray.get.
2. Неизменность структуры графа IEEE-118 (118 узлов, 186 ветвей, словарь смежности)
   после прохождения через разделяемую память Object Store.
3. Санитарную обработку нефизичных емкостей (Z-score артефактов).
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
from grid_ray.topology import (  # noqa: E402
    GridTopology,
    build_topology_from_data,
    prepare_cluster_topology,
    sanitize_branch_capacity,
)


@pytest.fixture(scope="module", autouse=True)
def ray_cluster_session():
    """Инициализирует локальный рантайм Ray (num_cpus=2) на время выполнения модуля тестов."""
    ray.init(local_mode=False, num_cpus=2, ignore_reinit_error=True)
    yield


def test_put_and_get_topology_in_object_store() -> None:
    """
    Проверка: объект GridTopology успешно помещается в Ray Object Store через
    prepare_cluster_topology (ray.put) и возвращает валидный ray.ObjectRef,
    из которого ray.get() восстанавливает объект без потерь.
    """
    topology = build_topology_from_data()
    topology_ref = prepare_cluster_topology(topology)

    assert isinstance(topology_ref, ray.ObjectRef), (
        f"Ожидался тип ray.ObjectRef, получено {type(topology_ref)}"
    )

    restored_topology = ray.get(topology_ref)
    assert isinstance(restored_topology, GridTopology)
    assert restored_topology.num_buses == topology.num_buses
    assert restored_topology.num_branches == topology.num_branches
    assert restored_topology.total_load == pytest.approx(topology.total_load)


def test_graph_invariance_after_object_store() -> None:
    """
    Проверка неизменности графа (ровно 118 узлов и 186 ветвей, сохранность потоков,
    емкостей и списков смежности) после прохождения через Ray Object Store.
    """
    original = build_topology_from_data()
    ref = ray.put(original)
    extracted: GridTopology = ray.get(ref)

    # 1. Размерность графа IEEE-118
    assert len(extracted.buses) == 118, f"Ожидалось 118 узлов, получено {len(extracted.buses)}"
    assert len(extracted.branches) == 186, f"Ожидалось 186 ветвей, получено {len(extracted.branches)}"

    # 2. Идентичность характеристик ветвей
    for orig_b, ext_b in zip(original.branches, extracted.branches):
        assert orig_b.id == ext_b.id
        assert orig_b.from_bus == ext_b.from_bus
        assert orig_b.to_bus == ext_b.to_bus
        assert orig_b.flow == pytest.approx(ext_b.flow)
        assert orig_b.capacity == pytest.approx(ext_b.capacity)
        assert ext_b.capacity > 0.0

    # 3. Идентичность словаря смежности
    assert set(extracted.adjacency.keys()) == set(original.adjacency.keys())
    for bus_id, branch_list in original.adjacency.items():
        assert extracted.adjacency[bus_id] == branch_list


def test_prepare_cluster_topology_type_validation() -> None:
    """
    Проверка: передача некорректного объекта вместо GridTopology вызывает TypeError.
    """
    with pytest.raises(TypeError):
        prepare_cluster_topology({"invalid": "dict"})  # type: ignore[arg-type]

    assert sanitize_branch_capacity(-0.25, 0.5) == pytest.approx(1.0)
