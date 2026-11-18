"""
Тесты модуля топологии сети (topology.py).

Проверяют:
1. Корректность сборки графа IEEE-118 (ровно 118 узлов и 186 ветвей).
2. Обработку Z-score артефактов и отрицательных пропускных способностей (capacity <= 0).
3. Корректность структуры словаря смежности (adjacency).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Подключение пути к исходному коду
SOURCE_DIR = Path(__file__).resolve().parents[1] / "source" / "grid-mimd"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from topology import (
    Branch,
    Bus,
    GridTopology,
    build_synthetic_ieee118_topology,
    build_topology_from_data,
    sanitize_branch_capacity,
)


def test_topology_graph_size() -> None:
    """
    Проверка: граф успешно собирается и содержит ровно 118 вершин и 186 ребер.
    """
    topology = build_topology_from_data()

    assert isinstance(topology, GridTopology)
    assert len(topology.buses) == 118, f"Ожидалось ровно 118 узлов, получено {len(topology.buses)}"
    assert len(topology.branches) == 186, f"Ожидалось ровно 186 ветвей, получено {len(topology.branches)}"
    assert topology.num_buses == 118
    assert topology.num_branches == 186
    assert topology.total_load > 0, "Суммарная нагрузка сети должна быть положительной"


def test_zscore_negative_capacity_sanitization() -> None:
    """
    Проверка: если в сырых данных capacity = -0.18 (Z-score артефакт),
    в объекте Branch значение скорректировано на строго положительное
    согласно правилу capacity = max(1.0, 2.0 * abs(flow)).
    """
    raw_capacity = -0.18
    initial_flow = 0.5
    safe_cap = sanitize_branch_capacity(raw_capacity=raw_capacity, initial_flow=initial_flow)

    assert safe_cap > 0.0, "Емкость должна быть строго положительной"
    # По правилу max(1.0, 2.0 * 0.5) = 1.0
    assert safe_cap == pytest.approx(1.0)

    # Проверка при большем начальном потоке
    large_flow = 3.0
    safe_cap_large = sanitize_branch_capacity(raw_capacity=-0.18, initial_flow=large_flow)
    assert safe_cap_large == pytest.approx(6.0)

    # Проверка через сборку топологии из сырых массивов
    blist = [[1, 2]]
    branch_features = [[0.5, 0.0, 0.0, -0.18]]  # flow=0.5, capacity=-0.18
    bus_features = [[-2.5, 0.0, 0.0], [1.0, 0.0, 0.0]]  # 2 узла

    custom_topo = build_topology_from_data(
        blist=blist,
        branch_features=branch_features,
        bus_features=bus_features,
    )
    assert len(custom_topo.branches) == 1
    created_branch = custom_topo.branches[0]
    assert created_branch.capacity > 0.0
    assert created_branch.capacity == pytest.approx(1.0)


def test_adjacency_structure() -> None:
    """
    Проверка корректности словаря смежности:
    1. Для каждого узла сети присутствует запись в словаре смежности.
    2. Каждая ветвь в списке инцидентна данному узлу (from_bus или to_bus равен номеру узла).
    3. Каждая ветвь учтена ровно дважды (на обоих своих концах).
    """
    topology = build_topology_from_data()
    adjacency = topology.adjacency

    # 1. Проверяем наличие всех 118 узлов
    for bus in topology.buses:
        assert bus.id in adjacency, f"Узел {bus.id} отсутствует в словаре смежности"
        assert isinstance(adjacency[bus.id], list)

    # 2. Проверяем инцидентность ветвей
    total_incidences = 0
    for bus_id, incident_branch_ids in adjacency.items():
        total_incidences += len(incident_branch_ids)
        for branch_id in incident_branch_ids:
            branch = topology.branches[branch_id]
            assert bus_id in (branch.from_bus, branch.to_bus), (
                f"Ветвь {branch_id} ({branch.from_bus} -> {branch.to_bus}) "
                f"ошибочно отнесена к узлу {bus_id}"
            )

    # 3. Каждое ребро связывает 2 узла, поэтому суммарное число вхождений = 2 * len(branches)
    assert total_incidences == 2 * len(topology.branches)
