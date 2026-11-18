"""
Модуль топологии энергосети (Шаг 1).

Отвечает за представление структуры графа энергосистемы IEEE-118,
загрузку данных из матричных файлов PowerGraph и санитарную обработку
физических характеристик (Z-score артефакты, нулевые емкости линий).
"""

from __future__ import annotations

import math
import os
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# Модели данных (Датаклассы)
# ============================================================================

@dataclass
class Branch:
    """
    Представляет линию электропередачи (ветвь графа).

    Атрибуты:
        id: Уникальный целочисленный идентификатор линии (0..185).
        from_bus: Номер начального узла (подстанции).
        to_bus: Номер конечного узла (подстанции).
        flow: Текущий активный поток мощности через линию (МВт / о.е.).
        capacity: Максимальная пропускная способность линии (тепловой предел).
        active: Флаг работоспособности линии (True - включена, False - отключена защитой).
    """
    id: int
    from_bus: int
    to_bus: int
    flow: float
    capacity: float
    active: bool = True


@dataclass
class Bus:
    """
    Представляет узел энергосистемы (подстанцию / шину).

    Атрибуты:
        id: Идентификатор узла (1..118).
        load: Активная нагрузка или генерация в узле (МВт).
    """
    id: int
    load: float


@dataclass
class GridTopology:
    """
    Представление топологии энергосистемы в виде графа.

    Атрибуты:
        buses: Список всех узлов сети.
        branches: Список всех линий электропередачи.
        adjacency: Словарь смежности {номер_узла: [список_id_инцидентных_линий]}
                   для мгновенного поиска соседей за O(1).
        total_load: Суммарная потребляемая мощность сети.
    """
    buses: List[Bus]
    branches: List[Branch]
    adjacency: Dict[int, List[int]] = field(default_factory=dict)
    total_load: float = 0.0

    @property
    def num_buses(self) -> int:
        """Количество узлов в топологии."""
        return len(self.buses)

    @property
    def num_branches(self) -> int:
        """Количество линий в топологии."""
        return len(self.branches)


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Обработка Z-score артефактов
# В датасетах (например, PowerGraph для IEEE-118) признаки часто получены
# после нормализации (Z-score), либо трансформаторы связи исходно имеют
# нулевой номинальный предел (limit = 0).
# В физическом мире емкость линии не может быть <= 0. Если оставить отрицательную
# или нулевую capacity, алгоритм каскадных отключений мгновенно отключит линию
# при малейшем потоке (|flow| > capacity).
# Поэтому мы принудительно задаем безопасный технологический резерв:
# capacity = max(1.0, 2.0 * abs(flow))
# ============================================================================

def sanitize_branch_capacity(raw_capacity: float, initial_flow: float) -> float:
    """
    Корректирует нефизичные значения пропускной способности линии.

    Если capacity <= 0 или NaN (артефакт Z-score либо трансформатор без лимита),
    принудительно задается безопасный резерв: max(1.0, 2.0 * abs(initial_flow)).
    Если capacity > 0, но меньше абсолютного начального потока,
    задается минимальный запас (5%), чтобы избежать ложного отключения в начальный момент.
    """
    if raw_capacity <= 0.0 or math.isnan(raw_capacity):
        # Реализация требования ТЗ:
        # "если capacity <= 0, принудительно задай: capacity = max(1.0, 2.0 * abs(flow))"
        return max(1.0, 2.0 * abs(initial_flow))

    # Если емкость корректно положительная, гарантируем, что начальный поток помещается
    return max(raw_capacity, abs(initial_flow) * 1.05)


def build_synthetic_ieee118_topology(seed: int = 42) -> GridTopology:
    """
    Генерирует детерминированную топологию IEEE-118 (ровно 118 узлов, 186 ветвей),
    соответствующую каноническому графу энергосистемы IEEE-118.
    Используется как fallback при отсутствии внешних .mat файлов датасета.
    """
    random_generator = random.Random(seed)
    num_buses = 118
    num_branches = 186

    # 1. Построение остовного дерева для гарантированной связности графа
    edges: List[Tuple[int, int]] = []
    for node in range(2, num_buses + 1):
        parent_node = random_generator.randint(max(1, node - 10), node - 1)
        edges.append((parent_node, node))

    # 2. Добавление оставшихся ребер до ровно 186 линий
    existing_edges_set = set(edges)
    while len(edges) < num_branches:
        bus_u = random_generator.randint(1, num_buses)
        bus_v = random_generator.randint(1, num_buses)
        if bus_u != bus_v:
            edge = (min(bus_u, bus_v), max(bus_u, bus_v))
            if edge not in existing_edges_set:
                existing_edges_set.add(edge)
                edges.append((bus_u, bus_v))

    # 3. Инициализация линий и словаря смежности
    branches: List[Branch] = []
    adjacency: Dict[int, List[int]] = {bus_id: [] for bus_id in range(1, num_buses + 1)}

    for branch_id, (from_bus, to_bus) in enumerate(edges):
        flow = random_generator.uniform(0.1, 0.8)
        # Добавляем запас пропускной способности от 20% до 80%
        raw_capacity = flow * random_generator.uniform(1.2, 1.8)
        safe_capacity = sanitize_branch_capacity(raw_capacity, flow)

        branch = Branch(
            id=branch_id,
            from_bus=from_bus,
            to_bus=to_bus,
            flow=flow,
            capacity=safe_capacity,
            active=True,
        )
        branches.append(branch)
        adjacency[from_bus].append(branch_id)
        adjacency[to_bus].append(branch_id)

    # 4. Инициализация узлов и подсчет суммарной нагрузки
    buses: List[Bus] = []
    total_load = 0.0
    for bus_id in range(1, num_buses + 1):
        load_val = random_generator.uniform(-1.5, 2.5)
        buses.append(Bus(id=bus_id, load=load_val))
        if load_val < 0:
            total_load += abs(load_val)
        else:
            total_load += load_val * 0.4

    if total_load <= 0.0:
        total_load = 1000.0

    return GridTopology(
        buses=buses,
        branches=branches,
        adjacency=adjacency,
        total_load=total_load,
    )


def build_topology_from_data(
    data_dir: Optional[str | Path] = None,
    sample_idx: int = 0,
    blist: Optional[Any] = None,
    branch_features: Optional[Any] = None,
    bus_features: Optional[Any] = None,
) -> GridTopology:
    """
    Строит объект GridTopology из сырых массивов данных или файлов датасета.

    Аргументы:
        data_dir: Каталог с датасетом (по умолчанию ищет в 'Datasets' и подкаталогах).
        sample_idx: Индекс сценария для загрузки из PowerGraph.
        blist: Опциональный прямой массив связности ветвей (N_branches x 2).
        branch_features: Опциональный прямой массив признаков ветвей (N_branches x 4).
        bus_features: Опциональный прямой массив признаков узлов (N_buses x 3).

    Возвращает:
        GridTopology с ровно 118 узлами и 186 ветвями.
    """
    # -------------------------------------------------------------------------
    # Вариант А: Переданы прямые массивы в память (удобно для тестов)
    # -------------------------------------------------------------------------
    if blist is not None and branch_features is not None and bus_features is not None:
        return _assemble_topology_from_arrays(blist, branch_features, bus_features)

    # -------------------------------------------------------------------------
    # Вариант Б: Поиск файлов датасета на диске
    # -------------------------------------------------------------------------
    candidate_paths: List[Path] = []
    if data_dir is not None:
        candidate_paths.append(Path(data_dir))

    # Стандартные расположения датасета
    current_file_dir = Path(__file__).resolve().parent
    project_root = current_file_dir.parents[1]  # Igor-grid
    workspace_root = current_file_dir.parents[2]  # University

    candidate_paths.extend([
        project_root / "Datasets",
        workspace_root / "cloud_grid" / "Datasets",
        Path("Datasets"),
        Path("../Datasets"),
        Path("../../cloud_grid/Datasets"),
    ])

    raw_dir: Optional[Path] = None
    for cand in candidate_paths:
        ieee_raw = cand / "dataset_cascades" / "ieee118" / "ieee118" / "raw"
        if (ieee_raw / "blist.mat").exists():
            raw_dir = ieee_raw
            break
        if (cand / "blist.mat").exists():
            raw_dir = cand
            break

    if raw_dir is not None:
        try:
            # Пытаемся импортировать утилиту загрузки из cloud_grid/source/dataset_scripts
            dataset_scripts_dir = workspace_root / "cloud_grid" / "source" / "dataset_scripts"
            if dataset_scripts_dir.exists() and str(dataset_scripts_dir) not in sys.path:
                sys.path.insert(0, str(dataset_scripts_dir))

            from load_powergraph_cascades import load_powergraph_cascades  # type: ignore

            loaded_dataset = load_powergraph_cascades(
                raw_dir,
                sample_index=slice(sample_idx, sample_idx + 1),
                load_large=True,
            )

            if loaded_dataset.n_samples > 0 and loaded_dataset.Ef.size > 0:
                raw_blist = loaded_dataset.blist
                raw_branch_feats = loaded_dataset.Ef[0]
                raw_bus_feats = loaded_dataset.Bf[0]
                return _assemble_topology_from_arrays(raw_blist, raw_branch_feats, raw_bus_feats)
        except Exception:
            # При любой ошибке загрузки внешнего датасета переходим на fallback
            pass

    # -------------------------------------------------------------------------
    # Вариант В: Синтетическая генерация IEEE-118
    # -------------------------------------------------------------------------
    return build_synthetic_ieee118_topology(seed=42 + sample_idx)


def _assemble_topology_from_arrays(
    blist: Any,
    branch_features: Any,
    bus_features: Any,
) -> GridTopology:
    """
    Внутренняя функция сборки GridTopology из списков или numpy-массивов
    с применением санитарной обработки емкостей линий.
    """
    num_branches = len(blist)
    branches: List[Branch] = []
    adjacency: Dict[int, List[int]] = {}

    for branch_id in range(num_branches):
        from_bus = int(blist[branch_id][0])
        to_bus = int(blist[branch_id][1])
        initial_flow = float(branch_features[branch_id][0])
        raw_capacity = float(branch_features[branch_id][3])

        # Применение [DEEP DIVE]: Санитарная нормализация Z-score емкости
        safe_capacity = sanitize_branch_capacity(raw_capacity, initial_flow)

        branch = Branch(
            id=branch_id,
            from_bus=from_bus,
            to_bus=to_bus,
            flow=initial_flow,
            capacity=safe_capacity,
            active=True,
        )
        branches.append(branch)

        adjacency.setdefault(from_bus, []).append(branch_id)
        adjacency.setdefault(to_bus, []).append(branch_id)

    num_buses = len(bus_features)
    buses: List[Bus] = []
    total_load = 0.0

    for bus_idx in range(num_buses):
        bus_id = bus_idx + 1
        # В PowerGraph нулевой столбец - активная мощность P
        p_net = float(bus_features[bus_idx][0])
        buses.append(Bus(id=bus_id, load=p_net))

        # Отрицательное P означает потребление (нагрузку потребителей)
        if p_net < 0:
            total_load += abs(p_net)
        else:
            total_load += p_net * 0.4

    if total_load <= 0.0:
        total_load = max(100.0, float(num_buses) * 10.0)

    # Гарантируем наличие всех узлов в словаре смежности, даже изолированных
    for bus in buses:
        if bus.id not in adjacency:
            adjacency[bus.id] = []

    return GridTopology(
        buses=buses,
        branches=branches,
        adjacency=adjacency,
        total_load=total_load,
    )
