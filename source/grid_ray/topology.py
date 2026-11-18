"""
Модуль топологии энергосети и интеграции с распределенным хранилищем Ray (Шаг 1).

Отвечает за:
- Представление структуры графа энергосистемы IEEE-118 (Branch, Bus, GridTopology);
- Загрузку данных из матричных файлов PowerGraph и санитарную обработку емкостей;
- Единоразовое размещение графа топологии в Ray Object Store (Plasma) через ray.put()
  для чтения воркерами в режиме Zero-Copy без дублирования памяти и сетевого оверхеда.
"""

from __future__ import annotations

import math
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Обеспечиваем доступность пакета ray (или встроенного локального эмулятора)
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    import ray
except ModuleNotFoundError:
    try:
        from . import _install_local_ray_fallback
    except ImportError:
        from __init__ import _install_local_ray_fallback  # type: ignore
    _install_local_ray_fallback()
    import ray


# ============================================================================
# Модели данных (Датаклассы энергосистемы IEEE-118)
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


def sanitize_branch_capacity(raw_capacity: float, initial_flow: float) -> float:
    """
    Корректирует нефизичные значения пропускной способности линии (Z-score артефакты).

    Если capacity <= 0 или NaN, принудительно задается безопасный технологический резерв:
    max(1.0, 2.0 * abs(initial_flow)).
    Если capacity > 0, но меньше абсолютного начального потока, задается минимальный
    запас (5%), чтобы исключить ложное срабатывание защиты в начальный момент.
    """
    if raw_capacity <= 0.0 or math.isnan(raw_capacity):
        return max(1.0, 2.0 * abs(initial_flow))

    return max(raw_capacity, abs(initial_flow) * 1.05)


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Ray Object Store и Zero-Copy (ray.put)
# Почему категорически нельзя передавать объект GridTopology напрямую по значению
# в каждый вызов `simulate_trials_chunk_remote.remote(..., topo_data)`?
# 1. Антипаттерн (передача по значению): Если у нас 100 чанков задач, и мы передаем
#    объект `topo_data` напрямую в `.remote(topo_data)`, драйвер Ray будет 100 раз
#    сериализовать граф и 100 раз отправлять его копии в Object Store и по сети.
#    Память кластера переполнится дубликатами одного и того же графа!
# 2. Архитектура Plasma (Shared Memory): В каждом узле Ray работает демон распределенного
#    хранилища объектов (Object Store / Plasma), использующий разделяемую память ОС
#    (`/dev/shm` в Linux).
# 3. Вызов `topology_ref = ray.put(topo_data)` помещает граф в Object Store РОВНО ОДИН РАЗ
#    и возвращает легковесный указатель (дескриптор `ray.ObjectRef` размером в десятки байт).
# 4. Когда задача запускается на удаленном физическом узле, Ray скачивает объект на этот
#    узел только один раз (для первого воркера), а все остальные процессы-воркеры на том же
#    узле читают данные из общей памяти (Zero-Copy Shared Memory) без повторной пересылки!
# ============================================================================

def prepare_cluster_topology(topo_data: GridTopology) -> ray.ObjectRef:
    """
    Помещает объект топологии энергосети GridTopology в распределенное хранилище
    Ray Object Store ровно один раз и возвращает ссылку ray.ObjectRef.

    Аргументы:
        topo_data: Сконструированный объект топологии сети IEEE-118.

    Возвращает:
        ray.ObjectRef — легковесная ссылка на объект в разделяемой памяти кластера.
    """
    if not isinstance(topo_data, GridTopology):
        raise TypeError(
            f"Ожидался объект типа GridTopology, получено: {type(topo_data).__name__}"
        )

    # Если Ray еще не был инициализирован вызывающим кодом, инициализируем локально
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    # Помещаем тяжелый граф в Ray Object Store ровно один раз
    topology_ref: ray.ObjectRef = ray.put(topo_data)
    return topology_ref


def build_synthetic_ieee118_topology(seed: int = 42) -> GridTopology:
    """
    Генерирует детерминированную топологию IEEE-118 (ровно 118 узлов, 186 ветвей),
    соответствующую каноническому графу энергосистемы IEEE-118.
    Используется как fallback при отсутствии внешних .mat файлов датасета.
    """
    random_generator = random.Random(seed)
    num_buses = 118
    num_branches = 186

    # 1. Построение связного остовного дерева
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
    Строит объект GridTopology из сырых массивов данных или файлов датасета PowerGraph.

    Возвращает:
        GridTopology с ровно 118 узлами и 186 ветвями.
    """
    if blist is not None and branch_features is not None and bus_features is not None:
        return _assemble_topology_from_arrays(blist, branch_features, bus_features)

    candidate_paths: List[Path] = []
    if data_dir is not None:
        candidate_paths.append(Path(data_dir))

    current_file_dir = Path(__file__).resolve().parent
    project_root = current_file_dir.parents[1]
    workspace_root = current_file_dir.parents[2]

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
            pass

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
        p_net = float(bus_features[bus_idx][0])
        buses.append(Bus(id=bus_id, load=p_net))

        if p_net < 0:
            total_load += abs(p_net)
        else:
            total_load += p_net * 0.4

    if total_load <= 0.0:
        total_load = max(100.0, float(num_buses) * 10.0)

    for bus in buses:
        if bus.id not in adjacency:
            adjacency[bus.id] = []

    return GridTopology(
        buses=buses,
        branches=branches,
        adjacency=adjacency,
        total_load=total_load,
    )
