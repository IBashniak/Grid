"""
Распределенный вычислительный движок на базе Ray Tasks (Шаг 2).

Содержит:
- run_single_trial: детерминированный расчет одного испытания каскадной аварии;
- simulate_trials_chunk_local: локальное выполнение чанка (для калибровки 1 ядра);
- simulate_trials_chunk_remote: удаленная stateless-задача (@ray.remote) с поддержкой
  Zero-Copy чтения топологии и автоматических повторов при сбоях узлов (max_retries=3).
"""

from __future__ import annotations

import random
import socket
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

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

try:
    from .topology import GridTopology
except (ImportError, ValueError):
    if "topology" in sys.modules and Path(getattr(sys.modules["topology"], "__file__", "")).resolve().parent != CURRENT_DIR:
        del sys.modules["topology"]
    from topology import GridTopology  # type: ignore


def _get_worker_node_ip() -> str:
    """Возвращает IP-адрес текущего узла-исполнителя задачи Ray."""
    try:
        if hasattr(ray, "util") and hasattr(ray.util, "get_node_ip_address"):
            ip = ray.util.get_node_ip_address()
            if ip:
                return str(ip)
    except Exception:
        pass
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "127.0.0.1"


def run_single_trial(
    topo_data: GridTopology,
    k_fault: int,
    seed: int,
    max_steps: int = 20,
) -> Dict[str, Any]:
    """
    Выполняет одно независимое испытание каскадной аварии в энергосистеме (N-k shock).

    Логика полностью идентична модели Этапа 1:
        1. Инициализация изолированного генератора `random.Random(seed)`.
        2. Первичный шок: отключение ровно k_fault случайных активных линий.
        3. Каскадный цикл: перераспределение потерянной мощности на смежные активные
           линии пропорционально их свободному резерву (capacity - flow).
        4. Отключение перегруженных линий (|flow| > capacity) защитными реле.
        5. Вычисление метрик: cascade_trips, dns_proxy, blackout.

    Аргументы:
        topo_data: Топология энергосистемы (GridTopology).
        k_fault: Число одновременно отключаемых линий при первичном ударе (N-k).
        seed: Целочисленное зерно генератора случайных чисел.
        max_steps: Максимальное число итераций распространения каскада.

    Возвращает:
        Словарь с результатами одного испытания.
    """
    rng = random.Random(seed)

    num_branches = len(topo_data.branches)
    if num_branches == 0:
        return {
            "seed": seed,
            "blackout": False,
            "cascade_trips": 0,
            "dns_proxy": 0.0,
            "failed_branches": [],
        }

    # Локальные копии состояния линий (не мутируют общий объект в разделяемой памяти!)
    active_branches: List[bool] = [True] * num_branches
    flows: List[float] = [abs(branch.flow) for branch in topo_data.branches]
    capacities: List[float] = [branch.capacity for branch in topo_data.branches]
    branch_endpoints: List[Tuple[int, int]] = [
        (branch.from_bus, branch.to_bus) for branch in topo_data.branches
    ]
    adjacency = topo_data.adjacency

    # Первичный шок (N-k авария)
    k_actual = min(max(1, k_fault), num_branches)
    initial_shock_branches: List[int] = rng.sample(range(num_branches), k_actual)

    for branch_id in initial_shock_branches:
        active_branches[branch_id] = False

    all_failed_branches: List[int] = list(initial_shock_branches)
    newly_tripped_branches: List[int] = list(initial_shock_branches)

    cascade_trips = 0
    lost_flow = 0.0
    step = 0

    while newly_tripped_branches and step < max_steps:
        step += 1
        current_tripped = newly_tripped_branches
        newly_tripped_branches = []

        for tripped_id in current_tripped:
            tripped_flow = flows[tripped_id]
            from_node, to_node = branch_endpoints[tripped_id]

            neighbor_candidates = (
                adjacency.get(from_node, []) + adjacency.get(to_node, [])
            )
            active_neighbors = list({
                neighbor_id
                for neighbor_id in neighbor_candidates
                if active_branches[neighbor_id] and neighbor_id != tripped_id
            })

            if not active_neighbors:
                lost_flow += tripped_flow
            else:
                reserves = [
                    max(0.0, capacities[neighbor_id] - flows[neighbor_id])
                    for neighbor_id in active_neighbors
                ]
                total_reserve = sum(reserves)

                if total_reserve > 1e-6:
                    for neighbor_id, reserve in zip(active_neighbors, reserves):
                        share = reserve / total_reserve
                        flows[neighbor_id] += tripped_flow * share
                else:
                    lost_flow += tripped_flow

        for branch_id in range(num_branches):
            if active_branches[branch_id] and flows[branch_id] > capacities[branch_id]:
                active_branches[branch_id] = False
                cascade_trips += 1
                all_failed_branches.append(branch_id)
                newly_tripped_branches.append(branch_id)

    base_power = max(
        1.0,
        float(topo_data.total_load),
        sum(abs(branch.flow) for branch in topo_data.branches),
    )
    dns_proxy = min(1.0, max(0.0, lost_flow / base_power))
    blackout = bool(dns_proxy > 0.05)

    return {
        "seed": seed,
        "blackout": blackout,
        "cascade_trips": cascade_trips,
        "dns_proxy": float(dns_proxy),
        "failed_branches": all_failed_branches,
    }


def simulate_trials_chunk_local(
    chunk_id: int,
    num_trials: int,
    base_seed: int,
    k_fault: int,
    topo_data: GridTopology,
) -> Dict[str, Any]:
    """
    Внутренняя функция выполнения пакета (чанка) испытаний.
    Вызывается как внутри удаленной задачи Ray, так и при калибровке одного ядра.
    """
    start_ts = time.perf_counter()

    num_branches = len(topo_data.branches)
    branch_trips: List[int] = [0] * num_branches

    blackouts_count = 0
    cascade_trips_sum = 0
    dns_sum = 0.0
    dns_max = 0.0

    for i in range(num_trials):
        trial_seed = base_seed + i
        trial_result = run_single_trial(
            topo_data=topo_data,
            k_fault=k_fault,
            seed=trial_seed,
        )

        if trial_result["blackout"]:
            blackouts_count += 1

        cascade_trips_sum += trial_result["cascade_trips"]
        dns_val = trial_result["dns_proxy"]
        dns_sum += dns_val
        if dns_val > dns_max:
            dns_max = dns_val

        for branch_id in trial_result["failed_branches"]:
            if 0 <= branch_id < num_branches:
                branch_trips[branch_id] += 1

    execution_time_sec = time.perf_counter() - start_ts

    return {
        "chunk_id": chunk_id,
        "num_trials": num_trials,
        "blackouts_count": blackouts_count,
        "cascade_trips_sum": cascade_trips_sum,
        "total_cascade_trips": cascade_trips_sum,
        "dns_sum": dns_sum,
        "dns_max": dns_max,
        "branch_trips": branch_trips,
        "branch_trip_counts": branch_trips,
        "worker_ip": _get_worker_node_ip(),
        "execution_time_sec": execution_time_sec,
    }


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Ray Task vs Ray Actor
# В Ray существуют две фундаментальные абстракции распределенных вычислений:
# 1. Ray Task (`@ray.remote def func(...)`):
#    - Stateless-задача (без сохранения внутреннего состояния между вызовами).
#    - Может быть запланирована на ЛЮБОМ свободном воркере в кластере.
#    - Идеальна для метода Монте-Карло (Embarrassingly Parallel), где каждый чанк
#      зависит только от входных параметров (`base_seed`, `k_fault`) и неизменяемой
#      топологии (`topology_ref`).
# 2. Ray Actor (`@ray.remote class Worker(...)`):
#    - Stateful-сервис (живет на конкретном выделенном процессе воркера и хранит состояние
#      в атрибутах `self.*` между вызовами методов).
#    - Вызовы методов одного актора выполняются последовательно.
#    - Используется для координаторов, агрегаторов метрик, серверов параметров или
#      удержания тяжелой GPU-модели в видеопамяти (Этап 3).
# Для генерации стохастических траекторий Монте-Карло мы выбираем именно Ray Tasks,
# чтобы планировщик Ray мог свободно балансировать чанки по всем узлам кластера.
# ============================================================================

# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Отказоустойчивость задач (Fault Tolerance & Retries)
# В реальном грид-кластере из десятков машин узлы могут внезапно отключаться:
# пропал Wi-Fi, закрыли крышку ноутбука, ОС убила процесс из-за нехватки памяти (OOM).
# Как Ray спасает расчет:
# 1. Механизм Heartbeats: каждый узел (Raylet) регулярно отправляет сигналы жизни в GCS
#    (Global Control Store на Head-узле). Если узел молчит — он помечается как Dead.
# 2. Lineage Reconstruction и `max_retries=3`: Ray помнит граф вызовов. Если воркер
#    упал во время расчета чанка №42, Ray автоматически перезапускает эту же задачу
#    на другом живом узле кластера (до 3 попыток).
# 3. Почему критичны детерминизм и идемпотентность: поскольку задача вычисляет зерна
#    строго по формуле `seed = base_seed + i` и не изменяет внешнее состояние,
#    повторный запуск чанка №42 на другом ноутбуке даст БИТ-В-БИТ тот же результат!
# ============================================================================

@ray.remote(max_retries=3, retry_exceptions=True)
def simulate_trials_chunk_remote(
    chunk_id: int,
    num_trials: int,
    base_seed: int,
    k_fault: int,
    topology_ref: ray.ObjectRef,
) -> dict[str, Any]:
    """
    Удаленная stateless-задача Ray (Ray Task) для моделирования пакета испытаний.

    При передаче `topology_ref: ray.ObjectRef` в `.remote(...)` среда исполнения Ray
    автоматически разрешает ссылку через локальный Object Store (Plasma Shared Memory)
    на узле-исполнителе без повторной пересылки графа с главного узла.

    Аргументы:
        chunk_id: Уникальный номер пакета задач.
        num_trials: Количество испытаний Монте-Карло в данном пакете.
        base_seed: Начальное зерно ГПСЧ для первого испытания пакета.
        k_fault: Количество отключаемых линий (параметр N-k).
        topology_ref: Ссылка на объект GridTopology в Ray Object Store (или сам объект,
                      автоматически разыменованный воркером Ray из разделяемой памяти).

    Возвращает:
        Компактный агрегированный словарь чанка:
        - chunk_id, num_trials, blackouts_count, cascade_trips_sum, dns_sum, branch_trips
        - метаданные выполнения (worker_ip, execution_time_sec).
    """
    # Если передан неразыменованный ObjectRef (например, при вложенной передаче),
    # извлекаем его из локального Object Store. При прямой передаче в .remote()
    # Ray автоматически передает уже разыменованный объект из shared memory.
    if isinstance(topology_ref, ray.ObjectRef):
        topo_data: GridTopology = ray.get(topology_ref)
    else:
        topo_data = topology_ref  # type: ignore[assignment]

    return simulate_trials_chunk_local(
        chunk_id=chunk_id,
        num_trials=num_trials,
        base_seed=base_seed,
        k_fault=k_fault,
        topo_data=topo_data,
    )
