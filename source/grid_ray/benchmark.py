"""
Модуль бенчмаркинга и распределенных метрик кластера Ray (Шаг 4).

Отвечает за:
- Инициализацию подключения к кластеру Ray или локальному рантайму;
- Инспекцию ресурсов кластера (число живых узлов, суммарные CPU, IP-адреса);
- Помещение топологии сети в Ray Object Store (ray.put);
- Запуск динамического планировщика DynamicRayScheduler;
- Расчет Throughput, Speedup, Cluster Efficiency и оценку по закону Густафсона-Барсиса;
- Экспорт итогового отчета в JSON (benchmark_results_ray_grid.json).
"""

from __future__ import annotations

import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    from .ray_engine import simulate_trials_chunk_local
    from .scheduler import DynamicRayScheduler
    from .topology import (
        GridTopology,
        build_topology_from_data,
        prepare_cluster_topology,
    )
except (ImportError, ValueError):
    for _mod_name in ("topology", "ray_engine", "scheduler"):
        if _mod_name in sys.modules and Path(getattr(sys.modules[_mod_name], "__file__", "")).resolve().parent != CURRENT_DIR:
            del sys.modules[_mod_name]
    from ray_engine import simulate_trials_chunk_local  # type: ignore
    from scheduler import DynamicRayScheduler  # type: ignore
    from topology import (  # type: ignore
        GridTopology,
        build_topology_from_data,
        prepare_cluster_topology,
    )


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Закон Густафсона-Барсиса (Gustafson-Barsis's Law)
# Почему закон Амдала пессимистичен для распределенных грид-кластеров?
# 1. Закон Амдала предполагает ФИКСИРОВАННЫЙ размер задачи (например, N = 1 000 испытаний).
#    При малом N накладные расходы на инициализацию Ray, сеть и сериализацию (последовательная
#    доля s) занимают заметную часть времени, поэтому при добавлении 100 ядер ускорение
#    упирается в потолок 1/s.
# 2. Закон Густафсона-Барсиса смотрит на реальную практику суперкомпьютеров:
#    с ростом числа процессоров P исследователи увеличивают НЕ скорость маленькой задачи,
#    а МАСШТАБ самой задачи (N -> 10^6 .. 10^7 испытаний для оценки редких хвостов блэкаутов):
#        S_scaled(P) = s + P * (1 - s) = P - s * (P - 1)
# 3. Когда объем вычислений растет пропорционально размеру кластера, доля сетевых накладных
#    расходов s стремится к нулю (s -> 0), а масштабируемое ускорение растет почти линейно
#    с числом ядер во всем грид-кластере!
# ============================================================================

def collect_cluster_info(fallback_cpus: Optional[int] = None) -> Dict[str, Any]:
    """
    Собирает информацию о подключенных узлах кластера Ray:
    количество активных узлов, общее число ядер CPU и список IP-адресов.
    """
    raw_nodes = ray.nodes() if hasattr(ray, "nodes") else []
    alive_nodes = [node for node in raw_nodes if node.get("Alive", True)]

    if not alive_nodes and raw_nodes:
        alive_nodes = list(raw_nodes)

    node_ips: List[str] = []
    nodes_cpu_sum = 0

    for node in alive_nodes:
        ip = str(
            node.get("NodeManagerAddress")
            or node.get("node_ip_address")
            or "127.0.0.1"
        )
        if ip not in node_ips:
            node_ips.append(ip)
        resources = node.get("Resources", {}) or {}
        nodes_cpu_sum += int(resources.get("CPU", 0))

    if not node_ips:
        node_ips = ["127.0.0.1"]

    cluster_res = ray.cluster_resources() if hasattr(ray, "cluster_resources") else {}
    total_cpus = int(cluster_res.get("CPU", 0)) or nodes_cpu_sum
    if total_cpus <= 0:
        total_cpus = int(fallback_cpus if fallback_cpus is not None else (os.cpu_count() or 1))

    num_nodes = max(1, len(alive_nodes))

    return {
        "num_nodes": num_nodes,
        "total_cpus": max(1, total_cpus),
        "node_ips": node_ips,
    }


def run_ray_benchmark(
    trials: int = 10000,
    k_fault: int = 2,
    address: Optional[str] = None,
    num_cpus: Optional[int] = None,
    output_file: str | Path = "benchmark_results_ray_grid.json",
    data_dir: Optional[str | Path] = "Datasets",
    base_seed: int = 42,
    chunk_size: Optional[int] = None,
    topology: Optional[GridTopology] = None,
    num_trials: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Выполняет распределенный бенчмарк симуляции энергосистемы IEEE-118 на кластере Ray.

    Шаги:
        1. Инициализирует подключение к Ray (`ray.init`).
        2. Собирает топологию кластера (`ray.nodes()`, число CPU, IP узлов).
        3. Загружает топологию сети и помещает в Ray Object Store (`ray.put`).
        4. Проводит быструю калибровку 1 ядра для расчета `speedup_vs_single_cpu`.
        5. Запускает распределенный расчет через `DynamicRayScheduler` (`ray.wait`).
        6. Рассчитывает метрики производительности, закон Густафсона и надежность сети.
        7. Сохраняет отчет в JSON-файл.

    Возвращает:
        Словарь с полным отчетом бенчмарка.
    """
    if num_trials is not None:
        trials = num_trials

    trials = max(1, int(trials))
    k_fault = max(1, min(5, int(k_fault)))

    # 1. Инициализация Ray
    # Если задан num_cpus в локальном режиме и Ray уже был запущен с другим числом ядер,
    # переинициализируем локальный рантайм для точного соблюдения параметров.
    init_kwargs: Dict[str, Any] = {"ignore_reinit_error": True}
    if address is not None:
        init_kwargs["address"] = address
    if num_cpus is not None and address is None:
        init_kwargs["num_cpus"] = int(num_cpus)

    ray.init(**init_kwargs)

    # 2. Сбор сведений о кластере
    cluster_info = collect_cluster_info(fallback_cpus=num_cpus)
    total_cpus = cluster_info["total_cpus"]
    num_nodes = cluster_info["num_nodes"]
    node_ips = cluster_info["node_ips"]

    print("====================================================================")
    print("      БЕНЧМАРК RAY GRID CLUSTER: МОДЕЛИРОВАНИЕ СЕТИ IEEE-118        ")
    print("====================================================================")
    print(f"Режим кластера:       {'Распределенный (' + address + ')' if address else 'Локальный кластер Ray'}")
    print(f"Активных узлов:       {num_nodes} ({', '.join(node_ips)})")
    print(f"Всего ядер (CPUs):    {total_cpus}")
    print(f"Всего испытаний:      {trials:,}")
    print(f"Степень аварии (k):   {k_fault}")
    print("--------------------------------------------------------------------")

    # 3. Загрузка топологии и помещение в Ray Object Store (Zero-Copy)
    if topology is None:
        topology = build_topology_from_data(data_dir=data_dir)

    topo_ref: ray.ObjectRef = prepare_cluster_topology(topology)

    print(
        f"Топология в Object Store: {topology.num_buses} узлов, "
        f"{topology.num_branches} ветвей, ref={topo_ref}"
    )

    # 4. Калибровочный замер 1 локального ядра (T_1) для оценки ускорения
    calib_trials = min(500, trials)
    calib_start = time.perf_counter()
    simulate_trials_chunk_local(
        chunk_id=-1,
        num_trials=calib_trials,
        base_seed=base_seed,
        k_fault=k_fault,
        topo_data=topology,
    )
    calib_elapsed = max(1e-6, time.perf_counter() - calib_start)
    estimated_single_cpu_sec = calib_elapsed * (trials / calib_trials)

    # 5. Распределенное выполнение через DynamicRayScheduler (ray.wait)
    scheduler = DynamicRayScheduler(
        total_cpus=total_cpus,
        chunks_per_cpu_factor=3,
        custom_chunk_size=chunk_size,
    )

    start_time = time.perf_counter()
    schedule_output = scheduler.execute(
        total_trials=trials,
        k_fault=k_fault,
        topology_ref=topo_ref,
        base_seed=base_seed,
    )
    execution_time_sec = max(1e-6, time.perf_counter() - start_time)

    chunk_results: List[Dict[str, Any]] = schedule_output["chunk_results"]
    effective_chunk_size: int = schedule_output["chunk_size"]
    node_distribution: Dict[str, int] = schedule_output["node_distribution"]

    # 6. Расчет метрик производительности кластера
    throughput_trials_per_sec = trials / execution_time_sec
    speedup_vs_single_cpu = estimated_single_cpu_sec / execution_time_sec
    cluster_efficiency_percent = (speedup_vs_single_cpu / max(1, total_cpus)) * 100.0

    # 7. Агрегация физических метрик надежности энергосети
    total_blackouts = sum(int(chunk["blackouts_count"]) for chunk in chunk_results)
    blackout_probability = total_blackouts / trials

    combined_branch_trips = [0] * topology.num_branches
    for chunk in chunk_results:
        trips_list = chunk.get("branch_trips") or chunk.get("branch_trip_counts") or []
        for branch_idx, count in enumerate(trips_list):
            combined_branch_trips[branch_idx] += int(count)

    sorted_branches = sorted(
        range(topology.num_branches),
        key=lambda idx: combined_branch_trips[idx],
        reverse=True,
    )
    top_critical_branches = sorted_branches[:5]

    print("--------------------------------------------------------------------")
    print(f"Выполнено чанков:        {schedule_output['num_chunks']} (размер чанка ~{effective_chunk_size})")
    print(f"Распределение по узлам:  {node_distribution}")
    print(f"Время кластера (T_ray):  {execution_time_sec:.4f} сек")
    print(f"Пропускная способность:  {throughput_trials_per_sec:,.2f} траекторий/сек")
    print(f"Ускорение (Speedup):     {speedup_vs_single_cpu:.2f}x")
    print(f"Эффективность кластера:  {cluster_efficiency_percent:.1f}%")
    print(f"Вероятность блэкаута:    {blackout_probability * 100:.3f}%")
    print(f"Топ-5 критических линий: {top_critical_branches}")
    print("====================================================================")

    # 8. Формирование структуры JSON-отчета строго по спецификации Этапа 2
    report: Dict[str, Any] = {
        "benchmark_type": "Ray_Grid_Cluster",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "cluster_info": {
            "num_nodes": num_nodes,
            "total_cpus": total_cpus,
            "node_ips": node_ips,
        },
        "config": {
            "trials": trials,
            "chunk_size": effective_chunk_size,
            "k_fault": k_fault,
        },
        "performance": {
            "execution_time_sec": round(execution_time_sec, 4),
            "throughput_trials_per_sec": round(throughput_trials_per_sec, 2),
            "speedup_vs_single_cpu": round(speedup_vs_single_cpu, 2),
            "cluster_efficiency_percent": round(cluster_efficiency_percent, 2),
        },
        "metrics": {
            "blackout_probability": round(blackout_probability, 6),
            "top_critical_branches": top_critical_branches,
        },
    }

    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as file_handle:
        json.dump(report, file_handle, indent=2, ensure_ascii=False)

    print(f"JSON-отчет сохранен в: {out_path.resolve()}\n")
    return report
