"""
Модуль бенчмаркинга и параллельного оркестратора (Шаг 4).

Отвечает за:
- Разделение пула задач на сбалансированные пакеты (чанкование);
- Запуск параллельного пула процессов через multiprocessing.Pool;
- Замер времени выполнения (time.perf_counter) и расчет ускорения (Speedup);
- Агрегацию метрик надежности энергосети и экспорт результатов в JSON.
"""

from __future__ import annotations

import datetime
import json
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from .engine import simulate_trials_chunk
    from .topology import GridTopology, build_topology_from_data
except (ImportError, ValueError):
    from engine import simulate_trials_chunk
    from topology import GridTopology, build_topology_from_data


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] GIL (Global Interpreter Lock)
# Почему в Python для CPU-bound задач мы берем multiprocessing, а не threading?
# В стандартной реализации CPython существует глобальная блокировка интерпретатора (GIL).
# Она предотвращает одновременное выполнение байткода Python несколькими потоками
# одного процесса.
# Если запустить CPU-емкую симуляцию Монте-Карло на 4 потоках (threading.Thread),
# все 4 потока будут поочередно бороться за один GIL на одном ядре CPU. Скорость
# выполнения не только не вырастет, но даже упадет из-за накладных расходов
# на постоянное переключение контекста!
# Multiprocessing создает независимые системные процессы операционной системы.
# У каждого процесса — своя память, свой интерпретатор и свой собственный GIL.
# Это позволяет задействовать все физические ядра CPU на 100% (MIMD параллелизм).
# ============================================================================

# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Закон Амдала (Amdahl's Law)
# Почему ускорение при 8 воркерах никогда не составит ровно 8.0x?
# Согласно закону Амдала, максимальное ускорение системы ограничено ее
# последовательной частью (Serial Fraction, s = 1 - p):
# Speedup <= 1 / (s + (1 - s) / N)
# В нашей программе есть последовательные шаги, которые не масштабируются:
# 1. Загрузка топологии сети и дисковый ввод-вывод.
# 2. Создание пула процессов (системные вызовы fork / spawn).
# 3. Сериализация (pickle) входных данных и десериализация ответов воркеров (IPC).
# 4. Финальная агрегация массивов и запись JSON-отчета на диск.
# Даже если параллелизуемая часть составляет 95% (p = 0.95), а последовательная 5% (s = 0.05),
# при 8 ядрах теоретический предел составит: 1 / (0.05 + 0.95 / 8) ≈ 5.92x (но не 8x).
# ============================================================================

def partition_trials(num_trials: int, num_workers: int) -> List[int]:
    """
    Разделяет общее число испытаний num_trials на равные пакеты (chunks).

    Равномерно распределяет остаток от деления между первыми воркерами,
    исключая дисбаланс нагрузки (load imbalance).
    """
    base_chunk_size = num_trials // num_workers
    remainder = num_trials % num_workers

    chunk_sizes: List[int] = []
    for worker_index in range(num_workers):
        current_chunk = base_chunk_size + (1 if worker_index < remainder else 0)
        if current_chunk > 0:
            chunk_sizes.append(current_chunk)

    return chunk_sizes


def run_benchmark(
    num_trials: int = 10000,
    num_workers: Optional[int] = None,
    k_fault: int = 1,
    data_dir: Optional[str | Path] = "Datasets",
    output_file: str | Path = "benchmark_results_mimd_grid.json",
    base_seed: int = 42,
    topology: Optional[GridTopology] = None,
) -> Dict[str, Any]:
    """
    Выполняет полный цикл бенчмаркинга параллельной симуляции (MIMD на CPU).

    Шаги:
        1. Загрузка или использование переданной топологии сети IEEE-118.
        2. Формирование сбалансированных пакетов задач (task chunks).
        3. Замер одноядерного базового времени T_1 (для вычисления Speedup).
        4. Параллельный запуск задач в multiprocessing.Pool(num_workers).
        5. Расчет метрик производительности (время, Throughput, Speedup)
           и надежности сети (вероятность блэкаута, критические линии).
        6. Экспорт структурированного отчета в JSON.

    Возвращает:
        Словарь с результирующим JSON-отчетом.
    """
    # Определение числа воркеров
    available_cpus = os.cpu_count() or 1
    workers = max(1, num_workers if num_workers is not None else available_cpus)
    num_trials = max(1, num_trials)
    k_fault = max(1, min(5, k_fault))

    print("====================================================================")
    print("      БЕНЧМАРК MIMD НА CPU: МОДЕЛИРОВАНИЕ АВАРИЙ В ЭНЕРГОСЕТИ       ")
    print("====================================================================")
    print(f"Архитектура:          MIMD (CPU Multiprocessing Pool)")
    print(f"Число воркеров:       {workers} (Доступно CPU: {available_cpus})")
    print(f"Всего испытаний:      {num_trials:,}")
    print(f"Степень аварии (k):   {k_fault}")
    print("--------------------------------------------------------------------")

    # 1. Загрузка топологии сети
    if topology is None:
        print("Загрузка топологии сети IEEE-118...")
        topology = build_topology_from_data(data_dir=data_dir)

    print(
        f"Топология готова: {topology.num_buses} узлов, "
        f"{topology.num_branches} ветвей, суммарная нагрузка: {topology.total_load:.2f} МВт"
    )
    print("--------------------------------------------------------------------")

    # 2. Формирование пакетов (чанкование)
    chunk_sizes = partition_trials(num_trials=num_trials, num_workers=workers)
    tasks: List[Tuple[int, int, int, int, GridTopology]] = []
    seed_offset = 0

    for chunk_id, chunk_size in enumerate(chunk_sizes):
        task_tuple = (chunk_id, chunk_size, base_seed + seed_offset, k_fault, topology)
        tasks.append(task_tuple)
        seed_offset += chunk_size

    # 3. Измерение базового времени 1 ядра (T_1) для расчета Speedup
    time_single_core = 0.0
    if workers == 1:
        # При одном воркере T_1 будет измерено напрямую в основном прогоне
        pass
    else:
        # Для расчета Speedup замеряем скорость одного ядра на калибровочной выборке
        calibration_trials = min(1000, num_trials)
        calib_start = time.perf_counter()
        simulate_trials_chunk((0, calibration_trials, base_seed, k_fault, topology))
        calib_elapsed = time.perf_counter() - calib_start
        # Экстраполируем T_1 на полное число испытаний
        time_single_core = calib_elapsed * (num_trials / calibration_trials)

    # 4. Основной замер параллельного выполнения (T_n)
    print(f"Запуск {len(tasks)} пакетов задач на {workers} процессах...")
    start_time = time.perf_counter()

    if workers == 1:
        # Выполняем в текущем процессе без IPC оверхеда
        chunk_results = [simulate_trials_chunk(tasks[0])]
    else:
        # Параллельный запуск пула процессов
        with multiprocessing.Pool(processes=workers) as pool:
            chunk_results = pool.map(simulate_trials_chunk, tasks)

    end_time = time.perf_counter()
    execution_time_sec = end_time - start_time

    if workers == 1:
        time_single_core = execution_time_sec

    # 5. Расчет метрик производительности
    throughput = num_trials / max(1e-6, execution_time_sec)
    speedup = time_single_core / max(1e-6, execution_time_sec)
    # Защита: ускорение при 1 воркере всегда равно 1.0
    if workers == 1:
        speedup = 1.0

    # 6. Агрегация результатов испытаний
    total_blackouts = sum(chunk["blackouts_count"] for chunk in chunk_results)
    blackout_probability = total_blackouts / num_trials

    # Подсчет отключений по каждой линии электропередачи
    combined_branch_trips = [0] * topology.num_branches
    for chunk in chunk_results:
        for branch_idx, count in enumerate(chunk["branch_trip_counts"]):
            combined_branch_trips[branch_idx] += count

    # Определение Топ-5 наиболее уязвимых линий
    sorted_branch_indices = sorted(
        range(topology.num_branches),
        key=lambda idx: combined_branch_trips[idx],
        reverse=True,
    )
    top_critical_branches = sorted_branch_indices[:5]

    print("--------------------------------------------------------------------")
    print(f"Время выполнения (T_{workers}): {execution_time_sec:.4f} сек")
    print(f"Пропускная способность:  {throughput:,.2f} траекторий/сек")
    print(f"Ускорение (Speedup):     {speedup:.2f}x")
    print(f"Вероятность блэкаута:    {blackout_probability * 100:.3f}%")
    print(f"Топ-5 критических линий: {top_critical_branches}")
    print("====================================================================")

    # 7. Формирование структуры JSON согласно ТЗ
    report: Dict[str, Any] = {
        "benchmark_type": "MIMD_CPU_Grid",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "config": {
            "trials": num_trials,
            "workers": workers,
            "k_fault": k_fault,
        },
        "performance": {
            "execution_time_sec": round(execution_time_sec, 4),
            "throughput_trials_per_sec": round(throughput, 2),
            "speedup": round(speedup, 2),
        },
        "metrics": {
            "blackout_probability": round(blackout_probability, 6),
            "top_critical_branches": top_critical_branches,
        },
    }

    # Сохранение отчета на диск
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as file_handle:
        json.dump(report, file_handle, indent=2, ensure_ascii=False)

    print(f"Отчет успешно сохранен в: {out_path.resolve()}\n")
    return report
