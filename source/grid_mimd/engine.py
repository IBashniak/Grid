"""
Движок моделирования каскадных аварий методом Монте-Карло (Шаг 2 и Шаг 3).

Содержит:
- run_single_trial: расчет одной изолированной реализации каскадной аварии;
- simulate_trials_chunk: воркер для пакетного выполнения в multiprocessing.Pool.
"""

from __future__ import annotations

import random
from typing import Any, Dict, List, Optional, Tuple

try:
    from .topology import GridTopology
except (ImportError, ValueError):
    from topology import GridTopology


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Изоляция Seed в процессах
# Почему нельзя вызывать глобальный `random.seed()` в параллельных процессах?
# 1. При создании процессов через fork() дочерний процесс полностью копирует память
#    родителя, включая внутреннее состояние генератора `random`. Если не инициализировать
#    генератор индивидуально, все дочерние процессы выдадут одинаковые последовательности!
# 2. Глобальный random разделяет состояние внутри процесса, что мешает повторяемости.
# 3. Решение: создавать локальный экземпляр `random.Random(seed)` для каждого испытания,
#    где `seed = base_seed + trial_index`. Это гарантирует 100% изоляцию,
#    детерминизм и независимость параллельных симуляций.
# ============================================================================

def run_single_trial(
    topo_data: GridTopology,
    k_fault: int,
    seed: int,
    max_steps: int = 20,
) -> Dict[str, Any]:
    """
    Выполняет одно независимое испытание каскадной аварии в энергосистеме (N-k shock).

    Алгоритм:
        1. Инициализация детерминированного локального RNG.
        2. Копирование состояния линий (активность и потоки) в быстрые списки.
        3. Первичный удар: отключение ровно k_fault случайных активных линий.
        4. Каскадный цикл: перераспределение потоков на соседей по их свободному резерву
           (capacity - flow) и отключение перегруженных линий (|flow| > capacity).
        5. Расчет метрик (cascade_trips, dns_proxy, blackout).

    Аргументы:
        topo_data: Топология сети (объект GridTopology).
        k_fault: Число одновременно отключаемых линий при первичном ударе (N-k).
        seed: Целочисленное зерно для изоляции генератора псевдослучайных чисел.
        max_steps: Максимальное число шагов распространения каскада (защита от зацикливания).

    Возвращает:
        Словарь с метриками завершения:
            - seed: использованное зерно;
            - blackout: флаг системного блэкаута (dns_proxy > 0.05);
            - cascade_trips: число линий, отключившихся ПОСЛЕ первичного удара;
            - dns_proxy: доля потерянной мощности (0.0..1.0);
            - failed_branches: полный список индексов всех отключившихся линий.
    """
    # 1. Локальный изолированный генератор случайных чисел
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

    # 2. Легковесные локальные структуры для быстрого доступа
    active_branches: List[bool] = [True] * num_branches
    flows: List[float] = [abs(branch.flow) for branch in topo_data.branches]
    capacities: List[float] = [branch.capacity for branch in topo_data.branches]
    branch_endpoints: List[Tuple[int, int]] = [
        (branch.from_bus, branch.to_bus) for branch in topo_data.branches
    ]
    adjacency = topo_data.adjacency

    # 3. Первичный шок (N-k авария): отключаем ровно k_fault случайных линий
    k_actual = min(max(1, k_fault), num_branches)
    initial_shock_branches: List[int] = rng.sample(range(num_branches), k_actual)

    for branch_id in initial_shock_branches:
        active_branches[branch_id] = False

    all_failed_branches: List[int] = list(initial_shock_branches)
    newly_tripped_branches: List[int] = list(initial_shock_branches)

    # 4. Каскадный цикл
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

            # Ищем смежные активные линии через оба узла инцидентности
            neighbor_candidates = (
                adjacency.get(from_node, []) + adjacency.get(to_node, [])
            )
            # Убираем дубликаты, саму отключенную линию и неактивные линии
            active_neighbors = list({
                neighbor_id
                for neighbor_id in neighbor_candidates
                if active_branches[neighbor_id] and neighbor_id != tripped_id
            })

            if not active_neighbors:
                # Если смежных активных путей нет, поток теряется
                lost_flow += tripped_flow
            else:
                # Считаем свободный резерв каждой активной соседней линии: (capacity - flow)
                reserves = [
                    max(0.0, capacities[neighbor_id] - flows[neighbor_id])
                    for neighbor_id in active_neighbors
                ]
                total_reserve = sum(reserves)

                if total_reserve > 1e-6:
                    # Пропорциональное перераспределение потерянного потока
                    for neighbor_id, reserve in zip(active_neighbors, reserves):
                        share = reserve / total_reserve
                        flows[neighbor_id] += tripped_flow * share
                else:
                    # Резерва у соседей нет (все работают на пределе) — поток теряется
                    lost_flow += tripped_flow

        # Проверка перегрузок: если |flow| > capacity, защитное реле отключает линию
        for branch_id in range(num_branches):
            if active_branches[branch_id] and flows[branch_id] > capacities[branch_id]:
                active_branches[branch_id] = False
                cascade_trips += 1
                all_failed_branches.append(branch_id)
                newly_tripped_branches.append(branch_id)

    # 5. Метрики завершения
    # Суммарная база для оценки доли отключенной мощности (Demand Not Served proxy)
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


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Сериализация (Pickle IPC)
# Почему simulate_trials_chunk должна быть функцией верхнего уровня?
# multiprocessing.Pool передает задачи в дочерние процессы через IPC (Inter-Process
# Communication). Для этого Python использует pickle:
# 1. pickle не умеет сериализовать вложенные функции (closures) и лямбда-выражения,
#    так как дочерний процесс должен иметь возможность импортировать функцию по ее
#    полному имени `module.function_name`.
# 2. Передача гигантских структур данных (например, полного графа на каждое испытание)
#    приведет к тому, что процессорное время уйдет не на вычисления, а на упаковку
#    и распаковку данных (Pickle Overhead).
# 3. Поэтому воркер принимает один компактный кортеж `args_tuple` и возвращает
#    агрегированную статистику чанка, минимизируя трафик IPC.
# ============================================================================

def simulate_trials_chunk(args_tuple: Tuple[Any, ...]) -> Dict[str, Any]:
    """
    Воркер верхнего уровня для параллельного выполнения пакета (chunk) испытаний.

    Принимает кортеж аргументов:
        args_tuple: (chunk_id, num_trials, base_seed, k_fault, topo_data)

    Выполняет цикл по испытаниям с детерминированным расчетом зерен:
        seed = base_seed + i

    Возвращает:
        Агрегированный словарь с результатами пакета:
            - chunk_id: идентификатор пакета;
            - num_trials: число выполненных испытаний;
            - blackouts_count: количество сценариев блэкаута;
            - total_cascade_trips: суммарное число каскадных отключений;
            - dns_sum: сумма значений dns_proxy (для среднего);
            - dns_max: максимальное значение dns_proxy;
            - branch_trip_counts: список частоты отключения каждой линии [len(branches)].
    """
    chunk_id, num_trials, base_seed, k_fault, topo_data = args_tuple

    num_branches = len(topo_data.branches)
    branch_trip_counts: List[int] = [0] * num_branches

    blackouts_count = 0
    total_cascade_trips = 0
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

        total_cascade_trips += trial_result["cascade_trips"]
        dns = trial_result["dns_proxy"]
        dns_sum += dns
        if dns > dns_max:
            dns_max = dns

        for branch_id in trial_result["failed_branches"]:
            if 0 <= branch_id < num_branches:
                branch_trip_counts[branch_id] += 1

    return {
        "chunk_id": chunk_id,
        "num_trials": num_trials,
        "blackouts_count": blackouts_count,
        "total_cascade_trips": total_cascade_trips,
        "dns_sum": dns_sum,
        "dns_max": dns_max,
        "branch_trip_counts": branch_trip_counts,
    }
