"""
Пакет grid-mimd: Параллельная симуляция каскадных аварий в энергосети (MIMD на CPU).

Данный пакет реализует Этап 1 сквозного проекта:
- Загрузка топологии энергосистемы IEEE-118;
- Изолированное моделирование каскадных отключений методом Монте-Карло;
- Параллельное выполнение независимых испытаний через multiprocessing.Pool;
- Измерение ускорения (Speedup) и сохранение метрик надежности.
"""

try:
    from .topology import Branch, Bus, GridTopology, build_topology_from_data
    from .engine import run_single_trial, simulate_trials_chunk
    from .benchmark import run_benchmark
except (ImportError, ValueError):
    from topology import Branch, Bus, GridTopology, build_topology_from_data
    from engine import run_single_trial, simulate_trials_chunk
    from benchmark import run_benchmark

__all__ = [
    "Branch",
    "Bus",
    "GridTopology",
    "build_topology_from_data",
    "run_single_trial",
    "simulate_trials_chunk",
    "run_benchmark",
]
