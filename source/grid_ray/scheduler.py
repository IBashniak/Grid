"""
Модуль динамической диспетчеризации задач и защиты от отстающих узлов (Шаг 3).

Реализует класс DynamicRayScheduler, который разбивает пул испытаний Монте-Карло
на мелкие чанки (в 2–4 раза больше числа ядер кластера) и управляет ими через
неблокирующий вызов ray.wait(), устраняя простой быстрых узлов из-за Stragglers.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    from .ray_engine import simulate_trials_chunk_remote
except (ImportError, ValueError):
    if "ray_engine" in sys.modules and Path(getattr(sys.modules["ray_engine"], "__file__", "")).resolve().parent != CURRENT_DIR:
        del sys.modules["ray_engine"]
    from ray_engine import simulate_trials_chunk_remote  # type: ignore


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Проблема Stragglers и динамическая диспетчеризация
# Почему в гетерогенном грид-кластере нельзя просто разделить 100 000 задач поровну
# на 3 ноутбука и вызвать блокирующий барьер `ray.get(all_futures)`?
# 1. В грид-кластере объединяются РАЗНЫЕ машины: мощный игровой ПК (8 ядер, 4.5 ГГц)
#    и старый офисный ноутбук (2 ядра, фоновый антивирус, перегрев и троттлинг CPU).
# 2. Если раздать задачи поровну и вызвать жесткий барьер `ray.get(all_futures)`,
#    быстрый ПК завершит свою часть за 1 секунду и будет ПРОСТАИВАТЬ еще 9 секунд,
#    ожидая самого медленного участника — "отстающего узла" (Straggler).
# 3. Решение через `ray.wait`:
#    - Мы дробим работу на мелкие пакеты (в 2–4 раза больше суммарного числа ядер CPU).
#    - Выдаем стартовый пул задач по числу доступных воркеров.
#    - В цикле вызываем `ready_refs, remaining_refs = ray.wait(active_futures, num_returns=1, timeout=0.1)`.
#    - Как только быстрый воркер освобождается, мы мгновенно забираем его результат
#      и подбрасываем ему следующий чанк из очереди!
#    В итоге быстрый узел автоматически перерабатывает 75% чанков, медленный — 25%,
#    и все узлы финишируют почти одновременно без простоев (Dynamic Load Balancing).
# ============================================================================

class DynamicRayScheduler:
    """
    Динамический диспетчер задач для гетерогенного грид-кластера Ray.

    Обеспечивает:
    - Автоматический подбор размера чанка (oversubscription в 2–4 раза к числу ядер);
    - Постепенную подачу задач (Sliding Window / Pull-based scheduling);
    - Неблокирующий сбор результатов по мере готовности через ray.wait();
    - Сбор телеметрии: распределение выполненных чанков по IP-адресам узлов и временам.
    """

    def __init__(
        self,
        total_cpus: int,
        chunks_per_cpu_factor: int = 3,
        custom_chunk_size: Optional[int] = None,
    ) -> None:
        """
        Инициализирует диспетчер задач.

        Аргументы:
            total_cpus: Суммарное количество доступных CPU-ядер во всем кластере.
            chunks_per_cpu_factor: Коэффициент дробления (рекомендуется 2..4 чанка на ядро).
            custom_chunk_size: Явный размер чанка (если не задан, вычисляется автоматически).
        """
        self.total_cpus: int = max(1, int(total_cpus))
        # Ограничиваем коэффициент дробления разумным диапазоном (по умолчанию 3 — середина 2..4)
        self.chunks_per_cpu_factor: int = max(2, min(8, int(chunks_per_cpu_factor)))
        self.custom_chunk_size: Optional[int] = (
            max(1, int(custom_chunk_size)) if custom_chunk_size is not None else None
        )

    def compute_optimal_chunk_size(self, total_trials: int) -> int:
        """
        Вычисляет оптимальный размер одного пакета (чанка) испытаний так,
        чтобы общее количество чанков превышало число ядер кластера в 2–4 раза.
        """
        total_trials = max(1, int(total_trials))
        if self.custom_chunk_size is not None:
            return min(total_trials, self.custom_chunk_size)

        target_num_chunks = self.total_cpus * self.chunks_per_cpu_factor
        computed_size = math.ceil(total_trials / max(1, target_num_chunks))
        return max(1, computed_size)

    def build_chunk_plan(
        self,
        total_trials: int,
        base_seed: int = 42,
    ) -> Tuple[int, List[Tuple[int, int, int]]]:
        """
        Формирует очередь спецификаций чанков: (chunk_id, num_trials_in_chunk, chunk_seed).

        Гарантирует непересекающиеся диапазоны зерен ГПСЧ для каждого чанка.
        """
        total_trials = max(1, int(total_trials))
        chunk_size = self.compute_optimal_chunk_size(total_trials)

        chunk_plan: List[Tuple[int, int, int]] = []
        remaining = total_trials
        chunk_id = 0
        seed_offset = 0

        while remaining > 0:
            current_trials = min(chunk_size, remaining)
            chunk_seed = base_seed + seed_offset
            chunk_plan.append((chunk_id, current_trials, chunk_seed))

            remaining -= current_trials
            seed_offset += current_trials
            chunk_id += 1

        return chunk_size, chunk_plan

    def execute(
        self,
        total_trials: int,
        k_fault: int,
        topology_ref: ray.ObjectRef,
        base_seed: int = 42,
    ) -> Dict[str, Any]:
        """
        Выполняет распределенный пул задач с динамической балансировкой через ray.wait().

        Аргументы:
            total_trials: Общее количество испытаний Монте-Карло.
            k_fault: Число одновременно отключаемых линий (N-k).
            topology_ref: Ссылка ray.ObjectRef на топологию сети в Object Store.
            base_seed: Базовое зерно генератора случайных чисел.

        Возвращает:
            Словарь с результатами всех чанков и телеметрией планировщика.
        """
        chunk_size, pending_chunks = self.build_chunk_plan(
            total_trials=total_trials,
            base_seed=base_seed,
        )
        total_chunks_count = len(pending_chunks)

        # Размер стартового окна активных задач:
        # Подаем не все задачи разом, а поддерживаем буфер в ~1.5x..2x от числа ядер,
        # чтобы по мере завершения задач динамически подбрасывать новые из очереди.
        max_in_flight = max(2, min(total_chunks_count, self.total_cpus * 2))

        active_futures: List[ray.ObjectRef] = []
        next_chunk_idx = 0

        # 1. Отправка начального пула задач воркерам
        while next_chunk_idx < max_in_flight and next_chunk_idx < total_chunks_count:
            c_id, c_trials, c_seed = pending_chunks[next_chunk_idx]
            future_ref = simulate_trials_chunk_remote.remote(
                c_id,
                c_trials,
                c_seed,
                k_fault,
                topology_ref,
            )
            active_futures.append(future_ref)
            next_chunk_idx += 1

        completed_results: List[Dict[str, Any]] = []
        node_distribution: Dict[str, int] = {}
        chunk_timings_sec: List[float] = []

        # 2. Неблокирующий цикл опроса готовых задач (борьба со Stragglers)
        while active_futures:
            ready_refs, remaining_refs = ray.wait(
                active_futures,
                num_returns=1,
                timeout=0.1,
            )
            active_futures = list(remaining_refs)

            for ready_ref in ready_refs:
                # Немедленно забираем готовый результат освободившегося воркера
                chunk_result: Dict[str, Any] = ray.get(ready_ref)
                completed_results.append(chunk_result)

                # Сбор метаданных о выполнении чанка
                worker_ip = str(chunk_result.get("worker_ip", "127.0.0.1"))
                node_distribution[worker_ip] = node_distribution.get(worker_ip, 0) + 1

                duration = float(chunk_result.get("execution_time_sec", 0.0))
                chunk_timings_sec.append(duration)

                # Если в очереди еще остались нераспределенные чанки —
                # сразу подбрасываем освободившемуся воркеру новую задачу
                if next_chunk_idx < total_chunks_count:
                    c_id, c_trials, c_seed = pending_chunks[next_chunk_idx]
                    new_future_ref = simulate_trials_chunk_remote.remote(
                        c_id,
                        c_trials,
                        c_seed,
                        k_fault,
                        topology_ref,
                    )
                    active_futures.append(new_future_ref)
                    next_chunk_idx += 1

        # Сортируем результаты по chunk_id для детерминированного порядка агрегации
        completed_results.sort(key=lambda item: int(item["chunk_id"]))

        return {
            "chunk_results": completed_results,
            "chunk_size": chunk_size,
            "num_chunks": total_chunks_count,
            "node_distribution": node_distribution,
            "chunk_timings_sec": chunk_timings_sec,
        }
