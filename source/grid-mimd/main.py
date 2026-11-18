"""
Консольная точка входа (CLI) для Этапа 1 (Шаг 5).

Пример использования:
    python source/grid-mimd/main.py --trials 10000 --workers 4 --k 2
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Обеспечиваем корректный импорт модулей пакета при запуске как скрипта
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Импорт оркестратора бенчмарка
try:
    from .benchmark import run_benchmark
except (ImportError, ValueError):
    from benchmark import run_benchmark


def validate_k_fault(val_str: str) -> int:
    """
    Валидатор аргумента --k: проверяет, что число аварий лежит в диапазоне [1..5].
    При выходе за границы генерирует ошибку ArgumentTypeError,
    что приводит к штатному завершению argparse с кодом ошибки SystemExit.
    """
    try:
        val = int(val_str)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Значение --k должно быть целым числом, получено '{val_str}'") from exc

    if val < 1 or val > 5:
        raise argparse.ArgumentTypeError(f"Параметр --k должен быть в диапазоне от 1 до 5 (получено {val})")

    return val


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Создает и настраивает парсер аргументов командной строки.
    """
    parser = argparse.ArgumentParser(
        description="MIMD Monte Carlo симуляция каскадных аварий в энергосети IEEE-118 (Этап 1)"
    )

    parser.add_argument(
        "--trials",
        type=int,
        default=10000,
        help="Общее количество испытаний методом Монте-Карло (по умолчанию: 10000)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 1,
        help=f"Количество параллельных процессов воркеров (по умолчанию: {os.cpu_count() or 1})",
    )
    parser.add_argument(
        "--k",
        type=validate_k_fault,
        default=1,
        help="Степень первичного возмущения N-k: количество отключаемых линий (1..5, по умолчанию: 1)",
    )
    parser.add_argument(
        "--data",
        type=str,
        default="Datasets",
        help="Путь к каталогу с данными сети (по умолчанию: 'Datasets')",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="benchmark_results_mimd_grid.json",
        help="Путь к выходному JSON-файлу с результатами (по умолчанию: 'benchmark_results_mimd_grid.json')",
    )

    return parser


def main(argv: list[str] | None = None) -> None:
    """
    Главная точка входа в консольную утилиту.
    """
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if args.trials <= 0:
        parser.error("Параметр --trials должен быть строго больше 0")

    if args.workers <= 0:
        parser.error("Параметр --workers должен быть строго больше 0")

    run_benchmark(
        num_trials=args.trials,
        num_workers=args.workers,
        k_fault=args.k,
        data_dir=args.data,
        output_file=args.out,
    )


# ============================================================================
# [DEEP DIVE / СЛОЖНАЯ КОНЦЕПЦИЯ] Блок if __name__ == '__main__':
# Почему этот блок жизненно необходим при работе с multiprocessing?
# При создании новых процессов через механизм 'spawn' (по умолчанию в Windows и macOS,
# а также опционально в Linux) дочерний процесс запускает интерпретатор Python заново
# и заново импортирует главный файл скрипта.
# Если не защитить точку входа условием `if __name__ == '__main__':`, каждый
# дочерний процесс при импорте снова выполнит код `main()`, создавая новые процессы
# в бесконечной рекурсивной прогрессии (т.н. fork bomb), что приведет к зависанию ОС.
# ============================================================================
if __name__ == "__main__":
    main()
