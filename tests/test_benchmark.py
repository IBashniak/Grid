"""
Тесты модуля бенчмаркинга и консольного интерфейса (benchmark.py и main.py).

Проверяют:
1. Корректность запуска run_benchmark, расчет метрик и структуру JSON-отчета.
2. Валидацию аргументов CLI (передача некорректного k вызывает SystemExit).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Подключение пути к исходному коду
SOURCE_DIR = Path(__file__).resolve().parents[1] / "source" / "grid_mimd"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from benchmark import run_benchmark
from main import build_argument_parser, main, validate_k_fault


def test_benchmark_execution_and_json_structure(tmp_path: Path) -> None:
    """
    Проверка: запуск run_benchmark на малой выборке (50 trials, 2 воркера)
    успешно выполняется и сохраняет валидный JSON-файл со структурой согласно ТЗ.
    """
    out_file = tmp_path / "test_benchmark_report.json"

    result_report = run_benchmark(
        num_trials=50,
        num_workers=2,
        k_fault=1,
        output_file=out_file,
    )

    # 1. Проверяем наличие созданного файла
    assert out_file.exists(), f"Файл отчета {out_file} не был создан"

    # 2. Читаем и валидируем содержимое JSON
    with open(out_file, "r", encoding="utf-8") as f:
        loaded_json = json.load(f)

    # Проверка корневых ключей
    assert loaded_json["benchmark_type"] == "MIMD_CPU_Grid"
    assert "timestamp" in loaded_json

    # Проверка блока config
    assert loaded_json["config"]["trials"] == 50
    assert loaded_json["config"]["workers"] == 2
    assert loaded_json["config"]["k_fault"] == 1

    # Проверка блока performance
    perf = loaded_json["performance"]
    assert perf["execution_time_sec"] > 0.0
    assert perf["throughput_trials_per_sec"] > 0.0
    assert perf["speedup"] > 0.0

    # Проверка блока metrics
    metrics = loaded_json["metrics"]
    assert 0.0 <= metrics["blackout_probability"] <= 1.0
    assert isinstance(metrics["top_critical_branches"], list)
    assert len(metrics["top_critical_branches"]) <= 5


def test_cli_validation_invalid_k() -> None:
    """
    Проверка валидации CLI:
    Передача некорректного значения --k (например, 10 или 0)
    должна приводить к ошибке аргументов и вызову SystemExit.
    """
    parser = build_argument_parser()

    # Сценарий: --k 10 (больше 5)
    with pytest.raises(SystemExit):
        parser.parse_args(["--k", "10"])

    # Сценарий: --k 0 (меньше 1)
    with pytest.raises(SystemExit):
        parser.parse_args(["--k", "0"])

    # Сценарий: --k abc (не число)
    with pytest.raises(SystemExit):
        parser.parse_args(["--k", "abc"])

    # Прямая проверка функции main с передачей списка argv
    with pytest.raises(SystemExit):
        main(["--k", "10"])


def test_cli_valid_arguments() -> None:
    """
    Проверка корректного разбора валидных аргументов командной строки.
    """
    parser = build_argument_parser()
    args = parser.parse_args([
        "--trials", "5000",
        "--workers", "4",
        "--k", "3",
        "--data", "CustomDatasets",
        "--out", "custom_results.json",
    ])

    assert args.trials == 5000
    assert args.workers == 4
    assert args.k == 3
    assert args.data == "CustomDatasets"
    assert args.out == "custom_results.json"
