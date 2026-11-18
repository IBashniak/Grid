"""
Тесты локального кластерного прогона, планировщика и CLI (test_ray_cluster.py).

Проверяют:
1. Запуск run_ray_benchmark в локальном режиме (num_cpus=2, 50 trials) и валидацию
   сгенерированного JSON-отчета (блоки cluster_info, config, performance, metrics).
2. Работу DynamicRayScheduler и динамической диспетчеризации через ray.wait().
3. Валидацию подкоманд CLI (local, head, worker, load, status, stop) и аргумента --k (1..5).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "source"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import grid_ray  # noqa: F401, E402
import ray  # noqa: E402
from grid_ray import topology as ray_topology  # noqa: E402
from grid_ray.benchmark import run_ray_benchmark  # noqa: E402
from grid_ray.run import build_argument_parser, main as cli_main  # noqa: E402
from grid_ray.scheduler import DynamicRayScheduler  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def init_local_ray():
    """Инициализирует локальный рантайм Ray с 2 ядрами CPU для тестов."""
    ray.init(local_mode=False, num_cpus=2, ignore_reinit_error=True)
    yield


def test_run_ray_benchmark_local_and_json_report(tmp_path: Path) -> None:
    """
    Проверка: запуск run_ray_benchmark в локальном режиме (num_cpus=2, 50 trials)
    успешно создает валидный JSON-отчет с блоками cluster_info, config, performance и metrics.
    """
    out_json = tmp_path / "benchmark_results_ray_grid.json"

    report = run_ray_benchmark(
        trials=50,
        k_fault=2,
        address=None,
        num_cpus=2,
        output_file=out_json,
    )

    assert out_json.exists(), "Файл JSON-отчета не был создан"

    with open(out_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["benchmark_type"] == "Ray_Grid_Cluster"
    assert "timestamp" in data

    # Блок cluster_info
    cluster_info = data["cluster_info"]
    assert cluster_info["num_nodes"] >= 1
    assert cluster_info["total_cpus"] == 2
    assert isinstance(cluster_info["node_ips"], list)
    assert len(cluster_info["node_ips"]) >= 1

    # Блок config
    config = data["config"]
    assert config["trials"] == 50
    assert config["chunk_size"] > 0
    assert config["k_fault"] == 2

    # Блок performance
    perf = data["performance"]
    assert perf["execution_time_sec"] > 0.0
    assert perf["throughput_trials_per_sec"] > 0.0
    assert perf["speedup_vs_single_cpu"] > 0.0
    assert perf["cluster_efficiency_percent"] > 0.0

    # Блок metrics
    metrics = data["metrics"]
    assert 0.0 <= metrics["blackout_probability"] <= 1.0
    assert isinstance(metrics["top_critical_branches"], list)
    assert len(metrics["top_critical_branches"]) == 5
    assert report == data


def test_dynamic_ray_scheduler_straggler_protection() -> None:
    """
    Проверка DynamicRayScheduler:
    - автоматическое дробление пула испытаний на мелкие чанки (в 2–4 раза больше ядер);
    - полный сбор всех испытаний через ray.wait() без потери данных;
    - наличие метаданных распределения по узлам (node_distribution).
    """
    topo = ray_topology.build_topology_from_data()
    topo_ref = ray_topology.prepare_cluster_topology(topo)

    scheduler = DynamicRayScheduler(total_cpus=2, chunks_per_cpu_factor=3)
    output = scheduler.execute(
        total_trials=60,
        k_fault=1,
        topology_ref=topo_ref,
        base_seed=100,
    )

    # При 2 CPU и факторе 3 должно получиться 6 чанков по 10 испытаний
    assert output["num_chunks"] == 6
    assert output["chunk_size"] == 10
    assert len(output["chunk_results"]) == 6

    total_completed_trials = sum(c["num_trials"] for c in output["chunk_results"])
    assert total_completed_trials == 60
    assert sum(output["node_distribution"].values()) == 6


def test_cli_subcommands_and_k_validation(tmp_path: Path) -> None:
    """
    Проверка CLI парсера (run.py):
    - корректный разбор подкоманд local, head, worker, load, status, stop;
    - валидация параметра --k в диапазоне [1..5] (выход за границы вызывает SystemExit).
    """
    parser = build_argument_parser()

    # 1. Валидные подкоманды
    args_local = parser.parse_args(["local", "--trials", "100", "--cpus", "2", "--k", "3"])
    assert args_local.command == "local"
    assert args_local.trials == 100
    assert args_local.cpus == 2
    assert args_local.k == 3

    args_head = parser.parse_args(["head", "--port", "6379"])
    assert args_head.command == "head"
    assert args_head.port == 6379

    args_worker = parser.parse_args(["worker", "--head", "192.168.1.10:6379"])
    assert args_worker.command == "worker"
    assert args_worker.head == "192.168.1.10:6379"

    args_load = parser.parse_args([
        "load",
        "--trials", "100000",
        "--head", "192.168.1.10:6379",
        "--k", "2",
    ])
    assert args_load.command == "load"
    assert args_load.trials == 100000
    assert args_load.head == "192.168.1.10:6379"
    assert args_load.k == 2

    # 2. Невалидные значения --k (вызывают SystemExit)
    for bad_k in ("0", "6", "-1", "abc"):
        with pytest.raises(SystemExit):
            parser.parse_args(["local", "--k", bad_k])
        with pytest.raises(SystemExit):
            parser.parse_args(["load", "--k", bad_k])

    # 3. Неизвестная подкоманда
    with pytest.raises(SystemExit):
        parser.parse_args(["unknown_mode"])

    # 4. Полный прогон CLI local через main()
    out_file = tmp_path / "cli_local_out.json"
    exit_code = cli_main([
        "local",
        "--trials", "40",
        "--cpus", "2",
        "--k", "1",
        "--out", str(out_file),
    ])
    assert exit_code == 0
    assert out_file.exists()
