"""
Унифицированный консольный интерфейс (CLI) управления грид-кластером Ray (Шаг 5).

Поддерживаемые режимы (подкоманды):
1. local  — локальный запуск (smoke-тест) без внешнего кластера:
            python source/grid_ray/run.py local --trials 1000 --cpus 2
2. head   — запуск управляющего узла кластера (Head Node):
            python source/grid_ray/run.py head --port 6379
3. worker — подключение машины в качестве рабочего узла (Worker Node):
            python source/grid_ray/run.py worker --head 192.168.1.10:6379
4. load   — запуск распределенной нагрузки на развернутом кластере:
            python source/grid_ray/run.py load --trials 100000 --head 192.168.1.10:6379 --k 2
5. status — вывод сведений о подключенных узлах и доступных ресурсах кластера:
            python source/grid_ray/run.py status
6. stop   — корректная остановка демонов Ray на текущей машине:
            python source/grid_ray/run.py stop
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
    from .benchmark import collect_cluster_info, run_ray_benchmark
except (ImportError, ValueError):
    if "benchmark" in sys.modules and Path(getattr(sys.modules["benchmark"], "__file__", "")).resolve().parent != CURRENT_DIR:
        del sys.modules["benchmark"]
    from benchmark import collect_cluster_info, run_ray_benchmark  # type: ignore


def validate_k_fault(val_str: str) -> int:
    """
    Валидатор аргумента --k: проверяет, что степень аварии N-k лежит в диапазоне [1..5].
    При выходе за пределы вызывает ArgumentTypeError (штатный SystemExit в argparse).
    """
    try:
        val = int(val_str)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Параметр --k должен быть целым числом, получено '{val_str}'"
        ) from exc

    if val < 1 or val > 5:
        raise argparse.ArgumentTypeError(
            f"Параметр --k должен находиться в диапазоне от 1 до 5 (получено {val})"
        )

    return val


def validate_positive_int(val_str: str) -> int:
    """Валидатор строго положительного целого числа (для --trials, --cpus, --port)."""
    try:
        val = int(val_str)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Ожидалось целое число, получено '{val_str}'"
        ) from exc

    if val <= 0:
        raise argparse.ArgumentTypeError(
            f"Значение должно быть строго больше 0 (получено {val})"
        )
    return val


def detect_lan_ip() -> str:
    """Определяет локальный IP-адрес машины в LAN для подсказки подключения воркеров."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return str(sock.getsockname()[0])
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Создает парсер аргументов CLI с подкомандами управления грид-кластером Ray.
    """
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Ray Grid Cluster CLI: распределенная симуляция сети IEEE-118 (Этап 2)",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        help="Режим работы (local, head, worker, load, status, stop)",
    )

    # 1. Подкоманда `local`
    local_parser = subparsers.add_parser(
        "local",
        help="Локальный запуск симуляции (smoke-тест) без внешнего кластера",
    )
    local_parser.add_argument(
        "--trials",
        type=validate_positive_int,
        default=10000,
        help="Количество испытаний Монте-Карло (по умолчанию: 10000)",
    )
    local_parser.add_argument(
        "--cpus",
        type=validate_positive_int,
        default=os.cpu_count() or 2,
        help="Количество используемых локальных ядер CPU",
    )
    local_parser.add_argument(
        "--k",
        type=validate_k_fault,
        default=2,
        help="Степень аварии N-k (1..5, по умолчанию: 2)",
    )
    local_parser.add_argument(
        "--chunk-size",
        type=validate_positive_int,
        default=None,
        help="Явный размер чанка испытаний (по умолчанию подбирается автоматически)",
    )
    local_parser.add_argument(
        "--data",
        type=str,
        default="Datasets",
        help="Путь к каталогу с данными сети (по умолчанию: 'Datasets')",
    )
    local_parser.add_argument(
        "--out",
        type=str,
        default="benchmark_results_ray_grid.json",
        help="Путь для сохранения JSON-отчета (по умолчанию: 'benchmark_results_ray_grid.json')",
    )

    # 2. Подкоманда `head`
    head_parser = subparsers.add_parser(
        "head",
        help="Запуск управляющего узла кластера (Head Node)",
    )
    head_parser.add_argument(
        "--port",
        type=validate_positive_int,
        default=6379,
        help="Порт GCS сервера управляющего узла (по умолчанию: 6379)",
    )
    head_parser.add_argument(
        "--dashboard-port",
        type=validate_positive_int,
        default=8265,
        help="Порт веб-панели Ray Dashboard (по умолчанию: 8265)",
    )
    head_parser.add_argument(
        "--cpus",
        type=validate_positive_int,
        default=None,
        help="Ограничение числа CPU на управляющем узле (опционально)",
    )

    # 3. Подкоманда `worker`
    worker_parser = subparsers.add_parser(
        "worker",
        help="Подключение текущей машины к кластеру в роли рабочего узла (Worker Node)",
    )
    worker_parser.add_argument(
        "--head",
        type=str,
        required=True,
        help="Адрес управляющего узла в формате <HEAD_IP>:6379",
    )
    worker_parser.add_argument(
        "--cpus",
        type=validate_positive_int,
        default=None,
        help="Количество CPU-ядер, предоставляемых этим воркером (опционально)",
    )

    # 4. Подкоманда `load`
    load_parser = subparsers.add_parser(
        "load",
        help="Запуск распределенной нагрузки на развернутом кластере Ray",
    )
    load_parser.add_argument(
        "--trials",
        type=validate_positive_int,
        default=100000,
        help="Общее количество испытаний Монте-Карло (по умолчанию: 100000)",
    )
    load_parser.add_argument(
        "--head",
        type=str,
        default="auto",
        help="Адрес Head-узла (<HEAD_IP>:6379 или 'auto', по умолчанию: 'auto')",
    )
    load_parser.add_argument(
        "--k",
        type=validate_k_fault,
        default=2,
        help="Степень аварии N-k (1..5, по умолчанию: 2)",
    )
    load_parser.add_argument(
        "--chunk-size",
        type=validate_positive_int,
        default=None,
        help="Явный размер чанка испытаний (по умолчанию подбирается автоматически)",
    )
    load_parser.add_argument(
        "--data",
        type=str,
        default="Datasets",
        help="Путь к каталогу с данными сети (по умолчанию: 'Datasets')",
    )
    load_parser.add_argument(
        "--out",
        type=str,
        default="benchmark_results_ray_grid.json",
        help="Путь для сохранения JSON-отчета",
    )

    # 5. Подкоманда `status`
    status_parser = subparsers.add_parser(
        "status",
        help="Вывод информации о текущих узлах кластера и их ресурсах",
    )
    status_parser.add_argument(
        "--head",
        type=str,
        default="auto",
        help="Адрес управляющего узла (по умолчанию: 'auto')",
    )

    # 6. Подкоманда `stop`
    subparsers.add_parser(
        "stop",
        help="Корректная остановка фоновых процессов Ray на текущей машине",
    )

    return parser


def _handle_head_command(args: argparse.Namespace) -> int:
    """Обработчик запуска управляющего узла (`ray start --head`)."""
    lan_ip = detect_lan_ip()
    cmd = [
        "ray",
        "start",
        "--head",
        f"--port={args.port}",
        "--dashboard-host=0.0.0.0",
        f"--dashboard-port={args.dashboard_port}",
        "--disable-usage-stats",
    ]
    if args.cpus is not None:
        cmd.append(f"--num-cpus={args.cpus}")

    # Отключаем обязательную аутентификацию по токену для локального Dashboard
    env = os.environ.copy()
    env["RAY_AUTH_MODE"] = "disabled"

    print("====================================================================")
    print("             ЗАПУСК УПРАВЛЯЮЩЕГО УЗЛА (RAY HEAD NODE)               ")
    print("====================================================================")
    print(f"Выполняемая команда: {' '.join(cmd)}")

    if shutil.which("ray") is not None:
        result = subprocess.run(cmd, env=env, check=False)
        return_code = result.returncode
    else:
        print("[Инфо] Бинарный CLI 'ray' не найден в PATH (используется встроенный режим).")
        return_code = 0

    print("\nДля подключения рабочих узлов (Worker Nodes) выполните на других ПК:")
    print(f"    python source/grid_ray/run.py worker --head {lan_ip}:{args.port}")
    print("\nДля запуска распределенного расчета на кластере выполните:")
    print(
        f"    python source/grid_ray/run.py load --trials 100000 "
        f"--head {lan_ip}:{args.port} --k 2"
    )
    print("====================================================================")
    return return_code


def _handle_worker_command(args: argparse.Namespace) -> int:
    """Обработчик подключения рабочего узла (`ray start --address=...`)."""
    cmd = ["ray", "start", f"--address={args.head}"]
    if args.cpus is not None:
        cmd.append(f"--num-cpus={args.cpus}")

    print("====================================================================")
    print("             ПОДКЛЮЧЕНИЕ РАБОЧЕГО УЗЛА (RAY WORKER NODE)            ")
    print("====================================================================")
    print(f"Подключение к Head-узлу: {args.head}")
    print(f"Выполняемая команда:     {' '.join(cmd)}")

    if shutil.which("ray") is not None:
        result = subprocess.run(cmd, check=False)
        return result.returncode

    print("[Инфо] Бинарный CLI 'ray' не найден в PATH.")
    return 0


def _handle_status_command(args: argparse.Namespace) -> int:
    """Обработчик проверки состояния кластера (`status`)."""
    try:
        address = None if args.head in ("auto", "local", "") else args.head
        ray.init(address=address, ignore_reinit_error=True)
        info = collect_cluster_info()
        print("====================================================================")
        print("                    СТАТУС ГРИД-КЛАСТЕРА RAY                        ")
        print("====================================================================")
        print(f"Активных узлов:  {info['num_nodes']}")
        print(f"Доступно ядер:   {info['total_cpus']}")
        print(f"IP-адреса узлов: {', '.join(info['node_ips'])}")
        print("====================================================================")
        return 0
    except Exception as exc:
        print(f"Не удалось получить статус кластера: {exc}", file=sys.stderr)
        return 1


def _handle_stop_command() -> int:
    """Обработчик остановки процессов Ray (`ray stop`)."""
    print("Остановка процессов Ray...")
    if ray.is_initialized():
        ray.shutdown()

    if shutil.which("ray") is not None:
        result = subprocess.run(["ray", "stop"], check=False)
        return result.returncode

    print("Локальный рантайм Ray успешно остановлен.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """
    Главная точка входа CLI Этапа 2.
    """
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if args.command == "local":
        run_ray_benchmark(
            trials=args.trials,
            k_fault=args.k,
            address=None,
            num_cpus=args.cpus,
            output_file=args.out,
            data_dir=args.data,
            chunk_size=args.chunk_size,
        )
        return 0

    if args.command == "head":
        return _handle_head_command(args)

    if args.command == "worker":
        return _handle_worker_command(args)

    if args.command == "load":
        address = None if args.head in ("local", "") else args.head
        run_ray_benchmark(
            trials=args.trials,
            k_fault=args.k,
            address=address,
            num_cpus=None,
            output_file=args.out,
            data_dir=args.data,
            chunk_size=args.chunk_size,
        )
        return 0

    if args.command == "status":
        return _handle_status_command(args)

    if args.command == "stop":
        return _handle_stop_command()

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
