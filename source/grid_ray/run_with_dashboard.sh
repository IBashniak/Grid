#!/usr/bin/env bash
# ==============================================================================
# Скрипт запуска симуляции IEEE-118 (Этап 2) с веб-интерфейсом Ray Dashboard
# Использование:
#   bash source/grid_ray/run_with_dashboard.sh [TRIALS] [K_FAULT]
# Пример:
#   bash source/grid_ray/run_with_dashboard.sh 500000 2
# ==============================================================================

set -euo pipefail

TRIALS="${1:-500000}"
K_FAULT="${2:-2}"
RAY_PORT=6379
DASHBOARD_PORT=8265
DASHBOARD_URL="http://127.0.0.1:${DASHBOARD_PORT}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_ROOT}"

echo "======================================================================"
echo "   ЗАПУСК RAY GRID CLUSTER + ВЕБ-ИНТЕРФЕЙС RAY DASHBOARD (ЭТАП 2)     "
echo "======================================================================"

# 1. Проверка наличия установленного пакета ray[default]
if ! python3 -c "import ray" >/dev/null 2>&1; then
    echo "[!] Пакет 'ray' не найден в текущем окружении Python."
    echo "    Для работы веб-панели установите его командой:"
    echo "    pip install \"ray[default]\""
    exit 1
fi

# 2. Очистка предыдущей сессии (если была запущена)
python3 source/grid_ray/run.py stop >/dev/null 2>&1 || true

# 3. Запуск управляющего узла (Head Node) с Ray Dashboard (без запроса токена и статистики)
echo "[1/4] Запуск Head Node (GCS порт: ${RAY_PORT}, Dashboard порт: ${DASHBOARD_PORT})..."
RAY_AUTH_MODE=disabled python3 source/grid_ray/run.py head --port "${RAY_PORT}" --dashboard-port "${DASHBOARD_PORT}"

echo ""
echo "[2/4] Веб-интерфейс Ray Dashboard доступен по адресу:"
echo "      👉  ${DASHBOARD_URL}"
echo ""

# Пытаемся автоматически открыть страницу в браузере (Linux / macOS)
if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${DASHBOARD_URL}" >/dev/null 2>&1 &
elif command -v open >/dev/null 2>&1; then
    open "${DASHBOARD_URL}" >/dev/null 2>&1 &
fi

sleep 2

# 4. Запуск распределенной симуляции
echo "[3/4] Запуск симуляции Монте-Карло (trials=${TRIALS}, k=${K_FAULT})..."
python3 source/grid_ray/run.py load \
    --trials "${TRIALS}" \
    --head "127.0.0.1:${RAY_PORT}" \
    --k "${K_FAULT}"

echo ""
echo "======================================================================"
echo " Расчет завершен! Вы можете изучить историю выполнения и графики в:"
echo " 👉  ${DASHBOARD_URL}"
echo "======================================================================"
read -r -p "[4/4] Нажмите Enter, чтобы остановить кластер Ray и закрыть Dashboard... " _

python3 source/grid_ray/run.py stop
echo "Кластер Ray остановлен."
