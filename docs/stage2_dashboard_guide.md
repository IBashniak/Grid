# Руководство по мониторингу симуляции через веб-интерфейс Ray Dashboard (Этап 2)

В экосистеме **Ray** встроен полноценный веб-интерфейс телеметрии и мониторинга в реальном времени — **Ray Dashboard**. В консольном интерфейсе [run.py](file:///home/ihor/Business/University/Igor-grid/source/grid_ray/run.py) для него по умолчанию настроен порт **`8265`** (`--dashboard-port 8265`).

---

## 1. Что можно увидеть в Ray Dashboard во время прогона симуляции

После открытия `http://localhost:8265` в браузере вам доступны следующие вкладки:

1. **Cluster (Узлы кластера и ресурсы):**
   - Список всех подключенных узлов (`Head Node` и `Worker Nodes`) с их IP-адресами;
   - Загрузка каждого ядра CPU в реальном времени (в процентах);
   - Потребление оперативной памяти (RAM) и заполнение разделяемой памяти **Object Store (Plasma)**, куда через `ray.put()` помещается наш граф `GridTopology` (118 узлов, 186 линий).
2. **Jobs / Tasks (Задачи и чанки):**
   - Живой прогресс-бар выполнения удаленных задач `simulate_trials_chunk_remote`;
   - Распределение чанков по состояниям: `RUNNING` (выполняются сейчас), `FINISHED` (завершены), `PENDING` (ожидают свободного ядра в очереди `DynamicRayScheduler`);
   - Наглядная демонстрация борьбы со **Stragglers**: видно, как быстрые узлы разбирают больше чанков через `ray.wait()`.
3. **Actors / Logs (Логи и диагностика):**
   - Стандартный вывод (`stdout` / `stderr`) с каждого рабочего ноутбука кластера;
   - Счетчики повторных попыток (`Retries`) при сбоях воркеров.

---

## 2. Предварительное требование

Для работы веб-интерфейса должен быть установлен пакет `ray[default]` (модификатор `[default]` устанавливает веб-компоненты Dashboard; базовый `pip install ray` ставит только вычислительное ядро без UI):

```bash
pip install "ray[default]"
```

---

## 3. Пошаговый ручной запуск

1. **Запустите управляющий узел (Head Node) с веб-панелью:**
   ```bash
   python3 source/grid_ray/run.py head --port 6379 --dashboard-port 8265
   ```
2. **Откройте веб-интерфейс в браузере:**
   - Локально: [http://localhost:8265](http://localhost:8265) (или `http://127.0.0.1:8265`)
   - Из локальной сети (с другого ноутбука): `http://<LAN_IP_главного_узла>:8265`
3. **Запустите длительную нагрузку**, чтобы успеть рассмотреть графики и движение задач в реальном времени (при малом числе испытаний вроде `1000` симуляция завершается быстрее чем за секунду):
   ```bash
   python3 source/grid_ray/run.py load --trials 500000 --head 127.0.0.1:6379 --k 2
   ```
4. **Остановите кластер после завершения работы:**
   ```bash
   python3 source/grid_ray/run.py stop
   ```

---

## 4. Готовый скрипт автоматического запуска с веб-интерфейсом

Этот скрипт также сохранен в проекте по пути [run_with_dashboard.sh](file:///home/ihor/Business/University/Igor-grid/source/grid_ray/run_with_dashboard.sh). Он автоматически:
1. Проверяет наличие `ray`;
2. Поднимает `Head Node` с веб-панелью на порту `8265`;
3. Открывает браузер на странице `http://127.0.0.1:8265`;
4. Запускает нагрузку Монте-Карло (`500 000` испытаний по умолчанию);
5. Оставляет Dashboard открытым для изучения графиков до нажатия `Enter`, после чего корректно останавливает кластер.

### Код скрипта (`source/grid_ray/run_with_dashboard.sh`):

```bash
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

# 3. Запуск управляющего узла (Head Node) с Ray Dashboard
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
```

### Как запустить скрипт одной командой:
```bash
bash source/grid_ray/run_with_dashboard.sh 500000 2
```
