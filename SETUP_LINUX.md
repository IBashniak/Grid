# ПРОМПТ / РУКОВОДСТВО: Пошаговая настройка окружения для Linux (Ubuntu / Debian / Mint)

> **Роль для ИИ:** Ты выступаешь в роли Senior DevOps / Linux Infrastructure Engineer и ментора. Твоя задача — подготовить, установить и проверить 100% готовность локального окружения для выполнения всех 5 этапов сквозного семестрового проекта (IEEE-118 Monte Carlo Grid).
>
> **Целевая аудитория:** Junior-разработчик. Все команды должны быть готовыми к выполнению (copy-paste), снабженными проверками версий (healthchecks) и пояснениями, зачем нужен каждый компонент.

---

## 🗺️ Обзор необходимых компонентов (Этапы 1–5)

| Этап | Необходимые инструменты | Назначение |
| :---: | :--- | :--- |
| **Этап 1 (MIMD)** | `python 3.11-3.13`, `numpy`, `scipy`, `h5py`, `pytest` | Локальные параллельные вычисления на CPU |
| **Этап 2 (Ray Grid)** | `ray[default]>=2.9`, `net-tools`, `ufw` порты (6379, 8265) | Локальный и сетевой грид-кластер Ray |
| **Этап 3 (Docker)** | `docker-ce`, `docker-compose-plugin` | Контейнеризация, Web UI + API + Redis |
| **Этап 4 (K8s)** | `kubectl`, `minikube` или `kind` (K8s in Docker) | Локальная оркестрация и автоскейлинг Jobs |
| **Этап 5 (Chaos/FinOps)** | `jaegertracing/all-in-one`, `opentelemetry-*`, `stress-ng` | Трассировка, стресс-тесты, расчет Spot TCO |

---

## 📋 Пошаговый план установки

```text
[Шаг 1: Системные пакеты и Git]
             │
             ▼
[Шаг 2: Python и Conda-окружение 'grid']
             │
             ▼
[Шаг 3: Docker Engine и Docker Compose]
             │
             ▼
[Шаг 4: Локальный Kubernetes (kubectl + kind/minikube)]
             │
             ▼
[Шаг 5: Сеть и Firewall для Ray (порты 6379, 8265)]
             │
             ▼
[Шаг 6: Инструменты трассировки (Jaeger) и Chaos]
             │
             ▼
[Шаг 7: Автоматизированный скрипт полной проверки (Healthcheck)]
```

---

### Шаг 1. Обновление системы и базовые утилиты

Установим компиляторы, утилиты для работы с сетью и архивами:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y \
    curl \
    wget \
    git \
    build-essential \
    pkg-config \
    net-tools \
    iproute2 \
    ca-certificates \
    gnupg \
    lsb-release \
    htop \
    stress-ng
```

*Проверка:*
```bash
git --version
curl --version
```

---

### Шаг 2. Настройка Python и Conda-окружения (`grid`)

Для изоляции зависимостей рекомендуется использовать **Miniconda** или **Anaconda**. 

#### 2.1. Установка Miniconda (если еще не установлена)
```bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh
source ~/miniconda3/bin/activate
conda init --all
```

#### 2.2. Создание и активация изолированного окружения `grid`
```bash
conda create -n grid python=3.12 -y
conda activate grid
```

#### 2.3. Установка Python-библиотек для всех 5 этапов
```bash
pip install --upgrade pip

# Библиотеки для Этапа 1 (Численные методы, матрицы, тесты)
pip install numpy scipy h5py networkx matplotlib pytest

# Библиотеки для Этапа 2 (Ray Grid)
pip install "ray[default]>=2.9.0" psutil aiohttp

# Библиотеки для Этапа 3 (FastAPI, Redis для сервисной архитектуры)
pip install fastapi uvicorn redis pydantic requests

# Библиотеки для Этапа 5 (OpenTelemetry и Jaeger трейсинг)
pip install \
    opentelemetry-api \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp \
    opentelemetry-instrumentation-fastapi
```

*Проверка:*
```bash
python -c "import numpy, scipy, h5py, ray, pytest; print('Python окружение: OK')"
ray --version
```

---

### Шаг 3. Установка Docker Engine и Docker Compose (Этап 3)

Устанавливаем официальный **Docker Community Edition** (без тяжелого GUI, с максимальной производительностью ядра Linux):

```bash
# 1. Добавление официального GPG ключа Docker
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# 2. Добавление репозитория в источники apt
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update

# 3. Установка Docker Engine, CLI и плагина Docker Compose
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 4. Разрешение запуска docker без sudo (добавление текущего пользователя в группу docker)
sudo usermod -aG docker $USER
```

> **Важно:** Чтобы изменения группы `docker` вступили в силу, выполните команду:
> ```bash
> newgrp docker
> ```
> (или перезайдите в терминал/систему).

*Проверка:*
```bash
docker --version
docker compose version
docker run --rm hello-world
```

---

### Шаг 4. Установка локального Kubernetes (Этап 4)

Для локального выполнения K8s-манифестов без платных облаков установим официальный клиент `kubectl` и сверхлегковесный кластер **`kind` (Kubernetes in Docker)** или **`minikube`**.

#### 4.1. Установка `kubectl` (CLI для управления K8s)
```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl
```

#### 4.2. Установка `kind` (Kubernetes in Docker — рекомендуемый быстрый выбор)
`kind` запускает узлы кластера K8s как обычные легковесные Docker-контейнеры, не требуя виртуализации:
```bash
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.22.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
```

*Проверка:*
```bash
kubectl version --client
kind --version
```

---

### Шаг 5. Настройка сети и Firewall для Ray Grid (Этап 2)

Если вы будете подключать физические ноутбуки по локальной сети (LAN / Wi-Fi), управляющему узлу (Head Node) требуется открыть доступ к порту координатора GCS (`6379`), веб-дашборду (`8265`) и диапазону портов воркеров:

```bash
# Если в вашей системе активен ufw (Ubuntu Firewall):
sudo ufw allow 6379/tcp comment "Ray GCS"
sudo ufw allow 8265/tcp comment "Ray Dashboard"
sudo ufw allow 10000:10100/tcp comment "Ray Worker Ports"
```

Команда для быстрого определения вашего IP в локальной сети:
```bash
hostname -I | awk '{print $1}'
```

---

### Шаг 6. Настройка Jaeger UI для Observability (Этап 5)

Для распределенной трассировки каскадных аварий подгрузим официальный легковесный Docker-образ Jaeger:

```bash
docker pull jaegertracing/all-in-one:latest
```

*Тестовый запуск Jaeger (в фоновом режиме):*
```bash
docker run -d --name test-jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```
- Откройте в браузере: `http://localhost:16686` — должен отобразиться дашборд Jaeger.
- Остановка тестового контейнера:
```bash
docker stop test-jaeger && docker rm test-jaeger
```

---

## 🚀 Шаг 7. Автоматизированный скрипт валидации (Healthcheck)

Скопируйте и выполните этот однострочник для полной проверки готовности всей инфраструктуры:

```bash
cat << 'EOF' > /tmp/check_env.sh
#!/bin/bash
set -e
echo "=========================================================="
echo "          ПРОВЕРКА ГОТОВНОСТИ ОКРУЖЕНИЯ (LINUX)          "
echo "=========================================================="

check_cmd() {
    if command -v $1 &>/dev/null; then
        echo -e "[OK] $1: $($1 --version 2>&1 | head -n 1)"
    else
        echo -e "[FAIL] $1 НЕ УСТАНОВЛЕН!"
        exit 1
    fi
}

check_cmd git
check_cmd python3
check_cmd pip
check_cmd docker
check_cmd kubectl
check_cmd kind

echo "----------------------------------------------------------"
echo "Проверка Python модулей в conda-окружении..."
python3 -c "
mods = ['numpy', 'scipy', 'h5py', 'networkx', 'pytest', 'ray', 'fastapi', 'opentelemetry']
for m in mods:
    __import__(m)
    print(f'  [OK] {m}')
"

echo "----------------------------------------------------------"
echo "Проверка запуска Docker без sudo..."
docker info >/dev/null && echo "  [OK] Docker Daemon доступен без sudo"

echo "=========================================================="
echo " 🎉 ВСЕ КОМПОНЕНТЫ УСПЕШНО УСТАНОВЛЕНЫ И ГОТОВЫ К РАБОТЕ! "
echo "=========================================================="
EOF
chmod +x /tmp/check_env.sh && /tmp/check_env.sh
```
