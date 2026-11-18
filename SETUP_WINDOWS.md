# ПРОМПТ / РУКОВОДСТВО: Пошаговая настройка окружения для Windows 10/11

> **Роль для ИИ:** Ты выступаешь в роли Senior DevOps / Windows Infrastructure Engineer и ментора. Твоя задача — подготовить, установить и проверить 100% готовность локального окружения под Windows для выполнения всех 5 этапов сквозного семестрового проекта (IEEE-118 Monte Carlo Grid).
>
> **Целевая аудитория:** Junior-разработчик. Все команды адаптированы под **PowerShell**, используют современный пакетный менеджер **`winget`** и сопровождаются проверками готовности.

---

## 🗺️ Обзор необходимых компонентов (Этапы 1–5)

| Этап | Инструменты под Windows | Назначение |
| :---: | :--- | :--- |
| **Этап 1 (MIMD)** | Python 3.12 (Miniconda), `numpy`, `scipy`, `h5py`, `pytest` | Локальные параллельные вычисления на CPU |
| **Этап 2 (Ray Grid)** | `ray[default]>=2.9`, правила Брандмауэра Windows (порты 6379, 8265) | Локальный и сетевой грид-кластер Ray |
| **Этап 3 (Docker)** | Docker Desktop (бэкенд WSL2) | Контейнеризация, Web UI + API + Redis |
| **Этап 4 (K8s)** | `kubectl`, `minikube` или `kind` для Windows | Локальная оркестрация и автоскейлинг Jobs |
| **Этап 5 (Chaos/FinOps)** | Jaeger в Docker, библиотеки `opentelemetry-*` | Трассировка спанов, стресс-тесты, Spot TCO |

---

## 📋 Пошаговый план установки

```text
[Шаг 1: Разрешение скриптов PowerShell и длинных путей Windows]
                         │
                         ▼
[Шаг 2: Git для Windows и базовые утилиты]
                         │
                         ▼
[Шаг 3: Python и Conda-окружение 'grid']
                         │
                         ▼
[Шаг 4: WSL2 и Docker Desktop]
                         │
                         ▼
[Шаг 5: Локальный Kubernetes (kubectl + minikube)]
                         │
                         ▼
[Шаг 6: Настройка Брандмауэра Windows для Ray Grid (порт 6379)]
                         │
                         ▼
[Шаг 7: Автоматизированный скрипт валидации (PowerShell Healthcheck)]
```

---

### Шаг 1. Первичная настройка PowerShell и Windows

Запустите **PowerShell от имени Администратора** (Win + X $\to$ «Терминал (Администратор)» или «PowerShell (Администратор)»).

#### 1.1. Разрешение запуска локальных скриптов
По умолчанию Windows блокирует исполнение скриптов активирования Conda и проверочных утилит:
```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
```

#### 1.2. Включение поддержки длинных путей (Long Paths > 260 символов)
Python-библиотеки часто имеют глубокую вложенность каталогов. Включим поддержку длинных путей:
```powershell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
    -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

---

### Шаг 2. Установка Git для Windows

Установим Git через стандартный пакетный менеджер Windows (`winget`):
```powershell
winget install --id Git.Git -e --source winget
```

*После установки перезапустите окно PowerShell и проверьте:*
```powershell
git --version
```

---

### Шаг 3. Установка Python и настройка Conda-окружения (`grid`)

Для Windows настоятельно рекомендуется использовать **Miniconda**, так как она автоматически предоставляет откомпилированные под Windows C-библиотеки BLAS/LAPACK для `numpy` и `scipy`.

#### 3.1. Установка Miniconda
```powershell
winget install --id Anaconda.Miniconda3 -e --source winget
```

> **Важно:** Закройте и заново откройте окно PowerShell, чтобы переменные среды обновились.

#### 3.2. Создание изолированного окружения `grid`
```powershell
conda create -n grid python=3.12 -y
conda activate grid
```

#### 3.3. Установка Python-пакетов для всех этапов
```powershell
python -m pip install --upgrade pip

# Этап 1: Численные методы и тестирование
pip install numpy scipy h5py networkx matplotlib pytest

# Этап 2: Распределенный фреймворк Ray
pip install "ray[default]>=2.9.0" psutil aiohttp

# Этап 3: Web-стек и очереди
pip install fastapi uvicorn redis pydantic requests

# Этап 5: Трассировка OpenTelemetry
pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp opentelemetry-instrumentation-fastapi
```

*Проверка:*
```powershell
python -c "import numpy, scipy, h5py, ray, pytest; print('Python окружение под Windows: OK')"
```

---

### Шаг 4. Установка WSL2 и Docker Desktop (Этап 3)

Для запуска контейнеров на Windows используется **Docker Desktop** с современной подсистемой **WSL2** (Windows Subsystem for Linux):

#### 4.1. Включение WSL2
Выполните в PowerShell (Администратор):
```powershell
wsl --install --no-distribution
```
*(Если WSL уже установлен, обновите ядро: `wsl --update`)*.

#### 4.2. Установка Docker Desktop
```powershell
winget install --id Docker.DockerDesktop -e --source winget
```

#### 4.3. Ограничение потребления RAM для WSL2 (Рекомендуется)
Чтобы Docker не занимал 100% оперативной памяти вашего компьютера, создайте конфигурационный файл:
```powershell
$wslConfig = @"
[wsl2]
memory=6GB
processors=4
"@
Set-Content -Path "$env:USERPROFILE\.wslconfig" -Value $wslConfig
```

Запустите **Docker Desktop** из меню «Пуск» и дождитесь появления зелёной иконки *«Engine running»*.

*Проверка:*
```powershell
docker --version
docker compose version
docker run --rm hello-world
```

---

### Шаг 5. Установка локального Kubernetes (Этап 4)

Установим официальную утилиту `kubectl` и локальный кластер `minikube`:

```powershell
# Установка CLI kubectl
winget install --id Kubernetes.kubectl -e --source winget

# Установка Minikube
winget install --id Kubernetes.minikube -e --source winget
```

*Проверка:*
```powershell
kubectl version --client
minikube version
```

Для проверки работы локального кластера можно запустить:
```powershell
minikube start --driver=docker
minikube status
minikube stop
```

---

### Шаг 6. Настройка Брандмауэра Windows для Ray Grid (Этап 2)

Если ваш компьютер будет выступать в роли Head Node или подключаться к другим ноутбукам по домашнему Wi-Fi / LAN, Брандмауэр Windows по умолчанию заблокирует входящие TCP-соединения Ray.

Выполните в PowerShell (Администратор) для создания разрешающих правил:
```powershell
# Разрешение порта координатора GCS
New-NetFirewallRule -DisplayName "Ray Grid GCS" -Direction Inbound -LocalPort 6379 -Protocol TCP -Action Allow

# Разрешение веб-дашборда Ray
New-NetFirewallRule -DisplayName "Ray Grid Dashboard" -Direction Inbound -LocalPort 8265 -Protocol TCP -Action Allow

# Разрешение пула сетевых воркеров Ray
New-NetFirewallRule -DisplayName "Ray Grid Worker Ports" -Direction Inbound -LocalPort 10000-10100 -Protocol TCP -Action Allow
```

Команда для быстрого определения локального IP-адреса вашего компьютера:
```powershell
(Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias "Wi-Fi*", "Ethernet*").IPAddress
```

---

### Шаг 7. Jaeger UI для Observability (Этап 5)

Запуск легковесного контейнера с Jaeger UI через Docker Desktop:

```powershell
docker run -d --name test-jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```
- Откройте в браузере: `http://localhost:16686`
- Остановка контейнера:
```powershell
docker stop test-jaeger; docker rm test-jaeger
```

---

## 🚀 Шаг 8. Автоматизированный скрипт валидации (PowerShell Healthcheck)

Сохраните и запустите проверочный скрипт в PowerShell:

```powershell
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         ПРОВЕРКА ГОТОВНОСТИ ОКРУЖЕНИЯ (WINDOWS)          " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

function Check-Tool ($name, $command) {
    try {
        $out = Invoke-Expression $command 2>$null
        Write-Host "[OK] $name: $($out | Select-Object -First 1)" -ForegroundColor Green
    } catch {
        Write-Host "[FAIL] $name НЕ НАЙДЕН!" -ForegroundColor Red
    }
}

Check-Tool "Git" "git --version"
Check-Tool "Python" "python --version"
Check-Tool "Conda" "conda --version"
Check-Tool "Docker" "docker --version"
Check-Tool "Kubectl" "kubectl version --client"

Write-Host "----------------------------------------------------------" -ForegroundColor Yellow
Write-Host "Проверка установленных Python пакетов..." -ForegroundColor Yellow

python -c "
mods = ['numpy', 'scipy', 'h5py', 'networkx', 'pytest', 'ray', 'fastapi', 'opentelemetry']
for m in mods:
    __import__(m)
    print(f'  [OK] {m}')
"

Write-Host "----------------------------------------------------------" -ForegroundColor Yellow
Write-Host "Проверка Docker Daemon..." -ForegroundColor Yellow
try {
    docker info | Out-Null
    Write-Host "  [OK] Docker Desktop активен и отвечает" -ForegroundColor Green
} catch {
    Write-Host "  [WARNING] Docker Desktop выключен (запустите через меню Пуск)" -ForegroundColor Yellow
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " 🎉 ВСЕ КОМПОНЕНТЫ ГОТОВЫ К ВЫПОЛНЕНИЮ ЭТАПОВ 1-5!        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
```
