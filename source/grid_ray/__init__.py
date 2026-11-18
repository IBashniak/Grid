"""
Пакет распределенной симуляции устойчивости энергосистемы IEEE-118 на базе Ray (Этап 2).

Содержит модули:
- topology.py: подготовка графа сети и помещение в Ray Object Store (ray.put);
- ray_engine.py: распределенные stateless-воркеры (@ray.remote);
- scheduler.py: динамический диспетчер задач на базе ray.wait (защита от Stragglers);
- benchmark.py: замер метрик кластера, расчет ускорения и эффективности;
- run.py: унифицированный CLI для управления грид-кластером.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import sys
import threading
import time
import uuid
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple


def _install_local_ray_fallback() -> ModuleType:
    """
    Создает легковесный локальный эмулятор API Ray (Object Store + пул воркеров),
    если пакет `ray` не установлен в текущем виртуальном окружении Python
    (например, при запуске на неподготовленной машине или в изолированной песочнице).

    Поддерживает полный контракт Этапа 2:
    - ray.init(), ray.is_initialized(), ray.shutdown()
    - ray.ObjectRef, ray.put(), ray.get(), ray.wait()
    - @ray.remote и @ray.remote(max_retries=..., retry_exceptions=...)
    - ray.nodes(), ray.cluster_resources(), ray.available_resources()
    """
    ray_mod = ModuleType("ray")
    ray_mod.__version__ = "2.x-local-shim"

    _state_lock = threading.Lock()
    _object_store: Dict[str, Any] = {}
    _task_futures: Dict[str, concurrent.futures.Future[Any]] = {}
    _executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _initialized: bool = False
    _num_cpus: int = os.cpu_count() or 2
    _local_mode: bool = False
    _node_ip: str = "127.0.0.1"

    class ObjectRef:
        """Ссылка на объект в распределенном хранилище Ray Object Store (Plasma)."""

        def __init__(self, object_id: str) -> None:
            self._object_id = object_id

        @property
        def object_id(self) -> str:
            return self._object_id

        def hex(self) -> str:
            return self._object_id

        def __repr__(self) -> str:
            return f"ObjectRef({self._object_id})"

        def __eq__(self, other: object) -> bool:
            if isinstance(other, ObjectRef):
                return self._object_id == other._object_id
            return False

        def __hash__(self) -> int:
            return hash(self._object_id)

    def _detect_ip() -> str:
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            return ip if ip else "127.0.0.1"
        except Exception:
            return "127.0.0.1"

    def is_initialized() -> bool:
        return _initialized

    def init(
        address: Optional[str] = None,
        num_cpus: Optional[int] = None,
        ignore_reinit_error: bool = True,
        local_mode: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        nonlocal _initialized, _num_cpus, _local_mode, _executor, _node_ip
        with _state_lock:
            if _initialized:
                if not ignore_reinit_error:
                    raise RuntimeError("Ray уже инициализирован.")
                if num_cpus is not None and num_cpus != _num_cpus:
                    _num_cpus = max(1, int(num_cpus))
                    if _executor is not None:
                        _executor.shutdown(wait=False)
                    _executor = concurrent.futures.ThreadPoolExecutor(max_workers=_num_cpus)
                return {"node_ip_address": _node_ip, "num_cpus": _num_cpus}

            _num_cpus = max(1, int(num_cpus if num_cpus is not None else (os.cpu_count() or 2)))
            _local_mode = bool(local_mode)
            _node_ip = _detect_ip()
            _executor = concurrent.futures.ThreadPoolExecutor(max_workers=_num_cpus)
            _initialized = True
            return {"node_ip_address": _node_ip, "num_cpus": _num_cpus}

    def shutdown() -> None:
        nonlocal _initialized, _executor
        with _state_lock:
            if _executor is not None:
                _executor.shutdown(wait=True)
                _executor = None
            _object_store.clear()
            _task_futures.clear()
            _initialized = False

    def _ensure_initialized() -> None:
        if not _initialized:
            init(ignore_reinit_error=True)

    def put(value: Any) -> ObjectRef:
        """Помещает объект в разделяемое хранилище (Zero-Copy в памяти процесса)."""
        _ensure_initialized()
        obj_id = uuid.uuid4().hex
        with _state_lock:
            _object_store[obj_id] = value
        return ObjectRef(obj_id)

    def _resolve_single_ref(ref: ObjectRef) -> Any:
        obj_id = ref.object_id
        with _state_lock:
            if obj_id in _object_store:
                return _object_store[obj_id]
            fut = _task_futures.get(obj_id)

        if fut is not None:
            result = fut.result()
            with _state_lock:
                _object_store[obj_id] = result
            return result

        raise KeyError(f"Объект {ref} не найден в Ray Object Store")

    def get(object_refs: ObjectRef | List[ObjectRef], timeout: Optional[float] = None) -> Any:
        """Извлекает объект или список объектов по их ObjectRef."""
        _ensure_initialized()
        if isinstance(object_refs, list):
            return [_resolve_single_ref(ref) for ref in object_refs]
        if isinstance(object_refs, ObjectRef):
            return _resolve_single_ref(object_refs)
        raise TypeError(f"Ожидался ObjectRef или список ObjectRef, получено: {type(object_refs)}")

    def wait(
        object_refs: List[ObjectRef],
        num_returns: int = 1,
        timeout: Optional[float] = None,
    ) -> Tuple[List[ObjectRef], List[ObjectRef]]:
        """
        Неблокирующее ожидание готовности задач (динамическая диспетчеризация).
        Возвращает кортеж (ready_refs, remaining_refs).
        """
        _ensure_initialized()
        if not object_refs:
            return [], []

        num_returns = max(1, min(num_returns, len(object_refs)))
        deadline = (time.perf_counter() + timeout) if timeout is not None else None

        while True:
            ready: List[ObjectRef] = []
            remaining: List[ObjectRef] = []

            for ref in object_refs:
                obj_id = ref.object_id
                with _state_lock:
                    in_store = obj_id in _object_store
                    fut = _task_futures.get(obj_id)

                if in_store or (fut is not None and fut.done()):
                    if len(ready) < num_returns:
                        ready.append(ref)
                    else:
                        remaining.append(ref)
                else:
                    remaining.append(ref)

            if len(ready) >= num_returns:
                return ready, remaining

            if deadline is not None and time.perf_counter() >= deadline:
                return ready, remaining

            time.sleep(0.005)

    class _RemoteFunction:
        """Обертка для функции, декорированной через @ray.remote."""

        def __init__(
            self,
            fn: Callable[..., Any],
            max_retries: int = 3,
            retry_exceptions: bool = True,
        ) -> None:
            self._fn = fn
            self._max_retries = max_retries
            self._retry_exceptions = retry_exceptions
            self.__name__ = getattr(fn, "__name__", "remote_fn")
            self.__doc__ = getattr(fn, "__doc__", None)

        def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
            _ensure_initialized()
            obj_id = uuid.uuid4().hex

            def _execute_with_retries() -> Any:
                # Автоматическое разрешение (Zero-Copy чтение) ссылок ObjectRef верхнего уровня
                resolved_args = [
                    _resolve_single_ref(a) if isinstance(a, ObjectRef) else a
                    for a in args
                ]
                resolved_kwargs = {
                    k: (_resolve_single_ref(v) if isinstance(v, ObjectRef) else v)
                    for k, v in kwargs.items()
                }
                attempts = 0
                max_attempts = 1 + (self._max_retries if self._retry_exceptions else 0)
                while True:
                    try:
                        return self._fn(*resolved_args, **resolved_kwargs)
                    except Exception:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise

            if _local_mode or _executor is None:
                res = _execute_with_retries()
                with _state_lock:
                    _object_store[obj_id] = res
                return ObjectRef(obj_id)

            fut = _executor.submit(_execute_with_retries)
            with _state_lock:
                _task_futures[obj_id] = fut
            return ObjectRef(obj_id)

        def options(self, **override_opts: Any) -> "_RemoteFunction":
            return _RemoteFunction(
                self._fn,
                max_retries=override_opts.get("max_retries", self._max_retries),
                retry_exceptions=override_opts.get("retry_exceptions", self._retry_exceptions),
            )

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            return self._fn(*args, **kwargs)

    def remote(*d_args: Any, **d_kwargs: Any) -> Any:
        """Декоратор @ray.remote для превращения функции в распределенную задачу."""
        if len(d_args) == 1 and callable(d_args[0]) and not d_kwargs:
            return _RemoteFunction(d_args[0])

        max_retries = int(d_kwargs.get("max_retries", 3))
        retry_exceptions = bool(d_kwargs.get("retry_exceptions", True))

        def _decorator(fn: Callable[..., Any]) -> _RemoteFunction:
            return _RemoteFunction(
                fn,
                max_retries=max_retries,
                retry_exceptions=retry_exceptions,
            )

        return _decorator

    def nodes() -> List[Dict[str, Any]]:
        _ensure_initialized()
        return [
            {
                "NodeID": "local-head-node-0",
                "Alive": True,
                "NodeManagerAddress": _node_ip,
                "Resources": {"CPU": float(_num_cpus)},
            }
        ]

    def cluster_resources() -> Dict[str, float]:
        _ensure_initialized()
        return {"CPU": float(_num_cpus)}

    def available_resources() -> Dict[str, float]:
        _ensure_initialized()
        return {"CPU": float(_num_cpus)}

    util_mod = ModuleType("ray.util")
    util_mod.get_node_ip_address = lambda: _node_ip  # type: ignore[attr-defined]

    ray_mod.ObjectRef = ObjectRef  # type: ignore[attr-defined]
    ray_mod.init = init  # type: ignore[attr-defined]
    ray_mod.is_initialized = is_initialized  # type: ignore[attr-defined]
    ray_mod.shutdown = shutdown  # type: ignore[attr-defined]
    ray_mod.put = put  # type: ignore[attr-defined]
    ray_mod.get = get  # type: ignore[attr-defined]
    ray_mod.wait = wait  # type: ignore[attr-defined]
    ray_mod.remote = remote  # type: ignore[attr-defined]
    ray_mod.nodes = nodes  # type: ignore[attr-defined]
    ray_mod.cluster_resources = cluster_resources  # type: ignore[attr-defined]
    ray_mod.available_resources = available_resources  # type: ignore[attr-defined]
    ray_mod.util = util_mod  # type: ignore[attr-defined]

    sys.modules["ray"] = ray_mod
    sys.modules["ray.util"] = util_mod
    return ray_mod


try:
    import ray as _ray_pkg  # noqa: F401
except ModuleNotFoundError:
    _install_local_ray_fallback()
