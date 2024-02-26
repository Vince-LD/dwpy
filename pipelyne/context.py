from dataclasses import dataclass, field
from threading import Lock

@dataclass
class BasePipelineContext:
    thread_count = 4
    _thread_lock: Lock = field(init=False, default_factory=lambda: Lock())

    def __enter__(self):
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._thread_lock.release()