import threading
import time
import importlib

from framework.logger import get_logger, format_error

from pydantic import BaseModel
from typing import Optional, Dict


# =========================
# Pipeline Worker
# =========================
class PipelineWorker:
    def __init__(self, name, pipeline, interval: int = 300):
        self.name = name
        self.pipeline = pipeline
        self.interval = interval
        self.thread = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.logger = get_logger(name)

    def start(self, interval: Optional[int] = None):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return f"{self.name} already running"

            # use passed interval or fallback to default
            if interval is not None:
                self.interval = interval

            self.stop_event.clear()

            self.thread = threading.Thread(
                target=self._run_loop,
                daemon=True
            )
            self.thread.start()

        return f"{self.name} started"

    def _run_loop(self):
        self.logger.info(f"Worker started (interval={self.interval}s)")

        while not self.stop_event.is_set():
            try:
                # retry logic
                for attempt in range(3):
                    try:
                        self.pipeline.run()
                        break
                    except Exception as e:
                        self.logger.error(f"Attempt {attempt + 1} escaped run(): {format_error(e)}")

                        if attempt < 2:
                            time.sleep(2)

            except Exception as e:
                self.logger.error(f"Worker loop error: {format_error(e)}")

            # interruptible sleep
            self.stop_event.wait(self.interval)

        self.logger.info("Worker stopped")

    def stop(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                self.stop_event.set()
                return f"{self.name} stopping"

        return f"{self.name} not running"

    def status(self):
        return {
            "running": self.thread.is_alive() if self.thread else False,
            "interval": self.interval
        }


# =========================
# Orchestrator
# =========================
class Orchestrator:
    def __init__(self):
        self.workers: Dict[str, PipelineWorker] = {}
        self.lock = threading.Lock()

    def load_pipeline(self, module_path: str, class_name: str):
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls()

    def register_dynamic(
        self,
        name: str,
        module: str,
        class_name: str,
        interval: int
    ):
        with self.lock:
            if name in self.workers:
                raise ValueError("Pipeline already exists")

            pipeline = self.load_pipeline(module, class_name)

            # store default interval during registration
            self.workers[name] = PipelineWorker(
                name=name,
                pipeline=pipeline,
                interval=interval
            )

    def start(self, name: str, interval: Optional[int] = None):
        with self.lock:
            if name not in self.workers:
                raise ValueError("Pipeline not found")

            return self.workers[name].start(interval)

    def stop(self, name: str):
        with self.lock:
            if name not in self.workers:
                raise ValueError("Pipeline not found")

            return self.workers[name].stop()

    def status(self, name: str):
        with self.lock:
            if name not in self.workers:
                raise ValueError("Pipeline not found")

            return self.workers[name].status()

    def list_pipelines(self):
        with self.lock:
            return {
                name: worker.status()
                for name, worker in self.workers.items()
            }
    
    
    def start_all(self):
        results = {}

        with self.lock:
            for name, worker in self.workers.items():
                try:
                    # uses worker's registered default interval
                    results[name] = worker.start()
                    time.sleep(1)
                except Exception as e:
                    results[name] = str(e)

        return results

    def stop_all(self):
        results = {}

        with self.lock:
            for name, worker in self.workers.items():
                try:
                    results[name] = worker.stop()
                except Exception as e:
                    results[name] = str(e)

        return results

# =========================
# Request Models
# =========================
class RegisterRequest(BaseModel):
    name: str
    module: str
    class_name: str
    interval: int


class ControlRequest(BaseModel):
    method: str
    interval: Optional[int] = None

class BulkControlRequest(BaseModel):
    method: str