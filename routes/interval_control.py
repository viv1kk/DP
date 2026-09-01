"""
=============================================================================
  BACKEND CHANGE  -  OPTIONAL ADD-ON  (v2, non-blocking)
=============================================================================

A NEW, standalone FastAPI router for the BACC backend. It does NOT modify
orchestrator.py, PipelineWorker, main.py's existing routes, or any pipeline
execution logic. It calls only the existing public Orchestrator methods:

    orchestrator.status(name)
    orchestrator.stop(name)
    orchestrator.start(name, interval)

WHY v2
------
v1 did stop -> wait -> start inside the request handler, so the HTTP call did
not return until the pipeline had actually stopped. Because PipelineWorker.stop()
only sets a stop event, a worker that is mid-run() finishes that run first - so
the request hung for however long the current execution had left. That is
correct behaviour, but a bad thing to do to an HTTP client.

v2 returns immediately (HTTP 202) and does the stop -> wait -> start on a
background thread. Poll GET /pipeline/{name}/interval to see how it is going.
The guarantee is unchanged: `start` is NEVER called until status reports
running=false.

ENDPOINTS ADDED
---------------
  POST /pipeline/{name}/interval
      body: {"interval": 600, "start_if_stopped": false, "wait": false}

      RUNNING pipeline:
        wait=false (default) -> returns 202 immediately:
            {"status": "scheduled", "name": ..., "interval": 600,
             "job": {...}, "poll": "GET /pipeline/{name}/interval"}
        wait=true            -> blocks until done, then returns the final job
                                record. For scripts that want one sync call.

      STOPPED pipeline -> the interval is stored on the worker and the
        pipeline STAYS STOPPED. Returns 200 {"status": "set"} immediately.
        See "SETTING THE INTERVAL WHILE STOPPED" below.
        Pass start_if_stopped=true to also start it right away.

  GET  /pipeline/{name}/interval
      Current or last interval-change job for that pipeline:
          {"state": "stopping" | "waiting_for_stop" | "starting" |
                    "done" | "timeout" | "failed" | "none",
           "name": ..., "interval": ..., "waited_seconds": 12.5,
           "started_at": ..., "finished_at": ..., "error": ..., "done": bool}

SETTING THE INTERVAL WHILE STOPPED
---------------------------------
`PipelineWorker.interval` already persists across stop/start - a worker keeps
whatever interval it last started with, and `start_all()` passes no interval so
it reuses the stored one. What the existing API has no path for is *setting*
that value without also starting the pipeline: `/pipeline/{name}` only touches
`interval` under `method: "run"`, and `run` calls `start()`, which launches the
thread and executes the pipeline immediately.

So for a stopped pipeline this router assigns the worker's stored interval
directly:

    with worker.lock:
        if worker.thread and worker.thread.is_alive():   # re-check under lock
            ...treat as running instead...
        worker.interval = interval

That is the same assignment `start()` performs (`self.interval = interval`),
taken under the same `worker.lock` that `start()` and `stop()` use, and only
ever while the worker thread is not alive. It does not start, stop, or
otherwise touch pipeline execution - the value simply sits there until the next
start, exactly as it would have if it had been passed to `start()`.

STATE
-----
The job records are a small in-memory dict inside the backend process (one
entry per pipeline, overwritten each time). Nothing is written to disk and
nothing persists across a backend restart. The interval itself lives on the
worker, where it already lived.

INSTALL
-------
1. Save as a NEW file in the backend project, e.g. routes/interval_control.py
2. In main.py, after `orchestrator = Orchestrator()`, add exactly two lines:

       from routes.interval_control import build_router
       app.include_router(build_router(orchestrator))

   Nothing else in main.py changes.
3. Restart the backend.
4. On the frontend set: BACC_USE_BACKEND_INTERVAL_ENDPOINT=1
=============================================================================
"""

import threading
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# How long to wait for a worker thread to actually exit before giving up.
STOP_WAIT_TIMEOUT_SECONDS = 600.0
STOP_WAIT_POLL_SECONDS = 1.0

TERMINAL_STATES = ("done", "timeout", "failed", "none")


class IntervalRequest(BaseModel):
    interval: int
    # For a stopped pipeline the interval is stored and the pipeline stays
    # stopped. Set this to also start it at the new interval. Default false so
    # nothing that was stopped on purpose gets started as a side effect.
    start_if_stopped: bool = False
    # true = block until finished (v1 behaviour). Default false = return 202.
    wait: bool = False


# --- job bookkeeping --------------------------------------------------------
_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()

# One lock per pipeline, so a change on pipeline A never blocks pipeline B.
_pipeline_locks: Dict[str, threading.Lock] = {}
_pipeline_locks_guard = threading.Lock()


def _lock_for(name: str) -> threading.Lock:
    with _pipeline_locks_guard:
        if name not in _pipeline_locks:
            _pipeline_locks[name] = threading.Lock()
        return _pipeline_locks[name]


def _set_job(name: str, **fields) -> Dict[str, Any]:
    with _jobs_lock:
        job = _jobs.setdefault(name, {"name": name})
        job.update(fields)
        job["done"] = job.get("state") in TERMINAL_STATES
        return dict(job)


def _get_job(name: str) -> Optional[Dict[str, Any]]:
    with _jobs_lock:
        job = _jobs.get(name)
        return dict(job) if job else None


def _job_in_flight(name: str) -> bool:
    job = _get_job(name)
    return bool(job) and not job.get("done", True)


# --- the actual work --------------------------------------------------------
def _change_interval_worker(orchestrator, name: str, interval: int):
    """stop -> wait until really stopped -> start with the new interval.

    Runs on a background daemon thread. Never calls start() until status
    reports running=false.
    """
    lock = _lock_for(name)
    if not lock.acquire(blocking=False):
        _set_job(name, state="failed",
                 error="another interval change is already running for this pipeline",
                 finished_at=time.time())
        return
    try:
        _set_job(name, state="stopping", waited_seconds=0.0)
        orchestrator.stop(name)

        waited = 0.0
        _set_job(name, state="waiting_for_stop")
        while True:
            time.sleep(STOP_WAIT_POLL_SECONDS)
            waited += STOP_WAIT_POLL_SECONDS

            if not orchestrator.status(name).get("running"):
                break

            _set_job(name, waited_seconds=round(waited, 1))

            if waited >= STOP_WAIT_TIMEOUT_SECONDS:
                _set_job(
                    name,
                    state="timeout",
                    waited_seconds=round(waited, 1),
                    finished_at=time.time(),
                    error=(
                        f"{name} did not stop within {STOP_WAIT_TIMEOUT_SECONDS:.0f}s. "
                        f"start was NOT called, so the interval is unchanged. The "
                        f"pipeline was asked to stop and will exit after its current run."
                    ),
                )
                return

        _set_job(name, state="starting", waited_seconds=round(waited, 1))
        orchestrator.start(name, interval)
        _set_job(name, state="done", waited_seconds=round(waited, 1),
                 finished_at=time.time(), error=None)

    except Exception as exc:  # noqa: BLE001 - report anything, never crash the thread
        _set_job(name, state="failed", error=f"{type(exc).__name__}: {exc}",
                 finished_at=time.time())
    finally:
        lock.release()


# --- router -----------------------------------------------------------------
def build_router(orchestrator) -> APIRouter:
    router = APIRouter()

    @router.post("/pipeline/{name}/interval")
    def change_interval(name: str, req: IntervalRequest):
        if req.interval <= 0:
            raise HTTPException(status_code=400, detail="interval must be > 0")

        try:
            current = orchestrator.status(name)
        except ValueError:
            raise HTTPException(status_code=404, detail=f"{name} not found")

        # --- already stopped: nothing to wait for -------------------------
        if not current.get("running"):
            if req.start_if_stopped:
                orchestrator.start(name, req.interval)
                job = _set_job(name, state="done", interval=req.interval,
                               was_running=False, waited_seconds=0.0,
                               started_at=time.time(), finished_at=time.time(),
                               error=None)
                return {"status": "started", "name": name,
                        "interval": req.interval, "job": job}

            # Store the interval and leave the pipeline stopped. See the
            # module docstring for why this assignment is the safe one.
            worker = orchestrator.workers.get(name)
            if worker is None:
                raise HTTPException(status_code=404, detail=f"{name} not found")

            with worker.lock:
                # Re-check under the lock: it may have been started between
                # the status() call above and here.
                if worker.thread is not None and worker.thread.is_alive():
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"{name} started while the request was in flight. "
                            f"Retry to change the interval of the running pipeline."
                        ),
                    )
                previous = worker.interval
                worker.interval = req.interval

            job = _set_job(name, state="done", interval=req.interval,
                           was_running=False, waited_seconds=0.0,
                           started_at=time.time(), finished_at=time.time(),
                           error=None)
            return {
                "status": "set",
                "name": name,
                "interval": req.interval,
                "previous_interval": previous,
                "running": False,
                "note": (
                    f"{name} is stopped and stays stopped. It will use "
                    f"interval={req.interval} the next time it is started, "
                    f"including via /pipelines/all."
                ),
                "job": job,
            }

        # --- running: needs a stop, which is not instant -------------------
        if _job_in_flight(name):
            raise HTTPException(
                status_code=409,
                detail=f"An interval change for {name} is already in progress. "
                       f"Poll GET /pipeline/{name}/interval.",
            )

        _set_job(name, state="stopping", interval=req.interval, was_running=True,
                 waited_seconds=0.0, started_at=time.time(), finished_at=None,
                 error=None)

        if req.wait:
            # Synchronous mode - blocks for as long as the current run has left.
            _change_interval_worker(orchestrator, name, req.interval)
            job = _get_job(name) or {}
            if job.get("state") != "done":
                raise HTTPException(status_code=409,
                                    detail=job.get("error", "interval change failed"))
            return {"status": "changed", "name": name,
                    "interval": req.interval, "job": job}

        # Asynchronous mode (default) - return now, work continues in background.
        threading.Thread(
            target=_change_interval_worker,
            args=(orchestrator, name, req.interval),
            name=f"interval-change-{name}",
            daemon=True,
        ).start()

        return JSONResponse(
            status_code=202,
            content={
                "status": "scheduled",
                "name": name,
                "interval": req.interval,
                "note": (
                    "Stopping now. The pipeline will restart at the new interval "
                    "once its current run finishes. start is not called until "
                    "status reports running=false."
                ),
                "poll": f"GET /pipeline/{name}/interval",
                "job": _get_job(name),
            },
        )

    @router.get("/pipeline/{name}/interval")
    def interval_job(name: str):
        job = _get_job(name)
        if job is None:
            return {"state": "none", "name": name, "done": True,
                    "detail": "no interval change has been requested for this pipeline"}
        return job

    return router
