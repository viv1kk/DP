"""
Maintenance operations for the pipelines' SQL tables and data files.

Backs routes/maintenance.py, but has no FastAPI dependency so it can be
called and tested on its own.

TABLES
------
reset_table(table, mode)
    truncate -> empty the table, keep the schema.
    recreate -> DROP + CREATE from the YAML schema (MSSQL_TABLE_MANAGER).
                The SAP URL base table is reseeded from its CSV afterwards,
                the same way SAP_URL_MONITORING.reset_base_mssql_table() does.

The Zabbix delta pipelines (hosts, host group, host problems) keep the last
frame they loaded in memory and only upsert what changed against it. After a
reset that cache no longer describes the table, so it is cleared and the
next run does a full load.

FILES
-----
replace_file(file_id, upload_name, content)
    The upload is written next to the current file, parsed with the same
    reader the pipeline uses and checked for the columns the pipeline reads.
    Only then is the current file copied to <dir>/archive/<stem>__<IST time><ext>
    and the upload moved over it under the CURRENT file's name, whatever the
    upload was called. The swap is a single os.replace, so a reader never sees
    a half-written or missing file.

JOB STATUS
----------
job_status(pipeline=None)
    Read-only. Reports the last run of each pipeline from BACC_ETL_JOB_CONTROL,
    grouped by pipeline and joined with the live worker state. Not subject to
    the SAFETY rules below.

SAFETY
------
Every operation is refused (MaintenanceConflict -> 409) while a pipeline that
uses the resource is running. "Running" means what it means everywhere else
in the API: the worker thread is alive, including while it sleeps between
runs. The worker locks are held for the whole operation, so the pipeline
cannot be started halfway through; a start request simply waits. Only one
maintenance operation runs at a time.

A pipeline executed with POST /pipeline/{name} {"method": "run_once"} runs on
the request thread, not on its worker, and cannot be seen by this check.
"""

import importlib
import json
import os
import shutil
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from config.mssql_config import MSSQL_Config
from config.sap_config import SAP_URL_MONITORING_CONFIG
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from framework.logger import get_logger, format_error
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
from framework.paths import DATA_DIR, ROOT

IST = ZoneInfo("Asia/Kolkata")
RESET_MODES = ("truncate", "recreate")
ARCHIVE_DIR_NAME = "archive"

# Written by framework.decorators.pipeline_run_status: one row per pipeline
# table, keyed job_name = sql_table_name.
JOB_CONTROL_TABLE = "BACC_ETL_JOB_CONTROL"
JOB_FIELDS = ("last_attempt_time", "last_success_time", "last_status", "last_message")


# =========================
# Errors
# =========================
class MaintenanceError(Exception):
    status_code = 500
    code = "MAINTENANCE_FAILED"

    def __init__(self, message: str, code: Optional[str] = None, **extra):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        self.extra = extra

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, **self.extra}


class MaintenanceNotFound(MaintenanceError):
    status_code = 404
    code = "NOT_FOUND"


class MaintenanceInvalid(MaintenanceError):
    status_code = 400
    code = "INVALID_REQUEST"


class MaintenanceConflict(MaintenanceError):
    status_code = 409
    code = "CONFLICT"


# =========================
# Registry
# =========================
@dataclass(frozen=True)
class PipelineRef:
    module: str
    class_name: str


# Keyed by class name, which is also each pipeline's pipeline_name.
PIPELINES: Dict[str, PipelineRef] = {
    "TIBCO_AUDIT_OPERATIONAL_ERRORS": PipelineRef("pipelines.tibco.operational_errors.pipeline", "TIBCO_AUDIT_OPERATIONAL_ERRORS"),
    "TIBCO_HAWK": PipelineRef("pipelines.tibco.hawk.pipeline", "TIBCO_HAWK"),
    "TIBCO_B2B_ERRORS": PipelineRef("pipelines.tibco.b2b.pipeline", "TIBCO_B2B_ERRORS"),
    "TIBCO_TRA_LOG": PipelineRef("pipelines.tibco.tra_log.pipeline", "TIBCO_TRA_LOG"),
    "TIBCO_EMS": PipelineRef("pipelines.tibco.ems.pipeline", "TIBCO_EMS"),
    "HAWK_APPLICATION_MONITORING": PipelineRef("pipelines.hawk.pipeline", "HAWK_APPLICATION_MONITORING"),
    "SAP_SOLMAN": PipelineRef("pipelines.sap.solman.pipeline", "SAP_SOLMAN"),
    "SAP_URL_MONITORING": PipelineRef("pipelines.sap.url_monitoring.pipeline", "SAP_URL_MONITORING"),
    "SNOW_OPEN_INCIDENTS": PipelineRef("pipelines.servicenow.open_incident.pipeline", "SNOW_OPEN_INCIDENTS"),
    "ZABBIX_HOST_GROUP": PipelineRef("pipelines.zabbix.host_group.pipeline", "ZABBIX_HOST_GROUP"),
    "ZABBIX_HOSTS": PipelineRef("pipelines.zabbix.hosts.pipeline", "ZABBIX_HOSTS"),
    "ZABBIX_HOST_PROBLEMS": PipelineRef("pipelines.zabbix.host_problems.pipeline", "ZABBIX_HOST_PROBLEMS"),
    "ZABBIX_SERVICE_MONITORING": PipelineRef("pipelines.zabbix.service_monitoring.pipeline", "ZABBIX_SERVICE_MONITORING"),
}

# Pipelines that read another pipeline's table and must also be stopped
# before it is reset. Keyed by (owning pipeline, table key).
TABLE_READERS: Dict[Tuple[str, str], List[str]] = {
    # SAP_SOLMAN's transform aggregates the URL poll results by SID.
    ("SAP_URL_MONITORING", "poll_table"): ["SAP_SOLMAN"],
}

# Tables whose recreate is followed by a reseed from a data file.
# (owning pipeline, table key) -> file_id in DATA_FILES.
TABLE_RESEED: Dict[Tuple[str, str], str] = {
    ("SAP_URL_MONITORING", "base_table"): "sap_urls",
}

# Pipelines whose _cache["last_cached_df"] drives a delta load.
DELTA_CACHE_PIPELINES = ("ZABBIX_HOST_GROUP", "ZABBIX_HOSTS", "ZABBIX_HOST_PROBLEMS")


def _pipeline_class(name: str):
    ref = PIPELINES[name]
    return getattr(importlib.import_module(ref.module), ref.class_name)


@dataclass(frozen=True)
class ResolvedFile:
    path: Path
    encoding: Optional[str]          # None = Excel
    required_columns: Tuple[str, ...]


@dataclass(frozen=True)
class DataFileSpec:
    file_id: str
    pipeline: str
    description: str
    applies: str                     # when the pipeline picks up a new version
    resolve: Callable[[], ResolvedFile]
    after_replace: Optional[Callable[[], None]] = field(default=None)


def _resolve_hawk_applications() -> ResolvedFile:
    return ResolvedFile(
        path=DATA_DIR / "hawk" / "TIBCO HAWK APPLICATIONS FORMATTED.xlsx",
        encoding=None,
        required_columns=("dns", "domain", "host_name", "type", "deployment", "component"),
    )


def _resolve_sap_urls() -> ResolvedFile:
    # File name, encoding and columns all live in sap_config.yaml.
    cfg = SAP_URL_MONITORING_CONFIG(file="sap_config.yaml", pipeline_name="SAP_URL_MONITORING")
    base_columns = tuple(json.loads(cfg.base_table["sql_columns"]).keys())
    return ResolvedFile(
        path=DATA_DIR / "sap" / cfg.url_file_name,
        encoding=cfg.url_file_encoding,
        # reset_base_mssql_table() drops "Instance Number" by name.
        required_columns=("Instance Number",) + base_columns,
    )


def _resolve_zabbix_item_name_mapping() -> ResolvedFile:
    return ResolvedFile(
        path=DATA_DIR / "zabbix" / "host_problems" / "item_name_mapping.csv",
        encoding="utf-8",
        required_columns=("name", "category"),
    )


def _resolve_zabbix_service_itemids() -> ResolvedFile:
    return ResolvedFile(
        path=DATA_DIR / "zabbix" / "service_monitoring" / "service_monitoring_itemids.csv",
        encoding="utf-8-sig",
        required_columns=("itemids",),
    )


def _rebuild_hawk_cache():
    # The inventory is read once, when the singleton is built. Rebuild it so
    # the new file is used now rather than at the next backend restart.
    cls = _pipeline_class("HAWK_APPLICATION_MONITORING")
    instance = getattr(cls, "_instance", None)
    if instance is not None:
        instance._build_cache()


def _clear_service_itemids_cache():
    _pipeline_class("ZABBIX_SERVICE_MONITORING")._cache.pop("itemids", None)


DATA_FILES: Dict[str, DataFileSpec] = {
    spec.file_id: spec for spec in (
        DataFileSpec(
            file_id="hawk_applications",
            pipeline="HAWK_APPLICATION_MONITORING",
            description="TIBCO HAWK application inventory (microagents to track per host).",
            applies="Immediately: the in-memory application inventory is rebuilt from the new file.",
            resolve=_resolve_hawk_applications,
            after_replace=_rebuild_hawk_cache,
        ),
        DataFileSpec(
            file_id="sap_urls",
            pipeline="SAP_URL_MONITORING",
            description="SAP landscape URL list that seeds the SAP URL base table.",
            applies="When the SAP URL base table is next recreated (reset with mode=recreate).",
            resolve=_resolve_sap_urls,
        ),
        DataFileSpec(
            file_id="zabbix_item_name_mapping",
            pipeline="ZABBIX_HOST_PROBLEMS",
            description="Maps Zabbix item names to problem categories.",
            applies="On the next pipeline run.",
            resolve=_resolve_zabbix_item_name_mapping,
        ),
        DataFileSpec(
            file_id="zabbix_service_itemids",
            pipeline="ZABBIX_SERVICE_MONITORING",
            description="Zabbix item ids polled by service monitoring.",
            applies="On the next pipeline run: the cached item id list is cleared.",
            resolve=_resolve_zabbix_service_itemids,
            after_replace=_clear_service_itemids_cache,
        ),
    )
}


# =========================
# Helpers
# =========================
def _now_ist() -> datetime:
    return datetime.now(IST)


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _file_info(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "size_bytes": None, "modified_at": None}
    stat = path.stat()
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, IST).isoformat(),
    }


def _json_value(value):
    if value is None or (not isinstance(value, (str, bytes)) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        # The decorator writes IST; a DATETIME column hands it back naive.
        if value.tzinfo is None:
            value = value.replace(tzinfo=IST)
        return value.isoformat()
    return value


def _summarise_jobs(jobs: List[dict]) -> dict:
    """One pipeline-level verdict from its per-table rows.

    The decorator writes every table of a pipeline in the same pass, so the
    rows normally agree; when they do not, FAILED wins.
    """
    statuses = [job["last_status"] for job in jobs]
    if not any(job["recorded"] for job in jobs):
        status = "NEVER_RUN"
    elif "FAILED" in statuses:
        status = "FAILED"
    elif statuses and all(s == "SUCCESS" for s in statuses):
        status = "SUCCESS"
    else:
        # A row exists but no result was written: the first run is in
        # progress, or it died before reporting.
        status = "NO_RESULT"

    attempts = [job["last_attempt_time"] for job in jobs if job["last_attempt_time"]]
    successes = [job["last_success_time"] for job in jobs]
    failed = next((job for job in jobs if job["last_status"] == "FAILED"), None)
    # ISO strings in one fixed offset compare correctly as text.
    return {
        "last_status": status,
        "last_attempt_time": max(attempts) if attempts else None,
        "last_success_time": min(successes) if successes and all(successes) else None,
        "last_message": (failed or (jobs[0] if jobs else {})).get("last_message"),
    }


def _read_frame(path: Path, encoding: Optional[str]) -> pd.DataFrame:
    if encoding is None:
        return pd.read_excel(path, engine="openpyxl")
    return pd.read_csv(path, encoding=encoding)


@dataclass
class TableEntry:
    table: str
    pipeline: str
    table_key: str
    config: TableConfig

    @property
    def blocked_by(self) -> List[str]:
        return [self.pipeline] + TABLE_READERS.get((self.pipeline, self.table_key), [])

    @property
    def reseed_file_id(self) -> Optional[str]:
        return TABLE_RESEED.get((self.pipeline, self.table_key))


# =========================
# Service
# =========================
class Maintenance:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = get_logger("MAINTENANCE")
        self._busy = threading.Lock()

    # --- pipelines / workers ------------------------------------------------
    def _workers_of(self, pipelines) -> list:
        """(registered name, worker) for every worker running one of `pipelines`."""
        with self.orchestrator.lock:
            workers = sorted(self.orchestrator.workers.items())

        refs = [PIPELINES[p] for p in pipelines]
        return [
            (name, worker) for name, worker in workers
            if any(type(worker.pipeline).__module__ == ref.module
                   and type(worker.pipeline).__name__ == ref.class_name
                   for ref in refs)
        ]

    def _pipeline_instance(self, name: str):
        # A registered pipeline is reused so its state is the one that runs.
        for _, worker in self._workers_of([name]):
            return worker.pipeline
        return _pipeline_class(name)()

    @contextmanager
    def _exclusive(self, pipelines):
        """Refuse if any of `pipelines` is running, else keep them from starting.

        Holds each worker's lock (the one PipelineWorker.start() takes) until
        the block exits. Locks are taken in name order, and never while holding
        the orchestrator lock, so this cannot deadlock against start/stop.
        """
        if not self._busy.acquire(blocking=False):
            raise MaintenanceConflict(
                "Another maintenance operation is in progress. Retry when it finishes.",
                code="MAINTENANCE_BUSY",
            )

        held = []
        try:
            workers = self._workers_of(pipelines)
            for _, worker in workers:
                worker.lock.acquire()
                held.append(worker)

            running = [
                name for name, worker in workers
                if worker.thread is not None and worker.thread.is_alive()
            ]
            if running:
                raise MaintenanceConflict(
                    f"Stop these pipelines first: {', '.join(running)}. A stopping "
                    f"pipeline finishes its current run before it reports running=false.",
                    code="PIPELINE_RUNNING",
                    running_pipelines=running,
                )
            yield
        finally:
            for worker in reversed(held):
                worker.lock.release()
            self._busy.release()

    # --- tables ---------------------------------------------------------------
    def _tables_of(self, pipeline_name: str) -> List[TableEntry]:
        pipeline = self._pipeline_instance(pipeline_name)
        # SAP_URL_MONITORING has both; tbls_config is the complete set.
        configs = getattr(pipeline, "tbls_config", None) or {"main": pipeline.tbl_config}
        return [
            TableEntry(table=cfg.sql_table_name, pipeline=pipeline_name, table_key=key, config=cfg)
            for key, cfg in configs.items()
        ]

    def _all_tables(self, pipelines=None) -> Tuple[List[TableEntry], List[dict]]:
        tables, errors = [], []
        for name in pipelines or PIPELINES:
            try:
                tables.extend(self._tables_of(name))
            except Exception as e:
                errors.append({"pipeline": name, "error": format_error(e)})
        return tables, errors

    def _find_table(self, table_name: str) -> TableEntry:
        tables, errors = self._all_tables()
        for entry in tables:
            # SQL Server object names are case-insensitive by default.
            if entry.table.lower() == table_name.lower():
                return entry
        raise MaintenanceNotFound(
            f"No pipeline table named {table_name}.",
            code="TABLE_NOT_FOUND",
            config_errors=errors,
        )

    def list_tables(self) -> dict:
        tables, errors = self._all_tables()
        return {
            "tables": [
                {
                    "table": entry.table,
                    "pipeline": entry.pipeline,
                    "table_key": entry.table_key,
                    "blocked_by": entry.blocked_by,
                    "running_pipelines": [
                        name for name, worker in self._workers_of(entry.blocked_by)
                        if worker.status()["running"]
                    ],
                    "modes": list(RESET_MODES),
                    "recreate_reseeds_from": entry.reseed_file_id,
                    "clears_pipeline_cache": entry.pipeline in DELTA_CACHE_PIPELINES,
                }
                for entry in tables
            ],
            "errors": errors,
        }

    def reset_table(self, table_name: str, mode: str) -> dict:
        if mode not in RESET_MODES:
            raise MaintenanceInvalid(f"mode must be one of {list(RESET_MODES)}, got {mode!r}.")

        entry = self._find_table(table_name)

        with self._exclusive(entry.blocked_by):
            self.logger.info(f"Resetting {entry.table} ({entry.pipeline}) mode={mode}")
            reseeded_from = None
            try:
                if mode == "truncate":
                    mssql = MSSQL_Connector(db=MSSQL_Config())
                    # truncate_table alone never commits; run_queries owns the transaction.
                    mssql.run_queries(lambda cursor: mssql.truncate_table(table=entry.config))

                elif entry.reseed_file_id:
                    # Recreates the table, then loads it from the CSV.
                    self._pipeline_instance(entry.pipeline).reset_base_mssql_table()
                    reseeded_from = _display_path(DATA_FILES[entry.reseed_file_id].resolve().path)

                else:
                    MSSQL_TABLE_MANAGER(tbl_config=entry.config).recreate()

            except Exception as e:
                self.logger.error(f"Reset of {entry.table} failed: {format_error(e)}")
                raise MaintenanceError(
                    f"Reset of {entry.table} (mode={mode}) failed: {format_error(e)}",
                    code="RESET_FAILED",
                )

            cache_cleared = False
            if entry.pipeline in DELTA_CACHE_PIPELINES:
                # The cache describes rows the table no longer has; without it
                # the next run is an initial run and reloads everything.
                _pipeline_class(entry.pipeline)._cache.pop("last_cached_df", None)
                cache_cleared = True

        self.logger.info(f"Reset of {entry.table} done (mode={mode})")
        return {
            "status": "done",
            "table": entry.table,
            "pipeline": entry.pipeline,
            "mode": mode,
            "reseeded_from": reseeded_from,
            "cache_cleared": cache_cleared,
            "finished_at": _now_ist().isoformat(),
        }

    # --- job status -----------------------------------------------------------
    def _read_job_control(self) -> List[dict]:
        mssql = MSSQL_Connector(db=MSSQL_Config())
        df = mssql.select_query(f"SELECT * FROM {JOB_CONTROL_TABLE}")
        return [
            {key: _json_value(value) for key, value in record.items()}
            for record in df.astype(object).to_dict("records")
        ]

    def _resolve_pipeline(self, name: str) -> str:
        """Pipeline class name from a class name or a registered worker name."""
        for pipeline in PIPELINES:
            if pipeline.lower() == name.lower():
                return pipeline

        with self.orchestrator.lock:
            worker = self.orchestrator.workers.get(name)
        if worker is not None:
            for pipeline, ref in PIPELINES.items():
                if type(worker.pipeline).__name__ == ref.class_name:
                    return pipeline

        raise MaintenanceNotFound(f"No pipeline named {name}.", code="PIPELINE_NOT_FOUND")

    def job_status(self, pipeline: Optional[str] = None) -> dict:
        names = [self._resolve_pipeline(pipeline)] if pipeline else list(PIPELINES)

        try:
            rows = self._read_job_control()
        except Exception as e:
            self.logger.error(f"Reading {JOB_CONTROL_TABLE} failed: {format_error(e)}")
            raise MaintenanceError(
                f"Could not read {JOB_CONTROL_TABLE}: {format_error(e)}",
                code="JOB_STATUS_FAILED",
            )

        # job_name is a table name, and SQL Server names are case-insensitive.
        rows_by_job = {str(row.get("job_name")).lower(): row for row in rows}
        tables, errors = self._all_tables(names)

        result = []
        for name in names:
            jobs = []
            for entry in (t for t in tables if t.pipeline == name):
                row = rows_by_job.pop(entry.table.lower(), None)
                job = {field_name: None for field_name in JOB_FIELDS}
                job.update(row or {})
                job.update(job_name=entry.table, table_key=entry.table_key, recorded=row is not None)
                jobs.append(job)

            workers = [
                {"name": worker_name, **worker.status()}
                for worker_name, worker in self._workers_of([name])
            ]
            result.append({
                "pipeline": name,
                "registered": bool(workers),
                "running": any(w["running"] for w in workers),
                "workers": workers,
                **_summarise_jobs(jobs),
                "jobs": jobs,
            })

        return {
            "pipelines": result,
            # Rows no pipeline table accounts for: renamed tables, or jobs
            # written by something outside this backend. Only meaningful
            # when every pipeline was asked for.
            "unmapped_jobs": [] if pipeline else list(rows_by_job.values()),
            "errors": errors,
            "read_at": _now_ist().isoformat(),
        }

    # --- files ----------------------------------------------------------------
    def _file_spec(self, file_id: str) -> DataFileSpec:
        spec = DATA_FILES.get(file_id)
        if spec is None:
            raise MaintenanceNotFound(f"No data file with id {file_id}.", code="FILE_NOT_FOUND")
        return spec

    @staticmethod
    def _archive_dir(path: Path) -> Path:
        return path.parent / ARCHIVE_DIR_NAME

    def _versions(self, path: Path) -> List[dict]:
        archive = self._archive_dir(path)
        if not archive.is_dir():
            return []

        prefix = f"{path.stem}__"
        versions = []
        for p in archive.iterdir():
            if not (p.is_file() and p.name.startswith(prefix) and p.suffix == path.suffix):
                continue
            stamp = p.name[len(prefix):len(prefix) + 15]  # YYYYMMDD_HHMMSS
            try:
                archived_at = datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=IST).isoformat()
            except ValueError:
                archived_at = None
            versions.append({
                "version": p.name,
                "archived_at": archived_at,
                "size_bytes": p.stat().st_size,
                "path": _display_path(p),
            })
        # Names embed the archive time, so name order is time order.
        return sorted(versions, key=lambda v: v["version"], reverse=True)

    def _describe_file(self, spec: DataFileSpec) -> dict:
        target = spec.resolve()
        return {
            "file_id": spec.file_id,
            "pipeline": spec.pipeline,
            "description": spec.description,
            "file_name": target.path.name,
            "path": _display_path(target.path),
            "format": "xlsx" if target.encoding is None else "csv",
            "accepted_extension": target.path.suffix,
            "encoding": target.encoding,
            "required_columns": list(target.required_columns),
            "applies": spec.applies,
            "version_count": len(self._versions(target.path)),
            **_file_info(target.path),
        }

    def list_files(self) -> dict:
        files, errors = [], []
        for spec in DATA_FILES.values():
            try:
                files.append(self._describe_file(spec))
            except Exception as e:
                errors.append({"file_id": spec.file_id, "error": format_error(e)})
        return {"files": files, "errors": errors}

    def get_file(self, file_id: str) -> dict:
        return self._describe_file(self._file_spec(file_id))

    def list_versions(self, file_id: str) -> dict:
        spec = self._file_spec(file_id)
        target = spec.resolve()
        return {
            "file_id": file_id,
            "file_name": target.path.name,
            "archive_dir": _display_path(self._archive_dir(target.path)),
            "versions": self._versions(target.path),
        }

    def file_path(self, file_id: str, version: Optional[str] = None) -> Path:
        """Path of the current file, or of an archived version by its name."""
        target = self._file_spec(file_id).resolve()
        if version is None:
            if not target.path.exists():
                raise MaintenanceNotFound(f"{target.path.name} does not exist.", code="FILE_NOT_FOUND")
            return target.path

        # Only names that _versions() lists are served, so no path can escape
        # the archive directory.
        if version not in {v["version"] for v in self._versions(target.path)}:
            raise MaintenanceNotFound(f"No archived version {version} of {file_id}.", code="VERSION_NOT_FOUND")
        return self._archive_dir(target.path) / version

    def _validate(self, path: Path, target: ResolvedFile) -> int:
        try:
            df = _read_frame(path, target.encoding)
        except Exception as e:
            raise MaintenanceInvalid(f"Could not read the uploaded file: {format_error(e)}", code="INVALID_FILE")

        missing = [col for col in target.required_columns if col not in df.columns]
        if missing:
            raise MaintenanceInvalid(
                f"The uploaded file is missing required column(s): {missing}.",
                code="INVALID_FILE",
                missing_columns=missing,
                found_columns=[str(c) for c in df.columns],
            )

        rows = len(df[list(target.required_columns)].dropna(how="all"))
        if rows == 0:
            raise MaintenanceInvalid("The uploaded file has no data rows.", code="INVALID_FILE")
        return rows

    def _archive(self, path: Path) -> Path:
        archive = self._archive_dir(path)
        archive.mkdir(exist_ok=True)

        stamp = _now_ist().strftime("%Y%m%d_%H%M%S")
        dest = archive / f"{path.stem}__{stamp}{path.suffix}"
        n = 1
        while dest.exists():
            dest = archive / f"{path.stem}__{stamp}_{n}{path.suffix}"
            n += 1

        shutil.copy2(path, dest)
        return dest

    def replace_file(self, file_id: str, upload_name: str, content: bytes) -> dict:
        spec = self._file_spec(file_id)
        target = spec.resolve()

        suffix = Path(upload_name or "").suffix.lower()
        if suffix != target.path.suffix.lower():
            raise MaintenanceInvalid(
                f"Expected a {target.path.suffix} file, got {upload_name!r}.",
                code="INVALID_FILE",
            )

        with self._exclusive([spec.pipeline]):
            target.path.parent.mkdir(parents=True, exist_ok=True)
            # Same directory as the target, so os.replace below is atomic.
            # Keeps the real extension: openpyxl refuses anything but .xlsx.
            staging = target.path.with_name(f".{target.path.stem}.upload{target.path.suffix}")
            try:
                staging.write_bytes(content)
                rows = self._validate(staging, target)

                archived = self._archive(target.path) if target.path.exists() else None
                os.replace(staging, target.path)
            finally:
                staging.unlink(missing_ok=True)

            if spec.after_replace:
                try:
                    spec.after_replace()
                except Exception as e:
                    # The file is already in place; report, do not undo.
                    self.logger.error(f"{file_id}: post-replace step failed: {format_error(e)}")
                    raise MaintenanceError(
                        f"{target.path.name} was replaced, but refreshing {spec.pipeline} "
                        f"failed: {format_error(e)}. Restart the backend to pick it up.",
                        code="POST_REPLACE_FAILED",
                    )

        self.logger.info(
            f"Replaced {_display_path(target.path)} with upload {upload_name!r} "
            f"({rows} rows); previous version archived as "
            f"{_display_path(archived) if archived else 'none'}"
        )
        return {
            "status": "replaced",
            "file_id": file_id,
            "pipeline": spec.pipeline,
            "file_name": target.path.name,
            "path": _display_path(target.path),
            "uploaded_name": upload_name,
            "rows": rows,
            "archived_as": archived.name if archived else None,
            "archived_path": _display_path(archived) if archived else None,
            "applies": spec.applies,
            "replaced_at": _now_ist().isoformat(),
        }
