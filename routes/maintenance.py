"""
Maintenance API: reset pipeline tables and replace pipeline data files.

All logic lives in framework/maintenance.py; this module only maps HTTP to it.
Frontend reference: docs/MAINTENANCE_API.md.

ENDPOINTS
---------
  GET  /maintenance/tables                        list resettable tables
  POST /maintenance/tables/{table}/reset          {"mode": "truncate" | "recreate"}

  GET  /maintenance/job-status                    last run per pipeline (BACC_ETL_JOB_CONTROL)
                                                  ?pipeline=<class or registered name>

  GET  /maintenance/files                         list replaceable data files
  GET  /maintenance/files/{file_id}               one file's details
  PUT  /maintenance/files/{file_id}               multipart upload, field "file"
  GET  /maintenance/files/{file_id}/versions      archived versions, newest first
  GET  /maintenance/files/{file_id}/download      current file, or ?version=<name>

Errors are {"detail": {"code": ..., "message": ..., ...}} with 400, 404, 409,
413 or 500. 409 means a pipeline using the resource is running (stop it with
POST /pipeline/{name} {"method": "stop"}) or another maintenance operation is
in progress.

INSTALL
-------
In main.py, after `orchestrator = Orchestrator()`:

    from routes.maintenance import build_router as build_maintenance_router
    app.include_router(build_maintenance_router(orchestrator))
"""

from typing import Literal, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from framework.maintenance import Maintenance, MaintenanceError

MAX_UPLOAD_BYTES = 20 * 1024 * 1024


class ResetRequest(BaseModel):
    mode: Literal["truncate", "recreate"]


def _http(error: MaintenanceError) -> HTTPException:
    return HTTPException(status_code=error.status_code, detail=error.to_dict())


def build_router(orchestrator) -> APIRouter:
    router = APIRouter(prefix="/maintenance", tags=["maintenance"])
    maintenance = Maintenance(orchestrator)

    # Plain `def` handlers: every operation blocks on SQL Server or the disk,
    # so FastAPI runs them on its thread pool instead of the event loop.

    # --- tables ---------------------------------------------------------------
    @router.get("/tables")
    def list_tables():
        return maintenance.list_tables()

    @router.post("/tables/{table_name}/reset")
    def reset_table(table_name: str, req: ResetRequest):
        try:
            return maintenance.reset_table(table_name, req.mode)
        except MaintenanceError as e:
            raise _http(e)

    # --- job status -----------------------------------------------------------
    @router.get("/job-status")
    def job_status(pipeline: Optional[str] = None):
        try:
            return maintenance.job_status(pipeline)
        except MaintenanceError as e:
            raise _http(e)

    # --- files ----------------------------------------------------------------
    @router.get("/files")
    def list_files():
        return maintenance.list_files()

    @router.get("/files/{file_id}")
    def get_file(file_id: str):
        try:
            return maintenance.get_file(file_id)
        except MaintenanceError as e:
            raise _http(e)

    @router.put("/files/{file_id}")
    def replace_file(file_id: str, file: UploadFile = File(...)):
        # Read one byte past the limit to tell "exactly at" from "over".
        content = file.file.read(MAX_UPLOAD_BYTES + 1)
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail={
                "code": "FILE_TOO_LARGE",
                "message": f"Uploads are limited to {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.",
            })
        try:
            return maintenance.replace_file(file_id, file.filename, content)
        except MaintenanceError as e:
            raise _http(e)

    @router.get("/files/{file_id}/versions")
    def list_versions(file_id: str):
        try:
            return maintenance.list_versions(file_id)
        except MaintenanceError as e:
            raise _http(e)

    @router.get("/files/{file_id}/download")
    def download_file(file_id: str, version: Optional[str] = None):
        try:
            path = maintenance.file_path(file_id, version)
        except MaintenanceError as e:
            raise _http(e)
        return FileResponse(path, filename=path.name)

    return router
