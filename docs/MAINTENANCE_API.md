# BACC Maintenance API

Reference for building the frontend's maintenance screens. The API covers three jobs:

1. **Show each pipeline's last run** (success or failure, when, and the error message), read from `BACC_ETL_JOB_CONTROL` and combined with whether the pipeline is running now.
2. **Reset a pipeline's SQL table**, either by emptying it (`truncate`) or by dropping and rebuilding it from the configured schema (`recreate`).
3. **Replace a data file a pipeline depends on.** The current file is archived, and the upload takes its place under the current file's name.

Base URL: the BACC backend (`run_server.py` serves on port `8000`). There is no authentication: these endpoints have the same access as the rest of the API. Every path below is under `/maintenance`. FastAPI's interactive docs at `/docs` list them under the **maintenance** tag.

---

## Contents

- [Rules that apply to every call](#rules-that-apply-to-every-call)
- [Workflow: reset a table](#workflow-reset-a-table)
- [Workflow: replace a file](#workflow-replace-a-file)
- [Endpoints](#endpoints)
  - [GET /maintenance/job-status](#get-maintenancejob-status)
  - [GET /maintenance/tables](#get-maintenancetables)
  - [POST /maintenance/tables/{table}/reset](#post-maintenancetablestablereset)
  - [GET /maintenance/files](#get-maintenancefiles)
  - [GET /maintenance/files/{file_id}](#get-maintenancefilesfile_id)
  - [PUT /maintenance/files/{file_id}](#put-maintenancefilesfile_id)
  - [GET /maintenance/files/{file_id}/versions](#get-maintenancefilesfile_idversions)
  - [GET /maintenance/files/{file_id}/download](#get-maintenancefilesfile_iddownload)
- [Errors](#errors)
- [What exists today](#what-exists-today)
- [TypeScript types](#typescript-types)
- [Example client](#example-client)
- [Limitations](#limitations)

---

## Rules that apply to every call

**The pipeline must be stopped.** A reset or replace is refused with `409 PIPELINE_RUNNING` while any pipeline that uses the table or file is running. The error lists the names to stop in `detail.running_pipelines`. "Running" means the same as in `GET /pipelines`: the worker is scheduled, including while it waits between runs.

`GET /maintenance/job-status` is read-only and is exempt from these rules: it can be called at any time.

**Stopping is not instant.** `POST /pipeline/{name}` with `{"method": "stop"}` asks the worker to stop, but a run already in progress finishes first. Poll `POST /pipeline/{name}` with `{"method": "status"}` until `running` is `false`, then call maintenance.

**Starts wait for maintenance.** While a reset or replace is in progress, a start request for the affected pipeline is held until the operation finishes, which usually takes well under a second. The pipeline never starts halfway through an operation.

**One operation at a time.** A second maintenance call made while one is in progress gets `409 MAINTENANCE_BUSY`. Retry after a short delay.

**Every operation is synchronous.** The response arrives once the work is done. There is no job to poll.

---

## Workflow: reset a table

```
GET  /maintenance/tables                      → find the table, read running_pipelines
POST /pipeline/{name} {"method":"stop"}       → for each name in running_pipelines
POST /pipeline/{name} {"method":"status"}     → repeat until running == false
POST /maintenance/tables/{table}/reset        → {"mode": "truncate" | "recreate"}
POST /pipeline/{name} {"method":"run", "interval": <previous interval>}   → restart, if it was running
```

Choosing the mode:

| Mode | What it does | Use when |
|---|---|---|
| `truncate` | Deletes every row. Schema, keys and indexes are unchanged. | The data is bad but the table is fine. |
| `recreate` | `DROP TABLE` then `CREATE TABLE` from the schema in `config_yaml/*.yaml`. | The table is missing, or its columns no longer match the config (e.g. a column was added to the YAML). |

Both modes delete all data in the table, so ask the user to confirm in the UI, naming the table.

Side effects to show the user (both are reported per table by `GET /maintenance/tables`):

- **`recreate_reseeds_from`**: `Phoenix_Landscape_URLs_V1` is reloaded from the `sap_urls` file after a `recreate`. A `truncate` leaves it empty, and SAP URL monitoring then has nothing to poll until it is recreated.
- **`clears_pipeline_cache`**: the Zabbix hosts, host group and host problems pipelines only write changes against what they loaded last time. The reset clears that memory, so their next run reloads everything.

To restart a pipeline at the interval it had, read the `interval` from `GET /pipelines` before stopping it.

## Workflow: replace a file

```
GET  /maintenance/files                       → show the file, its required columns and accepted extension
POST /pipeline/{name} {"method":"stop"}       → stop the owning pipeline (see "pipeline")
POST /pipeline/{name} {"method":"status"}     → repeat until running == false
PUT  /maintenance/files/{file_id}             → multipart upload, field name "file"
POST /pipeline/{name} {"method":"run", ...}   → restart
```

What happens on upload:

1. The extension must match the current file (`accepted_extension`). Otherwise `400 INVALID_FILE`.
2. The upload is parsed the same way the pipeline reads it, with the same encoding, and must contain every column in `required_columns` plus at least one data row. Otherwise `400 INVALID_FILE`, with `missing_columns` and `found_columns` included.
3. The current file is copied to `<its folder>/archive/<name>__<YYYYMMDD_HHMMSS><ext>`, timestamped in IST.
4. The upload replaces the current file and **takes the current file's name**, whatever it was called on the user's machine. A user can upload `SAP_URLs_new_final.csv` and it is saved as `SAP_URLs_V1.csv`.

If step 1 or 2 fails, nothing on disk changes. Old versions are never deleted.

When the pipeline picks up the new file differs per file (see `applies`). In particular, a new **`sap_urls`** file is only loaded into the database when `Phoenix_Landscape_URLs_V1` is reset with `mode: "recreate"`, so offer that reset straight after the upload.

---

## Endpoints

### GET /maintenance/job-status

Reports the last run of every pipeline. The data comes from the `BACC_ETL_JOB_CONTROL` table and is combined with whether each pipeline's worker is running. Use it for a status dashboard.

**Query parameters**

| Name | Required | Meaning |
|---|---|---|
| `pipeline` | No | Return only this pipeline. Accepts the pipeline class name (case-insensitive, e.g. `TIBCO_EMS`) or a registered name from `GET /pipelines`. |

**Response 200**

```json
{
  "pipelines": [
    {
      "pipeline": "HAWK_APPLICATION_MONITORING",
      "registered": true,
      "running": true,
      "workers": [
        { "name": "HAWK_APPLICATION_MONITORING", "running": true, "interval": 300 }
      ],
      "last_status": "FAILED",
      "last_attempt_time": "2026-10-13T10:05:00+05:30",
      "last_success_time": "2026-10-13T10:00:00+05:30",
      "last_message": "Recoverable Error: load_data -> ...",
      "jobs": [
        {
          "job_name": "TIBCO_ADMIN_APPLICATION_MONITORING",
          "table_key": "application_status_table",
          "recorded": true,
          "last_attempt_time": "2026-10-13T10:05:00+05:30",
          "last_success_time": "2026-10-13T10:00:00+05:30",
          "last_status": "FAILED",
          "last_message": "Recoverable Error: load_data -> ..."
        },
        {
          "job_name": "TIBCO_ADMIN_APPLICATION_MONITORING_HOSTS",
          "table_key": "host_status_table",
          "recorded": true,
          "last_attempt_time": "2026-10-13T10:05:00+05:30",
          "last_success_time": "2026-10-13T10:00:00+05:30",
          "last_status": "FAILED",
          "last_message": "Recoverable Error: load_data -> ..."
        }
      ]
    }
  ],
  "unmapped_jobs": [],
  "errors": [],
  "read_at": "2026-10-13T10:06:12.481023+05:30"
}
```

Every pipeline the backend knows about is listed, even if it has never run or is not registered.

Pipeline fields:

| Field | Meaning |
|---|---|
| `pipeline` | Pipeline class name. |
| `registered` | Whether it is registered with the orchestrator. Unregistered pipelines never run. |
| `running` | Whether any of its workers is running (scheduled), same as `GET /pipelines`. |
| `workers` | One entry per registered worker: `name` (use it with `/pipeline/{name}`), `running` and `interval` in seconds. |
| `last_status` | Summary of the pipeline's `jobs`, using the values below. |
| `last_attempt_time` | When the most recent run started. |
| `last_success_time` | When the pipeline last succeeded on every one of its tables. `null` if it never has. |
| `last_message` | The failure message if a table failed, otherwise the latest message, usually `Completed successfully`. |
| `jobs` | The raw control rows, one per table the pipeline writes. |

`last_status` values:

| Value | Meaning |
|---|---|
| `SUCCESS` | The last run succeeded on every table. |
| `FAILED` | The last run failed on at least one table. `last_message` says why. |
| `NO_RESULT` | A run was started but no result was ever recorded. Either the first run is still in progress, or it stopped before reporting. |
| `NEVER_RUN` | No row in the control table. |

Job fields: `job_name` is the SQL table name. `recorded` is `false` when the control table has no row for it, in which case the four `last_*` fields are `null`. `last_status` here is the raw `SUCCESS`, `FAILED` or `null`. Any extra columns in the control table are passed through as well.

All times are ISO 8601 with the IST offset (`+05:30`).

**How to read "is it running right now?"** `last_attempt_time` is written when a run starts, and `last_status` when it ends. If `last_attempt_time` is later than `last_success_time` while `last_status` is `SUCCESS`, a new run is probably in progress. The control table does not record failure times, so this can't be told apart after a failure. `running` only says the worker is scheduled, not that a run is executing at this moment.

`unmapped_jobs` lists control rows that match no current pipeline table, such as tables renamed in the config or jobs written by something else. It is always empty when `pipeline` is given. `errors` lists pipelines whose config could not be loaded; their `jobs` list is empty.

**Errors**: `404 PIPELINE_NOT_FOUND`, `500 JOB_STATUS_FAILED` (the database could not be read; `message` has the reason).

### GET /maintenance/tables

Lists every table the pipelines own, with what is blocking a reset right now.

**Response 200**

```json
{
  "tables": [
    {
      "table": "Phoenix_Landscape_URLs_V1_POLL",
      "pipeline": "SAP_URL_MONITORING",
      "table_key": "poll_table",
      "blocked_by": ["SAP_URL_MONITORING", "SAP_SOLMAN"],
      "running_pipelines": ["SAP_SOLMAN"],
      "modes": ["truncate", "recreate"],
      "recreate_reseeds_from": null,
      "clears_pipeline_cache": false
    }
  ],
  "errors": []
}
```

| Field | Meaning |
|---|---|
| `table` | SQL table name. Use it in the reset URL. |
| `pipeline` | Pipeline class that writes the table. |
| `table_key` | The table's role within the pipeline (`main` for single-table pipelines). |
| `blocked_by` | Pipeline classes that must not be running for a reset. Some tables are read by a second pipeline. |
| `running_pipelines` | **Registered names** (as used in `/pipeline/{name}`) of workers of those classes that are running now. Empty means the reset is allowed. |
| `modes` | Always `["truncate", "recreate"]`. |
| `recreate_reseeds_from` | `file_id` the table is reloaded from after `recreate`, or `null`. |
| `clears_pipeline_cache` | `true` if a reset also clears the pipeline's in-memory delta cache. |
| `errors` | Pipelines whose config could not be loaded, as `{"pipeline", "error"}`. Their tables are missing from `tables`. |

`running_pipelines` is a snapshot. The reset call re-checks it.

### POST /maintenance/tables/{table}/reset

`{table}` is the SQL table name. It is matched case-insensitively, like SQL Server.

**Request body** (`application/json`)

```json
{ "mode": "truncate" }
```

`mode` is `"truncate"` or `"recreate"` and is required.

**Response 200**

```json
{
  "status": "done",
  "table": "Phoenix_Landscape_URLs_V1",
  "pipeline": "SAP_URL_MONITORING",
  "mode": "recreate",
  "reseeded_from": "data/sap/SAP_URLs_V1.csv",
  "cache_cleared": false,
  "finished_at": "2026-10-13T15:42:07.512304+05:30"
}
```

`reseeded_from` is `null` unless the table was reloaded from a file.

**Errors**: `404 TABLE_NOT_FOUND`, `409 PIPELINE_RUNNING`, `409 MAINTENANCE_BUSY`, `422` (missing or invalid `mode`), `500 RESET_FAILED` (the database refused; `message` has the reason).

A failed `truncate` or plain `recreate` is rolled back, leaving the table as it was. For `Phoenix_Landscape_URLs_V1`, the recreate and the reload from CSV are separate steps: if the reload fails, the table exists but is empty. Fix the cause (usually the file) and recreate again.

### GET /maintenance/files

Lists every data file a pipeline depends on.

**Response 200**

```json
{
  "files": [
    {
      "file_id": "zabbix_item_name_mapping",
      "pipeline": "ZABBIX_HOST_PROBLEMS",
      "description": "Maps Zabbix item names to problem categories.",
      "file_name": "item_name_mapping.csv",
      "path": "data/zabbix/host_problems/item_name_mapping.csv",
      "format": "csv",
      "accepted_extension": ".csv",
      "encoding": "utf-8",
      "required_columns": ["name", "category"],
      "applies": "On the next pipeline run.",
      "version_count": 0,
      "exists": true,
      "size_bytes": 8082,
      "modified_at": "2026-09-01T19:36:03.824613+05:30"
    }
  ],
  "errors": []
}
```

| Field | Meaning |
|---|---|
| `file_id` | Stable id. Use it in the URLs below. |
| `pipeline` | Pipeline class that reads the file. It must be stopped to replace the file. |
| `file_name`, `path` | Current name and location, relative to the backend root. |
| `format` | `csv` or `xlsx`. |
| `accepted_extension` | Use it as the file picker's `accept` value. |
| `encoding` | Encoding the CSV is read with (`null` for Excel). Uploads must use it. |
| `required_columns` | Header names the upload must contain, exactly, including case and spacing. Extra columns are allowed. |
| `applies` | When the pipeline starts using a new version. Show it after a successful upload. |
| `version_count` | Number of archived versions. |
| `exists`, `size_bytes`, `modified_at` | Current file on disk. `size_bytes` and `modified_at` are `null` when it does not exist. |

### GET /maintenance/files/{file_id}

Same object as one entry of `files` above. **Errors**: `404 FILE_NOT_FOUND`.

### PUT /maintenance/files/{file_id}

Replaces the file. Send it as `multipart/form-data` with a single field named **`file`**. Uploads are limited to 20 MB.

**Response 200**

```json
{
  "status": "replaced",
  "file_id": "sap_urls",
  "pipeline": "SAP_URL_MONITORING",
  "file_name": "SAP_URLs_V1.csv",
  "path": "data/sap/SAP_URLs_V1.csv",
  "uploaded_name": "SAP URLs - October.csv",
  "rows": 214,
  "archived_as": "SAP_URLs_V1__20261013_154207.csv",
  "archived_path": "data/sap/archive/SAP_URLs_V1__20261013_154207.csv",
  "applies": "When the SAP URL base table is next recreated (reset with mode=recreate).",
  "replaced_at": "2026-10-13T15:42:07.611290+05:30"
}
```

`rows` is the number of data rows read from the upload. `archived_as` and `archived_path` are `null` if there was no current file to archive.

**Errors**: `400 INVALID_FILE` (wrong extension, unreadable, missing columns, no rows), `404 FILE_NOT_FOUND`, `409 PIPELINE_RUNNING`, `409 MAINTENANCE_BUSY`, `413 FILE_TOO_LARGE`, `422` (no `file` field), `500 POST_REPLACE_FAILED`.

`POST_REPLACE_FAILED` only applies to `hawk_applications`: the file was replaced, but rebuilding the HAWK inventory in memory failed. The new file is in place and is used after a backend restart.

Example `400` for a file with a renamed column:

```json
{
  "detail": {
    "code": "INVALID_FILE",
    "message": "The uploaded file is missing required column(s): ['category'].",
    "missing_columns": ["category"],
    "found_columns": ["name", "Category"]
  }
}
```

### GET /maintenance/files/{file_id}/versions

Lists archived versions, newest first.

**Response 200**

```json
{
  "file_id": "sap_urls",
  "file_name": "SAP_URLs_V1.csv",
  "archive_dir": "data/sap/archive",
  "versions": [
    {
      "version": "SAP_URLs_V1__20261013_154207.csv",
      "archived_at": "2026-10-13T15:42:07+05:30",
      "size_bytes": 40311,
      "path": "data/sap/archive/SAP_URLs_V1__20261013_154207.csv"
    }
  ]
}
```

`archived_at` is when the version was replaced, not when it was created. **Errors**: `404 FILE_NOT_FOUND`.

### GET /maintenance/files/{file_id}/download

Downloads the current file. With `?version=<version>` it downloads that archived version instead. Use the `version` value exactly as returned by `/versions`. The response is the raw file with a `Content-Disposition` filename.

**Errors**: `404 FILE_NOT_FOUND`, `404 VERSION_NOT_FOUND`.

The API has no restore endpoint. To roll back, download an archived version and upload it again with `PUT`. That archives the current file in turn, so nothing is lost.

---

## Errors

Every maintenance error has this shape:

```json
{ "detail": { "code": "PIPELINE_RUNNING", "message": "Stop these pipelines first: ...", "running_pipelines": ["SAP_SOLMAN"] } }
```

`message` can be shown to the user as is. Branch on `code`:

| HTTP | `code` | Meaning | Suggested UI |
|---|---|---|---|
| 400 | `INVALID_FILE` | Wrong extension, unreadable, missing columns or no rows. Extra fields: `missing_columns` and `found_columns` when columns are missing. | Show the message and list the missing columns. |
| 404 | `PIPELINE_NOT_FOUND` | No pipeline with that class or registered name (`job-status`). | Refresh the pipeline list. |
| 404 | `TABLE_NOT_FOUND` | No pipeline table with that name. Extra field: `config_errors` lists pipelines whose config failed to load. | Refresh the table list. |
| 404 | `FILE_NOT_FOUND` | Unknown `file_id`, or the current file does not exist (download). | Refresh the file list. |
| 404 | `VERSION_NOT_FOUND` | No archived version with that name. | Refresh the version list. |
| 409 | `PIPELINE_RUNNING` | Extra field: `running_pipelines` lists the registered names to stop. | Offer "Stop and retry". |
| 409 | `MAINTENANCE_BUSY` | Another maintenance call is in progress. | Retry after a short delay. |
| 413 | `FILE_TOO_LARGE` | Upload over 20 MB. | Show the message. |
| 500 | `JOB_STATUS_FAILED` | `BACC_ETL_JOB_CONTROL` could not be read. | Show the message and keep the last known status. |
| 500 | `RESET_FAILED` | The database rejected the reset. | Show the message. |
| 500 | `POST_REPLACE_FAILED` | File replaced, refresh failed (HAWK only). | Show it as a warning, not as a failure. |
| 422 | none | Request body failed validation. FastAPI's standard error: `detail` is a **list**. | Indicates a frontend bug. |

Note that the older pipeline endpoints (`/pipeline/...`, `/pipelines`) report most failures as **HTTP 200 with `{"error": "..."}`**. Check for an `error` key when calling them.

---

## What exists today

The lists come from the backend config and should be read from the API at runtime, not hard-coded. This is the current state, for reference.

**Tables**

| Table | Pipeline | Notes |
|---|---|---|
| `TIBCO_EVENTS_OPERATIONAL_ERRORS` | `TIBCO_AUDIT_OPERATIONAL_ERRORS` | |
| `TIBCO_HAWK` | `TIBCO_HAWK` | |
| `TIBCO_B2B_ERRORS` | `TIBCO_B2B_ERRORS` | |
| `TIBCO_TRA_LOG` | `TIBCO_TRA_LOG` | |
| `TIBCO_EMS` | `TIBCO_EMS` | |
| `TIBCO_ADMIN_APPLICATION_MONITORING` | `HAWK_APPLICATION_MONITORING` | Application status. |
| `TIBCO_ADMIN_APPLICATION_MONITORING_HOSTS` | `HAWK_APPLICATION_MONITORING` | Host status. |
| `SAP_SYSMON_LIVE` | `SAP_SOLMAN` | |
| `Phoenix_Landscape_URLs_V1` | `SAP_URL_MONITORING` | URL list. `recreate` reloads it from `sap_urls`. |
| `Phoenix_Landscape_URLs_V1_POLL` | `SAP_URL_MONITORING` | Poll results. `SAP_SOLMAN` reads it too, so it must also be stopped. |
| `SERVICENOW_PROD` | `SNOW_OPEN_INCIDENTS` | |
| `ZBX_HOST_GROUP` | `ZABBIX_HOST_GROUP` | Clears the delta cache. |
| `ZBX_HOSTS` | `ZABBIX_HOSTS` | Clears the delta cache. |
| `ZBX_HOST_PROBLEMS` | `ZABBIX_HOST_PROBLEMS` | Clears the delta cache. |
| `BACC_Service_Monitoring` | `ZABBIX_SERVICE_MONITORING` | |

`BACC_ETL_JOB_CONTROL` (last run status per job) cannot be reset because its schema is not defined in the backend config. Read it with `GET /maintenance/job-status`.

**Files**

| `file_id` | Current file | Pipeline | Required columns | Takes effect |
|---|---|---|---|---|
| `hawk_applications` | `data/hawk/TIBCO HAWK APPLICATIONS FORMATTED.xlsx` | `HAWK_APPLICATION_MONITORING` | `dns`, `domain`, `host_name`, `type`, `deployment`, `component` | Immediately |
| `sap_urls` | `data/sap/SAP_URLs_V1.csv` (latin1) | `SAP_URL_MONITORING` | `Instance Number`, `Solution`, `Environment`, `SID`, `Location`, `Hostname`, `Application Category`, `Application stack`, `link to Fiori Launchpad`, `Central Web Dispatcher`, `DB Type`, `Cluster`, `OS  Type`, `OS version` | On the next `recreate` of `Phoenix_Landscape_URLs_V1` |
| `zabbix_item_name_mapping` | `data/zabbix/host_problems/item_name_mapping.csv` | `ZABBIX_HOST_PROBLEMS` | `name`, `category` | Next run |
| `zabbix_service_itemids` | `data/zabbix/service_monitoring/service_monitoring_itemids.csv` | `ZABBIX_SERVICE_MONITORING` | `itemids` | Next run |

`OS  Type` has **two spaces**. It matches the existing file and the SQL column.

---

## TypeScript types

```ts
export type ResetMode = "truncate" | "recreate";

export type PipelineRunStatus = "SUCCESS" | "FAILED" | "NO_RESULT" | "NEVER_RUN";

export interface JobRow {
  job_name: string;            // SQL table name
  table_key: string;
  recorded: boolean;
  last_attempt_time: string | null;
  last_success_time: string | null;
  last_status: "SUCCESS" | "FAILED" | null;
  last_message: string | null;
  [extra: string]: unknown;    // any other control-table columns
}

export interface PipelineJobStatus {
  pipeline: string;
  registered: boolean;
  running: boolean;
  workers: { name: string; running: boolean; interval: number }[];
  last_status: PipelineRunStatus;
  last_attempt_time: string | null;
  last_success_time: string | null;
  last_message: string | null;
  jobs: JobRow[];
}

export interface JobStatusResponse {
  pipelines: PipelineJobStatus[];
  unmapped_jobs: Record<string, unknown>[];
  errors: { pipeline: string; error: string }[];
  read_at: string;
}

export interface MaintenanceTable {
  table: string;
  pipeline: string;
  table_key: string;
  blocked_by: string[];
  running_pipelines: string[];
  modes: ResetMode[];
  recreate_reseeds_from: string | null;
  clears_pipeline_cache: boolean;
}

export interface TableList {
  tables: MaintenanceTable[];
  errors: { pipeline: string; error: string }[];
}

export interface ResetResult {
  status: "done";
  table: string;
  pipeline: string;
  mode: ResetMode;
  reseeded_from: string | null;
  cache_cleared: boolean;
  finished_at: string; // ISO 8601, IST offset
}

export interface DataFile {
  file_id: string;
  pipeline: string;
  description: string;
  file_name: string;
  path: string;
  format: "csv" | "xlsx";
  accepted_extension: string;
  encoding: string | null;
  required_columns: string[];
  applies: string;
  version_count: number;
  exists: boolean;
  size_bytes: number | null;
  modified_at: string | null;
}

export interface FileList {
  files: DataFile[];
  errors: { file_id: string; error: string }[];
}

export interface ReplaceResult {
  status: "replaced";
  file_id: string;
  pipeline: string;
  file_name: string;
  path: string;
  uploaded_name: string;
  rows: number;
  archived_as: string | null;
  archived_path: string | null;
  applies: string;
  replaced_at: string;
}

export interface FileVersion {
  version: string;
  archived_at: string | null;
  size_bytes: number;
  path: string;
}

export interface VersionList {
  file_id: string;
  file_name: string;
  archive_dir: string;
  versions: FileVersion[];
}

export type MaintenanceErrorCode =
  | "INVALID_FILE" | "PIPELINE_NOT_FOUND" | "TABLE_NOT_FOUND" | "FILE_NOT_FOUND" | "VERSION_NOT_FOUND"
  | "PIPELINE_RUNNING" | "MAINTENANCE_BUSY" | "FILE_TOO_LARGE"
  | "JOB_STATUS_FAILED" | "RESET_FAILED" | "POST_REPLACE_FAILED";

export interface MaintenanceErrorDetail {
  code: MaintenanceErrorCode;
  message: string;
  running_pipelines?: string[];
  missing_columns?: string[];
  found_columns?: string[];
  config_errors?: { pipeline: string; error: string }[];
}
```

## Example client

```ts
const BASE = "/api"; // wherever the frontend reaches the BACC backend

export class MaintenanceError extends Error {
  constructor(public status: number, public detail: MaintenanceErrorDetail | unknown) {
    super(typeof detail === "object" && detail && "message" in detail
      ? String((detail as MaintenanceErrorDetail).message)
      : `HTTP ${status}`);
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  const body = await res.json().catch(() => null);
  if (!res.ok) throw new MaintenanceError(res.status, body?.detail);
  return body as T;
}

export const maintenance = {
  jobStatus: (pipeline?: string) =>
    call<JobStatusResponse>("/maintenance/job-status" +
      (pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : "")),

  listTables: () => call<TableList>("/maintenance/tables"),

  resetTable: (table: string, mode: ResetMode) =>
    call<ResetResult>(`/maintenance/tables/${encodeURIComponent(table)}/reset`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode }),
    }),

  listFiles: () => call<FileList>("/maintenance/files"),

  replaceFile: (fileId: string, file: File) => {
    const form = new FormData();
    form.append("file", file); // field name must be "file"
    // Do not set Content-Type; the browser adds the multipart boundary.
    return call<ReplaceResult>(`/maintenance/files/${encodeURIComponent(fileId)}`, {
      method: "PUT",
      body: form,
    });
  },

  listVersions: (fileId: string) =>
    call<VersionList>(`/maintenance/files/${encodeURIComponent(fileId)}/versions`),

  downloadUrl: (fileId: string, version?: string) =>
    `${BASE}/maintenance/files/${encodeURIComponent(fileId)}/download` +
    (version ? `?version=${encodeURIComponent(version)}` : ""),
};

// Stop the given pipelines and wait until each reports running=false.
export async function stopAndWait(names: string[], timeoutMs = 10 * 60_000) {
  const post = (name: string, body: object) =>
    fetch(`${BASE}/pipeline/${encodeURIComponent(name)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(r => r.json());

  await Promise.all(names.map(n => post(n, { method: "stop" })));

  const deadline = Date.now() + timeoutMs;
  for (const name of names) {
    while ((await post(name, { method: "status" })).running) {
      if (Date.now() > deadline) throw new Error(`${name} did not stop in time`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}
```

Handling a `409 PIPELINE_RUNNING` with "Stop and retry":

```ts
try {
  await maintenance.resetTable(table, mode);
} catch (e) {
  if (e instanceof MaintenanceError && (e.detail as MaintenanceErrorDetail)?.code === "PIPELINE_RUNNING") {
    const names = (e.detail as MaintenanceErrorDetail).running_pipelines ?? [];
    if (confirm(`Stop ${names.join(", ")} and continue?`)) {
      await stopAndWait(names);
      await maintenance.resetTable(table, mode);
      // Restart afterwards: POST /pipeline/{name} {"method":"run","interval":...}
    }
  } else throw e;
}
```

---

## Limitations

- **`run_once` is invisible.** A pipeline started with `POST /pipeline/{name}` `{"method": "run_once"}` runs on the request thread, not on its worker, so the running check cannot see it. Do not start maintenance while a `run_once` call is still pending.
- **Archives are never pruned.** Each replace adds one file under `archive/`. Archive folders are git-ignored on the server.
- **Replaced files differ from git.** The live data files are tracked in git, so a server updated with `git pull` will show them as modified. Commit the new version, or keep the server's copy, before pulling.
- **No authentication.** Anyone who can reach the backend can drop tables and replace files. Keep the backend on the internal network, behind the frontend.
