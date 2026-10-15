from fastapi import FastAPI
from orchestrator.orchestrator import Orchestrator, RegisterRequest, ControlRequest, BulkControlRequest
from typing import List

# =========================
# FastAPI App
# =========================
app = FastAPI()
orchestrator = Orchestrator()

from routes.interval_control import build_router
app.include_router(build_router(orchestrator))

from routes.maintenance import build_router as build_maintenance_router
app.include_router(build_maintenance_router(orchestrator))

# =========================
# API Endpoints
# =========================

"""
USE THIS API TO REGISTER AN INDIVIDUAL PIPELINE.

ex:
{
  "name": "TIBCO_AUDIT_OPERATIONAL_ERRORS",
  "module": "pipelines.tibco.operational_errors.pipeline",
  "class_name": "TIBCO_AUDIT_OPERATIONAL_ERRORS"
  "interval" : 60
}
"""
@app.post("/pipeline/register")
def register_pipeline(req: RegisterRequest):
    try:
        orchestrator.register_dynamic(req.name, req.module, req.class_name, req.interval)
        return {"status": f"{req.name} registered"}
    except Exception as e:
        return {"error": str(e)}


"""
USE THIS API TO REGISTER PIPELINES IN BULK.

ex:
[ pipeline1, pipeline2, ... ]
"""
@app.post("/pipeline/register/bulk")
def register_pipelines_bulk(reqs: List[RegisterRequest]):
    results = []

    for req in reqs:
        try:
            orchestrator.register_dynamic(req.name, req.module, req.class_name, req.interval)
            results.append({
                "name": req.name,
                "status": "registered"
            })
        except Exception as e:
            results.append({
                "name": req.name,
                "error": str(e)
            })

    return {
        "total": len(reqs),
        "results": results
    }

"""
USE THIS API TO RUN or STOP all the registered pipelines
"""
@app.post("/pipelines/all")
def control_all_pipelines(req: BulkControlRequest):
    try:
        if req.method == "run":
            return {
                "status": orchestrator.start_all()
            }

        elif req.method == "stop":
            return {
                "status": orchestrator.stop_all()
            }

        else:
            return {"error": "invalid method"}

    except Exception as e:
        return {"error": str(e)}


"""
USE THIS API TO CONTORL INDIVIDUAL PIPELINE.

ex:
{
    "method" : "run" # stop, status, run_once, status
    "interval" : 400 # can give new interval.
}
"""
@app.post("/pipeline/{name}")
def control_pipeline(name: str, req: ControlRequest):
    try:
        if req.method == "run":
            if not req.interval:
                return {"error": "interval required"}
            return {"status": orchestrator.start(name, req.interval)}

        elif req.method == "stop":
            return {"status": orchestrator.stop(name)}

        elif req.method == "status":
            return orchestrator.status(name)

        elif req.method == "run_once":
            worker = orchestrator.workers.get(name)
            if not worker:
                return {"error": "pipeline not found"}

            worker.pipeline.run()
            return {"status": f"{name} executed once"}

        else:
            return {"error": "invalid method"}

    except Exception as e:
        return {"error": str(e)}

    
@app.get("/pipelines")
def list_all():
    return orchestrator.list_pipelines()