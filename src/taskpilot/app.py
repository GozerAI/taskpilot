"""TaskPilot FastAPI application."""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from pydantic import BaseModel

from taskpilot.service import SchedulerService

FRONTEND_DIR = Path(__file__).parent.parent.parent.parent / "frontend"

logger = logging.getLogger(__name__)


def _setup_logging():
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    env = os.environ.get("ENV", os.environ.get("TASKPILOT_ENV", "development"))
    if env.lower() == "production":
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format=fmt, force=True)


_service: SchedulerService | None = None

ZUULTIMATE_BASE_URL = os.environ.get("ZUULTIMATE_BASE_URL", "http://localhost:8000")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "http://localhost:3000").split(",")]


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    global _service
    storage_path = os.environ.get("TASKPILOT_DB_PATH", "./data/taskpilot.db")
    Path(storage_path).parent.mkdir(parents=True, exist_ok=True)
    _service = SchedulerService(storage_path=storage_path)
    await _service.start()
    logger.info("TaskPilot started (db=%s)", storage_path)
    yield
    if _service and _service.is_running():
        await _service.stop()
    logger.info("TaskPilot shutting down")


app = FastAPI(title="TaskPilot", version="0.1.0", lifespan=lifespan)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
)

# Mount static files and serve dashboard if frontend dir exists
_static_dir = FRONTEND_DIR / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── Auth dependency ────────────────────────────────────────────────────────────

async def get_tenant(request: Request) -> dict:
    """Validate bearer token against Zuultimate and return tenant context."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = auth[7:]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{ZUULTIMATE_BASE_URL}/v1/identity/auth/validate",
                headers={"Authorization": f"Bearer {token}"},
            )
    except httpx.RequestError as e:
        logger.error("Zuultimate unreachable: %s", e)
        raise HTTPException(status_code=503, detail="Auth service unavailable")

    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Auth service error")

    return resp.json()


def require_entitlement(entitlement: str):
    """Dependency factory: blocks if tenant lacks the required entitlement."""
    async def _check(tenant: dict = Depends(get_tenant)) -> dict:
        if entitlement not in tenant.get("entitlements", []):
            raise HTTPException(
                status_code=403,
                detail=f"Your plan does not include '{entitlement}'. Upgrade to access this feature.",
            )
        return tenant
    return _check


def _svc() -> SchedulerService:
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return _service


# ── Models ─────────────────────────────────────────────────────────────────────

class CreateTaskRequest(BaseModel):
    name: str
    handler_name: str
    description: Optional[str] = None
    frequency: str = "daily"
    priority: str = "medium"
    category: Optional[str] = None
    owner_executive: Optional[str] = None
    handler_params: Optional[dict] = None
    cron_expression: Optional[str] = None
    hour: Optional[int] = None
    minute: Optional[int] = None
    day_of_week: Optional[int] = None
    enabled: bool = True


class CreateWorkflowRequest(BaseModel):
    name: str
    description: Optional[str] = None
    owner_executive: Optional[str] = None
    parallel_execution: bool = False
    stop_on_failure: bool = True


class AddWorkflowStepRequest(BaseModel):
    name: str
    handler_name: str
    description: Optional[str] = None
    depends_on: Optional[list[str]] = None
    handler_params: Optional[dict] = None
    continue_on_failure: bool = False
    timeout_seconds: Optional[int] = None


# ── Frontend pages ─────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def landing():
    """Serve the landing page."""
    page = FRONTEND_DIR / "landing.html"
    if page.exists():
        return FileResponse(str(page))
    return HTMLResponse("<h1>Taskpilot</h1><p>Landing page not found.</p>", status_code=503)


@app.get("/dashboard", include_in_schema=False)
async def dashboard():
    """Serve the single-page dashboard."""
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return HTMLResponse("<h1>Dashboard not built</h1><p>Run the frontend build step.</p>", status_code=503)


# ── Auth proxy ─────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str


class RegisterRequest(BaseModel):
    name: str
    email: str
    password: str


@app.post("/api/auth/login")
async def auth_login(body: LoginRequest):
    """Proxy login to Zuultimate and return access token."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{ZUULTIMATE_BASE_URL}/v1/identity/auth/login",
                json={"email": body.email, "password": body.password},
            )
    except httpx.RequestError as e:
        logger.error("Zuultimate unreachable during login: %s", e)
        raise HTTPException(status_code=503, detail="Auth service unavailable")

    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Auth service error")

    return resp.json()


@app.post("/api/auth/register", status_code=201)
async def auth_register(body: RegisterRequest):
    """Proxy registration to Zuultimate."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{ZUULTIMATE_BASE_URL}/v1/identity/auth/register",
                json={"name": body.name, "email": body.email, "password": body.password},
            )
    except httpx.RequestError as e:
        logger.error("Zuultimate unreachable during register: %s", e)
        raise HTTPException(status_code=503, detail="Auth service unavailable")

    if resp.status_code == 409:
        raise HTTPException(status_code=409, detail="Email already registered")
    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail="Registration failed")

    return resp.json()


# ── Basic endpoints (taskpilot:basic) ──────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "taskpilot", "version": app.version}


@app.get("/health/ready")
async def health_ready():
    """Readiness probe for orchestrator."""
    from fastapi.responses import JSONResponse
    checks = {
        "service_initialized": _service is not None,
    }
    all_ready = all(checks.values())
    return JSONResponse(
        status_code=200 if all_ready else 503,
        content={"ready": all_ready, "checks": checks},
    )


@app.get("/health/detailed")
async def health_detailed():
    checks = {}
    status = "ok"

    # Service layer check
    try:
        svc = _svc()
        stats = svc.get_stats()
        checks["service"] = {
            "status": "ok",
            "scheduler_running": stats.get("scheduler_running", False),
            "registered_handlers": stats.get("registered_handlers", 0),
        }
    except Exception as e:
        checks["service"] = {"status": "error", "error": str(e)}
        status = "degraded"

    # Telemetry check
    try:
        svc = _svc()
        telemetry = svc.get_telemetry()
        checks["telemetry"] = {"status": "ok" if telemetry else "unavailable"}
    except Exception:
        checks["telemetry"] = {"status": "unavailable"}

    return {"status": status, "service": "taskpilot", "version": app.version, "checks": checks}


@app.post("/v1/tasks")
async def create_task(
    body: CreateTaskRequest,
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    task_id = _svc().schedule_task(
        name=body.name,
        handler_name=body.handler_name,
        description=body.description,
        frequency=body.frequency,
        priority=body.priority,
        category=body.category,
        owner_executive=body.owner_executive,
        handler_params=body.handler_params,
        cron_expression=body.cron_expression,
        hour=body.hour,
        minute=body.minute,
        day_of_week=body.day_of_week,
        enabled=body.enabled,
    )
    return {"id": task_id}


@app.get("/v1/tasks")
async def list_tasks(
    owner_executive: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    return _svc().list_tasks(
        owner_executive=owner_executive,
        category=category,
        status=status,
    )


@app.get("/v1/tasks/{task_id}")
async def get_task(
    task_id: str,
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    result = _svc().get_task(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@app.post("/v1/tasks/{task_id}/execute")
async def execute_task(
    task_id: str,
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    result = await _svc().run_task_now(task_id)
    return result


@app.delete("/v1/tasks/{task_id}")
async def remove_task(
    task_id: str,
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    success = _svc().remove_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"success": True}


@app.get("/v1/workflows")
async def list_workflows(
    owner_executive: Optional[str] = Query(None),
    tenant: dict = Depends(require_entitlement("taskpilot:basic")),
):
    return _svc().list_workflows(owner_executive=owner_executive)


@app.get("/v1/stats")
async def get_stats(tenant: dict = Depends(require_entitlement("taskpilot:basic"))):
    return _svc().get_stats()


# ── Pro endpoints (taskpilot:full) ─────────────────────────────────────────────

@app.post("/v1/workflows")
async def create_workflow(
    body: CreateWorkflowRequest,
    tenant: dict = Depends(require_entitlement("taskpilot:full")),
):
    workflow_id = _svc().create_workflow(
        name=body.name,
        description=body.description,
        owner_executive=body.owner_executive,
        parallel_execution=body.parallel_execution,
        stop_on_failure=body.stop_on_failure,
    )
    return {"id": workflow_id}


@app.post("/v1/workflows/{workflow_id}/steps")
async def add_workflow_step(
    workflow_id: str,
    body: AddWorkflowStepRequest,
    tenant: dict = Depends(require_entitlement("taskpilot:full")),
):
    step_id = _svc().add_workflow_step(
        workflow_id=workflow_id,
        name=body.name,
        handler_name=body.handler_name,
        description=body.description,
        depends_on=body.depends_on,
        handler_params=body.handler_params,
        continue_on_failure=body.continue_on_failure,
        timeout_seconds=body.timeout_seconds,
    )
    if step_id is None:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"id": step_id}


@app.post("/v1/workflows/{workflow_id}/execute")
async def execute_workflow(
    workflow_id: str,
    tenant: dict = Depends(require_entitlement("taskpilot:full")),
):
    return await _svc().execute_workflow(workflow_id)


@app.get("/v1/workflows/{workflow_id}/executions")
async def get_workflow_executions(
    workflow_id: str,
    limit: int = Query(10, le=50),
    tenant: dict = Depends(require_entitlement("taskpilot:full")),
):
    return _svc().get_workflow_executions(workflow_id, limit=limit)


@app.post("/v1/autonomous/cycle")
async def run_autonomous_cycle(tenant: dict = Depends(require_entitlement("taskpilot:full"))):
    return await _svc().run_autonomous_cycle()


# ── Enterprise endpoints ───────────────────────────────────────────────────────

@app.get("/v1/executive/{executive_code}")
async def get_executive_report(
    executive_code: str,
    tenant: dict = Depends(require_entitlement("taskpilot:full")),
):
    if executive_code not in ("COO", "CEO", "CFO", "CRO", "CMO"):
        raise HTTPException(
            status_code=400,
            detail="Invalid executive code. Use COO, CEO, CFO, CRO, or CMO.",
        )
    return _svc().get_executive_report(executive_code)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("taskpilot.app:app", host="0.0.0.0", port=8005)
