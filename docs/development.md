# Development

This document is the fastest practical guide for running, validating, and debugging Jorb locally.

## Local Setup

```bash
cd ~/projects/jorb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python scripts/init_db.py
```

## Recommended First-Boot Env

For a stable local boot:

```bash
DEMO_MODE=true
AUTONOMY_ENABLED=false
ENABLE_SCHEDULER=false
GREENHOUSE_ENABLED=false
SEARCH_DISCOVERY_ENABLED=false
```

Keep runtime paused until you intentionally test worker activity.

## Start The Local Stack

API:

```bash
cd ~/projects/jorb
source .venv/bin/activate
uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Worker:

```bash
cd ~/projects/jorb
source .venv/bin/activate
python scripts/run_worker.py
```

Streamlit UI:

```bash
cd ~/projects/jorb
source .venv/bin/activate
streamlit run ui/app.py --server.headless true --server.address 127.0.0.1 --server.port 8500
```

## Useful Local Checks

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/runtime-control
curl -s http://127.0.0.1:8000/autonomy-status
curl -s http://127.0.0.1:8000/discovery-status
curl -I http://127.0.0.1:8500
```

## Validation Workflow

### Focused tests

Run the tests closest to the thing you changed first.

Examples:

```bash
pytest tests/test_workbench.py -q
pytest tests/test_sync.py -q
pytest tests/test_company_discovery.py -q
pytest tests/test_production_runtime.py -q
```

### Full suite

```bash
pytest
```

### Preflight

```bash
./scripts/preflight_check.sh
```

### Runtime smoke

Use this when the local stack is already running:

```bash
./scripts/runtime_self_check.sh
```

## Local Versus VM

### Local

Use local SQLite by default:

```bash
DATABASE_URL=sqlite:///./opportunity_scout.db
```

This path is resolved at the repo root.

### Ubuntu VM

Use the deployment flow in [`DEPLOY.md`](../DEPLOY.md) and the operating instructions in [`OPERATIONS.md`](../OPERATIONS.md).

Important runtime rule:
- the API and worker must use the same `DATABASE_URL`
- env changes do not apply until the processes restart

## Important Scripts

- [`scripts/init_db.py`](../scripts/init_db.py)
- [`scripts/run_worker.py`](../scripts/run_worker.py)
- [`scripts/reset_demo.py`](../scripts/reset_demo.py)
- [`scripts/seed_demo_data.py`](../scripts/seed_demo_data.py)
- [`scripts/preflight_check.sh`](../scripts/preflight_check.sh)
- [`scripts/runtime_self_check.sh`](../scripts/runtime_self_check.sh)
- [`scripts/reset_connector_health.py`](../scripts/reset_connector_health.py)

## Common Debugging Situations

## Config errors on startup

First check:
- `.env`
- `DATABASE_URL`
- scheduler-related env values
- active virtualenv

## `/leads` feels slow

Check:
- whether read-time AI is enabled unexpectedly
- worker/database contention
- logs for `LEADS_STAGE_TIMING` and `LEADS_TIMING`

## Discovery appears unproductive

Check:
- `/discovery-status`
- query-family metrics
- recent expansions
- recent planner output
- connector health and runtime state

## Worker does not run

Check:
- `/runtime-control`
- whether runtime is paused
- whether `run_once_requested` is set
- whether the worker process is actually alive

## Design/UI work

Jobs-first design source of truth lives in:

- [`design/figma/`](../design/figma)

Do not invent alternate layouts when the design files already define the intended structure.
