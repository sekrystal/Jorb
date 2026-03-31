# Jorb

*Apologies for the AI slop content below, I'll make a human pass once I think this is ready for humans to use. *

Jorb is a jobs-first opportunity intelligence workbench.

It helps a single operator or candidate:
- ingest public job listings and weak hiring signals
- discover new companies and job surfaces
- rank opportunities against a profile
- track saved and applied roles
- inspect why a role is visible, hidden, stale, or suppressed
- run bounded agent loops with clear runtime controls and diagnostics

Jorb is not a generic chat UI and not a multi-tenant SaaS. It is a stateful local-first product plus a deployable single-instance runtime.

## Current Product Shape

Today the repo ships:
- a FastAPI backend
- a worker loop for bounded pipeline cycles
- a Streamlit operator UI
- a jobs-first shell for `Jobs`, `Saved`, `Applied`, and `Profile`
- operator views for `Discovery`, `Agent Activity`, `Investigations`, `Learning`, and `Autonomy Ops`
- live-ready Greenhouse ingestion plus bounded search-based discovery and Ashby support
- deterministic tests, preflight checks, and runtime smoke scripts

## Quickstart

### 1. Create and activate a virtualenv

```bash
cd ~/projects/jorb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create your local env file

```bash
cp .env.example .env
```

Recommended first boot settings:

```bash
DEMO_MODE=true
AUTONOMY_ENABLED=false
ENABLE_SCHEDULER=false
GREENHOUSE_ENABLED=false
SEARCH_DISCOVERY_ENABLED=false
```

### 3. Initialize the database

```bash
python scripts/init_db.py
```

### 4. Start the stack

Terminal 1:

```bash
cd ~/projects/jorb
source .venv/bin/activate
uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Terminal 2:

```bash
cd ~/projects/jorb
source .venv/bin/activate
python scripts/run_worker.py
```

Terminal 3:

```bash
cd ~/projects/jorb
source .venv/bin/activate
streamlit run ui/app.py --server.headless true --server.address 127.0.0.1 --server.port 8500
```

### 5. Smoke check

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/runtime-control
curl -s http://127.0.0.1:8000/discovery-status
curl -I http://127.0.0.1:8500
```

## Common Validation Commands

Focused tests:

```bash
pytest tests/test_production_runtime.py -q
pytest tests/test_workbench.py -q
```

Full test suite:

```bash
pytest
```

Repo preflight:

```bash
./scripts/preflight_check.sh
```

Runtime smoke against a running stack:

```bash
./scripts/runtime_self_check.sh
```

## How Jorb Works

At a high level:

1. Connectors ingest listings and weak signals.
2. `services/pipeline.py` and `services/sync.py` normalize and upsert source data.
3. Leads are ranked against the candidate profile.
4. The Critic suppresses stale, expired, duplicate, muted, or mismatched roles.
5. The worker and runtime control system execute bounded cycles.
6. Discovery and learning write diagnostics, run summaries, and follow-up state.
7. The UI surfaces both the jobs-first product shell and the operator console.

## Architecture At A Glance

- API: [`api/`](./api)
- core config, DB, models, schemas: [`core/`](./core)
- product services and agent loops: [`services/`](./services)
- scripts for init, worker, reset, preflight, and smoke checks: [`scripts/`](./scripts)
- Streamlit UI: [`ui/`](./ui)
- tests: [`tests/`](./tests)
- deploy and systemd units: [`deploy/`](./deploy)
- design source of truth: [`design/figma/`](./design/figma)
- optional JS shell work: [`frontend/`](./frontend)

For a fuller walkthrough:

- [`docs/repo_map.md`](./docs/repo_map.md)
- [`docs/architecture.md`](./docs/architecture.md)
- [`docs/development.md`](./docs/development.md)
- [`OPERATIONS.md`](./OPERATIONS.md)
- [`DEPLOY.md`](./DEPLOY.md)

## Repo Map

High-signal directories:

- `api/`: FastAPI app and route modules
- `connectors/`: web/ATS search and discovery connectors
- `core/`: settings, DB engine/session, SQLAlchemy models, response schemas
- `services/`: ranking, pipeline, runtime control, discovery, learning, applications
- `scripts/`: init, worker, reset, preflight, runtime smoke
- `ui/`: Streamlit app and UI modules
- `tests/`: deterministic test suite
- `design/figma/`: canonical design source for layout and jobs UX
- `frontend/`: separate JS shell workstream

## Local, Validation, And VM Workflows

### Local development

Use the three-process setup above:
- API
- worker
- Streamlit UI

Keep autonomy disabled until you intentionally want live mutation.

### Local validation

Use:

```bash
pytest
./scripts/preflight_check.sh
./scripts/runtime_self_check.sh
```

### Ubuntu VM / deployed validation

Use:
- systemd services from [`DEPLOY.md`](./DEPLOY.md)
- operational commands from [`OPERATIONS.md`](./OPERATIONS.md)

Acceptance-critical runtime work should be proven with:
- route health
- worker process presence
- `run_once` execution
- UI reachability

## Important Scripts

- [`scripts/init_db.py`](./scripts/init_db.py): initialize schema
- [`scripts/run_worker.py`](./scripts/run_worker.py): start worker loop
- [`scripts/reset_demo.py`](./scripts/reset_demo.py): reset demo data
- [`scripts/seed_demo_data.py`](./scripts/seed_demo_data.py): seed demo data
- [`scripts/preflight_check.sh`](./scripts/preflight_check.sh): compile/import/test smoke gate
- [`scripts/runtime_self_check.sh`](./scripts/runtime_self_check.sh): running-stack smoke test
- [`scripts/reset_connector_health.py`](./scripts/reset_connector_health.py): clear stuck connector health state intentionally

## Generated State And Outputs

During local work you will commonly see:
- SQLite files like `opportunity_scout.db`
- test/runtime validation SQLite files
- `.pytest_cache/`
- `__pycache__/`

Operational logs live in:
- terminal output locally
- `journalctl` on Ubuntu/systemd deployments

The builder/orchestration workspace is intentionally separate from this product repo.

## Known Limitations / Current Status

- The repo still contains historical naming from Opportunity Scout in some runtime paths and service names.
- The primary shipped UI is Streamlit; there is also a separate JS shell workstream in `frontend/`.
- Search-based discovery is instrumented and bounded, but live yield still depends heavily on provider quality and discovered surface quality.
- Some UI design targets assume richer frontend interaction than Streamlit supports natively; the current product shell is the closest faithful implementation without rewriting the app architecture.
- Greenhouse is the clearest live-ready connector family today; other discovery paths are narrower and more diagnostic than comprehensive.

## Design Source Of Truth

For jobs UX, navigation, and card structure, use:

- [`design/figma/`](./design/figma)

Do not invent alternate UI patterns when the design files already define the structure.
