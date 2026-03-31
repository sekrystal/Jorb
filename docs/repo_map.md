# Repo Map

This file is the fastest way to understand where things live in Jorb.

## Top Level

- [`api/`](../api): FastAPI app entrypoint and route modules
- [`connectors/`](../connectors): search and ATS discovery logic
- [`core/`](../core): settings, DB, models, schemas, shared internals
- [`design/`](../design): design source material, including `figma/`
- [`frontend/`](../frontend): separate JS shell workstream
- [`scripts/`](../scripts): initialization, worker, reset, and validation commands
- [`services/`](../services): the actual product logic
- [`tests/`](../tests): deterministic regression and system tests
- [`ui/`](../ui): Streamlit UI

## API Surface

- [`api/main.py`](../api/main.py): FastAPI app setup
- [`api/routes/health.py`](../api/routes/health.py): health endpoint
- [`api/routes/opportunities.py`](../api/routes/opportunities.py): `/leads` and `/opportunities`
- [`api/routes/agents.py`](../api/routes/agents.py): runtime, autonomy, discovery, and agent routes
- [`api/routes/applications.py`](../api/routes/applications.py): saved/applied/status updates
- [`api/routes/profile.py`](../api/routes/profile.py): candidate profile endpoints
- [`api/routes/feedback.py`](../api/routes/feedback.py): feedback capture

## Core Runtime

- [`core/config.py`](../core/config.py): settings and env loading
- [`core/db.py`](../core/db.py): engine, session, DB init
- [`core/models.py`](../core/models.py): SQLAlchemy models
- [`core/schemas.py`](../core/schemas.py): Pydantic schemas and response contracts

## Product Services

### Pipeline and lead surfacing

- [`services/sync.py`](../services/sync.py): lead listing, ingest, and read path
- [`services/pipeline.py`](../services/pipeline.py): sequential agent pipeline
- [`services/ranking.py`](../services/ranking.py): ranking and score shaping
- [`services/freshness.py`](../services/freshness.py): stale/expired logic
- [`services/location_policy.py`](../services/location_policy.py): location gating

### Runtime and autonomy

- [`services/runtime_control.py`](../services/runtime_control.py): play/pause/run-once state
- [`services/worker_runtime.py`](../services/worker_runtime.py): worker-cycle execution logic
- [`services/autonomy.py`](../services/autonomy.py): autonomy-status aggregation
- [`services/alerts.py`](../services/alerts.py): operational alerts

### Discovery and learning

- [`services/company_discovery.py`](../services/company_discovery.py): discovery status and company-surface truth
- [`services/discovery_agents.py`](../services/discovery_agents.py): planner/triage/discovery logic
- [`services/query_learning.py`](../services/query_learning.py): source query performance memory
- [`services/learning.py`](../services/learning.py): watchlist/follow-up learning
- [`services/governance.py`](../services/governance.py): learned item governance

### Applications and profile

- [`services/applications.py`](../services/applications.py): save/apply/status lifecycle
- [`services/profile.py`](../services/profile.py): profile load/save helpers
- [`services/profile_ingest.py`](../services/profile_ingest.py): structured profile review helpers
- [`services/document_ingest.py`](../services/document_ingest.py): resume parsing helpers

## UI

- [`ui/app.py`](../ui/app.py): Streamlit entrypoint
- [`ui/components/`](../ui/components): sidebar, job card, top bar modules
- [`ui/screens/`](../ui/screens): jobs-first screen composition

## Scripts

- [`scripts/init_db.py`](../scripts/init_db.py): initialize schema
- [`scripts/run_worker.py`](../scripts/run_worker.py): start the worker
- [`scripts/reset_demo.py`](../scripts/reset_demo.py): clear and reseed demo state
- [`scripts/seed_demo_data.py`](../scripts/seed_demo_data.py): demo data seed helpers
- [`scripts/preflight_check.sh`](../scripts/preflight_check.sh): compile/import/test smoke gate
- [`scripts/runtime_self_check.sh`](../scripts/runtime_self_check.sh): live local smoke check

## Tests

Start here for fast confidence:

- [`tests/test_production_runtime.py`](../tests/test_production_runtime.py): runtime connector and production-mode semantics
- [`tests/test_sync.py`](../tests/test_sync.py): lead read path and core regressions
- [`tests/test_workbench.py`](../tests/test_workbench.py): Streamlit helper and UI logic
- [`tests/test_company_discovery.py`](../tests/test_company_discovery.py): discovery status truth
- [`tests/test_hybrid_runtime.py`](../tests/test_hybrid_runtime.py): runtime control semantics

## Design Source Of Truth

The jobs-first product UX design lives in:

- [`design/figma/`](../design/figma)

Important exported design files:

- [`design/figma/src/app/components/JobsPage.tsx`](../design/figma/src/app/components/JobsPage.tsx)
- [`design/figma/src/app/components/JobCard.tsx`](../design/figma/src/app/components/JobCard.tsx)
- [`design/figma/src/app/components/JobDetailPanel.tsx`](../design/figma/src/app/components/JobDetailPanel.tsx)
- [`design/figma/src/imports/pasted_text/job-discovery-ui-spec.md`](../design/figma/src/imports/pasted_text/job-discovery-ui-spec.md)
