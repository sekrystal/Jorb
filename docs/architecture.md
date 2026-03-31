# Architecture

This document explains the current product architecture without pretending the system is more distributed or cloud-native than it really is.

## Core Shape

Jorb is a single-instance system with:

- FastAPI backend
- worker loop
- Streamlit UI
- SQLite for local development and optional Postgres for deployed Linux setups

The architecture is intentionally simple enough to run on one machine.

## Main Subsystems

## 1. API layer

The FastAPI app in [`api/main.py`](../api/main.py) exposes:
- health checks
- `/leads` and `/opportunities`
- profile endpoints
- feedback and applications
- runtime control
- autonomy and discovery status

This layer is thin. Most product behavior lives in `services/`.

## 2. Persistence layer

The DB stack is defined in:
- [`core/config.py`](../core/config.py)
- [`core/db.py`](../core/db.py)
- [`core/models.py`](../core/models.py)

Key persistent entities include:
- listings
- signals
- leads
- applications
- feedback
- agent activity and agent runs
- discovery state
- watchlist/query learning
- digests and connector health

## 3. Ingestion and pipeline

The ingestion and ranking path is centered around:
- [`services/sync.py`](../services/sync.py)
- [`services/pipeline.py`](../services/pipeline.py)

Typical flow:

1. connectors fetch or discover source data
2. records are normalized into `listings` and `signals`
3. leads are upserted
4. ranking and explanation payloads are computed
5. the Critic decides what remains visible

## 4. Runtime control

Runtime state is not inferred from logs alone.

It is persisted and surfaced through:
- [`services/runtime_control.py`](../services/runtime_control.py)
- [`services/worker_runtime.py`](../services/worker_runtime.py)
- [`services/autonomy.py`](../services/autonomy.py)

Important states:
- paused
- running
- run once requested
- idle
- sleeping
- error

The worker polls this persisted state and executes bounded cycles.

## 5. Discovery

Discovery extends beyond seeded ATS boards, but stays bounded.

Relevant modules:
- [`connectors/search_web.py`](../connectors/search_web.py)
- [`services/discovery_agents.py`](../services/discovery_agents.py)
- [`services/company_discovery.py`](../services/company_discovery.py)

Current discovery path:

1. planner chooses queries
2. search connector fetches candidate results
3. results are filtered into supported job/careers surfaces
4. discovery candidates are selected for expansion
5. expansions yield zero or more listings
6. results are persisted with discovery lineage and cycle metrics

Discovery is intentionally instrumented heavily because provider quality and surface quality are still the biggest live uncertainty.

## 6. Learning and governance

Learning is not an unconstrained model loop.

Relevant modules:
- [`services/feedback.py`](../services/feedback.py)
- [`services/query_learning.py`](../services/query_learning.py)
- [`services/learning.py`](../services/learning.py)
- [`services/governance.py`](../services/governance.py)

What it does today:
- updates query/source priors from explicit feedback
- proposes watchlist additions
- creates follow-up tasks
- governs learned items through promotion, suppression, rollback, and expiration

## 7. UI surfaces

There are currently two UI layers in the repo:

### Streamlit UI

The primary shipped interface today is the Streamlit app in [`ui/`](../ui).

It includes:
- jobs-first product shell
- saved/applied/profile flows
- operator views for runtime, discovery, learning, and investigations

### Design and JS shell workstream

The repo also contains:
- [`design/figma/`](../design/figma): design source of truth
- [`frontend/`](../frontend): JS shell workstream

Those exist alongside the Streamlit product surface. They are not yet a wholesale replacement for the shipped app.

## System Boundaries

What Jorb currently is:
- a local or single-VM product
- a bounded agentic workbench
- a job discovery and evaluation system

What it is not:
- a generic chatbot
- a multi-tenant SaaS
- an unbounded autonomous crawler

## Hot Paths To Preserve

The most sensitive paths are:
- `/leads`
- runtime-control/worker coordination
- discovery status truth
- connector health persistence

Changes to these areas should favor deterministic logic, bounded budgets, and direct observability.
