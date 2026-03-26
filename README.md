# Opportunity Scout

Opportunity Scout is a functional opportunity intelligence workbench for startup job discovery, tracking, and agent-driven iteration.

It does eleven things:

1. ingests weak hiring signals and public listings
2. separates `signals`, `listings`, and `combined` leads
3. ranks leads against a candidate profile
4. tracks saved and applied workflow state
5. runs manual or scheduled discovery loops
6. opens investigations for unresolved weak signals
7. learns from feedback and evolves source queries
8. proposes watchlist expansions and application follow-ups
9. exposes a visible agent pipeline and audit trail
10. governs learned expansions with promotion, suppression, rollback, and expiration
11. records connector health, run digests, and autonomy health state

This repo is optimized for a credible local demo plus one truthfully hardened live connector family: public Greenhouse boards.

Deployment and operations docs:

- [`DEPLOY.md`](./DEPLOY.md)
- [`OPERATIONS.md`](./OPERATIONS.md)

## What The Product Is Now

The app is a table-first workbench with eight views:

- `Leads`: active ranked shortlist
- `Saved`: saved-for-later roles
- `Applied`: application tracker
- `Profile`: resume upload and candidate profile editing
- `Agent Activity`: pipeline controls and event log
- `Investigations`: unresolved weak-signal cases and rechecks
- `Learning`: query performance, watchlist growth, and follow-up tasks
- `Autonomy Ops`: scheduler, connector health, governance, failures, and digests

Visible table columns include:

- surfaced date
- posted date
- company
- title
- lead type
- freshness
- fit
- confidence
- current status
- source
- change marker
- last agent action

The main workflow is operational, not decorative:

- refresh the pipeline
- inspect fresh leads
- save or apply
- update application status
- leave feedback
- watch ranking and suppression behavior change

## Data Model

Core tables:

- `resume_documents`
- `candidate_profiles`
- `signals`
- `listings`
- `leads`
- `applications`
- `feedback`
- `recheck_queue`
- `source_queries`
- `agent_activities`
- `agent_runs`
- `investigations`
- `source_query_stats`
- `watchlist_items`
- `follow_up_tasks`
- `connector_health`
- `run_digests`
- `daily_digests`

Key distinctions:

- `signals` are weak clues from social or public web
- `listings` are structured jobs with freshness and expiration checks
- `leads` are the surfaced workbench rows
- `applications` track saved/applied state and notes
- `investigations` keep unresolved weak signals alive until they resolve or age out
- `source_query_stats` and `watchlist_items` store learning and expansion memory
- `follow_up_tasks` drive proactive application reminders
- `agent_activities` and `agent_runs` make the pipeline visible
- `connector_health` records live-vs-demo source reliability and circuit breaker state
- `run_digests` and `daily_digests` keep autonomy readable over long horizons

## Agent Pipeline

Opportunity Scout now exposes a minimal but real sequential agent pipeline.

Agents:

- `Scout`: ingests new source records into the database
- `Resolver`: links signals to companies or listings when possible
- `Fit`: records fit and qualification context
- `Ranker`: reprioritizes leads after feedback or profile changes
- `Critic`: suppresses stale, expired, duplicate, and mismatched rows
- `Tracker`: records save, apply, and status changes
- `Learning`: evolves source queries, watchlist proposals, and follow-up memory

The `Agent Activity` view shows:

- timestamp
- agent name
- action
- target count or entity
- result summary

Runtime controls:

- `Play`
- `Pause`
- `Run once`

The default demo boot keeps both autonomy and the scheduler off so the baseline does not mutate before the user does anything.

If `ENABLE_SCHEDULER=true`, the local scheduler waits one interval before the first run, then executes the full pipeline on a loop. That keeps autonomous behavior available without mutating the demo immediately after reset.

Health and trust signals now visible in `Agent Activity`:

- last successful run time
- last failed run time
- derived runtime phase (`disabled`, `paused`, `queued`, `running`, `sleeping`, `idle`, `error`)
- operator hints tied to the current runtime phase
- open investigations
- suppressed leads
- due follow-ups
- latest success summary
- latest failure summary
- latest run summary digest

## Autonomous Loops

Opportunity Scout now supports lightweight local autonomy without distributed infrastructure.

- Discovery loop: Scout ingests the next source batch and marks newly surfaced rows.
- Investigation loop: unresolved signals open investigations with retry state and `next_check_at`.
- Critic loop: stale, expired, duplicate, and mismatched rows are suppressed automatically.
- Learning loop: positive and negative feedback update query performance and watchlist proposals.
- Tracker loop: stale applications generate follow-up tasks and `next_action` suggestions.

The loops can be:

- run continuously by the worker when runtime state is `running`
- stepped once from `Agent Activity` with `Run once`
- run on a schedule when `ENABLE_SCHEDULER=true`

## Learning Governance

Learned expansions are governed, not blindly activated.

Governance states:

- `proposed`
- `active`
- `suppressed`
- `expired`
- `rolled_back`

Governance rules:

- promote learned items after enough positive evidence
- suppress noisy or disliked items
- roll back active items that turn negative
- expire stale proposed items that never earn evidence
- persist decision reasons and evaluation timestamps

These states are visible in `Learning` and `Autonomy Ops`.

## Connector Reliability

Connector health is now first-class.

Each connector tracks:

- status
- trust score
- last mode (`live` or `demo`)
- consecutive failures
- recent success and failure streaks
- last error
- last failure classification
- freshness lag
- quarantine count
- circuit-breaker state
- whether the connector is currently approved for unattended live mode
- blocked reason when it is intentionally or operationally unavailable

Behavior:

- retry with short backoff
- graceful fallback where demo mode exists
- per-source failure recording
- circuit breaker on repeated failures
- per-source isolation so one failing connector does not poison the whole pipeline
- recovery from `failed` or `circuit_open` back to `recovering` and `healthy`

Current live-ready connector family:

- `greenhouse`: public Greenhouse boards can run in real live mode when `GREENHOUSE_BOARD_TOKENS` is set

Health states:

- `healthy`
- `degraded`
- `failed`
- `circuit_open`
- `recovering`

Failure classifications include:

- `rate_limited`
- `auth_error`
- `transient_network`
- `parsing_error`
- `source_empty`
- `schema_drift`
- `source_not_found`

## Search Discovery

The repo now has a first pragmatic search-discovery layer in addition to structured ATS polling.

How it works:

- `search_web` generates targeted queries from core titles, adjacent titles, preferred domains, and watchlist companies
- queries currently use the DuckDuckGo HTML surface
- results are filtered to supported ATS job surfaces only:
  - `job-boards.greenhouse.io`
  - `jobs.ashbyhq.com`
- discovered ATS identifiers are merged into the existing Greenhouse and Ashby fetch flow
- fetched jobs still go through the same normalization, freshness, Critic, and ranking pipeline

This is intentionally narrow:

- search expands recall
- ATS fetch remains the source of truth
- Critic remains the final visibility gate

Search discovery flags:

- `SEARCH_DISCOVERY_ENABLED`
- `SEARCH_DISCOVERY_PROVIDER=duckduckgo_html`
- `SEARCH_DISCOVERY_QUERY_LIMIT`
- `SEARCH_DISCOVERY_RESULT_LIMIT`

Truthful runtime source matrix:

- `/discovery-status` now exposes an explicit source matrix for `greenhouse`, `ashby`, `search_web`, bounded search scraping fallback, `x_search`, and `user_submitted`
- each source is classified as `working`, `partially_working`, or `not_working`
- unsupported or disabled sources are reported explicitly instead of reading like ordinary zero-yield discovery
- `search_web` and search-driven scraping remain recall-expansion layers only; trusted listings still come from normalized ATS fetches or user-supplied links

What is tested today:

- query generation
- ATS URL filtering
- Greenhouse/Ashby token extraction
- merge into the existing ATS pipeline
- evidence labeling like `greenhouse+search_web`

What is not yet fully live-validated in this sandbox:

- a real outbound DuckDuckGo discovery run end to end

## Connector Recovery

Connector state is persisted, so fixing env alone is not always enough.

Operator path:

1. update env
2. restart API and worker
3. inspect `/autonomy-status`
4. if the connector is still stuck in persisted cooldown/circuit state, intentionally reset just that connector

Reset API:

```bash
curl -s -X POST http://127.0.0.1:8000/connectors/greenhouse/reset-health \
  -H 'Content-Type: application/json' \
  -d '{"confirm":true}'
```

Common blocked reasons:

- `missing_tokens`
- `disabled`
- `config_error`
- `cooldown`
- `circuit_open`

## Soak Testing

Use the accelerated local soak runner to validate unattended behavior:

```bash
python scripts/soak_test_scheduler.py --cycles 24 --interval 1 --initial-delay 1 --timeout 60
```

Run a real Greenhouse live soak with public boards:

```bash
GREENHOUSE_BOARD_TOKENS=stripe,airtable DEMO_MODE=false \
python scripts/soak_test_scheduler.py \
  --mode live \
  --connector-family greenhouse \
  --cycles 24 \
  --interval 1 \
  --initial-delay 1 \
  --timeout 240 \
  --report-file agent_system/reports/live_greenhouse_24cycle.md
```

The soak test:

- resets the database
- reseeds demo data only in `--mode demo`
- runs repeated scheduled cycles
- records every run in `agent_runs`
- keeps activity visible in `agent_activities`
- reports duplicates, visible stale rows, open investigations, watchlist growth, connector health, anomalies, and the latest digest
- writes an optional final report to disk

The `Autonomy Ops` view shows:

- connector health status
- connector incidents and failure classifications
- circuit state
- last successful fetch
- last failure
- trust score
- quarantine count
- unattended approval state
- latest run digest
- daily digest

## Freshness And Suppression

Freshness is strict by default.

- default visible lead window: `14 days`
- expired and stale listings are hidden by default
- signal-only leads do not dominate the default shortlist
- underqualified and overqualified leads stay hidden unless explicitly revealed

Signal-only leads no longer dominate the default shortlist. They remain available when explicitly included or when you inspect investigations.

Expiration patterns include:

- `job no longer available`
- `position has been filled`
- `position filled`
- `no longer accepting applications`
- `archived`
- `page not found`
- `posting closed`

## Saved And Applied Tracking

Saved and Applied are real persisted states.

The tracker stores:

- `lead_id`
- `company_name`
- `title`
- `date_saved`
- `date_applied`
- `current_status`
- `notes`
- `updated_at`

The tracker now also powers:

- `next_action` suggestions on applied leads
- due follow-up flags for roles that have gone quiet
- persisted follow-up tasks in the Learning view

Supported statuses:

- `saved`
- `applied`
- `recruiter screen`
- `hiring manager`
- `interview loop`
- `final round`
- `rejected`
- `offer`
- `archived`

## Feedback Loop

Feedback is wired to real behavior.

Supported actions:

- relevant
- not relevant
- save
- apply
- mute company
- mute title pattern

Feedback updates:

- profile learning weights
- query learning state
- watchlist proposals
- explanations
- ranking on the next rerank pass
- suppression via muted companies or muted title patterns

Positive feedback can create or boost:

- query proposals
- inferred title families
- company/domain watchlist items

Negative feedback can suppress:

- poor-performing query patterns
- muted companies
- muted title patterns

## Resume Upload

Supported inputs:

- PDF
- TXT
- MD
- pasted text

PDF parsing uses `pypdf`.

If a PDF does not contain extractable text, the app raises a clear error and the user can paste resume text manually.

## Demo Mode

Demo mode works without API keys.

Seeded demo data includes:

- fresh active listings
- expired listings filtered out by default
- weak signal with no listing yet
- weak signal that resolves into a listing
- saved and applied examples
- underqualified and overqualified rows that stay hidden

Best demo story:

- Mercor starts as a weak hiring signal
- Resolver links it to a real listing
- it surfaces as a combined lead
- its explanation and agent trace make the path visible
- unresolved signals stay visible in `Investigations` until the resolver upgrades or retries them

The refresh loop is also real:

- a clean reset starts with a stable baseline
- `Play` lets the worker keep cycling independently
- `Run once` executes exactly one pipeline cycle while leaving the worker paused
- investigations, query stats, watchlist items, and follow-up tasks update as the pipeline runs
- the `change` column shows whether a row is new, updated, reranked, or suppressed

## X / Social Signal Handling

Live X support is limited and should be treated as optional.

- demo mode uses seeded, canonical-looking URLs
- canonical format is `https://x.com/<handle>/status/<id>`
- if live username lookup is unavailable, the connector falls back to a safe generic status URL
- broken placeholder links should not appear in the UI

## Reset Demo Data

Use this to wipe the local SQLite database and reseed only current demo data:

```bash
python scripts/reset_demo.py
```

Reset behavior:

1. drops existing schema
2. removes legacy `opportunities` table data
3. deletes SQLite sidecar files
4. recreates all tables
5. seeds fresh demo records only

## Local Run

```bash
cd /Users/samuelkrystal/projects/jorb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python scripts/reset_demo.py
uvicorn api.main:app --host 127.0.0.1 --port 8000
```

In a second terminal:

```bash
cd /Users/samuelkrystal/projects/jorb
source .venv/bin/activate
streamlit run ui/app.py --server.headless true --server.address 127.0.0.1 --server.port 8500
```

In a third terminal, bootstrap the production JS shell:

```bash
cd /Users/samuelkrystal/projects/jorb/frontend
npm install
npm run dev
```

Production JS shell notes:

- framework choice: Vite + React + TypeScript
- primary product route: `http://127.0.0.1:5173/jobs`
- dev API wiring: the Vite proxy forwards `/api/*` to `http://127.0.0.1:8000/*`
- backend contract pattern: the JS client reads `/opportunities`, `/candidate-profile`, and `/applications/status`
- Streamlit remains the temporary validation harness for operator and diagnostic surfaces at `http://127.0.0.1:8500`
- the JS shell intentionally does not expose `source matrix`, `discovery internals`, `learning`, `autonomy ops`, `agent activity`, `investigations`, `diagnostics`, or `operator controls` as product entry points

Portable SQLite config:

```bash
DATABASE_URL=sqlite:///./opportunity_scout.db
```

`core/config.py` resolves that path at the repo root automatically.

The example env keeps `ENABLE_SCHEDULER=false` by default so a fresh demo reset stays stable until you run agents manually.

It also keeps `AUTONOMY_ENABLED=false` and `GREENHOUSE_ENABLED=false` by default so first local bring-up is intentionally safe and idle.

To opt into local autonomy:

```bash
AUTONOMY_ENABLED=true
GREENHOUSE_ENABLED=true
ENABLE_SCHEDULER=true
SYNC_INTERVAL_SECONDS=60
SCHEDULER_INITIAL_DELAY_SECONDS=60
SCHEDULER_MAX_CYCLES=0
```

Guardrails:

- repeated no-op activity is deduped in the activity feed
- repeated scout runs do not create duplicate listings or leads
- investigations are upserted per signal instead of duplicated
- tracker follow-up creation is idempotent
- watchlist growth is capped per learning cycle
- generated query growth is capped per feedback event
- repeated connector failures open a circuit breaker
- no-op cycles produce a concise digest instead of cumulative change spam

## Tests

```bash
pytest
```

## Employer Demo Flow

1. Run `python scripts/reset_demo.py`.
2. Open `Leads` and show the clean default shortlist.
3. Open Mercor and show the explanation, source evidence, and agent trace.
4. Switch to `Saved` and `Applied` to show real workflow state.
5. Open `Investigations` and show unresolved weak signals, confidence, attempts, and next recheck time.
6. Open `Learning` and show source query stats, watchlist items, and the Mercor follow-up task.
7. Open `Agent Activity`, click `Run once`, and show the next cycle summary and activity updates.
8. Click `Play` to let the worker continue independently, then `Pause` once new rows and investigation updates appear.
9. Open `Profile`, upload a PDF, and show the extracted profile fields.
10. Reveal hidden rows only if you want to prove stale and mismatched suppression.

## Remaining Limitations

- live X ingestion is still intentionally limited
- PDF extraction depends on text being extractable from the source PDF
- Streamlit provides a practical workbench, not a full spreadsheet engine
- the production JS shell now carries the jobs-first screen, while operator-heavy workflows still remain in Streamlit
- scheduled autonomy is local and sequential, not distributed infrastructure
- unattended demo mode is reasonable for bounded local cycles, but still benefits from human review before long-running live use
- live connectors remain the main barrier to true set-and-forget autonomy
