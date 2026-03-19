# AGENTS

This repository contains two distinct systems:

1. the product itself: Opportunity Scout
2. a repo-local code-agent layer for building and maintaining Opportunity Scout

The code-agent layer exists to help Codex and similar agents work with bounded autonomy. It should reduce supervision, not create chaos.

## Product Direction

Any code agent working in this repo must preserve these product truths:

- strict freshness filtering
- working PDF resume parsing
- editable candidate profile
- deterministic runtime control for worker autonomy
- clean distinction between `signals`, `listings`, and `leads`
- save-for-later and application tracking
- Clay-like, table-first workbench UI
- weak signal to real lead demo path
- clean demo mode with reset and reseed

Do not redesign the product away from these.

## Default Execution Loop

Expected agent flow:

1. Planner decomposes work and defines acceptance criteria.
2. Builder implements the smallest credible change.
3. Debugger runs the stack locally, fixes startup or runtime issues, and closes obvious breakages.
4. QA validates acceptance criteria and regression risk.
5. Refactor simplifies noisy or brittle code after functionality works.
6. Docs updates README, run steps, and demo notes when behavior changes.

## Repo Rules

- Prefer simplification over accretion.
- Preserve local demo reliability over speculative live integrations.
- Never claim live access or source validation that does not exist.
- Keep seeded demo data fresh, legible, and relevant to the target profile.
- Hide expired, stale, underqualified, and overqualified leads by default.
- Keep Critic as the final visibility gate; Ranker may order visible leads but must not override suppression.
- Every surfaced lead must be explainable in plain language.
- If a fix changes behavior, update the relevant documentation or agent artifacts in the same pass.

## Required Verification Before Marking Work Complete

Unless the task is docs-only, completion means:

1. run tests
2. run `python scripts/reset_demo.py`
3. boot FastAPI locally
4. boot Streamlit locally
5. verify the changed workflow in the live app or API

If any of those steps cannot be completed, say exactly what failed and why.

## Completion Criteria

A task is only complete when all of the following are true:

- the requested change is implemented
- the local app still runs
- demo mode still works without API keys
- obvious regressions were checked
- the README or agent docs reflect meaningful workflow changes

## Agent Assets

Repo-local prompts, skills, tasks, and reports live in [`agent_system/`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system).
