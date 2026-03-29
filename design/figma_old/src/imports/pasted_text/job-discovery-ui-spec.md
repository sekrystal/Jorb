## Product Overview

Design a clean, modern web application UI for an AI-powered job discovery and matching platform.

The product automatically finds jobs and ranks them based on how well they match the user. The interface must be jobs-first, highly scannable, and intuitive for a first-time user.

The design should feel similar to Linear, Notion, or modern SaaS tools: minimal, structured, and information-dense without being cluttered.

---

## Global Layout

Use a simple, consistent layout:

* Left sidebar navigation
* Main content area (primary workspace)
* Optional right-side detail panel (for expanded job view)

### Sidebar Navigation (fixed)

Include only 4 items:

1. Jobs (default landing)
2. Saved
3. Applied
4. Profile

Optional small section at bottom:

* System Status (lightweight, not intrusive)

---

## Screen 1: Jobs (Main Screen)

This is the most important screen. It should immediately show a list of jobs without requiring onboarding.

### Top Bar

Include:

* Search input (role or keyword)
* Filters:

  * Location
  * Remote toggle
* Sort dropdown:

  * Best Match (default)
  * Newest

Right side:

* “Refresh Jobs” button
* “Last updated: X minutes ago”

---

## Job List Layout

Display jobs as a vertical list of cards.

Each card must be highly scannable and information-dense.

---

## Job Card Design (CRITICAL)

Each job card must include:

### 1. Header Row

* Job title (most prominent)
* Company name
* Location
* Work mode (remote, hybrid, onsite)

### 2. Short Description

* 1 to 2 line summary of the role
* Plain language, readable at a glance
* Truncated cleanly if too long

### 3. Match Information

* Prominent match score (example: 82)
* Optional label:

  * Strong Match
  * Medium Match
  * Stretch
* Clean visual style (badge or subtle bar)

### 4. Recommendation Explanation

* One short sentence explaining why it matches
  Examples:
* Matches your product operations background
* Strong overlap with automation and systems experience

### 5. Key Signals (Tags)

Show 2 to 4 tags max:

* skills match
* seniority fit
* fresh
* remote
* salary
* visa

Tags should be compact and scannable.

### 6. Freshness and Metadata

* Posted date or freshness indicator
* Salary if available
* Source (optional, subtle)

### 7. Actions

* Save
* Apply
* Dismiss

Buttons should be compact and consistent.

### 8. State Support

Cards must visually support:

* new
* saved
* applied
* dismissed

Use subtle visual differences (icon, color, or badge).

---

## Visual Hierarchy for Job Card

1. Job title
2. Match score
3. Company and location
4. Description
5. Explanation
6. Tags
7. Actions

The user should decide in under 3 seconds if the job is worth clicking.

---

## Interaction: Job Detail Panel

Clicking a job opens a right-side panel (not a new page).

Include:

* Full job description
* Expanded explanation:

  * Why this job
  * What you are missing
  * Suggested next steps
* Full metadata
* Apply button
* Save / dismiss actions

---

## Empty States (IMPORTANT)

Design all of the following:

### 1. No Jobs Found

* Message: “No matching jobs found”
* Suggest adjusting filters

### 2. Loading State

* Skeleton job cards

### 3. Error State

* Message: “Job search failed”
* Button: Retry

---

## Screen 2: Saved

List of saved jobs.

Same card format, but:

* Clearly marked as saved
* Easy to move to applied
* Easy to remove

---

## Screen 3: Applied

Track jobs the user has applied to.

Each item includes:

* Job card (simplified)
* Status (applied, interviewing, rejected)
* Optional notes area

Leave space for future:

* status progression
* timeline
* follow-ups

---

## Screen 4: Profile

Keep simple for now.

Include:

### Profile Info

* Resume upload
* LinkedIn input
* Target role
* Preferred location

### Extracted Data (placeholder)

* Skills
* Experience summary

### Future Placeholder Sections

* Skill gaps
* Resume suggestions
* Match tuning

---

## System Status (Small Component)

Do not expose technical internals.

Show:

* Last run time
* Jobs found
* Basic health indicator

---

## Discovery / Refresh Control

Include:

* “Refresh Jobs” button
* Timestamp of last update

Do not expose pipelines or internal processes.

---

## Feedback Placeholder

On each job card, leave space for:

* thumbs up
* thumbs down

Do not fully design system, just placeholder.

---

## Design Style

* Clean, minimal, modern SaaS
* Light mode preferred
* Subtle borders, soft shadows
* No heavy colors
* Typography should prioritize readability

---

## Constraints

* Do not require onboarding before showing jobs
* Do not expose internal system concepts like “discovery”, “learning”, or “agents”
* Focus on outcomes, not process
* Keep navigation simple and obvious
* Avoid clutter

---

## Goal

The UI must make it immediately clear:

* what the product does
* what jobs are available
* why those jobs match the user
* what action to take next

The experience should feel fast, intelligent, and trustworthy.
