import { Link } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";
import { getLeads, setApplicationStatus, type Lead } from "../lib/api";

const SAVED_PARAMS = { only_saved: true };
const APPLIED_PARAMS = { only_applied: true };

type LeadViewProps = {
  surface: "jobs" | "saved" | "applied";
  title: string;
  description: string;
  params?: Record<string, string | boolean | number | undefined>;
};

type SortMode = "match" | "newest";

type JobViewModel = {
  id: number;
  title: string;
  company: string;
  location: string;
  workMode: string;
  description: string;
  fullDescription: string;
  matchScore: string;
  matchLabel: string;
  explanation: string;
  whyThisJob: string;
  whatYouAreMissing: string | null;
  suggestedNextSteps: string;
  tags: string[];
  postedDate: string;
  source: string;
  sourceProvenance: string;
  currentStatus: string;
  state: "new" | "saved" | "applied";
  link: string | null;
  savedAt: string | null;
  appliedAt: string | null;
  applicationUpdatedAt: string | null;
  nextAction: string | null;
  followUpDue: boolean;
  notes: string | null;
  rawLead: Lead;
};

function recommendationLabel(lead: Lead) {
  const band = String(
    (lead.score_breakdown_json?.recommendation_band as string | undefined) ?? lead.rank_label ?? "weak",
  ).toLowerCase();
  if (band === "strong") {
    return "Strong Match";
  }
  if (band === "medium") {
    return "Medium Match";
  }
  return "Stretch";
}

function recommendationScore(lead: Lead) {
  const raw =
    (lead.score_breakdown_json?.final_score as number | string | undefined) ??
    (lead.score_breakdown_json?.composite as number | string | undefined);
  const score = Number(raw);
  return Number.isFinite(score) ? score.toFixed(1) : "n/a";
}

function compactText(value: string, maxLength: number) {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength - 1)}…` : normalized;
}

function isoToDateLabel(value?: string | null) {
  if (!value) {
    return "Unknown date";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Unknown date";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

function relativeTimeLabel(value?: string | null) {
  if (!value) {
    return "unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "unknown";
  }
  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.max(Math.round(diffMs / 60000), 0);
  if (diffMinutes < 1) {
    return "just now";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes} min ago`;
  }
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours} hr ago`;
  }
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays} day${diffDays === 1 ? "" : "s"} ago`;
}

function relativeDayBucket(value?: string | null) {
  if (!value) {
    return "unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "unknown";
  }
  const diffMs = Date.now() - date.getTime();
  const diffDays = Math.floor(diffMs / 86400000);
  if (diffDays <= 0) {
    return "today";
  }
  if (diffDays === 1) {
    return "yesterday";
  }
  return "earlier";
}

function inferLocation(lead: Lead) {
  const location = lead.evidence_json?.location;
  return typeof location === "string" && location.trim() ? location.trim() : "Location not provided";
}

function inferWorkMode(lead: Lead) {
  const location = inferLocation(lead).toLowerCase();
  const locationScope = String(lead.evidence_json?.location_scope ?? "").toLowerCase();
  if (location.includes("remote") || locationScope.startsWith("remote")) {
    return "remote";
  }
  if (location.includes("hybrid")) {
    return "hybrid";
  }
  if (location.includes("onsite") || location.includes("on-site") || location.includes("on site")) {
    return "onsite";
  }
  return "unspecified";
}

function inferDescription(lead: Lead) {
  const rawDescription = lead.evidence_json?.description_text;
  if (typeof rawDescription === "string" && rawDescription.trim()) {
    const normalized = rawDescription.trim();
    return {
      short: compactText(normalized, 180),
      full: normalized,
    };
  }
  const snippets = Array.isArray(lead.evidence_json?.snippets)
    ? (lead.evidence_json?.snippets as unknown[]).filter((item): item is string => typeof item === "string" && item.trim())
    : [];
  if (snippets.length) {
    const normalized = snippets.join(" ").trim();
    return {
      short: compactText(normalized, 180),
      full: snippets.join("\n\n"),
    };
  }
  return {
    short: "Job details are available from the source listing.",
    full: "No expanded job description is currently available from the backend payload.",
  };
}

function explanationFields(lead: Lead) {
  const explanationPayload =
    lead.score_breakdown_json && typeof lead.score_breakdown_json.explanation === "object" && lead.score_breakdown_json.explanation
      ? (lead.score_breakdown_json.explanation as Record<string, unknown>)
      : {};
  const headline = typeof explanationPayload.headline === "string" ? explanationPayload.headline : "";
  const summary = typeof explanationPayload.summary === "string" ? explanationPayload.summary : "";
  const action = typeof lead.score_breakdown_json?.action_explanation === "string" ? lead.score_breakdown_json.action_explanation : "";
  const explanation = headline || summary || lead.explanation || "Recommendation details are still limited for this lead.";
  return {
    explanation,
    whyThisJob: summary || lead.explanation || explanation,
    whatYouAreMissing:
      lead.qualification_fit_label === "stretch"
        ? "Qualification fit is marked as stretch."
        : lead.qualification_fit_label === "unclear"
          ? "Qualification fit is still unclear from the current evidence."
          : null,
    suggestedNextSteps: action || "Open the source listing and review the role requirements before applying.",
  };
}

function sourceFields(lead: Lead) {
  const source =
    lead.source_type ||
    (typeof lead.evidence_json?.source_type === "string" ? lead.evidence_json.source_type : "") ||
    lead.source_platform ||
    "unknown";
  const provenance =
    lead.source_lineage ||
    lead.source_provenance ||
    (typeof lead.evidence_json?.source_lineage === "string" ? lead.evidence_json.source_lineage : "") ||
    source;
  return { source, provenance };
}

function buildJobViewModel(lead: Lead): JobViewModel {
  const description = inferDescription(lead);
  const explanation = explanationFields(lead);
  const source = sourceFields(lead);
  const workMode = inferWorkMode(lead);
  const tags = [
    lead.freshness_label,
    lead.qualification_fit_label,
    lead.confidence_label,
    workMode !== "unspecified" ? workMode : "",
  ]
    .filter((item): item is string => Boolean(item))
    .slice(0, 4);

  return {
    id: lead.id,
    title: lead.primary_title,
    company: lead.company_name,
    location: inferLocation(lead),
    workMode,
    description: description.short,
    fullDescription: description.full,
    matchScore: recommendationScore(lead),
    matchLabel: recommendationLabel(lead),
    explanation: explanation.explanation,
    whyThisJob: explanation.whyThisJob,
    whatYouAreMissing: explanation.whatYouAreMissing,
    suggestedNextSteps: explanation.suggestedNextSteps,
    tags,
    postedDate: isoToDateLabel(lead.posted_at || lead.surfaced_at),
    source: source.source,
    sourceProvenance: source.provenance,
    currentStatus: lead.current_status || (lead.applied ? "applied" : lead.saved ? "saved" : "new"),
    state: lead.applied ? "applied" : lead.saved ? "saved" : "new",
    link: lead.url || null,
    savedAt: lead.date_saved || null,
    appliedAt: lead.date_applied || null,
    applicationUpdatedAt: lead.application_updated_at || null,
    nextAction: lead.next_action || null,
    followUpDue: Boolean(lead.follow_up_due),
    notes: lead.application_notes || null,
    rawLead: lead,
  };
}

function sortJobs(items: JobViewModel[], sortBy: SortMode) {
  if (sortBy === "newest") {
    return [...items].sort((left, right) => {
      const leftDate = new Date(left.rawLead.posted_at || left.rawLead.surfaced_at || 0).getTime();
      const rightDate = new Date(right.rawLead.posted_at || right.rawLead.surfaced_at || 0).getTime();
      return rightDate - leftDate;
    });
  }
  return [...items].sort((left, right) => Number(right.matchScore) - Number(left.matchScore));
}

function JobsListSummary({ jobs, surface }: { jobs: JobViewModel[]; surface: LeadViewProps["surface"] }) {
  const remoteCount = jobs.filter((job) => job.workMode === "remote").length;
  const todayCount = jobs.filter((job) => relativeDayBucket(job.rawLead.posted_at || job.rawLead.surfaced_at) === "today").length;
  const strongCount = jobs.filter((job) => job.matchLabel === "Strong Match").length;
  const uniqueSources = new Set(jobs.map((job) => job.sourceProvenance || job.source)).size;
  const followUpDueCount = jobs.filter((job) => job.followUpDue).length;
  const appliedCount = jobs.filter((job) => job.state === "applied").length;

  const cards =
    surface === "saved"
      ? [
          { label: "Saved roles", value: jobs.length, copy: "Roles carried forward from the main shortlist for follow-up." },
          { label: "Ready to apply", value: strongCount, copy: "Saved roles still sitting in the strongest recommendation band." },
          { label: "Remote", value: remoteCount, copy: "Saved roles marked remote in normalized listing evidence." },
          { label: "Moved forward", value: appliedCount, copy: "Saved roles that already advanced into the applied tracker." },
        ]
      : surface === "applied"
        ? [
            { label: "Applied roles", value: jobs.length, copy: "Live tracker rows sourced from persisted application records." },
            { label: "Follow-up due", value: followUpDueCount, copy: "Applied roles with a due follow-up task attached to the tracker." },
            { label: "Updated today", value: todayCount, copy: "Tracker rows with a recent status or application update timestamp." },
            { label: "Sources", value: uniqueSources, copy: "Distinct provenance lines represented in the applied tracker." },
          ]
        : [
            { label: "Today", value: todayCount, copy: "Fresh opportunities surfaced in the current shortlist." },
            { label: "Remote", value: remoteCount, copy: "Roles marked remote in listing evidence or normalized location." },
            { label: "Strong Match", value: strongCount, copy: "Roles currently sitting in the strongest recommendation band." },
            { label: "Sources", value: uniqueSources, copy: "Distinct source provenance lines represented in this shortlist." },
          ];

  return (
    <section className="jobs-summary-grid" aria-label="Jobs summary">
      {cards.map((card) => (
        <article className="jobs-summary-card" key={card.label}>
          <span className="detail-label">{card.label}</span>
          <strong>{card.value}</strong>
          <p>{card.copy}</p>
        </article>
      ))}
    </section>
  );
}

function JobsListHeader({
  filteredJobs,
  searchQuery,
  remoteOnly,
  sortBy,
}: {
  filteredJobs: JobViewModel[];
  searchQuery: string;
  remoteOnly: boolean;
  sortBy: SortMode;
}) {
  const activeFilters = [
    searchQuery.trim() ? `Search: ${searchQuery.trim()}` : null,
    remoteOnly ? "Remote only" : null,
    sortBy === "newest" ? "Sorted: Newest" : "Sorted: Best Match",
  ].filter((item): item is string => Boolean(item));

  return (
    <div className="jobs-list-header">
      <div>
        <p className="eyebrow">Shortlist</p>
        <h3>Browse surfaced jobs</h3>
      </div>
      <div className="jobs-list-header-meta">
        <span className="jobs-list-count">
          {filteredJobs.length} {filteredJobs.length === 1 ? "role" : "roles"}
        </span>
        <div className="tag-row">
          {activeFilters.map((filter) => (
            <span className="tag-pill" key={filter}>
              {filter}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

function JobCard({
  job,
  selected,
  onSelect,
  onSave,
  onApply,
  saving,
  applying,
}: {
  job: JobViewModel;
  selected: boolean;
  onSelect: () => void;
  onSave: () => void;
  onApply: () => void;
  saving: boolean;
  applying: boolean;
}) {
  return (
    <article className={`job-card${selected ? " is-selected" : ""}`} onClick={onSelect}>
      <div className="job-card-header">
        <div className="job-card-heading">
          <div className="job-state-row">
            {job.state !== "new" ? <span className={`state-pill is-${job.state}`}>{job.state}</span> : null}
            <span className="job-status-copy">{job.currentStatus}</span>
          </div>
          <h3>{job.title}</h3>
          <p className="job-company-line">
            <span>{job.company}</span>
            <span className="dot">•</span>
            <span>{job.location}</span>
            <span className="dot">•</span>
            <span className={`mode-pill is-${job.workMode}`}>{job.workMode}</span>
          </p>
        </div>
        <div className="score-tile" aria-label={`Match score ${job.matchScore}`}>
          <strong>{job.matchScore}</strong>
          <span>{job.matchLabel}</span>
        </div>
      </div>
      <p className="job-description">{job.description}</p>
      <div className="job-explanation">
        <p>{job.explanation}</p>
      </div>
      <div className="tag-row">
        {job.tags.map((tag) => (
          <span className="tag-pill" key={`${job.id}-${tag}`}>
            {tag}
          </span>
        ))}
      </div>
      <div className="job-meta-row">
        <span>{job.postedDate}</span>
        <span className="dot">•</span>
        <span>{job.source}</span>
        {job.sourceProvenance !== job.source ? (
          <>
            <span className="dot">•</span>
            <span>{job.sourceProvenance}</span>
          </>
        ) : null}
      </div>
      <div className="job-actions">
        <button
          className="secondary-button"
          disabled={job.state === "saved" || job.state === "applied" || saving}
          onClick={(event) => {
            event.stopPropagation();
            onSave();
          }}
          type="button"
        >
          {saving ? "Saving..." : job.state === "saved" ? "Saved" : "Save"}
        </button>
        <button
          className="primary-button"
          disabled={job.state === "applied" || applying}
          onClick={(event) => {
            event.stopPropagation();
            onApply();
          }}
          type="button"
        >
          {applying ? "Applying..." : job.state === "applied" ? "Applied" : "Apply"}
        </button>
        <button className="ghost-button" disabled type="button">
          Dismiss
        </button>
      </div>
    </article>
  );
}

function DetailPanel({
  job,
  surface,
  onClose,
  onSave,
  onApply,
  saving,
  applying,
}: {
  job: JobViewModel;
  surface: LeadViewProps["surface"];
  onClose: () => void;
  onSave: () => void;
  onApply: () => void;
  saving: boolean;
  applying: boolean;
}) {
  const trackerRoute = job.state === "applied" ? "/applied" : "/saved";
  const trackerLabel = job.state === "applied" ? "Open Applied tracker" : "Open Saved queue";

  return (
    <aside className="job-detail-panel" aria-label="Job detail">
      <div className="job-detail-header">
        <div>
          <p className="eyebrow">Selected job</p>
          <h3>{job.title}</h3>
          <p className="job-company-line">
            <span>{job.company}</span>
            <span className="dot">•</span>
            <span>{job.location}</span>
          </p>
        </div>
        <button className="icon-button" onClick={onClose} type="button">
          Close
        </button>
      </div>
      <div className="job-detail-body">
        <section className="job-detail-summary">
          <div className="score-tile is-large">
            <strong>{job.matchScore}</strong>
            <span>{job.matchLabel}</span>
          </div>
          <div className="job-detail-summary-copy">
            <p>{job.explanation}</p>
            <div className="tag-row">
              {job.tags.map((tag) => (
                <span className="tag-pill" key={`${job.id}-detail-${tag}`}>
                  {tag}
                </span>
              ))}
            </div>
          </div>
        </section>
        <section className="detail-metadata-grid">
          <article>
            <span className="detail-label">Work mode</span>
            <strong>{job.workMode}</strong>
          </article>
          <article>
            <span className="detail-label">Posted</span>
            <strong>{job.postedDate}</strong>
          </article>
          <article>
            <span className="detail-label">Source</span>
            <strong>{job.source}</strong>
          </article>
          <article>
            <span className="detail-label">Provenance</span>
            <strong>{job.sourceProvenance}</strong>
          </article>
        </section>
        <div className="job-actions">
          <button
            className="primary-button"
            disabled={job.state === "applied" || applying}
            onClick={onApply}
            type="button"
          >
            {applying ? "Applying..." : job.state === "applied" ? "Applied" : "Apply"}
          </button>
          <button
            className="secondary-button"
            disabled={job.state === "saved" || job.state === "applied" || saving}
            onClick={onSave}
            type="button"
          >
            {saving ? "Saving..." : job.state === "saved" ? "Saved" : "Save"}
          </button>
          {job.link ? (
            <a className="ghost-link" href={job.link} rel="noreferrer" target="_blank">
              Open source
            </a>
          ) : null}
          {job.state !== "new" && surface === "jobs" ? (
            <Link className="ghost-link" to={trackerRoute}>
              {trackerLabel}
            </Link>
          ) : null}
        </div>
        <section className="detail-callout is-blue">
          <span className="detail-label">Why this job</span>
          <p>{job.whyThisJob}</p>
        </section>
        {job.whatYouAreMissing ? (
          <section className="detail-callout is-amber">
            <span className="detail-label">What you are missing</span>
            <p>{job.whatYouAreMissing}</p>
          </section>
        ) : null}
        <section className="detail-callout is-green">
          <span className="detail-label">Suggested next steps</span>
          <p>{job.suggestedNextSteps}</p>
        </section>
        <section className="detail-description">
          <span className="detail-label">Full description</span>
          {job.fullDescription.split("\n").map((paragraph, index) => (
            <p key={`${job.id}-paragraph-${index}`}>{paragraph}</p>
          ))}
        </section>
        <section className="detail-callout is-neutral">
          <span className="detail-label">Current status</span>
          <p>{job.currentStatus}</p>
        </section>
        {job.savedAt || job.appliedAt || job.applicationUpdatedAt ? (
          <section className="detail-callout is-neutral">
            <span className="detail-label">Tracker timeline</span>
            <p>
              {job.savedAt ? `Saved ${isoToDateLabel(job.savedAt)}. ` : ""}
              {job.appliedAt ? `Applied ${isoToDateLabel(job.appliedAt)}. ` : ""}
              {job.applicationUpdatedAt ? `Last tracker update ${relativeTimeLabel(job.applicationUpdatedAt)}.` : ""}
            </p>
          </section>
        ) : null}
        {job.nextAction ? (
          <section className={`detail-callout${job.followUpDue ? " is-amber" : " is-green"}`}>
            <span className="detail-label">{job.followUpDue ? "Follow-up due" : "Next action"}</span>
            <p>{job.nextAction}</p>
          </section>
        ) : null}
        {job.notes ? (
          <section className="detail-callout is-neutral">
            <span className="detail-label">Tracker notes</span>
            <p>{job.notes}</p>
          </section>
        ) : null}
      </div>
    </aside>
  );
}

function LoadingState() {
  return (
    <div className="jobs-list">
      {[1, 2, 3].map((item) => (
        <article className="job-card is-loading" key={item}>
          <div className="loading-line is-title" />
          <div className="loading-line is-meta" />
          <div className="loading-block" />
          <div className="loading-block is-short" />
          <div className="tag-row">
            <span className="loading-pill" />
            <span className="loading-pill" />
            <span className="loading-pill" />
          </div>
        </article>
      ))}
    </div>
  );
}

function JobsWorkspace({ surface, title, description, params }: LeadViewProps) {
  const [items, setItems] = useState<Lead[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [locationQuery, setLocationQuery] = useState("");
  const [remoteOnly, setRemoteOnly] = useState(false);
  const [sortBy, setSortBy] = useState<SortMode>("match");
  const [pendingAction, setPendingAction] = useState<{ leadId: number; action: "saved" | "applied" } | null>(null);
  const [recentTransition, setRecentTransition] = useState<"saved" | "applied" | null>(null);
  const paramsKey = JSON.stringify(params ?? {});

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    getLeads(params ? JSON.parse(paramsKey) : {})
      .then((rows) => {
        if (!active) {
          return;
        }
        setItems(rows);
        setLastLoadedAt(new Date().toISOString());
        setSelectedId((current) => (current && rows.some((row) => row.id === current) ? current : rows[0]?.id ?? null));
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, [paramsKey, refreshNonce]);

  const jobs = useMemo(() => items.map(buildJobViewModel), [items]);
  const filteredJobs = useMemo(() => {
    let next = jobs.filter((job) => {
      const query = searchQuery.trim().toLowerCase();
      if (!query) {
        return true;
      }
      return (
        job.title.toLowerCase().includes(query) ||
        job.company.toLowerCase().includes(query) ||
        job.description.toLowerCase().includes(query)
      );
    });
    if (locationQuery.trim()) {
      const query = locationQuery.trim().toLowerCase();
      next = next.filter((job) => job.location.toLowerCase().includes(query));
    }
    if (remoteOnly) {
      next = next.filter((job) => job.workMode === "remote");
    }
    return sortJobs(next, sortBy);
  }, [jobs, locationQuery, remoteOnly, searchQuery, sortBy]);

  const selectedJob = filteredJobs.find((job) => job.id === selectedId) ?? null;
  const trackerTransition =
    recentTransition === "saved"
      ? { label: "Role moved into Saved", route: "/saved", action: "Open Saved queue" }
      : recentTransition === "applied"
        ? { label: "Role moved into Applied", route: "/applied", action: "Open Applied tracker" }
        : null;
  const workspaceCopy =
    surface === "saved"
      ? "Keep follow-up roles in the same product flow, then move the right ones into Applied when you act."
      : surface === "applied"
        ? "Review live tracker state, follow-up prompts, and the original job context without leaving the main workbench."
        : "Review the live shortlist first, then inspect one role in context without leaving the page.";

  function updateLeadStatus(leadId: number, currentStatus: "saved" | "applied") {
    setPendingAction({ leadId, action: currentStatus });
    setError(null);
    setRecentTransition(null);
    setItems((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              saved: currentStatus === "saved" ? true : lead.saved,
              applied: currentStatus === "applied" ? true : lead.applied,
              current_status: currentStatus,
              date_saved: currentStatus === "saved" ? new Date().toISOString() : lead.date_saved,
              date_applied: currentStatus === "applied" ? new Date().toISOString() : lead.date_applied,
            }
          : lead,
      ),
    );
    setApplicationStatus({ lead_id: leadId, current_status: currentStatus })
      .then(() => {
        setRecentTransition(currentStatus);
        setRefreshNonce((value) => value + 1);
      })
      .catch((err: Error) => {
        setError(err.message);
        setRefreshNonce((value) => value + 1);
      })
      .finally(() => {
        setPendingAction(null);
      });
  }

  return (
    <section className="jobs-page">
      <div className="jobs-page-header">
        <div>
          <p className="eyebrow">{title}</p>
          <h2>{description}</h2>
        </div>
        <div className="jobs-page-summary">
          <div>
            <strong>{filteredJobs.length}</strong>
            <span>{filteredJobs.length === 1 ? "job" : "jobs"}</span>
          </div>
          <p>{workspaceCopy}</p>
        </div>
      </div>
      {trackerTransition ? (
        <div className="tracker-transition-banner">
          <div>
            <span className="detail-label">Tracker updated</span>
            <p>{trackerTransition.label}. The destination surface is backed by the persisted tracker state.</p>
          </div>
          <Link className="secondary-button" to={trackerTransition.route}>
            {trackerTransition.action}
          </Link>
        </div>
      ) : null}
      <div className="jobs-topbar">
        <label className="field-shell">
          <span className="field-label">Search</span>
          <input
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="Role, company, or keyword"
            type="search"
            value={searchQuery}
          />
        </label>
        <label className="field-shell">
          <span className="field-label">Location</span>
          <input
            onChange={(event) => setLocationQuery(event.target.value)}
            placeholder="Remote, San Francisco, New York"
            type="search"
            value={locationQuery}
          />
        </label>
        <label className="toggle-shell">
          <input checked={remoteOnly} onChange={(event) => setRemoteOnly(event.target.checked)} type="checkbox" />
          <span>Remote only</span>
        </label>
        <label className="field-shell is-select">
          <span className="field-label">Sort</span>
          <select onChange={(event) => setSortBy(event.target.value as SortMode)} value={sortBy}>
            <option value="match">Best Match</option>
            <option value="newest">Newest</option>
          </select>
        </label>
        <div className="jobs-topbar-actions">
          <span className="last-updated-copy">Last updated: {relativeTimeLabel(lastLoadedAt)}</span>
          <button className="secondary-button" onClick={() => setRefreshNonce((value) => value + 1)} type="button">
            Refresh Jobs
          </button>
        </div>
      </div>
      {loading ? <LoadingState /> : null}
      {!loading && error ? (
        <div className="empty-state">
          <h3>Job search failed</h3>
          <p>{error}</p>
          <button className="secondary-button" onClick={() => setRefreshNonce((value) => value + 1)} type="button">
            Retry
          </button>
        </div>
      ) : null}
      {!loading && !error ? (
        <>
          <JobsListSummary jobs={filteredJobs} surface={surface} />
          <div className={`jobs-workspace${selectedJob ? " has-detail" : ""}`}>
            <div className="jobs-list-column">
              <JobsListHeader
                filteredJobs={filteredJobs}
                remoteOnly={remoteOnly}
                searchQuery={searchQuery}
                sortBy={sortBy}
              />
            {filteredJobs.length ? (
              <div className="jobs-list">
                {filteredJobs.map((job) => {
                  const saving = pendingAction?.leadId === job.id && pendingAction.action === "saved";
                  const applying = pendingAction?.leadId === job.id && pendingAction.action === "applied";
                  return (
                    <JobCard
                      applying={applying}
                      job={job}
                      key={job.id}
                      onApply={() => updateLeadStatus(job.id, "applied")}
                      onSave={() => updateLeadStatus(job.id, "saved")}
                      onSelect={() => setSelectedId(job.id)}
                      saving={saving}
                      selected={selectedJob?.id === job.id}
                    />
                  );
                })}
              </div>
            ) : (
              <div className="empty-state">
                <h3>No matching jobs found</h3>
                <p>Try adjusting your filters or search criteria to see more results.</p>
                <button
                  className="secondary-button"
                  onClick={() => {
                    setSearchQuery("");
                    setLocationQuery("");
                    setRemoteOnly(false);
                  }}
                  type="button"
                >
                  Clear Filters
                </button>
              </div>
            )}
            </div>
            {selectedJob ? (
              <DetailPanel
                applying={pendingAction?.leadId === selectedJob.id && pendingAction.action === "applied"}
                job={selectedJob}
                surface={surface}
                onApply={() => updateLeadStatus(selectedJob.id, "applied")}
                onClose={() => setSelectedId(null)}
                onSave={() => updateLeadStatus(selectedJob.id, "saved")}
                saving={pendingAction?.leadId === selectedJob.id && pendingAction.action === "saved"}
              />
            ) : null}
          </div>
        </>
      ) : null}
    </section>
  );
}

export function JobsPage() {
  return (
    <JobsWorkspace
      surface="jobs"
      title="Jobs"
      description="Review ranked opportunities from the live FastAPI shortlist."
    />
  );
}

export function SavedPage() {
  return (
    <JobsWorkspace
      surface="saved"
      title="Saved"
      description="Continue from the main jobs flow with saved roles backed by persisted tracker records."
      params={SAVED_PARAMS}
    />
  );
}

export function AppliedPage() {
  return (
    <JobsWorkspace
      surface="applied"
      title="Applied"
      description="Work the applied tracker as a first-class product surface with real status and follow-up data."
      params={APPLIED_PARAMS}
    />
  );
}
