import { useEffect, useMemo, useState } from "react";
import { getLeads, setApplicationStatus, type Lead } from "../lib/api";

const SAVED_PARAMS = { only_saved: true };
const APPLIED_PARAMS = { only_applied: true };

type LeadViewProps = {
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

function JobsListSummary({ jobs }: { jobs: JobViewModel[] }) {
  const remoteCount = jobs.filter((job) => job.workMode === "remote").length;
  const todayCount = jobs.filter((job) => relativeDayBucket(job.rawLead.posted_at || job.rawLead.surfaced_at) === "today").length;
  const strongCount = jobs.filter((job) => job.matchLabel === "Strong Match").length;
  const uniqueSources = new Set(jobs.map((job) => job.sourceProvenance || job.source)).size;

  return (
    <section className="jobs-summary-grid" aria-label="Jobs summary">
      <article className="jobs-summary-card">
        <span className="detail-label">Today</span>
        <strong>{todayCount}</strong>
        <p>Fresh opportunities surfaced in the current shortlist.</p>
      </article>
      <article className="jobs-summary-card">
        <span className="detail-label">Remote</span>
        <strong>{remoteCount}</strong>
        <p>Roles marked remote in listing evidence or normalized location.</p>
      </article>
      <article className="jobs-summary-card">
        <span className="detail-label">Strong Match</span>
        <strong>{strongCount}</strong>
        <p>Roles currently sitting in the strongest recommendation band.</p>
      </article>
      <article className="jobs-summary-card">
        <span className="detail-label">Sources</span>
        <strong>{uniqueSources}</strong>
        <p>Distinct source provenance lines represented in this shortlist.</p>
      </article>
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
  onClose,
  onSave,
  onApply,
  saving,
  applying,
}: {
  job: JobViewModel;
  onClose: () => void;
  onSave: () => void;
  onApply: () => void;
  saving: boolean;
  applying: boolean;
}) {
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

function JobsWorkspace({ title, description, params }: LeadViewProps) {
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

  function updateLeadStatus(leadId: number, currentStatus: "saved" | "applied") {
    setPendingAction({ leadId, action: currentStatus });
    setError(null);
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
          <p>Review the live shortlist first, then inspect one role in context without leaving the page.</p>
        </div>
      </div>
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
          <JobsListSummary jobs={filteredJobs} />
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

function LeadsTableView({ title, description, params }: LeadViewProps) {
  const [items, setItems] = useState<Lead[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const paramsKey = JSON.stringify(params ?? {});

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    getLeads(params ? JSON.parse(paramsKey) : {})
      .then((rows) => {
        if (active) {
          setItems(rows);
        }
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
  }, [paramsKey]);

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Workbench</p>
          <h3>{title}</h3>
        </div>
        <p className="panel-copy">{description}</p>
      </div>
      {loading ? <p className="state-copy">Loading current leads from FastAPI.</p> : null}
      {error ? <p className="state-copy error-copy">{error}</p> : null}
      {!loading && !error ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Company</th>
                <th>Title</th>
                <th>Type</th>
                <th>Freshness</th>
                <th>Fit</th>
                <th>Status</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.id}>
                  <td>{item.company_name}</td>
                  <td>{item.primary_title}</td>
                  <td>{item.lead_type}</td>
                  <td>{item.freshness_label}</td>
                  <td>{item.qualification_fit_label}</td>
                  <td>{item.current_status || "new"}</td>
                  <td>{item.source_platform || "unknown"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!items.length ? <p className="state-copy">No rows matched the current backend query.</p> : null}
        </div>
      ) : null}
    </section>
  );
}

export function JobsPage() {
  return (
    <JobsWorkspace
      title="Jobs"
      description="Review ranked opportunities from the live FastAPI shortlist."
    />
  );
}

export function SavedPage() {
  return (
    <LeadsTableView
      title="Saved"
      description="Saved rows reuse the same opportunities endpoint with a scoped product query."
      params={SAVED_PARAMS}
    />
  );
}

export function AppliedPage() {
  return (
    <LeadsTableView
      title="Applied"
      description="Applied rows stay in the product path and will later carry the richer tracker workflow."
      params={APPLIED_PARAMS}
    />
  );
}
