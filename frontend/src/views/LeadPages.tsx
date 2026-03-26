import { useEffect, useState } from "react";
import { getLeads, type Lead } from "../lib/api";

const SAVED_PARAMS = { only_saved: true };
const APPLIED_PARAMS = { only_applied: true };

type LeadViewProps = {
  title: string;
  description: string;
  params?: Record<string, string | boolean | number | undefined>;
};

function LeadsView({ title, description, params }: LeadViewProps) {
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
    <LeadsView
      title="Jobs"
      description="This is the explicit product entry point for the JS shell. It reads the default visible shortlist from /opportunities."
    />
  );
}

export function SavedPage() {
  return (
    <LeadsView
      title="Saved"
      description="Saved rows reuse the same opportunities endpoint with a scoped product query."
      params={SAVED_PARAMS}
    />
  );
}

export function AppliedPage() {
  return (
    <LeadsView
      title="Applied"
      description="Applied rows stay in the product path and will later carry the richer tracker workflow."
      params={APPLIED_PARAMS}
    />
  );
}
