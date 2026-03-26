import { useEffect, useState } from "react";
import { getCandidateProfile, type CandidateProfile } from "../lib/api";

export function ProfilePage() {
  const [profile, setProfile] = useState<CandidateProfile | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    getCandidateProfile()
      .then((payload) => {
        if (active) {
          setProfile(payload);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Candidate profile</p>
          <h3>Editable profile surface placeholder</h3>
        </div>
        <p className="panel-copy">
          The shell already reads the current FastAPI profile payload. Resume upload and editing can layer onto this contract next.
        </p>
      </div>
      {error ? <p className="state-copy error-copy">{error}</p> : null}
      {profile ? (
        <div className="profile-grid">
          <article>
            <h4>Target titles</h4>
            <p>{profile.target_titles.join(", ") || "No titles configured."}</p>
          </article>
          <article>
            <h4>Target locations</h4>
            <p>{profile.target_locations.join(", ") || "No locations configured."}</p>
          </article>
          <article>
            <h4>Preferred domains</h4>
            <p>{profile.preferred_domains.join(", ") || "No domains configured."}</p>
          </article>
          <article>
            <h4>Focus keywords</h4>
            <p>{profile.focus_keywords.join(", ") || "No focus keywords configured."}</p>
          </article>
        </div>
      ) : (
        <p className="state-copy">Loading candidate profile from FastAPI.</p>
      )}
    </section>
  );
}
