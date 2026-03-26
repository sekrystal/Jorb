export function ValidationHarnessPage() {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Temporary coexistence</p>
          <h3>Streamlit remains the validation harness</h3>
        </div>
        <p className="panel-copy">
          Internal and operator-heavy surfaces still live in Streamlit while the production JS shell grows around product routes first.
        </p>
      </div>
      <div className="callout-grid">
        <article className="callout-card">
          <h4>Use the JS shell for</h4>
          <p>Jobs, saved state, applied tracking, profile editing, and the production route hierarchy.</p>
        </article>
        <article className="callout-card">
          <h4>Use Streamlit for now</h4>
          <p>Agent activity, investigations, learning, autonomy ops, and other diagnostic or operator surfaces.</p>
        </article>
      </div>
      <a className="primary-link" href="http://127.0.0.1:8500" target="_blank" rel="noreferrer">
        Open Streamlit validation harness
      </a>
    </section>
  );
}
