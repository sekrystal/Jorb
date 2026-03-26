import { NavLink, Outlet } from "react-router-dom";

const navigation = [
  { to: "/jobs", label: "Jobs", detail: "Primary product path" },
  { to: "/saved", label: "Saved", detail: "Intentional follow-up queue" },
  { to: "/applied", label: "Applied", detail: "Application tracker" },
  { to: "/profile", label: "Profile", detail: "Editable candidate profile" },
  { to: "/validation-harness", label: "Validation Harness", detail: "Temporary Streamlit path" },
];

export function AppShell() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <p className="eyebrow">JORB</p>
        <h1>Production front end shell</h1>
        <p className="sidebar-copy">
          Product routes come first. Internal operator surfaces stay behind the API and Streamlit harness while the
          production UI is bootstrapped.
        </p>
        <nav className="nav-list" aria-label="Primary">
          {navigation.map((item) => (
            <NavLink
              className={({ isActive }) => `nav-item${isActive ? " is-active" : ""}`}
              key={item.to}
              to={item.to}
            >
              <span>{item.label}</span>
              <small>{item.detail}</small>
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="content">
        <header className="frame-header">
          <div>
            <p className="eyebrow">Product-first routing</p>
            <h2>Clay-like workbench shell for the JS app</h2>
          </div>
          <a className="ghost-link" href="http://127.0.0.1:8000/docs" target="_blank" rel="noreferrer">
            FastAPI docs
          </a>
        </header>
        <Outlet />
      </main>
    </div>
  );
}
