import { Navigate, createBrowserRouter } from "react-router-dom";
import { AppShell } from "./shell/AppShell";
import { JobsPage, SavedPage, AppliedPage } from "./views/LeadPages";
import { ProfilePage } from "./views/ProfilePage";
import { ValidationHarnessPage } from "./views/ValidationHarnessPage";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/jobs" replace /> },
      { path: "jobs", element: <JobsPage /> },
      { path: "saved", element: <SavedPage /> },
      { path: "applied", element: <AppliedPage /> },
      { path: "profile", element: <ProfilePage /> },
      { path: "validation-harness", element: <ValidationHarnessPage /> },
    ],
  },
]);
