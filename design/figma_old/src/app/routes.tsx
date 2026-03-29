import { createBrowserRouter } from "react-router";
import { Root } from "./components/Root";
import { JobsPage } from "./components/JobsPage";
import { SavedPage } from "./components/SavedPage";
import { AppliedPage } from "./components/AppliedPage";
import { ProfilePage } from "./components/ProfilePage";

export const router = createBrowserRouter([
  {
    path: "/",
    Component: Root,
    children: [
      { index: true, Component: JobsPage },
      { path: "saved", Component: SavedPage },
      { path: "applied", Component: AppliedPage },
      { path: "profile", Component: ProfilePage },
    ],
  },
]);
