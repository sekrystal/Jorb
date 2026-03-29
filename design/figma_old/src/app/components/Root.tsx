import { Outlet } from "react-router";
import { Sidebar } from "./Sidebar";

export function Root() {
  return (
    <div className="flex h-screen bg-gray-50">
      <Sidebar />
      <div className="ml-56 flex-1 overflow-hidden">
        <Outlet />
      </div>
    </div>
  );
}
