import { NavLink } from "react-router";
import { Briefcase, Bookmark, CheckCircle, User, Activity } from "lucide-react";

export function Sidebar() {
  const navItems = [
    { to: "/", label: "Jobs", icon: Briefcase },
    { to: "/saved", label: "Saved", icon: Bookmark },
    { to: "/applied", label: "Applied", icon: CheckCircle },
    { to: "/profile", label: "Profile", icon: User },
  ];

  return (
    <div className="w-56 border-r border-gray-200 bg-white flex flex-col h-screen fixed left-0 top-0">
      <div className="p-4 border-b border-gray-200">
        <h1 className="font-semibold text-lg">Jorb</h1>
      </div>
      
      <nav className="flex-1 p-3">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2 rounded-md mb-1 transition-colors ${
                  isActive
                    ? "bg-gray-100 text-gray-900"
                    : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                }`
              }
            >
              <Icon className="w-4 h-4" />
              <span className="text-sm">{item.label}</span>
            </NavLink>
          );
        })}
      </nav>

      <div className="p-4 border-t border-gray-200">
        <div className="text-xs text-gray-500 space-y-1">
          <div className="flex items-center gap-2">
            <Activity className="w-3 h-3" />
            <span className="font-medium">System Status</span>
          </div>
          <div className="pl-5 space-y-0.5">
            <div>Last run: 12 min ago</div>
            <div>Jobs found: 847</div>
            <div className="flex items-center gap-1">
              <div className="w-1.5 h-1.5 rounded-full bg-green-500" />
              <span>Healthy</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}