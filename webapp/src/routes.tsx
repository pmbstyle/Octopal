import { createBrowserRouter, Navigate } from "react-router-dom";

import { ControlCenterPage } from "./pages/ControlCenterPage";
import { IncidentsPage } from "./pages/IncidentsPage";
import { SkillsPage } from "./pages/SkillsPage";
import { SystemPage } from "./pages/SystemPage";
import { WorkersPage } from "./pages/WorkersPage";
import { AppShell } from "./ui/AppShell";
import { RouteErrorBoundary } from "./ui/RouteErrorBoundary";

export const appRouter = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    errorElement: <RouteErrorBoundary />,
    children: [
      {
        index: true,
        element: <ControlCenterPage />,
      },
      {
        path: "incidents",
        element: <IncidentsPage />,
      },
      {
        path: "workers",
        element: <WorkersPage />,
      },
      {
        path: "skills",
        element: <SkillsPage />,
      },
      {
        path: "system",
        element: <SystemPage />,
      },
    ],
  },
  {
    path: "*",
    element: <Navigate to="/" replace />,
  },
], {
  basename: "/dashboard",
});
