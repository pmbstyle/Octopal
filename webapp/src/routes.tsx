import { createBrowserRouter, Navigate } from "react-router-dom";

import { AppShell } from "./ui/AppShell";
import { RouteErrorBoundary } from "./ui/RouteErrorBoundary";

export const appRouter = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    errorElement: <RouteErrorBoundary />,
  },
  {
    path: "*",
    element: <Navigate to="/" replace />,
  },
], {
  basename: "/dashboard",
});
