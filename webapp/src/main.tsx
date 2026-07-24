import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router";

import { TooltipProvider } from "@/components/ui/tooltip";
import { appRouter } from "./routes";
import "./styles.css";

const rootElement = document.getElementById("app");

if (!rootElement) {
  throw new Error("Cannot find #app root element.");
}

createRoot(rootElement).render(
  <StrictMode>
    <TooltipProvider>
      <RouterProvider router={appRouter} />
    </TooltipProvider>
  </StrictMode>,
);
