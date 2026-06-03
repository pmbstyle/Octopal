import { render, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { appRouter } from "../routes";

describe("AppShell", () => {
  it("renders control center shell", () => {
    const router = createMemoryRouter(appRouter.routes, {
      initialEntries: ["/"],
    });

    render(<RouterProvider router={router} />);

    expect(screen.getByRole("heading", { name: "Control" })).toBeInTheDocument();
    expect(screen.getAllByText("Live operating surface").length).toBeGreaterThan(0);
    expect(screen.getByText("Dashboard token")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Skills Installed skill library/i })).toBeInTheDocument();
    expect(screen.getByText("Loading live operations view...")).toBeInTheDocument();
  });
});
