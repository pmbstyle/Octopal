import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { Activity, Puzzle, Settings2, Siren, Wrench } from "lucide-react";

import octopalLogo from "../assets/octopal-logo.png";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import type { DashboardFilters } from "./GlobalFiltersBar";

const tokenStorageKey = "octopal.webapp.token";

const defaultFilters: DashboardFilters = {
  windowMinutes: 60,
  service: "all",
  environment: "all",
  token: "",
};

export type AppShellOutletContext = {
  filters: DashboardFilters;
  setFilters: (next: DashboardFilters) => void;
};

type NavItem = {
  to: string;
  label: string;
  description: string;
  icon: typeof Activity;
};

const navGroups: { title: string; items: NavItem[] }[] = [
  {
    title: "Operations",
    items: [
      { to: "/", label: "Control", description: "Live operating surface", icon: Activity },
      { to: "/incidents", label: "Incidents", description: "Open signals and pressure", icon: Siren },
    ],
  },
  {
    title: "Workspace",
    items: [
      { to: "/workers", label: "Workers", description: "Templates and worker setup", icon: Wrench },
      { to: "/skills", label: "Skills", description: "Installed skill library", icon: Puzzle },
      { to: "/system", label: "System", description: "Host, queues, and stability", icon: Settings2 },
    ],
  },
];

const pageMeta = new Map<string, { title: string; description: string }>(
  navGroups.flatMap((group) => group.items.map((item) => [item.to, { title: item.label, description: item.description }])),
);

function getPageMeta(pathname: string): { title: string; description: string } {
  if (pathname === "/") {
    return pageMeta.get("/") ?? { title: "Control", description: "Live operating surface" };
  }

  const match = Array.from(pageMeta.entries()).find(([to]) => to !== "/" && pathname.startsWith(to));
  return match?.[1] ?? { title: "Dashboard", description: "Operator workspace" };
}

export function AppShell() {
  const location = useLocation();
  const [filters, setFilters] = useState<DashboardFilters>(() => ({
    ...defaultFilters,
    token: sessionStorage.getItem(tokenStorageKey) ?? "",
  }));
  const [draftToken, setDraftToken] = useState<string>(filters.token);

  useEffect(() => {
    if (filters.token) {
      sessionStorage.setItem(tokenStorageKey, filters.token);
    } else {
      sessionStorage.removeItem(tokenStorageKey);
    }
  }, [filters.token]);

  useEffect(() => {
    setDraftToken(filters.token);
  }, [filters.token]);

  const currentPage = getPageMeta(location.pathname);

  return (
      <div className="min-h-screen bg-[var(--app-bg)] text-[var(--text-strong)] md:grid md:grid-cols-[288px_minmax(0,1fr)]">
        <aside className="hidden h-screen flex-col border-r border-white/6 bg-[var(--sidebar-bg)] md:sticky md:top-0 md:flex">
          <div className="gap-4 px-3 py-4">
            <div className="flex items-center gap-3 px-3 py-3">
              <img src={octopalLogo} alt="Octopal" className="object-contain" />
            </div>
          </div>

          <div className="flex min-h-0 flex-1 flex-col overflow-y-auto px-2">
            {navGroups.map((group) => (
              <section key={group.title} className="px-2 py-2">
                <p className="px-2 pb-3 text-xs font-medium text-sidebar-foreground/70">{group.title}</p>
                <nav className="flex flex-col gap-1">
                    {group.items.map((item) => {
                      const Icon = item.icon;
                      const isActive = item.to === "/" ? location.pathname === "/" : location.pathname.startsWith(item.to);
                      return (
                        <NavLink
                          key={item.to}
                          to={item.to}
                          end={item.to === "/"}
                          className={cn(
                            "flex items-start gap-3 rounded-2xl px-3 py-3 transition-colors hover:bg-white/[0.04]",
                            isActive && "bg-white/[0.06] text-white",
                          )}
                        >
                          <Icon className="mt-0.5 size-4 shrink-0" />
                          <div className="min-w-0">
                            <div className="text-sm font-medium">{item.label}</div>
                            <div className="mt-0.5 text-xs text-[var(--text-dim)]">{item.description}</div>
                          </div>
                        </NavLink>
                      );
                    })}
                </nav>
              </section>
            ))}
          </div>

          <div className="px-3 pb-4">
            <div className="mx-0 h-px bg-white/6" />
            <Card className="border-white/6 bg-white/[0.03] py-0 shadow-none group-data-[collapsible=icon]:hidden">
              <CardContent className="p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-[var(--text-dim)]">Live mode</p>
                <p className="mt-2 text-sm text-[var(--text-strong)]">Data refreshes continuously across the dashboard.</p>
                <div className="mt-3 flex items-center gap-2 text-xs text-[var(--text-muted)]">
                  <span className="size-2 rounded-full bg-emerald-400" />
                  Poll every 15s plus stream updates when available
                </div>
              </CardContent>
            </Card>
          </div>
        </aside>

        <div className="min-w-0 bg-transparent">
          <header className="sticky top-0 z-20 border-b border-white/6 bg-[var(--surface-top)]/88 backdrop-blur-xl">
            <div className="flex min-w-0 items-center justify-between gap-4 px-4 py-4 md:px-6 lg:px-8">
              <div className="flex min-w-0 items-center gap-3">
                <div className="min-w-0">
                  <p className="text-[11px] uppercase tracking-[0.24em] text-[var(--text-dim)]">Operations dashboard</p>
                  <div className="mt-1 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
                    <h2 className="truncate text-lg font-semibold tracking-[-0.03em] text-white">{currentPage.title}</h2>
                    <p className="truncate text-sm text-[var(--text-muted)]">{currentPage.description}</p>
                  </div>
                </div>
              </div>

              <div className="flex shrink-0 items-center gap-3">
                <div className="hidden items-center gap-2 rounded-full border border-white/8 bg-white/[0.03] px-2 py-1.5 lg:flex">
                  <span className="pl-2 text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Dashboard token</span>
                  <Input
                    value={draftToken}
                    onChange={(event) => setDraftToken(event.target.value)}
                    type="password"
                    placeholder="Optional access token"
                    className="h-8 w-72 border-0 bg-transparent px-2 text-sm text-[var(--text-strong)] shadow-none focus-visible:ring-0"
                  />
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    className="rounded-full bg-white/[0.08] text-[var(--text-strong)] hover:bg-white/[0.12]"
                    onClick={() => setFilters((current) => ({ ...current, token: draftToken.trim() }))}
                  >
                    Apply
                  </Button>
                </div>
              </div>
            </div>
            <div className="border-t border-white/6 px-4 py-3 lg:hidden md:px-6 lg:px-8">
              <div className="flex flex-col gap-2 sm:flex-row">
                <Input
                  value={draftToken}
                  onChange={(event) => setDraftToken(event.target.value)}
                  type="password"
                  placeholder="Optional dashboard token"
                  className="rounded-2xl border-white/8 bg-[var(--field-bg)]"
                />
                <Button
                  type="button"
                  variant="secondary"
                  className="rounded-2xl bg-white/[0.08] text-[var(--text-strong)] hover:bg-white/[0.12]"
                  onClick={() => setFilters((current) => ({ ...current, token: draftToken.trim() }))}
                >
                  Apply token
                </Button>
              </div>
            </div>
          </header>

          <main className={cn("min-w-0 px-4 py-5 md:px-6 lg:px-8 lg:py-8")}>
            <Outlet context={{ filters, setFilters }} />
          </main>
        </div>
      </div>
  );
}
