import { useEffect, useMemo, useState } from "react";
import { useOutletContext } from "react-router-dom";
import {
  CheckCircle2,
  Download,
  Power,
  PowerOff,
  RefreshCw,
  Trash2,
  TriangleAlert,
} from "lucide-react";

import {
  deleteSkill,
  fetchSkills,
  installSkill,
  setSkillEnabled,
  type DashboardSkill,
  type DashboardSkillsResponse,
} from "../api/dashboardClient";
import type { AppShellOutletContext } from "../ui/AppShell";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const inputClass = "rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 text-white";

function sortSkills(skills: DashboardSkill[]): DashboardSkill[] {
  return [...skills].sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

function statusTone(skill: DashboardSkill): string {
  if (!skill.enabled) {
    return "border-slate-400/20 bg-slate-400/10 text-slate-200";
  }
  if (skill.ready) {
    return "border-emerald-400/25 bg-emerald-500/12 text-emerald-200";
  }
  return "border-amber-400/25 bg-amber-500/12 text-amber-200";
}

function statusLabel(skill: DashboardSkill): string {
  if (!skill.enabled) {
    return "Disabled";
  }
  if (skill.ready) {
    return "Ready";
  }
  return skill.status || "Needs setup";
}

function sourceLabel(skill: DashboardSkill): string {
  const label = skill.source.label || skill.source.path || skill.origin;
  return label || "local";
}

function RequirementList({ title, values }: { title: string; values: string[] }) {
  if (values.length === 0) {
    return null;
  }
  return (
    <div className="rounded-[18px] border border-white/6 bg-[var(--surface-panel-strong)] p-3">
      <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">{title}</div>
      <div className="mt-2 flex flex-wrap gap-2">
        {values.map((value) => (
          <code key={value} className="rounded-full bg-white/[0.06] px-2.5 py-1 text-xs text-amber-100">
            {value}
          </code>
        ))}
      </div>
    </div>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded-[18px] border border-white/6 bg-[var(--surface-panel-strong)] p-3">
      <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">{label}</div>
      <div className="mt-1 truncate text-sm text-[var(--text-strong)]" title={value}>
        {value || "n/a"}
      </div>
    </div>
  );
}

function replaceSkill(skills: DashboardSkill[], skill: DashboardSkill): DashboardSkill[] {
  const next = skills.some((item) => item.id === skill.id)
    ? skills.map((item) => (item.id === skill.id ? skill : item))
    : [...skills, skill];
  return sortSkills(next);
}

export function SkillsPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [payload, setPayload] = useState<DashboardSkillsResponse | null>(null);
  const [selectedId, setSelectedId] = useState("");
  const [source, setSource] = useState("");
  const [clawhubSite, setClawhubSite] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  async function reload(nextSelectedId?: string): Promise<void> {
    setLoading(true);
    setError("");
    try {
      const nextPayload = await fetchSkills(filters.token || undefined);
      const sorted = sortSkills(nextPayload.skills ?? []);
      const selected = nextSelectedId && sorted.some((item) => item.id === nextSelectedId)
        ? nextSelectedId
        : sorted[0]?.id ?? "";
      setPayload({ ...nextPayload, skills: sorted });
      setSelectedId(selected);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to load skills");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [filters.token]);

  const skills = payload?.skills ?? [];
  const selectedSkill = useMemo(
    () => skills.find((item) => item.id === selectedId) ?? null,
    [selectedId, skills],
  );
  const enabledCount = skills.filter((skill) => skill.enabled).length;
  const readyCount = skills.filter((skill) => skill.enabled && skill.ready).length;
  const needsSetupCount = skills.filter((skill) => skill.enabled && !skill.ready).length;
  const defaultClawhubSite = payload?.install.default_clawhub_site ?? "https://clawhub.ai";

  async function handleInstall(): Promise<void> {
    const trimmedSource = source.trim();
    if (!trimmedSource) {
      setError("Skill source is required");
      return;
    }
    setSaving(true);
    setError("");
    setNotice("");
    try {
      const response = await installSkill(
        {
          source: trimmedSource,
          clawhub_site: clawhubSite.trim() || undefined,
        },
        filters.token || undefined,
      );
      setPayload((current) => current ? { ...current, skills: replaceSkill(current.skills, response.skill) } : current);
      setSelectedId(response.skill.id);
      setSource("");
      setNotice(`Skill "${response.skill.name}" installed.`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to install skill");
    } finally {
      setSaving(false);
    }
  }

  async function handleToggle(skill: DashboardSkill, enabled: boolean): Promise<void> {
    setSaving(true);
    setError("");
    setNotice("");
    try {
      const response = await setSkillEnabled(skill.id, enabled, filters.token || undefined);
      setPayload((current) => current ? { ...current, skills: replaceSkill(current.skills, response.skill) } : current);
      setSelectedId(response.skill.id);
      setNotice(`Skill "${response.skill.name}" ${enabled ? "enabled" : "disabled"}.`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : `Failed to ${enabled ? "enable" : "disable"} skill`);
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(skill: DashboardSkill): Promise<void> {
    const confirmed = window.confirm(`Delete skill "${skill.name}" (${skill.id})?`);
    if (!confirmed) {
      return;
    }
    setSaving(true);
    setError("");
    setNotice("");
    try {
      const response = await deleteSkill(skill.id, filters.token || undefined);
      const sorted = sortSkills(response.skills.skills ?? []);
      setPayload({ ...response.skills, skills: sorted });
      setSelectedId(sorted[0]?.id ?? "");
      setNotice(`Skill "${skill.name}" deleted.`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to delete skill");
    } finally {
      setSaving(false);
    }
  }

  if (loading) {
    return (
      <section className="rounded-[30px] border border-white/6 bg-[var(--surface-panel)] p-8 text-[var(--text-strong)]">
        <h2 className="text-2xl font-semibold text-white">Skills</h2>
        <p className="mt-2 text-sm text-[var(--text-muted)]">Loading installed skills...</p>
      </section>
    );
  }

  return (
    <section className="grid gap-5">
      <section className="rounded-[32px] border border-white/6 bg-[var(--surface-panel)] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.24)]">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-[11px] uppercase tracking-[0.24em] text-[var(--text-dim)]">Skill library</p>
            <h2 className="mt-3 text-3xl font-semibold text-white">Installed skills</h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-muted)]">
              Skills are loaded from <code>workspace/skills</code> and the installer registry.
            </p>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-3 text-right">
              <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">Total</div>
              <div className="mt-1 text-2xl font-semibold text-white">{skills.length}</div>
            </div>
            <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-3 text-right">
              <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">Enabled</div>
              <div className="mt-1 text-2xl font-semibold text-white">{enabledCount}</div>
            </div>
            <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-3 text-right">
              <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">Ready</div>
              <div className="mt-1 text-2xl font-semibold text-white">{readyCount}</div>
            </div>
          </div>
        </div>
      </section>

      {error ? (
        <section className="rounded-[24px] border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-200">
          {error}
        </section>
      ) : null}
      {notice ? (
        <section className="rounded-[24px] border border-emerald-500/30 bg-emerald-950/20 p-4 text-sm text-emerald-200">
          {notice}
        </section>
      ) : null}

      <section className="rounded-[28px] border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-end">
          <label className="grid flex-1 gap-2 text-sm text-[var(--text-strong)]">
            <span className="text-xs uppercase tracking-[0.16em] text-white/92">Source</span>
            <Input
              value={source}
              onChange={(event) => setSource(event.target.value)}
              disabled={saving}
              placeholder="skill-name, https://..., or local path"
              className={inputClass}
            />
          </label>
          <label className="grid gap-2 text-sm text-[var(--text-strong)] lg:w-72">
            <span className="text-xs uppercase tracking-[0.16em] text-white/92">Clawhub site</span>
            <Input
              value={clawhubSite}
              onChange={(event) => setClawhubSite(event.target.value)}
              disabled={saving}
              placeholder={defaultClawhubSite}
              className={inputClass}
            />
          </label>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => void reload(selectedId)}
              disabled={saving}
              className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)] hover:bg-white/[0.08] hover:text-white"
            >
              <RefreshCw className="size-4" />
              Refresh
            </Button>
            <Button
              type="button"
              variant="secondary"
              onClick={() => void handleInstall()}
              disabled={saving || !source.trim()}
              className="rounded-full bg-white/[0.08] text-white hover:bg-white/[0.12]"
            >
              <Download className="size-4" />
              {saving ? "Installing..." : "Install"}
            </Button>
          </div>
        </div>
      </section>

      <div className="grid gap-5 xl:grid-cols-[360px_minmax(0,1fr)] xl:items-start">
        <aside className="flex min-h-0 flex-col rounded-[28px] border border-white/6 bg-[var(--surface-panel)] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.2)] xl:sticky xl:top-5 xl:max-h-[calc(100vh-10rem)]">
          <div className="flex items-center justify-between gap-3">
            <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Skill list</h3>
            {needsSetupCount > 0 ? (
              <Badge className="rounded-full border border-amber-400/25 bg-amber-500/12 text-amber-200">
                {needsSetupCount} setup
              </Badge>
            ) : null}
          </div>
          <div className="mt-4 min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
            {skills.length === 0 ? (
              <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-sm text-[var(--text-muted)]">
                No installed skills yet.
              </div>
            ) : (
              skills.map((skill) => {
                const selected = skill.id === selectedId;
                return (
                  <button
                    key={skill.id}
                    type="button"
                    onClick={() => {
                      setSelectedId(skill.id);
                      setNotice("");
                      setError("");
                    }}
                    className={[
                      "w-full rounded-[22px] border px-4 py-3 text-left transition",
                      selected
                        ? "border-white/10 bg-white/[0.07]"
                        : "border-white/6 bg-[var(--surface-panel-strong)] hover:bg-white/[0.04]",
                    ].join(" ")}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="truncate text-sm font-semibold text-white">{skill.name}</div>
                        <div className="mt-1 truncate font-mono text-xs text-cyan-300">{skill.id}</div>
                      </div>
                      <span className={`shrink-0 rounded-full border px-2.5 py-1 text-[11px] ${statusTone(skill)}`}>
                        {statusLabel(skill)}
                      </span>
                    </div>
                    <p className="mt-2 line-clamp-2 text-sm text-[var(--text-muted)]">
                      {skill.description || "No description"}
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2 text-[11px] uppercase tracking-[0.14em] text-[var(--text-dim)]">
                      <span>{skill.scope}</span>
                      <span>{skill.origin}</span>
                      {skill.trust.has_scripts ? <span>Scripts</span> : null}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </aside>

        <section className="rounded-[28px] border border-white/6 bg-[var(--surface-panel)] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
          {selectedSkill ? (
            <>
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="min-w-0">
                  <p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">Skill detail</p>
                  <h3 className="mt-2 break-words text-2xl font-semibold text-white">{selectedSkill.name}</h3>
                  <p className="mt-2 max-w-3xl text-sm leading-6 text-[var(--text-muted)]">
                    {selectedSkill.description || "No description"}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void handleToggle(selectedSkill, !selectedSkill.enabled)}
                    disabled={saving || (!selectedSkill.actions.can_enable && !selectedSkill.actions.can_disable)}
                    className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)] hover:bg-white/[0.08] hover:text-white"
                  >
                    {selectedSkill.enabled ? <PowerOff className="size-4" /> : <Power className="size-4" />}
                    {selectedSkill.enabled ? "Disable" : "Enable"}
                  </Button>
                  <Button
                    type="button"
                    variant="destructive"
                    onClick={() => void handleDelete(selectedSkill)}
                    disabled={saving || !selectedSkill.actions.can_remove}
                    className="rounded-full"
                  >
                    <Trash2 className="size-4" />
                    Delete
                  </Button>
                </div>
              </div>

              <div className="mt-5 flex flex-wrap gap-2">
                <Badge className={`rounded-full border ${statusTone(selectedSkill)}`}>
                  {selectedSkill.ready ? <CheckCircle2 className="mr-1 size-3.5" /> : <TriangleAlert className="mr-1 size-3.5" />}
                  {statusLabel(selectedSkill)}
                </Badge>
                <Badge className="rounded-full border border-white/8 bg-white/[0.05] text-[var(--text-muted)]">
                  {selectedSkill.scope}
                </Badge>
                <Badge className="rounded-full border border-white/8 bg-white/[0.05] text-[var(--text-muted)]">
                  {selectedSkill.origin}
                </Badge>
                {selectedSkill.trust.has_scripts ? (
                  <Badge className="rounded-full border border-cyan-400/25 bg-cyan-500/12 text-cyan-200">
                    Scripts
                  </Badge>
                ) : null}
              </div>

              {selectedSkill.reasons.length > 0 ? (
                <div className="mt-5 rounded-[20px] border border-amber-400/25 bg-amber-500/10 p-4 text-sm text-amber-100">
                  <div className="flex items-center gap-2 font-semibold text-amber-100">
                    <TriangleAlert className="size-4" />
                    Attention
                  </div>
                  <ul className="mt-2 list-disc space-y-1 pl-5">
                    {selectedSkill.reasons.map((reason) => (
                      <li key={reason}>{reason}</li>
                    ))}
                  </ul>
                </div>
              ) : null}

              <div className="mt-6 grid gap-3 md:grid-cols-2">
                <DetailRow label="ID" value={selectedSkill.id} />
                <DetailRow label="Source" value={sourceLabel(selectedSkill)} />
                <DetailRow label="Runtime" value={selectedSkill.runtime.kind || "none"} />
                <DetailRow label="Registry" value={payload?.registry_path ?? ""} />
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <RequirementList title="Missing bins" values={selectedSkill.requirements.missing_bins} />
                <RequirementList title="Missing env" values={selectedSkill.requirements.missing_env} />
                <RequirementList title="Missing config" values={selectedSkill.requirements.missing_config} />
              </div>

              {selectedSkill.runtime.next_step ? (
                <div className="mt-4 rounded-[18px] border border-white/6 bg-[var(--surface-panel-strong)] p-3 text-sm text-[var(--text-muted)]">
                  <span className="text-[var(--text-strong)]">Runtime:</span> {selectedSkill.runtime.next_step}
                </div>
              ) : null}
            </>
          ) : (
            <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-6 text-sm text-[var(--text-muted)]">
              Select a skill after installing one.
            </div>
          )}
        </section>
      </div>
    </section>
  );
}
