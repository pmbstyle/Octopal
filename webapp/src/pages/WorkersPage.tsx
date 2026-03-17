import { useEffect, useMemo, useState } from "react";
import { useOutletContext } from "react-router-dom";

import {
  createWorkerTemplate,
  deleteWorkerTemplate,
  fetchWorkerTemplates,
  type WorkerTemplate,
  updateWorkerTemplate,
} from "../api/dashboardClient";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime } from "../utils/dateTime";

type WorkerTemplateForm = {
  id: string;
  name: string;
  description: string;
  system_prompt: string;
  available_tools: string;
  required_permissions: string;
  model: string;
  max_thinking_steps: string;
  default_timeout_seconds: string;
  can_spawn_children: boolean;
  allowed_child_templates: string;
};

const emptyForm: WorkerTemplateForm = {
  id: "",
  name: "",
  description: "",
  system_prompt: "",
  available_tools: "",
  required_permissions: "",
  model: "",
  max_thinking_steps: "10",
  default_timeout_seconds: "300",
  can_spawn_children: false,
  allowed_child_templates: "",
};

function toForm(template?: WorkerTemplate | null): WorkerTemplateForm {
  if (!template) {
    return emptyForm;
  }
  return {
    id: template.id,
    name: template.name,
    description: template.description,
    system_prompt: template.system_prompt,
    available_tools: template.available_tools.join(", "),
    required_permissions: template.required_permissions.join(", "),
    model: template.model ?? "",
    max_thinking_steps: String(template.max_thinking_steps ?? 10),
    default_timeout_seconds: String(template.default_timeout_seconds ?? 300),
    can_spawn_children: Boolean(template.can_spawn_children),
    allowed_child_templates: template.allowed_child_templates.join(", "),
  };
}

function parseCommaList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function toPayload(form: WorkerTemplateForm): WorkerTemplate {
  return {
    id: form.id.trim(),
    name: form.name.trim(),
    description: form.description.trim(),
    system_prompt: form.system_prompt.trim(),
    available_tools: parseCommaList(form.available_tools),
    required_permissions: parseCommaList(form.required_permissions),
    model: form.model.trim() || null,
    max_thinking_steps: Number(form.max_thinking_steps || 10),
    default_timeout_seconds: Number(form.default_timeout_seconds || 300),
    can_spawn_children: form.can_spawn_children,
    allowed_child_templates: parseCommaList(form.allowed_child_templates),
  };
}

function sortTemplates(templates: WorkerTemplate[]): WorkerTemplate[] {
  return [...templates].sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

export function WorkersPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [templates, setTemplates] = useState<WorkerTemplate[]>([]);
  const [selectedId, setSelectedId] = useState<string>("");
  const [form, setForm] = useState<WorkerTemplateForm>(emptyForm);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");

    void fetchWorkerTemplates(filters.token || undefined)
      .then((payload) => {
        if (!active) {
          return;
        }
        const sorted = sortTemplates(payload);
        const nextSelectedId = sorted[0]?.id ?? "";
        setTemplates(sorted);
        setSelectedId((current) => {
          if (current && sorted.some((item) => item.id === current)) {
            const currentTemplate = sorted.find((item) => item.id === current) ?? null;
            setForm(toForm(currentTemplate));
            return current;
          }
          return nextSelectedId;
        });
        setForm(toForm(sorted[0] ?? null));
      })
      .catch((err: unknown) => {
        if (!active) {
          return;
        }
        setError(err instanceof Error ? err.message : "Failed to load worker templates");
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [filters.token]);

  const selectedTemplate = useMemo(
    () => templates.find((item) => item.id === selectedId) ?? null,
    [selectedId, templates],
  );

  useEffect(() => {
    if (!selectedTemplate && selectedId) {
      return;
    }
    setForm(toForm(selectedTemplate));
    setNotice("");
    setError("");
  }, [selectedId, selectedTemplate]);

  const isCreating = selectedId === "";

  function startCreate(): void {
    setSelectedId("");
    setForm(emptyForm);
    setNotice("");
    setError("");
  }

  function selectTemplate(template: WorkerTemplate): void {
    setSelectedId(template.id);
    setForm(toForm(template));
    setNotice("");
    setError("");
  }

  function handleChange<K extends keyof WorkerTemplateForm>(key: K, value: WorkerTemplateForm[K]): void {
    setForm((current) => ({ ...current, [key]: value }));
  }

  async function handleSave(): Promise<void> {
    setSaving(true);
    setError("");
    setNotice("");
    try {
      const payload = toPayload(form);
      const saved = isCreating
        ? await createWorkerTemplate(payload, filters.token || undefined)
        : await updateWorkerTemplate(payload, filters.token || undefined);
      const nextTemplates = sortTemplates(
        isCreating
          ? [...templates.filter((item) => item.id !== saved.id), saved]
          : templates.map((item) => (item.id === saved.id ? saved : item)),
      );
      setTemplates(nextTemplates);
      setSelectedId(saved.id);
      setForm(toForm(saved));
      setNotice(isCreating ? "Worker template created." : "Worker template saved.");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to save worker template");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(): Promise<void> {
    if (!selectedTemplate) {
      return;
    }
    const confirmed = window.confirm(`Delete worker "${selectedTemplate.name}" (${selectedTemplate.id})?`);
    if (!confirmed) {
      return;
    }
    setSaving(true);
    setError("");
    setNotice("");
    try {
      await deleteWorkerTemplate(selectedTemplate.id, filters.token || undefined);
      const nextTemplates = templates.filter((item) => item.id !== selectedTemplate.id);
      setTemplates(nextTemplates);
      setSelectedId(nextTemplates[0]?.id ?? "");
      if (nextTemplates.length === 0) {
        setForm(emptyForm);
      }
      setNotice(`Worker template "${selectedTemplate.name}" deleted.`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to delete worker template");
    } finally {
      setSaving(false);
    }
  }

  if (loading) {
    return (
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-8 text-slate-300">
        <h2 className="text-2xl font-semibold text-slate-100">Workers</h2>
        <p className="mt-2">Loading saved worker templates...</p>
      </section>
    );
  }

  return (
    <section className="grid gap-5">
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-slate-950/60">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Worker templates</p>
            <h2 className="mt-2 text-2xl font-semibold text-slate-100">Saved workers</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              Manage the worker templates stored in <code>workspace/workers</code>. Changes apply to future launches.
            </p>
          </div>
          <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-4 py-3 text-right">
            <div className="text-xs uppercase tracking-wide text-slate-500">Templates</div>
            <div className="mt-1 text-2xl font-semibold text-slate-100">{templates.length}</div>
          </div>
        </div>
      </section>

      {error ? (
        <section className="rounded-2xl border border-rose-500/40 bg-rose-950/30 p-4 text-sm text-rose-200">
          {error}
        </section>
      ) : null}
      {notice ? (
        <section className="rounded-2xl border border-emerald-500/30 bg-emerald-950/20 p-4 text-sm text-emerald-200">
          {notice}
        </section>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-[320px_minmax(0,1fr)] xl:items-start">
        <aside className="flex min-h-0 flex-col rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60 xl:sticky xl:top-5 xl:max-h-[calc(100vh-10rem)]">
          <div className="flex items-center justify-between gap-3">
            <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Worker list</h3>
            <button
              type="button"
              onClick={startCreate}
              className="rounded-full border border-cyan-400/40 bg-cyan-400/10 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-cyan-200 transition hover:border-cyan-300 hover:bg-cyan-400/20"
            >
              New
            </button>
          </div>
          <div className="mt-4 min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
            {templates.length === 0 ? (
              <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4 text-sm text-slate-400">
                No saved workers yet. Create the first template here.
              </div>
            ) : (
              templates.map((template) => {
                const selected = template.id === selectedId;
                return (
                  <button
                    key={template.id}
                    type="button"
                    onClick={() => selectTemplate(template)}
                    className={[
                      "w-full rounded-xl border px-4 py-3 text-left transition",
                      selected
                        ? "border-cyan-400/40 bg-cyan-400/10"
                        : "border-slate-800 bg-slate-950/70 hover:border-slate-700 hover:bg-slate-900",
                    ].join(" ")}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-slate-100">{template.name}</div>
                        <div className="mt-1 font-mono text-xs text-cyan-300">{template.id}</div>
                      </div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">
                        {template.can_spawn_children ? "Parent" : "Leaf"}
                      </div>
                    </div>
                    <p className="mt-2 line-clamp-2 text-sm text-slate-400">{template.description}</p>
                    <div className="mt-3 text-xs text-slate-500">
                      Updated {formatLocalDateTime(template.updated_at)}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </aside>

        <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-slate-950/60">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">
                {isCreating ? "Create worker" : "Edit worker"}
              </p>
              <h3 className="mt-2 text-2xl font-semibold text-slate-100">
                {isCreating ? "New template" : selectedTemplate?.name ?? "Worker template"}
              </h3>
              <p className="mt-2 text-sm text-slate-400">
                {isCreating
                  ? "Define a new worker template and save it into the workspace."
                  : "Update tools, prompt and runtime defaults for future worker launches."}
              </p>
            </div>
            <div className="flex gap-2">
              {!isCreating ? (
                <button
                  type="button"
                  onClick={handleDelete}
                  disabled={saving || !selectedTemplate}
                  className="rounded-full border border-rose-400/40 bg-rose-400/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-rose-200 transition hover:border-rose-300 hover:bg-rose-400/20 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Delete
                </button>
              ) : null}
              <button
                type="button"
                onClick={() => setForm(toForm(selectedTemplate))}
                disabled={saving}
                className="rounded-full border border-slate-700 bg-slate-950/70 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-slate-600 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Reset
              </button>
              <button
                type="button"
                onClick={() => void handleSave()}
                disabled={saving}
                className="rounded-full border border-cyan-400/40 bg-cyan-400/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-cyan-200 transition hover:border-cyan-300 hover:bg-cyan-400/20 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {saving ? "Saving..." : isCreating ? "Create" : "Save"}
              </button>
            </div>
          </div>

          <div className="mt-6 grid gap-5 lg:grid-cols-2">
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">ID</span>
              <input
                value={form.id}
                onChange={(event) => handleChange("id", event.target.value)}
                disabled={!isCreating || saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Name</span>
              <input
                value={form.name}
                onChange={(event) => handleChange("name", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300 lg:col-span-2">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Description</span>
              <input
                value={form.description}
                onChange={(event) => handleChange("description", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300 lg:col-span-2">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">System prompt</span>
              <textarea
                value={form.system_prompt}
                onChange={(event) => handleChange("system_prompt", event.target.value)}
                disabled={saving}
                rows={10}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Available tools</span>
              <textarea
                value={form.available_tools}
                onChange={(event) => handleChange("available_tools", event.target.value)}
                disabled={saving}
                rows={4}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Permissions</span>
              <textarea
                value={form.required_permissions}
                onChange={(event) => handleChange("required_permissions", event.target.value)}
                disabled={saving}
                rows={4}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Model override</span>
              <input
                value={form.model}
                onChange={(event) => handleChange("model", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Max thinking steps</span>
              <input
                type="number"
                min={1}
                value={form.max_thinking_steps}
                onChange={(event) => handleChange("max_thinking_steps", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Default timeout (sec)</span>
              <input
                type="number"
                min={1}
                value={form.default_timeout_seconds}
                onChange={(event) => handleChange("default_timeout_seconds", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
            <label className="flex items-center gap-3 rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-3 text-sm text-slate-300">
              <input
                type="checkbox"
                checked={form.can_spawn_children}
                onChange={(event) => handleChange("can_spawn_children", event.target.checked)}
                disabled={saving}
                className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-cyan-400 focus:ring-cyan-400"
              />
              Allow this worker to spawn child workers
            </label>
            <label className="grid gap-2 text-sm text-slate-300 lg:col-span-2">
              <span className="text-xs uppercase tracking-[0.16em] text-slate-500">Allowed child templates</span>
              <input
                value={form.allowed_child_templates}
                onChange={(event) => handleChange("allowed_child_templates", event.target.value)}
                disabled={saving}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-slate-100 outline-none transition focus:border-cyan-400/40"
              />
            </label>
          </div>

          {!isCreating && selectedTemplate ? (
            <div className="mt-6 flex flex-wrap gap-4 text-xs text-slate-500">
              <span>Created {formatLocalDateTime(selectedTemplate.created_at)}</span>
              <span>Updated {formatLocalDateTime(selectedTemplate.updated_at)}</span>
            </div>
          ) : null}
        </section>
      </div>
    </section>
  );
}
