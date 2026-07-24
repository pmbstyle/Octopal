import { useEffect, useMemo, useState } from "react";
import { useOutletContext } from "react-router";

import {
  createWorkerTemplate,
  deleteWorkerTemplate,
  fetchWorkerTemplates,
  type WorkerTemplate,
  updateWorkerTemplate,
} from "../api/dashboardClient";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime } from "../utils/dateTime";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";

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

const inputClass = "rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 text-white";
const textareaClass = "rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 py-2 text-white";

function L({ label, children, className = "" }: { label: string; children: React.ReactNode; className?: string }) {
  return (
    <label className={`grid gap-2 text-sm text-[var(--text-strong)] ${className}`.trim()}>
      <span className="text-xs uppercase tracking-[0.16em] text-white/92">{label}</span>
      {children}
    </label>
  );
}

function FormInput(props: React.ComponentProps<typeof Input>) {
  return <Input {...props} className={[inputClass, props.className].filter(Boolean).join(" ")} />;
}

function FormTextarea(props: React.ComponentProps<typeof Textarea>) {
  return <Textarea {...props} className={[textareaClass, props.className].filter(Boolean).join(" ")} />;
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
      <section className="rounded-[30px] border border-white/6 bg-[var(--surface-panel)] p-8 text-[var(--text-strong)]">
        <h2 className="text-2xl font-semibold text-white">Workers</h2>
        <p className="mt-2 text-sm text-[var(--text-muted)]">Loading saved worker templates...</p>
      </section>
    );
  }

  return (
    <section className="grid gap-5">
      <section className="rounded-[32px] border border-white/6 bg-[var(--surface-panel)] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.24)]">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-[11px] uppercase tracking-[0.24em] text-[var(--text-dim)]">Worker templates</p>
            <h2 className="mt-3 text-3xl font-semibold tracking-[-0.04em] text-white">Saved workers</h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-muted)]">
              Manage the worker templates stored in <code>workspace/workers</code>. Changes apply to future launches.
            </p>
          </div>
          <div className="rounded-[24px] border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-3 text-right">
            <div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Templates</div>
            <div className="mt-1 text-2xl font-semibold text-white">{templates.length}</div>
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

      <div className="grid gap-5 xl:grid-cols-[320px_minmax(0,1fr)] xl:items-start">
        <aside className="flex min-h-0 flex-col rounded-[28px] border border-white/6 bg-[var(--surface-panel)] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.2)] xl:sticky xl:top-5 xl:max-h-[calc(100vh-10rem)]">
          <div className="flex items-center justify-between gap-3">
            <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Worker list</h3>
            <Button
              type="button"
              variant="secondary"
              onClick={startCreate}
              className="rounded-full bg-white/[0.06] text-white hover:bg-white/[0.1]"
            >
              New
            </Button>
          </div>
          <div className="mt-4 min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
            {templates.length === 0 ? (
              <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-sm text-[var(--text-muted)]">
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
                      "w-full rounded-[22px] border px-4 py-3 text-left transition",
                      selected
                        ? "border-white/10 bg-white/[0.07]"
                        : "border-white/6 bg-[var(--surface-panel-strong)] hover:bg-white/[0.04]",
                    ].join(" ")}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">{template.name}</div>
                        <div className="mt-1 font-mono text-xs text-cyan-300">{template.id}</div>
                      </div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">
                        {template.can_spawn_children ? "Parent" : "Leaf"}
                      </div>
                    </div>
                    <p className="mt-2 line-clamp-2 text-sm text-[var(--text-muted)]">{template.description}</p>
                    <div className="mt-3 text-xs text-[var(--text-dim)]">
                      Updated {formatLocalDateTime(template.updated_at)}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </aside>

        <section className="rounded-[28px] border border-white/6 bg-[var(--surface-panel)] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">
                {isCreating ? "Create worker" : "Edit worker"}
              </p>
              <h3 className="mt-2 text-2xl font-semibold text-white">
                {isCreating ? "New template" : selectedTemplate?.name ?? "Worker template"}
              </h3>
              <p className="mt-2 text-sm text-[var(--text-muted)]">
                {isCreating
                  ? "Define a new worker template and save it into the workspace."
                  : "Update tools, prompt and runtime defaults for future worker launches."}
              </p>
            </div>
            <div className="flex gap-2">
              {!isCreating ? (
                <Button
                  type="button"
                  variant="destructive"
                  onClick={handleDelete}
                  disabled={saving || !selectedTemplate}
                  className="rounded-full"
                >
                  Delete
                </Button>
              ) : null}
              <Button
                type="button"
                variant="outline"
                onClick={() => setForm(toForm(selectedTemplate))}
                disabled={saving}
                className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)] hover:bg-white/[0.08] hover:text-white"
              >
                Reset
              </Button>
              <Button
                type="button"
                variant="secondary"
                onClick={() => void handleSave()}
                disabled={saving}
                className="rounded-full bg-white/[0.08] text-white hover:bg-white/[0.12]"
              >
                {saving ? "Saving..." : isCreating ? "Create" : "Save"}
              </Button>
            </div>
          </div>

          <div className="mt-6 grid gap-5 lg:grid-cols-2">
            <L label="ID">
              <FormInput
                value={form.id}
                onChange={(event) => handleChange("id", event.target.value)}
                disabled={!isCreating || saving}
              />
            </L>
            <L label="Name">
              <FormInput
                value={form.name}
                onChange={(event) => handleChange("name", event.target.value)}
                disabled={saving}
              />
            </L>
            <L label="Description" className="lg:col-span-2">
              <FormInput
                value={form.description}
                onChange={(event) => handleChange("description", event.target.value)}
                disabled={saving}
              />
            </L>
            <L label="System prompt" className="lg:col-span-2">
              <FormTextarea
                value={form.system_prompt}
                onChange={(event) => handleChange("system_prompt", event.target.value)}
                disabled={saving}
                rows={10}
              />
            </L>
            <L label="Available tools">
              <FormTextarea
                value={form.available_tools}
                onChange={(event) => handleChange("available_tools", event.target.value)}
                disabled={saving}
                rows={4}
              />
            </L>
            <L label="Permissions">
              <FormTextarea
                value={form.required_permissions}
                onChange={(event) => handleChange("required_permissions", event.target.value)}
                disabled={saving}
                rows={4}
              />
            </L>
            <L label="Model override">
              <FormInput
                value={form.model}
                onChange={(event) => handleChange("model", event.target.value)}
                disabled={saving}
              />
            </L>
            <L label="Max thinking steps">
              <FormInput
                type="number"
                min={1}
                value={form.max_thinking_steps}
                onChange={(event) => handleChange("max_thinking_steps", event.target.value)}
                disabled={saving}
              />
            </L>
            <L label="Default timeout (sec)">
              <FormInput
                type="number"
                min={1}
                value={form.default_timeout_seconds}
                onChange={(event) => handleChange("default_timeout_seconds", event.target.value)}
                disabled={saving}
              />
            </L>
            <label className="flex items-center gap-3 rounded-[18px] border border-white/8 bg-[var(--surface-panel-strong)] px-3 py-3 text-sm text-[var(--text-strong)]">
              <input
                type="checkbox"
                checked={form.can_spawn_children}
                onChange={(event) => handleChange("can_spawn_children", event.target.checked)}
                disabled={saving}
                className="h-4 w-4 rounded border-white/10 bg-[var(--surface-panel)] text-cyan-400 focus:ring-[var(--ring)]"
              />
              Allow this worker to spawn child workers
            </label>
            <L label="Allowed child templates" className="lg:col-span-2">
              <FormInput
                value={form.allowed_child_templates}
                onChange={(event) => handleChange("allowed_child_templates", event.target.value)}
                disabled={saving}
              />
            </L>
          </div>

          {!isCreating && selectedTemplate ? (
            <div className="mt-6 flex flex-wrap gap-4 text-xs text-[var(--text-dim)]">
              <span>Created {formatLocalDateTime(selectedTemplate.created_at)}</span>
              <span>Updated {formatLocalDateTime(selectedTemplate.updated_at)}</span>
            </div>
          ) : null}
        </section>
      </div>
    </section>
  );
}
