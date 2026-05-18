import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { CodexAuthPanel } from "../CodexAuthPanel";
import { LlmForm } from "../LlmForm";
import { ProviderPicker } from "../ProviderPicker";
import { StepSection } from "../StepSection";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, providerRequiresApiBase, providerRequiresApiKey, type InstallForm } from "../../lib/install";

export function LlmStep({
  copy,
  values,
  form,
  errors,
  onProviderChange,
  codexAuthStatus,
  codexAuthBusy,
  onCodexAuthorize,
  onCodexRefresh,
  onCodexDisconnect,
}: {
  copy: CopyFn;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
  onProviderChange: (providerId: string) => void;
  codexAuthStatus: DesktopCodexAuthStatus | null;
  codexAuthBusy: boolean;
  onCodexAuthorize: () => void;
  onCodexRefresh: () => void;
  onCodexDisconnect: () => void;
}) {
  const showApiKey = providerRequiresApiKey(values.providerId);
  const showApiBase = values.providerId !== "codex";

  return (
    <StepSection body={copy("llmBody")}>
      <ProviderPicker selected={values.providerId} onSelect={onProviderChange} />
      <label className="worker-checkbox-row">
        <input type="checkbox" {...form.register("sameWorker")} />
        <span>{copy("sameWorker")}</span>
      </label>
      {values.providerId === "codex" ? (
        <CodexAuthPanel
          copy={copy}
          status={codexAuthStatus}
          busy={codexAuthBusy}
          onAuthorize={onCodexAuthorize}
          onRefresh={onCodexRefresh}
          onDisconnect={onCodexDisconnect}
        />
      ) : null}
      <LlmForm
        modelLabel={copy("model")}
        apiKeyLabel={copy("apiKey")}
        apiBaseLabel={copy("apiBase")}
        apiKeyHint={isExistingSecret(values.apiKey) ? copy("configured") : showApiKey ? copy("required") : copy("optional")}
        apiBaseHint={providerRequiresApiBase(values.providerId) ? copy("required") : copy("optional")}
        showApiKey={showApiKey}
        showApiBase={showApiBase}
        modelInvalid={!!errors.model}
        apiKeyInvalid={!!errors.apiKey}
        apiBaseInvalid={!!errors.apiBase}
        modelRegistration={form.register("model")}
        apiKeyRegistration={form.register("apiKey")}
        apiBaseRegistration={form.register("apiBase")}
      />
    </StepSection>
  );
}
