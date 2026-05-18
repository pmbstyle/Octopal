import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { CodexAuthPanel } from "../CodexAuthPanel";
import { LlmForm } from "../LlmForm";
import { ProviderPicker } from "../ProviderPicker";
import { StepSection } from "../StepSection";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, providerRequiresApiBase, providerRequiresApiKey, type InstallForm } from "../../lib/install";

export function WorkerLlmStep({
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
  const workerProviderId = values.workerProviderId || values.providerId;
  const showApiKey = providerRequiresApiKey(workerProviderId);
  const showApiBase = workerProviderId !== "codex";

  return (
    <StepSection body={copy("workerLlmBody")}>
      <ProviderPicker selected={workerProviderId} onSelect={onProviderChange} />
      {workerProviderId === "codex" ? (
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
        apiKeyHint={isExistingSecret(values.workerApiKey) ? copy("configured") : showApiKey ? copy("required") : copy("optional")}
        apiBaseHint={providerRequiresApiBase(workerProviderId) ? copy("required") : copy("optional")}
        showApiKey={showApiKey}
        showApiBase={showApiBase}
        modelInvalid={!!errors.workerModel}
        apiKeyInvalid={!!errors.workerApiKey}
        apiBaseInvalid={!!errors.workerApiBase}
        modelRegistration={form.register("workerModel")}
        apiKeyRegistration={form.register("workerApiKey")}
        apiBaseRegistration={form.register("workerApiBase")}
      />
    </StepSection>
  );
}
