import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { LlmForm } from "../LlmForm";
import { ProviderPicker } from "../ProviderPicker";
import { StepSection } from "../StepSection";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, type InstallForm } from "../../lib/install";

export function WorkerLlmStep({
  copy,
  values,
  form,
  errors,
  onProviderChange,
}: {
  copy: CopyFn;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
  onProviderChange: (providerId: string) => void;
}) {
  return (
    <StepSection body={copy("workerLlmBody")}>
      <ProviderPicker selected={values.workerProviderId || values.providerId} onSelect={onProviderChange} />
      <LlmForm
        modelLabel={copy("model")}
        apiKeyLabel={copy("apiKey")}
        apiBaseLabel={copy("apiBase")}
        apiKeyHint={isExistingSecret(values.workerApiKey) ? copy("configured") : values.workerProviderId === "custom" ? copy("optional") : copy("required")}
        apiBaseHint={values.workerProviderId === "custom" ? copy("required") : copy("optional")}
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
