import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { LlmForm } from "../LlmForm";
import { ProviderPicker } from "../ProviderPicker";
import { StepSection } from "../StepSection";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, type InstallForm } from "../../lib/install";

export function LlmStep({
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
    <StepSection body={copy("llmBody")}>
      <ProviderPicker selected={values.providerId} onSelect={onProviderChange} />
      <label className="worker-checkbox-row">
        <input type="checkbox" {...form.register("sameWorker")} />
        <span>{copy("sameWorker")}</span>
      </label>
      <LlmForm
        modelLabel={copy("model")}
        apiKeyLabel={copy("apiKey")}
        apiBaseLabel={copy("apiBase")}
        apiKeyHint={isExistingSecret(values.apiKey) ? copy("configured") : values.providerId === "custom" ? copy("optional") : copy("required")}
        apiBaseHint={values.providerId === "custom" ? copy("required") : copy("optional")}
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
