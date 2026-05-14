import { Eye, EyeOff, KeyRound, LayoutDashboard } from "lucide-react";
import { useState } from "react";
import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { Button } from "../Button";
import { Field, Input } from "../Field";
import { StepSection } from "../StepSection";
import { ToggleCard } from "../ToggleCard";
import { isExistingSecret, type InstallForm } from "../../lib/install";
import type { CopyFn } from "../../lib/appTypes";
import { generateDashboardToken } from "../../lib/security";

export function DashboardStep({
  copy,
  values,
  form,
  errors,
}: {
  copy: CopyFn;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
}) {
  const [tokenVisible, setTokenVisible] = useState(false);
  const tokenIsConfiguredSecret = isExistingSecret(values.dashboardToken);
  const updateEnabled = (enabled: boolean) => {
    form.setValue("dashboardEnabled", enabled, { shouldDirty: true, shouldValidate: true });
    if (enabled && !form.getValues("dashboardToken")?.trim()) {
      form.setValue("dashboardToken", generateDashboardToken(), { shouldDirty: true, shouldValidate: true });
    }
  };

  return (
    <StepSection body={copy("dashboardBody")}>
      <div className="choice-grid dashboard-grid">
        <ToggleCard
          active={values.dashboardEnabled}
          icon={<LayoutDashboard />}
          title={copy("dashboardEnabled")}
          body={copy("dashboardEnabledBody")}
          onClick={() => updateEnabled(true)}
        />
        <ToggleCard
          active={!values.dashboardEnabled}
          icon={<LayoutDashboard />}
          title={copy("dashboardDisabled")}
          body={copy("dashboardDisabledBody")}
          onClick={() => updateEnabled(false)}
        />
      </div>
      {values.dashboardEnabled ? (
        <div className="dashboard-form reveal-form">
          <Field label={copy("dashboardPort")} hint="1-65535" invalid={!!errors.dashboardPort}>
            <Input
              {...form.register("dashboardPort", { valueAsNumber: true })}
              inputMode="numeric"
              min={1}
              max={65535}
              type="number"
            />
          </Field>
          <Field
            label={copy("dashboardToken")}
            hint={isExistingSecret(values.dashboardToken) ? copy("configured") : copy("recommended")}
            help={{
              title: copy("dashboardTokenHelpTitle"),
              body: [copy("dashboardTokenHelpBody1"), copy("dashboardTokenHelpBody2"), copy("dashboardTokenHelpBody3")],
              closeLabel: copy("closeHelp"),
            }}
          >
            <div className="input-action-row">
              <Input {...form.register("dashboardToken")} type={tokenVisible && !tokenIsConfiguredSecret ? "text" : "password"} />
              <Button
                type="button"
                variant="secondary"
                disabled={!values.dashboardToken || tokenIsConfiguredSecret}
                onClick={() => setTokenVisible((current) => !current)}
              >
                {tokenVisible ? <EyeOff data-icon="inline-start" /> : <Eye data-icon="inline-start" />}
                {tokenVisible ? copy("hideToken") : copy("showToken")}
              </Button>
              <Button
                type="button"
                variant="secondary"
                onClick={() => {
                  form.setValue("dashboardToken", generateDashboardToken(), { shouldDirty: true, shouldValidate: true });
                  setTokenVisible(true);
                }}
              >
                <KeyRound data-icon="inline-start" />
                {copy("generateToken")}
              </Button>
            </div>
          </Field>
        </div>
      ) : null}
    </StepSection>
  );
}
