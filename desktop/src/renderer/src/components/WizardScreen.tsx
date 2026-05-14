import { ArrowLeft, ArrowRight, Check } from "lucide-react";
import { motion } from "framer-motion";
import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { Button } from "./Button";
import { SetupHeader } from "./SetupHeader";
import { LocationStep } from "./steps/LocationStep";
import { ChannelStep } from "./steps/ChannelStep";
import { LlmStep } from "./steps/LlmStep";
import { WorkerLlmStep } from "./steps/WorkerLlmStep";
import { SearchStep } from "./steps/SearchStep";
import { ConnectorsStep } from "./steps/ConnectorsStep";
import { DashboardStep } from "./steps/DashboardStep";
import { ReviewStep } from "./steps/ReviewStep";
import type { CopyFn, StepId, Theme } from "../lib/appTypes";
import type { Language } from "../lib/i18n";
import type { InstallForm } from "../lib/install";
import { stepLabels, stepSpeech } from "../lib/wizard";

export function WizardScreen({
  copy,
  language,
  theme,
  step,
  stepIndex,
  totalSteps,
  values,
  form,
  errors,
  onLanguageChange,
  onThemeChange,
  onChooseInstallDir,
  onProviderChange,
  onSearchProviderToggle,
  onConnectorToggle,
  onConnectorServiceToggle,
  onAuthorizeConnector,
  onBack,
  onNext,
  onPrepareInstall,
  onRefreshPrerequisites,
  reviewBody,
  reviewActionLabel,
  preflightChecks,
  preflightStatus,
  preflightError,
  preflightHasBlockingIssue,
  connectorStatus,
  connectorBusy,
  connectorMessage,
  connectorMessageTone,
  selectedConnector,
  canAuthorizeConnectors,
}: {
  copy: CopyFn;
  language: Language;
  theme: Theme;
  step: StepId;
  stepIndex: number;
  totalSteps: number;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
  onLanguageChange: (language: Language) => void;
  onThemeChange: (theme: Theme) => void;
  onChooseInstallDir: () => void;
  onProviderChange: (providerId: string, target: "octo" | "worker") => void;
  onSearchProviderToggle: (providerId: "brave" | "firecrawl") => void;
  onConnectorToggle: (name: DesktopConnectorName) => void;
  onConnectorServiceToggle: (name: DesktopConnectorName, serviceId: string) => void;
  onAuthorizeConnector: (name: DesktopConnectorName) => void;
  onBack: () => void;
  onNext: () => void;
  onPrepareInstall: () => void;
  onRefreshPrerequisites: () => void;
  reviewBody: string;
  reviewActionLabel: string;
  preflightChecks: DesktopPrerequisiteCheck[];
  preflightStatus: "idle" | "checking" | "ready" | "failed";
  preflightError: string;
  preflightHasBlockingIssue: boolean;
  connectorStatus: DesktopConnectorStatusResult | null;
  connectorBusy: DesktopConnectorName | null;
  connectorMessage: string;
  connectorMessageTone: "success" | "error" | "info";
  selectedConnector: DesktopConnectorName;
  canAuthorizeConnectors: boolean;
}) {
  return (
    <motion.section
      key={step}
      className={`setup-screen setup-screen-${step}`}
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.24 }}
    >
      <SetupHeader
        speech={copy(stepSpeech[step])}
        language={language}
        theme={theme}
        onLanguageChange={onLanguageChange}
        onThemeChange={onThemeChange}
        stepLabel={copy(stepLabels[step])}
        stepIndex={stepIndex}
        totalSteps={totalSteps}
      />

      <section className="setup-content">
        {step === "location" ? <LocationStep copy={copy} form={form} errors={errors} onChooseInstallDir={onChooseInstallDir} /> : null}
        {step === "channel" ? <ChannelStep copy={copy} values={values} form={form} errors={errors} /> : null}
        {step === "octo-llm" ? (
          <LlmStep
            copy={copy}
            values={values}
            form={form}
            errors={errors}
            onProviderChange={(providerId) => onProviderChange(providerId, "octo")}
          />
        ) : null}
        {step === "worker-llm" ? (
          <WorkerLlmStep
            copy={copy}
            values={values}
            form={form}
            errors={errors}
            onProviderChange={(providerId) => onProviderChange(providerId, "worker")}
          />
        ) : null}
        {step === "search" ? (
          <SearchStep copy={copy} values={values} form={form} errors={errors} onSearchProviderToggle={onSearchProviderToggle} />
        ) : null}
        {step === "connectors" ? (
          <ConnectorsStep
            copy={copy}
            values={values}
            form={form}
            errors={errors}
            connectorStatus={connectorStatus}
            connectorBusy={connectorBusy}
            connectorMessage={connectorMessage}
            connectorMessageTone={connectorMessageTone}
            selectedConnector={selectedConnector}
            canAuthorizeConnectors={canAuthorizeConnectors}
            onConnectorToggle={onConnectorToggle}
            onConnectorServiceToggle={onConnectorServiceToggle}
            onAuthorizeConnector={onAuthorizeConnector}
          />
        ) : null}
        {step === "dashboard" ? <DashboardStep copy={copy} values={values} form={form} errors={errors} /> : null}
        {step === "review" ? (
          <ReviewStep
            body={reviewBody}
            copy={copy}
            values={values}
            preflightChecks={preflightChecks}
            preflightStatus={preflightStatus}
            preflightError={preflightError}
            onRefreshPrerequisites={onRefreshPrerequisites}
          />
        ) : null}
      </section>

      <footer className="setup-footer">
        <Button type="button" variant="ghost" onClick={onBack}>
          <ArrowLeft data-icon="inline-start" />
          {copy("back")}
        </Button>
        {step !== "review" ? (
          <Button type="button" onClick={onNext}>
            {(step === "search" && !values.searchProvider) ||
            (step === "connectors" && !values.googleConnectorEnabled && !values.githubConnectorEnabled)
              ? copy("skip")
              : copy("next")}
            <ArrowRight data-icon="inline-end" />
          </Button>
        ) : (
          <Button type="button" onClick={onPrepareInstall} disabled={preflightStatus === "checking" || preflightHasBlockingIssue}>
            <Check data-icon="inline-start" />
            {reviewActionLabel}
          </Button>
        )}
      </footer>
    </motion.section>
  );
}
