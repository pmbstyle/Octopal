import { AlertTriangle, CheckCircle2, ChevronDown, RefreshCw } from "lucide-react";
import { motion } from "framer-motion";
import { useState } from "react";

import { providers, searchProviders, type InstallForm } from "../../lib/install";
import type { CopyFn } from "../../lib/appTypes";
import { Button } from "../Button";
import { ReviewItem } from "../ReviewItem";
import { StepSection } from "../StepSection";

function preflightHint(copy: CopyFn, check: DesktopPrerequisiteCheck) {
  if (check.ok) {
    return copy("available");
  }

  return check.required ? copy("required") : copy("recommended");
}

function preflightSummary(copy: CopyFn, status: "idle" | "checking" | "ready" | "failed", checks: DesktopPrerequisiteCheck[], error: string) {
  if (status === "checking") {
    return copy("checking");
  }

  if (error) {
    return copy("preflightFailed");
  }

  const availableCount = checks.filter((check) => check.ok).length;
  const missingRequiredCount = checks.filter((check) => check.required && !check.ok).length;
  const recommendedCount = checks.filter((check) => !check.required && !check.ok).length;

  return (
    [
      availableCount ? `${availableCount} ${copy("available")}` : "",
      missingRequiredCount ? `${missingRequiredCount} ${copy("missing")}` : "",
      recommendedCount ? `${recommendedCount} ${copy("recommended")}` : "",
    ]
      .filter(Boolean)
      .join(" · ") || copy("preflightReady")
  );
}

function ReviewRequirements({
  copy,
  checks,
  status,
  error,
  onRefresh,
}: {
  copy: CopyFn;
  checks: DesktopPrerequisiteCheck[];
  status: "idle" | "checking" | "ready" | "failed";
  error: string;
  onRefresh: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const hasBlockingIssue = checks.some((check) => check.required && !check.ok);

  return (
    <section className={expanded ? "requirements-card requirements-card-expanded" : "requirements-card"} aria-live="polite">
      <div className="requirements-head">
        <div>
          <strong>{copy("requirements")}</strong>
          <small>
            {expanded
              ? status === "checking"
                ? copy("checking")
                : hasBlockingIssue
                  ? copy("installBlocked")
                  : copy("preflightReady")
              : preflightSummary(copy, status, checks, error)}
          </small>
        </div>
        <div className="requirements-actions">
          <Button
            type="button"
            variant="ghost"
            className="requirements-toggle"
            aria-expanded={expanded}
            onClick={() => setExpanded((current) => !current)}
          >
            <ChevronDown data-icon="inline-start" />
            {expanded ? copy("hideDetails") : copy("showDetails")}
          </Button>
          <Button type="button" variant="ghost" onClick={onRefresh} disabled={status === "checking"}>
            <RefreshCw data-icon="inline-start" />
            {copy("refresh")}
          </Button>
        </div>
      </div>
      {expanded && error ? (
        <div className="review-error" role="alert">{error}</div>
      ) : null}
      {expanded && checks.length > 0 ? (
        <motion.div className="requirements-grid" initial={{ opacity: 0, y: -6 }} animate={{ opacity: 1, y: 0 }}>
          {checks.map((check) => (
            <div className={check.ok ? "requirement requirement-ok" : "requirement requirement-missing"} key={check.id}>
              <div className="requirement-title">
                {check.ok ? <CheckCircle2 /> : <AlertTriangle />}
                <strong>{check.label}</strong>
              </div>
              <small>{preflightHint(copy, check)}</small>
              <p title={check.detail}>{check.detail}</p>
            </div>
          ))}
        </motion.div>
      ) : null}
    </section>
  );
}

export function ReviewStep({
  body,
  copy,
  values,
  preflightChecks,
  preflightStatus,
  preflightError,
  onRefreshPrerequisites,
}: {
  body: string;
  copy: CopyFn;
  values: InstallForm;
  preflightChecks: DesktopPrerequisiteCheck[];
  preflightStatus: "idle" | "checking" | "ready" | "failed";
  preflightError: string;
  onRefreshPrerequisites: () => void;
}) {
  const enabledConnectors = [
    values.googleConnectorEnabled ? copy("googleConnector") : "",
    values.githubConnectorEnabled ? copy("githubConnector") : "",
  ].filter(Boolean);

  return (
    <StepSection body={body}>
      <div className="review-grid">
        <ReviewItem label={copy("installFolder")} value={values.installDir || "-"} />
        <ReviewItem label={copy("stepChannel")} value={values.channel === "telegram" ? copy("telegram") : copy("whatsapp")} />
        <ReviewItem label={copy("provider")} value={providers.find((item) => item.id === values.providerId)?.label ?? values.providerId} />
        <ReviewItem label={copy("model")} value={values.model || "-"} />
        <ReviewItem label={copy("stepWorkerLlm")} value={values.sameWorker ? copy("sameWorker") : values.workerModel || values.model || "-"} />
        <ReviewItem
          label={copy("stepTools")}
          value={
            !values.searchProvider
              ? copy("noSearch")
              : searchProviders.find((item) => item.id === values.searchProvider)?.label ?? values.searchProvider
          }
        />
        <ReviewItem
          label={copy("stepConnectors")}
          value={enabledConnectors.length > 0 ? enabledConnectors.join(", ") : copy("connectorsSkipped")}
        />
        <ReviewItem
          label={copy("stepDashboard")}
          value={values.dashboardEnabled ? `${copy("dashboardEnabled")} · ${values.dashboardPort}` : copy("dashboardDisabled")}
        />
      </div>
      <ReviewRequirements
        copy={copy}
        checks={preflightChecks}
        status={preflightStatus}
        error={preflightError}
        onRefresh={onRefreshPrerequisites}
      />
    </StepSection>
  );
}
