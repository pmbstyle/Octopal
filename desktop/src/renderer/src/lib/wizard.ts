import { providerRequiresApiBase, providerRequiresApiKey, type InstallForm } from "./install";
import { messages } from "./i18n";
import type { StepId } from "./appTypes";

export const stepLabels: Record<StepId, keyof typeof messages.en> = {
  location: "stepLocation",
  channel: "stepChannel",
  "octo-llm": "stepLlm",
  "worker-llm": "stepWorkerLlm",
  search: "stepTools",
  connectors: "stepConnectors",
  dashboard: "stepDashboard",
  review: "stepReview",
};

export const stepSpeech: Record<StepId, keyof typeof messages.en> = {
  location: "speechInstall",
  channel: "speechChannel",
  "octo-llm": "speechLlm",
  "worker-llm": "speechWorkerLlm",
  search: "speechSearch",
  connectors: "speechConnectors",
  dashboard: "speechDashboard",
  review: "speechReview",
};

export function getWizardSteps(useSameWorkerModel: boolean): StepId[] {
  return useSameWorkerModel
    ? ["location", "channel", "octo-llm", "search", "connectors", "dashboard", "review"]
    : ["location", "channel", "octo-llm", "worker-llm", "search", "connectors", "dashboard", "review"];
}

export function getValidationFields(step: StepId, values: InstallForm): Array<keyof InstallForm> {
  if (step === "location") {
    return ["installDir"];
  }

  if (step === "channel") {
    if (values.channel === "desktop") {
      return ["channel"];
    }
    return values.channel === "telegram" ? ["channel", "telegramToken"] : ["channel", "whatsappAllowedNumbers"];
  }

  if (step === "octo-llm") {
    if (providerRequiresApiBase(values.providerId)) {
      return ["providerId", "model", "apiBase"];
    }
    if (providerRequiresApiKey(values.providerId)) {
      return ["providerId", "model", "apiKey"];
    }
    return ["providerId", "model"];
  }

  if (step === "worker-llm") {
    if (providerRequiresApiBase(values.workerProviderId)) {
      return ["workerProviderId", "workerModel", "workerApiBase"];
    }
    if (providerRequiresApiKey(values.workerProviderId)) {
      return ["workerProviderId", "workerModel", "workerApiKey"];
    }
    return ["workerProviderId", "workerModel"];
  }

  if (step === "search") {
    if (values.searchProvider === "brave") {
      return ["searchProvider", "braveApiKey"];
    }
    if (values.searchProvider === "firecrawl") {
      return ["searchProvider", "firecrawlApiKey"];
    }
  }

  if (step === "connectors") {
    const fields: Array<keyof InstallForm> = [];
    if (values.googleConnectorEnabled) {
      fields.push("googleConnectorEnabled", "googleConnectorServices", "googleClientId", "googleClientSecret");
    }
    if (values.githubConnectorEnabled) {
      fields.push("githubConnectorEnabled", "githubConnectorServices", "githubToken");
    }
    return fields;
  }

  if (step === "dashboard") {
    return ["dashboardPort"];
  }

  return [];
}
