import { zodResolver } from "@hookform/resolvers/zod";
import { AnimatePresence } from "framer-motion";
import { Download, Play, Square } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useForm } from "react-hook-form";

import { AppShell } from "./components/AppShell";
import { Button } from "./components/Button";
import { DashboardScreen } from "./components/DashboardScreen";
import { InstallProgressScreen } from "./components/InstallProgressScreen";
import { StatusScreen } from "./components/StatusScreen";
import { WelcomeScreen } from "./components/WelcomeScreen";
import { WhatsAppLinkScreen } from "./components/WhatsAppLinkScreen";
import { WizardScreen } from "./components/WizardScreen";
import type { Screen, StepId, Theme } from "./lib/appTypes";
import {
  buildOctopalConfig,
  defaultInstallValues,
  formValuesFromOctopalConfig,
  installSchema,
  isExistingSecret,
  providers,
  type InstallForm,
} from "./lib/install";
import { messages, t, type Language } from "./lib/i18n";
import { generateDashboardToken } from "./lib/security";
import { getPreferredTheme, loadSettings, saveSettings } from "./lib/settings";
import { getValidationFields, getWizardSteps } from "./lib/wizard";

export function App() {
  const [language, setLanguage] = useState<Language>("en");
  const [theme, setTheme] = useState<Theme>("system");
  const [screen, setScreen] = useState<Screen>("welcome");
  const [stepIndex, setStepIndex] = useState(0);
  const [savedPlanPath, setSavedPlanPath] = useState("");
  const [savedInstallResult, setSavedInstallResult] = useState<DesktopInstallResult | null>(null);
  const [installEvents, setInstallEvents] = useState<DesktopInstallEvent[]>([]);
  const [installError, setInstallError] = useState("");
  const [startStatus, setStartStatus] = useState<"idle" | "starting" | "started" | "stopping" | "failed">("idle");
  const [startError, setStartError] = useState("");
  const [startErrorDetail, setStartErrorDetail] = useState("");
  const [runtimeStatus, setRuntimeStatus] = useState<DesktopRuntimeStatus | null>(null);
  const [updateStatus, setUpdateStatus] = useState<DesktopUpdateStatus | null>(null);
  const [updateBusy, setUpdateBusy] = useState(false);
  const [updateMessage, setUpdateMessage] = useState("");
  const [updateError, setUpdateError] = useState("");
  const [desktopUpdateStatus, setDesktopUpdateStatus] = useState<DesktopAppUpdateStatus | null>(null);
  const [desktopUpdateBusy, setDesktopUpdateBusy] = useState(false);
  const [desktopUpdateError, setDesktopUpdateError] = useState("");
  const [preflightChecks, setPreflightChecks] = useState<DesktopPrerequisiteCheck[]>([]);
  const [preflightStatus, setPreflightStatus] = useState<"idle" | "checking" | "ready" | "failed">("idle");
  const [preflightError, setPreflightError] = useState("");
  const [whatsappLinkStatus, setWhatsappLinkStatus] = useState<DesktopWhatsAppLinkStatus | null>(null);
  const [whatsappLinkBusy, setWhatsappLinkBusy] = useState(false);
  const [whatsappLinkError, setWhatsappLinkError] = useState("");
  const [whatsappLinkStarted, setWhatsappLinkStarted] = useState(false);
  const [connectorStatus, setConnectorStatus] = useState<DesktopConnectorStatusResult | null>(null);
  const [connectorBusy, setConnectorBusy] = useState<DesktopConnectorName | null>(null);
  const [connectorMessage, setConnectorMessage] = useState("");
  const [connectorMessageTone, setConnectorMessageTone] = useState<"success" | "error" | "info">("info");
  const [selectedConnector, setSelectedConnector] = useState<DesktopConnectorName>("google");
  const [configurationMode, setConfigurationMode] = useState<"install" | "edit">("install");
  const [loadedConfigChannel, setLoadedConfigChannel] = useState<InstallForm["channel"] | null>(null);
  const [installState, setInstallState] = useState<DesktopInstallState>({
    installed: false,
    installDir: "",
    configPath: "",
    planPath: "",
  });
  const [settingsLoaded, setSettingsLoaded] = useState(false);

  const form = useForm<InstallForm>({
    resolver: zodResolver(installSchema),
    defaultValues: defaultInstallValues,
    mode: "onChange",
  });

  const values = form.watch();
  const steps = useMemo(() => getWizardSteps(values.sameWorker), [values.sameWorker]);
  const step = steps[Math.min(stepIndex, steps.length - 1)] ?? "location";
  const copy = useMemo(() => (key: keyof typeof messages.en) => t(language, key), [language]);
  const runtimeInstallDir = savedInstallResult?.installDir || installState.installDir || values.installDir;
  const preflightHasBlockingIssue = useMemo(() => preflightChecks.some((check) => check.required && !check.ok), [preflightChecks]);
  const updateAvailable = Boolean(updateStatus?.updateAvailable);
  const updateBlocked = updateAvailable && updateStatus?.canUpdate === false;
  const updateSummary = updateStatus
    ? `v${updateStatus.localVersion ?? "unknown"} -> v${updateStatus.latestVersion ?? "latest"}`
    : "";
  const desktopUpdateAvailable =
    desktopUpdateStatus?.status === "available" ||
    desktopUpdateStatus?.status === "downloading" ||
    desktopUpdateStatus?.status === "downloaded";
  const desktopUpdateReady = desktopUpdateStatus?.status === "downloaded";
  const desktopUpdateSummary = desktopUpdateStatus
    ? `v${desktopUpdateStatus.currentVersion} -> v${desktopUpdateStatus.latestVersion ?? "latest"}${
        desktopUpdateStatus.status === "downloading" && typeof desktopUpdateStatus.percent === "number"
          ? ` (${Math.round(desktopUpdateStatus.percent)}%)`
          : ""
      }`
    : "";
  const runtimeView = useMemo(() => {
    if (startStatus === "starting") {
      return {
        state: "starting" as const,
        title: copy("octopalStarting"),
        detail: runtimeStatus?.detail || copy("octopalStartingDetail"),
      };
    }

    if (startStatus === "stopping") {
      return {
        state: "stopping" as const,
        title: copy("octopalStopping"),
        detail: runtimeStatus?.detail || copy("octopalStoppingDetail"),
      };
    }

    if (startStatus === "failed") {
      return {
        state: "error" as const,
        title: startError || runtimeStatus?.title || copy("runtimeStatusError"),
        detail: startErrorDetail || runtimeStatus?.detail || "",
      };
    }

    if (runtimeStatus) {
      return {
        state: runtimeStatus.state,
        title: runtimeStatus.title,
        detail: runtimeStatus.detail,
      };
    }

    if (installState.installed) {
      return {
        state: "checking" as const,
        title: copy("octopalStatusChecking"),
        detail: "",
      };
    }

    return {
      state: "stopped" as const,
      title: copy("octopalStopped"),
      detail: copy("octopalStoppedDetail"),
    };
  }, [copy, installState.installed, runtimeStatus, startError, startErrorDetail, startStatus]);

  const refreshPrerequisites = useCallback(async () => {
    if (!window.octopalDesktop) {
      setPreflightChecks([]);
      setPreflightStatus("ready");
      setPreflightError("");
      return;
    }

    setPreflightStatus("checking");
    setPreflightError("");
    try {
      const result = await window.octopalDesktop.checkPrerequisites();
      setPreflightChecks(result);
      setPreflightStatus("ready");
    } catch (error) {
      setPreflightStatus("failed");
      setPreflightError(error instanceof Error ? error.message : copy("preflightFailed"));
    }
  }, [copy]);

  const refreshRuntimeStatus = useCallback(async () => {
    if (!window.octopalDesktop || !installState.installed || !runtimeInstallDir) {
      return null;
    }

    const result = await window.octopalDesktop.getOctopalStatus(runtimeInstallDir);
    setRuntimeStatus(result);
    setStartStatus((current) => {
      if (!result.ok || result.state === "error") {
        return "failed";
      }

      if (result.state === "running") {
        return "started";
      }

      if (result.state === "stopped") {
        return current === "starting" ? current : "idle";
      }

      return current;
    });

    if (!result.ok || result.state === "error") {
      setStartError(result.title);
      setStartErrorDetail(result.detail);
      return;
    }

    setStartError("");
    setStartErrorDetail("");
    return result;
  }, [installState.installed, runtimeInstallDir]);

  const refreshUpdateStatus = useCallback(async () => {
    if (!window.octopalDesktop || !installState.installed || !runtimeInstallDir) {
      setUpdateStatus(null);
      return null;
    }

    const result = await window.octopalDesktop.checkOctopalUpdate(runtimeInstallDir);
    setUpdateStatus(result);
    return result;
  }, [installState.installed, runtimeInstallDir]);

  const startWhatsappLinkFlow = useCallback(async () => {
    if (!window.octopalDesktop || !runtimeInstallDir) {
      return;
    }

    setWhatsappLinkBusy(true);
    setWhatsappLinkError("");
    try {
      const result = await window.octopalDesktop.startWhatsAppLink(runtimeInstallDir);
      setWhatsappLinkStatus(result);
      setWhatsappLinkError("");
    } catch (error) {
      setWhatsappLinkError(error instanceof Error ? error.message : copy("whatsappLinkFailed"));
    } finally {
      setWhatsappLinkStarted(true);
      setWhatsappLinkBusy(false);
    }
  }, [copy, runtimeInstallDir]);

  const refreshWhatsappLinkStatus = useCallback(
    async (showBusy = false) => {
      if (!window.octopalDesktop || !runtimeInstallDir) {
        return;
      }

      if (showBusy) {
        setWhatsappLinkBusy(true);
      }
      try {
        const result = await window.octopalDesktop.getWhatsAppLinkStatus(runtimeInstallDir);
        setWhatsappLinkStatus(result);
        setWhatsappLinkError("");
      } catch (error) {
        setWhatsappLinkError(error instanceof Error ? error.message : copy("whatsappLinkFailed"));
      } finally {
        if (showBusy) {
          setWhatsappLinkBusy(false);
        }
      }
    },
    [copy, runtimeInstallDir],
  );

  const stopWhatsappLinkFlow = useCallback(async () => {
    if (!window.octopalDesktop || !runtimeInstallDir) {
      return;
    }

    try {
      await window.octopalDesktop.stopWhatsAppLink(runtimeInstallDir);
    } catch (error) {
      console.error("Unable to stop WhatsApp link bridge", error);
    }
  }, [runtimeInstallDir]);

  const refreshConnectorStatus = useCallback(async () => {
    if (!window.octopalDesktop || !runtimeInstallDir || !installState.installed) {
      setConnectorStatus(null);
      return;
    }

    const result = await window.octopalDesktop.getConnectorStatus(runtimeInstallDir);
    setConnectorStatus(result);
  }, [installState.installed, runtimeInstallDir]);

  useEffect(() => {
    void loadSettings().then(async (settings) => {
      setLanguage(settings.language);
      setTheme(settings.theme);
      if (settings.installDir) {
        form.setValue("installDir", settings.installDir, { shouldValidate: true });
      }
      if (window.octopalDesktop) {
        setInstallState(await window.octopalDesktop.getInstallState());
      }
      setSettingsLoaded(true);
    });
  }, [form]);

  useEffect(() => {
    document.documentElement.dataset.theme = getPreferredTheme(theme);
  }, [theme]);

  useEffect(() => {
    if (theme !== "system") {
      return;
    }

    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const updateSystemTheme = () => {
      document.documentElement.dataset.theme = media.matches ? "dark" : "light";
    };

    media.addEventListener("change", updateSystemTheme);
    return () => media.removeEventListener("change", updateSystemTheme);
  }, [theme]);

  useEffect(() => {
    if (!settingsLoaded) {
      return;
    }

    void saveSettings({ language, theme, installDir: values.installDir || "" });
  }, [language, settingsLoaded, theme, values.installDir]);

  useEffect(() => {
    if (!settingsLoaded || !window.octopalDesktop) {
      return;
    }

    const updateFromMain = (next: DesktopAppUpdateStatus) => {
      setDesktopUpdateStatus(next);
      setDesktopUpdateError(next.ok ? "" : next.error || next.detail);
      setDesktopUpdateBusy(next.status === "checking" || next.status === "downloading" || next.status === "installing");
    };
    const unsubscribe = window.octopalDesktop.onAppUpdateStatus(updateFromMain);
    void window.octopalDesktop.getAppUpdateStatus().then(updateFromMain);
    void window.octopalDesktop.checkAppUpdate().then(updateFromMain).catch((error) => {
      setDesktopUpdateError(error instanceof Error ? error.message : copy("desktopUpdateFailed"));
    });
    const interval = window.setInterval(() => {
      void window.octopalDesktop?.checkAppUpdate().then(updateFromMain).catch((error) => {
        setDesktopUpdateError(error instanceof Error ? error.message : copy("desktopUpdateFailed"));
      });
    }, 60 * 60 * 1000);

    return () => {
      unsubscribe();
      window.clearInterval(interval);
    };
  }, [copy, settingsLoaded]);

  useEffect(() => {
    if (!settingsLoaded || !installState.installed || screen !== "done") {
      return;
    }

    void refreshRuntimeStatus();
    void refreshUpdateStatus();
    const interval = window.setInterval(() => {
      void refreshRuntimeStatus();
    }, 5000);
    const updateInterval = window.setInterval(() => {
      void refreshUpdateStatus();
    }, 15 * 60 * 1000);

    return () => {
      window.clearInterval(interval);
      window.clearInterval(updateInterval);
    };
  }, [installState.installed, refreshRuntimeStatus, refreshUpdateStatus, screen, settingsLoaded]);

  useEffect(() => {
    if (!settingsLoaded || !installState.installed || screen !== "welcome") {
      return;
    }

    void refreshUpdateStatus();
  }, [installState.installed, refreshUpdateStatus, screen, settingsLoaded]);

  useEffect(() => {
    if (!settingsLoaded || screen !== "wizard" || step !== "connectors") {
      return;
    }

    void refreshConnectorStatus();
  }, [refreshConnectorStatus, screen, settingsLoaded, step]);

  useEffect(() => {
    if (!settingsLoaded || screen !== "wizard" || step !== "review") {
      return;
    }

    void refreshPrerequisites();
  }, [refreshPrerequisites, screen, settingsLoaded, step]);

  useEffect(() => {
    if (!settingsLoaded || !installState.installed || screen !== "done" || startStatus !== "idle") {
      return;
    }

    if (runtimeStatus?.state === "stopped") {
      setScreen("welcome");
    }
  }, [installState.installed, runtimeStatus?.state, screen, settingsLoaded, startStatus]);

  useEffect(() => {
    if (screen !== "whatsapp-link" || !runtimeInstallDir) {
      return;
    }

    if (!whatsappLinkStarted && !whatsappLinkBusy) {
      void startWhatsappLinkFlow();
      return;
    }

    const interval = window.setInterval(() => {
      void refreshWhatsappLinkStatus();
    }, 2500);

    return () => window.clearInterval(interval);
  }, [
    refreshWhatsappLinkStatus,
    runtimeInstallDir,
    screen,
    startWhatsappLinkFlow,
    whatsappLinkBusy,
    whatsappLinkStarted,
  ]);

  useEffect(() => {
    if (!settingsLoaded || !installState.installed || screen !== "welcome" || !window.octopalDesktop || !runtimeInstallDir) {
      return;
    }

    let cancelled = false;
    void window.octopalDesktop
      .getOctopalStatus(runtimeInstallDir)
      .then((result) => {
        if (cancelled) {
          return;
        }

        if (result.ok && result.state === "running") {
          setRuntimeStatus(result);
          setStartStatus("started");
          setStartError("");
          setStartErrorDetail("");
          setScreen("done");
          return;
        }

        setRuntimeStatus(null);
        setStartStatus("idle");
        setStartError("");
        setStartErrorDetail("");
      })
      .catch(() => {
        if (!cancelled) {
          setRuntimeStatus(null);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [installState.installed, runtimeInstallDir, screen, settingsLoaded]);

  useEffect(() => {
    setStepIndex((current) => Math.min(current, steps.length - 1));
  }, [steps.length]);

  useEffect(() => {
    window.scrollTo({ top: 0, left: 0 });
    document.querySelector(".setup-content")?.scrollTo({ top: 0, left: 0 });
  }, [stepIndex, step]);

  function updateLanguage(next: Language) {
    setLanguage(next);
    document.documentElement.lang = next;
  }

  function updateProvider(providerId: string, target: "octo" | "worker") {
    const provider = providers.find((item) => item.id === providerId);
    if (target === "octo") {
      if (form.getValues("providerId") !== providerId && isExistingSecret(form.getValues("apiKey"))) {
        form.setValue("apiKey", "", { shouldDirty: true, shouldValidate: true });
      }
      form.setValue("providerId", providerId, { shouldValidate: true });
      if (provider?.model) {
        form.setValue("model", provider.model, { shouldValidate: true });
      }
      return;
    }

    if (form.getValues("workerProviderId") !== providerId && isExistingSecret(form.getValues("workerApiKey"))) {
      form.setValue("workerApiKey", "", { shouldDirty: true, shouldValidate: true });
    }
    form.setValue("workerProviderId", providerId, { shouldValidate: true });
    if (provider?.model) {
      form.setValue("workerModel", provider.model, { shouldValidate: true });
    }
  }

  function toggleSearchProvider(providerId: "brave" | "firecrawl") {
    const nextProvider = values.searchProvider === providerId ? undefined : providerId;
    form.setValue("searchProvider", nextProvider, { shouldDirty: true, shouldValidate: true });
    if (!nextProvider) {
      form.clearErrors(["searchProvider", "braveApiKey", "firecrawlApiKey"]);
    }
  }

  function toggleConnector(name: DesktopConnectorName) {
    const enabledField = name === "google" ? "googleConnectorEnabled" : "githubConnectorEnabled";
    const currentEnabled = form.getValues(enabledField);
    const isSelected = selectedConnector === name;
    const nextEnabled = isSelected ? !currentEnabled : true;
    setSelectedConnector(name);
    setConnectorMessage("");
    setConnectorMessageTone("info");
    form.setValue(enabledField, nextEnabled, { shouldDirty: true, shouldValidate: true });
    if (!nextEnabled) {
      form.clearErrors(
        name === "google"
          ? ["googleConnectorEnabled", "googleConnectorServices", "googleClientId", "googleClientSecret"]
          : ["githubConnectorEnabled", "githubConnectorServices", "githubToken"],
      );
      if (name === "google" && form.getValues("githubConnectorEnabled")) {
        setSelectedConnector("github");
      }
      if (name === "github" && form.getValues("googleConnectorEnabled")) {
        setSelectedConnector("google");
      }
    }
  }

  function toggleConnectorService(name: DesktopConnectorName, serviceId: string) {
    if (name === "google") {
      const current = form.getValues("googleConnectorServices");
      const next = current.includes(serviceId as (typeof current)[number])
        ? current.filter((item) => item !== serviceId)
        : [...current, serviceId as (typeof current)[number]];
      form.setValue("googleConnectorServices", next, { shouldDirty: true, shouldValidate: true });
      return;
    }

    const current = form.getValues("githubConnectorServices");
    const next = current.includes(serviceId as (typeof current)[number])
      ? current.filter((item) => item !== serviceId)
      : [...current, serviceId as (typeof current)[number]];
    form.setValue("githubConnectorServices", next, { shouldDirty: true, shouldValidate: true });
  }

  async function authorizeConnector(name: DesktopConnectorName) {
    if (!window.octopalDesktop || !runtimeInstallDir || !installState.installed) {
      return;
    }

    const validationFields: Array<keyof InstallForm> =
      name === "google"
        ? ["googleConnectorEnabled", "googleConnectorServices", "googleClientId", "googleClientSecret"]
        : ["githubConnectorEnabled", "githubConnectorServices", "githubToken"];
    const ok = await form.trigger(validationFields);
    if (!ok) {
      return;
    }

    setConnectorBusy(name);
    setConnectorMessage("");
    setConnectorMessageTone("info");
    try {
      const currentValues = form.getValues();
      const nextState = await window.octopalDesktop.saveOctopalConfig(buildOctopalConfig(currentValues));
      setInstallState(nextState);
      const result = await window.octopalDesktop.authorizeConnector(
        runtimeInstallDir,
        name === "google"
          ? {
              name,
              clientId: currentValues.googleClientId,
              clientSecret: currentValues.googleClientSecret,
            }
          : {
              name,
              token: currentValues.githubToken,
            },
      );
      setConnectorMessage(result.ok ? "" : result.message);
      setConnectorMessageTone(result.ok ? "info" : "error");
      await refreshConnectorStatus();
    } catch (error) {
      setConnectorMessage(error instanceof Error ? error.message : "Connector authorization failed.");
      setConnectorMessageTone("error");
    } finally {
      setConnectorBusy(null);
    }
  }

  async function chooseInstallDir() {
    try {
      const selected = window.octopalDesktop ? await window.octopalDesktop.chooseInstallDir() : "C:\\Octopal";
      if (selected) {
        form.setValue("installDir", selected, { shouldDirty: true, shouldValidate: true });
      }
    } catch (error) {
      console.error("Unable to choose install folder", error);
    }
  }

  function controlWindow(action: "close" | "minimize" | "maximize") {
    if (!window.octopalDesktop) {
      return;
    }

    if (action === "close") {
      void window.octopalDesktop.closeWindow();
      return;
    }

    if (action === "minimize") {
      void window.octopalDesktop.minimizeWindow();
      return;
    }

    void window.octopalDesktop.toggleMaximizeWindow();
  }

  async function nextStep() {
    const ok = await form.trigger(getValidationFields(step as StepId, values));
    if (!ok) {
      return;
    }
    setStepIndex((current) => Math.min(current + 1, steps.length - 1));
  }

  function previousStep() {
    if (stepIndex === 0) {
      setScreen("welcome");
      return;
    }
    setStepIndex((current) => Math.max(current - 1, 0));
  }

  async function openConfiguration() {
    const installDir = installState.installDir || values.installDir;
    if (window.octopalDesktop && installState.installed && installDir) {
      try {
        const config = await window.octopalDesktop.loadOctopalConfig();
        const loadedValues = formValuesFromOctopalConfig(config, installDir);
        form.reset(loadedValues);
        setSelectedConnector(loadedValues.googleConnectorEnabled || !loadedValues.githubConnectorEnabled ? "google" : "github");
        setLoadedConfigChannel(loadedValues.channel);
        setConfigurationMode("edit");
      } catch (error) {
        console.error("Unable to load installed Octopal config", error);
        setLoadedConfigChannel(null);
        setConfigurationMode("install");
      }
    } else {
      setLoadedConfigChannel(null);
      setConfigurationMode("install");
      setSelectedConnector("google");
      if (!form.getValues("dashboardToken")?.trim()) {
        form.setValue("dashboardToken", generateDashboardToken(), { shouldDirty: true, shouldValidate: true });
      }
    }

    setStepIndex(0);
    setScreen("wizard");
  }

  async function saveConfiguration() {
    const ok = await form.trigger();
    if (!ok || !window.octopalDesktop) {
      return;
    }

    try {
      const shouldLinkWhatsApp = values.channel === "whatsapp" && loadedConfigChannel !== "whatsapp";
      const nextState = await window.octopalDesktop.saveOctopalConfig(buildOctopalConfig(values));
      setInstallState(nextState);
      setLoadedConfigChannel(values.channel);
      setSavedInstallResult(null);
      setSavedPlanPath("");
      setRuntimeStatus(null);
      setStartStatus("idle");
      setStartError("");
      setStartErrorDetail("");
      setWhatsappLinkStatus(null);
      setWhatsappLinkError("");
      setWhatsappLinkStarted(false);
      setConnectorStatus(null);
      setConnectorMessage("");
      setConnectorMessageTone("info");
      setScreen(shouldLinkWhatsApp ? "whatsapp-link" : "welcome");
    } catch (error) {
      setInstallError(error instanceof Error ? error.message : copy("installFailedBody"));
      setScreen("failed");
    }
  }

  async function submitReview() {
    if (configurationMode === "edit") {
      await saveConfiguration();
      return;
    }

    await prepareInstall();
  }

  async function prepareInstall() {
    const ok = await form.trigger();
    if (!ok) {
      return;
    }

    setScreen("installing");
    setInstallEvents([]);
    setInstallError("");
    setSavedInstallResult(null);
    setRuntimeStatus(null);
    setStartStatus("idle");
    setStartError("");
    setStartErrorDetail("");
    setWhatsappLinkStatus(null);
    setWhatsappLinkError("");
    setWhatsappLinkStarted(false);

    const payload = {
      createdBy: "Octopal Desktop",
      createdAt: new Date().toISOString(),
      installDir: values.installDir,
      octopalConfig: buildOctopalConfig(values),
    };

    if (window.octopalDesktop) {
      const unsubscribe = window.octopalDesktop.onInstallEvent((event) => {
        setInstallEvents((current) => [...current, event].slice(-80));
      });

      try {
        const result = await window.octopalDesktop.installOctopal(payload);
        setSavedInstallResult(result);
        setSavedPlanPath(result.planPath);
        setInstallState({
          installed: true,
          installDir: result.installDir,
          configPath: result.configPath,
          planPath: result.planPath,
        });
        setScreen(values.channel === "whatsapp" ? "whatsapp-link" : "done");
      } catch (error) {
        const message = error instanceof Error ? error.message : copy("installFailedBody");
        setInstallError(message);
        setScreen("failed");
      } finally {
        unsubscribe();
      }
    } else {
      setSavedPlanPath("browser-preview/.octopal-desktop/install-plan.json");
      setInstallEvents([{ kind: "done", message: "Browser preview", detail: "Electron installer API is not available." }]);
      window.setTimeout(() => setScreen("done"), 850);
    }
  }

  async function startInstalledOctopal() {
    const installDir = savedInstallResult?.installDir || installState.installDir || values.installDir;
    if (!window.octopalDesktop || !installDir) {
      return;
    }

    setStartStatus("starting");
    setScreen("done");
    setStartError("");
    setStartErrorDetail("");
    try {
      await window.octopalDesktop.stopWhatsAppLink(installDir).catch(() => undefined);
      const result = await window.octopalDesktop.startOctopal(installDir);
      if (!result.ok) {
        setStartStatus("failed");
        setStartError(result.error || copy("startFailed"));
        setStartErrorDetail(result.detail);
        return;
      }
      void refreshRuntimeStatus();
    } catch (error) {
      setStartStatus("failed");
      setStartError(error instanceof Error ? error.message : copy("startFailed"));
      setStartErrorDetail("");
    }
  }

  async function finishWhatsappLink() {
    await stopWhatsappLinkFlow();
    setWhatsappLinkBusy(false);
    setWhatsappLinkError("");
    setScreen(configurationMode === "edit" ? "welcome" : "done");
  }

  async function stopInstalledOctopal() {
    const installDir = savedInstallResult?.installDir || installState.installDir || values.installDir;
    if (!window.octopalDesktop || !installDir) {
      return;
    }

    setStartStatus("stopping");
    setStartError("");
    setStartErrorDetail("");
    try {
      const result = await window.octopalDesktop.stopOctopal(installDir);
      if (!result.ok) {
        setStartStatus("failed");
        setStartError(result.error || copy("stopFailed"));
        setStartErrorDetail(result.detail);
        return;
      }
      setStartStatus("idle");
      setScreen("welcome");
      void refreshRuntimeStatus();
    } catch (error) {
      setStartStatus("failed");
      setStartError(error instanceof Error ? error.message : copy("stopFailed"));
      setStartErrorDetail("");
    }
  }

  async function restartInstalledOctopal() {
    const installDir = savedInstallResult?.installDir || installState.installDir || values.installDir;
    if (!window.octopalDesktop || !installDir) {
      return;
    }

    setScreen("done");
    setStartStatus("stopping");
    setStartError("");
    setStartErrorDetail("");
    try {
      const stopped = await window.octopalDesktop.stopOctopal(installDir);
      if (!stopped.ok) {
        setStartStatus("failed");
        setStartError(stopped.error || copy("stopFailed"));
        setStartErrorDetail(stopped.detail);
        return;
      }

      setStartStatus("starting");
      const started = await window.octopalDesktop.startOctopal(installDir);
      if (!started.ok) {
        setStartStatus("failed");
        setStartError(started.error || copy("startFailed"));
        setStartErrorDetail(started.detail);
        return;
      }

      setStartStatus("started");
      void refreshRuntimeStatus();
      void refreshUpdateStatus();
    } catch (error) {
      setStartStatus("failed");
      setStartError(copy("startFailed"));
      setStartErrorDetail(error instanceof Error ? error.message : copy("startFailed"));
    }
  }

  async function updateInstalledOctopal() {
    const installDir = savedInstallResult?.installDir || installState.installDir || values.installDir;
    if (!window.octopalDesktop || !installDir || updateBusy) {
      return;
    }

    setUpdateBusy(true);
    setUpdateMessage("");
    setUpdateError("");
    try {
      const result = await window.octopalDesktop.updateOctopal(installDir);
      if (!result.ok) {
        setUpdateError(result.detail || result.error || copy("updateFailed"));
        if (result.before) {
          setUpdateStatus(result.before);
        }
        return;
      }
      setUpdateMessage(result.restarted ? copy("updateRestarted") : copy("updateInstalled"));
      if (result.after) {
        setUpdateStatus(result.after);
      } else {
        void refreshUpdateStatus();
      }
      void refreshRuntimeStatus();
    } catch (error) {
      setUpdateError(error instanceof Error ? error.message : copy("updateFailed"));
    } finally {
      setUpdateBusy(false);
    }
  }

  async function updateDesktopApp() {
    if (!window.octopalDesktop || desktopUpdateBusy) {
      return;
    }

    setDesktopUpdateBusy(true);
    setDesktopUpdateError("");
    try {
      const result = desktopUpdateStatus?.canInstall
        ? await window.octopalDesktop.installAppUpdate()
        : await window.octopalDesktop.downloadAppUpdate();
      setDesktopUpdateStatus(result);
      if (!result.ok) {
        setDesktopUpdateError(result.error || result.detail || copy("desktopUpdateFailed"));
      }
    } catch (error) {
      setDesktopUpdateError(error instanceof Error ? error.message : copy("desktopUpdateFailed"));
    } finally {
      setDesktopUpdateBusy(false);
    }
  }

  const doneTitle = startStatus === "idle" && !runtimeStatus ? copy("completeTitle") : runtimeView.title;
  const doneBody = runtimeView.state === "error" || (startStatus === "idle" && !runtimeStatus) ? "" : runtimeView.detail;
  const doneCanStop =
    runtimeView.state === "running" ||
    runtimeView.state === "stopping" ||
    (startStatus === "failed" && runtimeStatus?.state === "running");
  const doneBusy = runtimeView.state === "starting" || runtimeView.state === "stopping";

  return (
    <AppShell
      title={copy("appTitle")}
      onClose={() => controlWindow("close")}
      onMinimize={() => controlWindow("minimize")}
      onMaximize={() => controlWindow("maximize")}
    >
      <AnimatePresence mode="wait">
        {screen === "welcome" ? (
          <WelcomeScreen
            key="welcome"
            copy={copy}
            language={language}
            theme={theme}
            onLanguageChange={updateLanguage}
            onThemeChange={setTheme}
            onStart={() => void openConfiguration()}
            onStartOctopal={() => void startInstalledOctopal()}
            onUpdateOctopal={() => void updateInstalledOctopal()}
            onUpdateDesktopApp={() => void updateDesktopApp()}
            installed={installState.installed}
            desktopUpdateAvailable={desktopUpdateAvailable}
            desktopUpdateReady={desktopUpdateReady}
            desktopUpdateBusy={desktopUpdateBusy}
            desktopUpdateSummary={desktopUpdateSummary}
            desktopUpdateDetail={desktopUpdateError || desktopUpdateStatus?.detail || ""}
            updateAvailable={updateAvailable}
            updateBlocked={updateBlocked}
            updateBusy={updateBusy}
            updateSummary={updateSummary}
            updateDetail={updateStatus?.gitBlocker || updateError || updateMessage || updateStatus?.detail || ""}
          />
        ) : null}

        {screen === "wizard" ? (
          <WizardScreen
            key={step}
            copy={copy}
            language={language}
            theme={theme}
            step={step}
            stepIndex={stepIndex}
            totalSteps={steps.length}
            values={values}
            form={form}
            errors={form.formState.errors}
            onLanguageChange={updateLanguage}
            onThemeChange={setTheme}
            onChooseInstallDir={() => void chooseInstallDir()}
            onProviderChange={updateProvider}
            onSearchProviderToggle={toggleSearchProvider}
            onConnectorToggle={toggleConnector}
            onConnectorServiceToggle={toggleConnectorService}
            onAuthorizeConnector={(name) => void authorizeConnector(name)}
            onBack={previousStep}
            onNext={() => void nextStep()}
            onPrepareInstall={() => void submitReview()}
            onRefreshPrerequisites={() => void refreshPrerequisites()}
            reviewBody={configurationMode === "edit" ? copy("reviewBodyEdit") : copy("reviewBody")}
            reviewActionLabel={configurationMode === "edit" ? copy("saveConfiguration") : copy("startInstall")}
            preflightChecks={preflightChecks}
            preflightStatus={preflightStatus}
            preflightError={preflightError}
            preflightHasBlockingIssue={preflightHasBlockingIssue}
            connectorStatus={connectorStatus}
            connectorBusy={connectorBusy}
            connectorMessage={connectorMessage}
            connectorMessageTone={connectorMessageTone}
            selectedConnector={selectedConnector}
            canAuthorizeConnectors={installState.installed && configurationMode === "edit"}
          />
        ) : null}

        {screen === "installing" ? (
          <InstallProgressScreen
            key="installing"
            title={copy("installingTitle")}
            body={copy("installingBody")}
            events={installEvents}
            busy
          />
        ) : null}

        {screen === "done" && runtimeView.state === "running" ? (
          <DashboardScreen
            key="dashboard"
            copy={copy}
            installDir={runtimeInstallDir}
            runtimeView={runtimeView}
            updateAvailable={updateAvailable}
            updateBlocked={updateBlocked}
            updateBusy={updateBusy}
            desktopUpdateAvailable={desktopUpdateAvailable}
            desktopUpdateReady={desktopUpdateReady}
            desktopUpdateBusy={desktopUpdateBusy}
            onStart={() => void startInstalledOctopal()}
            onStop={() => void stopInstalledOctopal()}
            onRestart={() => void restartInstalledOctopal()}
            onUpdateOctopal={() => void updateInstalledOctopal()}
            onUpdateDesktopApp={() => void updateDesktopApp()}
          />
        ) : null}

        {screen === "done" && runtimeView.state !== "running" ? (
          <StatusScreen
            key="done"
            title={doneTitle}
            body={doneBody}
            octoAlt="Octopal mascot"
            action={
              <>
                {desktopUpdateAvailable ? (
                  <Button
                    type="button"
                    variant="primary"
                    className="status-action-button"
                    disabled={desktopUpdateBusy}
                    onClick={() => void updateDesktopApp()}
                  >
                    <Download data-icon="inline-start" />
                    {desktopUpdateReady
                      ? copy("installDesktopUpdate")
                      : desktopUpdateBusy
                        ? copy("downloadingDesktopUpdate")
                        : copy("updateDesktopApp")}
                  </Button>
                ) : null}
                {updateAvailable ? (
                  <Button
                    type="button"
                    variant="primary"
                    className="status-action-button"
                    disabled={updateBusy || updateBlocked}
                    onClick={() => void updateInstalledOctopal()}
                  >
                    <Download data-icon="inline-start" />
                    {updateBusy ? copy("updatingOctopal") : copy("updateOctopal")}
                  </Button>
                ) : null}
                {doneCanStop ? (
                  <Button
                    type="button"
                    variant="danger"
                    className="status-action-button"
                    disabled={doneBusy || updateBusy || desktopUpdateBusy}
                    onClick={() => void stopInstalledOctopal()}
                  >
                    <Square data-icon="inline-start" />
                    {runtimeView.state === "stopping" ? copy("stoppingOctopal") : copy("stopOctopal")}
                  </Button>
                ) : (
                  <Button
                    type="button"
                    variant="success"
                    className="status-action-button"
                    disabled={doneBusy || updateBusy || desktopUpdateBusy}
                    onClick={() => void startInstalledOctopal()}
                  >
                    <Play data-icon="inline-start" />
                    {runtimeView.state === "starting" ? copy("startingOctopal") : copy("startOctopal")}
                  </Button>
                )}
              </>
            }
            noticeTitle={
              desktopUpdateAvailable
                ? desktopUpdateReady
                  ? copy("desktopUpdateReady")
                  : copy("desktopUpdateAvailable")
                : updateAvailable
                  ? copy("updateAvailable")
                  : updateMessage
                    ? copy("updateComplete")
                    : ""
            }
            noticeDetail={
              desktopUpdateAvailable
                ? desktopUpdateSummary
                : updateAvailable
                  ? `${updateSummary}${updateBlocked && updateStatus?.gitBlocker ? ` - ${updateStatus.gitBlocker}` : ""}`
                  : updateMessage
            }
            errorTitle={
              runtimeView.state === "error"
                ? runtimeView.title
                : desktopUpdateError
                  ? copy("desktopUpdateFailed")
                  : updateError
                    ? copy("updateFailed")
                    : ""
            }
            errorDetail={runtimeView.state === "error" ? runtimeView.detail : desktopUpdateError || updateError}
          />
        ) : null}

        {screen === "whatsapp-link" ? (
          <WhatsAppLinkScreen
            key="whatsapp-link"
            copy={copy}
            status={whatsappLinkStatus}
            busy={whatsappLinkBusy}
            error={whatsappLinkError}
            onRefresh={() => void refreshWhatsappLinkStatus(true)}
            onContinue={() => void finishWhatsappLink()}
            onSkip={() => void finishWhatsappLink()}
          />
        ) : null}

        {screen === "failed" ? (
          <InstallProgressScreen
            key="failed"
            title={copy("installFailedTitle")}
            body={copy("installFailedBody")}
            events={installEvents}
            error={installError}
          />
        ) : null}
      </AnimatePresence>
    </AppShell>
  );
}
