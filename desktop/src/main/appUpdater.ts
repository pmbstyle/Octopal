import { app, BrowserWindow } from "electron";
import electronUpdater from "electron-updater";
import type { ProgressInfo, UpdateInfo } from "electron-updater";

const { autoUpdater } = electronUpdater;

export type DesktopAppUpdateStatus = {
  ok: boolean;
  status: "idle" | "checking" | "available" | "not-available" | "downloading" | "downloaded" | "installing" | "error";
  currentVersion: string;
  latestVersion?: string;
  releaseName?: string;
  releaseDate?: string;
  detail: string;
  canDownload: boolean;
  canInstall: boolean;
  percent?: number;
  isPackaged: boolean;
  error?: string;
};

let configured = false;
let status: DesktopAppUpdateStatus = {
  ok: true,
  status: "idle",
  currentVersion: app.getVersion(),
  detail: "Desktop update check has not run yet.",
  canDownload: false,
  canInstall: false,
  isPackaged: app.isPackaged,
};

function updateInfoFields(info: UpdateInfo): Pick<DesktopAppUpdateStatus, "latestVersion" | "releaseName" | "releaseDate"> {
  return {
    latestVersion: info.version,
    releaseName: info.releaseName ?? undefined,
    releaseDate: info.releaseDate,
  };
}

function setStatus(next: Partial<DesktopAppUpdateStatus>): DesktopAppUpdateStatus {
  status = {
    ...status,
    ...next,
    currentVersion: app.getVersion(),
    isPackaged: app.isPackaged,
  };
  for (const window of BrowserWindow.getAllWindows()) {
    window.webContents.send("desktop:app-update-status", status);
  }
  return status;
}

function configureUpdater(): void {
  if (configured) {
    return;
  }
  configured = true;
  autoUpdater.autoDownload = false;
  autoUpdater.autoInstallOnAppQuit = true;

  autoUpdater.on("checking-for-update", () => {
    setStatus({
      ok: true,
      status: "checking",
      detail: "Checking for a desktop app update.",
      canDownload: false,
      canInstall: false,
      percent: undefined,
      error: undefined,
    });
  });

  autoUpdater.on("update-available", (info) => {
    setStatus({
      ok: true,
      status: "available",
      ...updateInfoFields(info),
      detail: `Octopal Desktop ${info.version} is available.`,
      canDownload: true,
      canInstall: false,
      percent: undefined,
      error: undefined,
    });
  });

  autoUpdater.on("update-not-available", (info) => {
    setStatus({
      ok: true,
      status: "not-available",
      ...updateInfoFields(info),
      detail: "Octopal Desktop is up to date.",
      canDownload: false,
      canInstall: false,
      percent: undefined,
      error: undefined,
    });
  });

  autoUpdater.on("download-progress", (progress: ProgressInfo) => {
    setStatus({
      ok: true,
      status: "downloading",
      detail: "Downloading the desktop app update.",
      canDownload: false,
      canInstall: false,
      percent: progress.percent,
      error: undefined,
    });
  });

  autoUpdater.on("update-downloaded", (info) => {
    setStatus({
      ok: true,
      status: "downloaded",
      ...updateInfoFields(info),
      detail: "Desktop update is ready to install.",
      canDownload: false,
      canInstall: true,
      percent: 100,
      error: undefined,
    });
  });

  autoUpdater.on("error", (error) => {
    setStatus({
      ok: false,
      status: "error",
      detail: error.message || "Desktop update failed.",
      canDownload: false,
      canInstall: false,
      error: error.message || "Desktop update failed.",
    });
  });
}

function packagedOnlyStatus(): DesktopAppUpdateStatus {
  return setStatus({
    ok: true,
    status: "not-available",
    detail: "Desktop updates are available in packaged builds.",
    canDownload: false,
    canInstall: false,
  });
}

export function getDesktopAppUpdateStatus(): DesktopAppUpdateStatus {
  return status;
}

export async function checkDesktopAppUpdate(): Promise<DesktopAppUpdateStatus> {
  if (!app.isPackaged) {
    return packagedOnlyStatus();
  }
  configureUpdater();
  await autoUpdater.checkForUpdates();
  return status;
}

export async function downloadDesktopAppUpdate(): Promise<DesktopAppUpdateStatus> {
  if (!app.isPackaged) {
    return packagedOnlyStatus();
  }
  configureUpdater();
  if (status.status !== "available" && !status.canDownload) {
    await autoUpdater.checkForUpdates();
  }
  if (status.status === "available" || status.canDownload) {
    await autoUpdater.downloadUpdate();
  }
  return status;
}

export function installDesktopAppUpdate(): DesktopAppUpdateStatus {
  if (!app.isPackaged) {
    return packagedOnlyStatus();
  }
  configureUpdater();
  if (!status.canInstall) {
    return setStatus({
      ok: false,
      status: "error",
      detail: "Desktop update has not been downloaded yet.",
      canDownload: false,
      canInstall: false,
      error: "Desktop update has not been downloaded yet.",
    });
  }
  setStatus({
    ok: true,
    status: "installing",
    detail: "Restarting Octopal Desktop to install the update.",
    canDownload: false,
    canInstall: false,
  });
  autoUpdater.quitAndInstall(false, true);
  return status;
}

export function scheduleDesktopAppUpdateCheck(delayMs = 5000): void {
  if (!app.isPackaged) {
    return;
  }
  setTimeout(() => {
    void checkDesktopAppUpdate().catch((error) => {
      setStatus({
        ok: false,
        status: "error",
        detail: error instanceof Error ? error.message : "Desktop update failed.",
        canDownload: false,
        canInstall: false,
        error: error instanceof Error ? error.message : "Desktop update failed.",
      });
    });
  }, delayMs);
}
