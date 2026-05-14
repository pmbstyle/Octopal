import type { Language } from "./i18n";
import type { Theme } from "./appTypes";

export type DesktopSettings = {
  language: Language;
  theme: Theme;
  installDir: string;
};

export function getPreferredTheme(theme: Theme): "light" | "dark" {
  if (theme === "system") {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  return theme;
}

export async function loadSettings(): Promise<DesktopSettings> {
  if (window.octopalDesktop) {
    return window.octopalDesktop.loadSettings();
  }

  return {
    language: (localStorage.getItem("octopal.language") as Language) || "en",
    theme: (localStorage.getItem("octopal.theme") as Theme) || "system",
    installDir: localStorage.getItem("octopal.installDir") || "",
  };
}

export async function saveSettings(settings: DesktopSettings): Promise<DesktopSettings> {
  if (window.octopalDesktop) {
    return window.octopalDesktop.saveSettings(settings);
  }

  localStorage.setItem("octopal.language", settings.language);
  localStorage.setItem("octopal.theme", settings.theme);
  localStorage.setItem("octopal.installDir", settings.installDir);
  return settings;
}
