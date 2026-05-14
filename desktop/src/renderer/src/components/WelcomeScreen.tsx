import { ArrowRight, Download, Globe2, Moon, Play, Settings, Sun } from "lucide-react";
import { motion } from "framer-motion";

import octoImage from "../../../../assets/octo.png";
import { languages, type Language } from "../lib/i18n";
import type { CopyFn, Theme } from "../lib/appTypes";
import { Button } from "./Button";
import { LabeledSelect } from "./LabeledSelect";

export function WelcomeScreen({
  copy,
  language,
  theme,
  onLanguageChange,
  onThemeChange,
  onStart,
  onStartOctopal,
  onUpdateOctopal,
  onUpdateDesktopApp,
  installed,
  desktopUpdateAvailable,
  desktopUpdateReady,
  desktopUpdateBusy,
  desktopUpdateSummary,
  desktopUpdateDetail,
  updateAvailable,
  updateBlocked,
  updateBusy,
  updateSummary,
  updateDetail,
}: {
  copy: CopyFn;
  language: Language;
  theme: Theme;
  onLanguageChange: (language: Language) => void;
  onThemeChange: (theme: Theme) => void;
  onStart: () => void;
  onStartOctopal: () => void;
  onUpdateOctopal: () => void;
  onUpdateDesktopApp: () => void;
  installed: boolean;
  desktopUpdateAvailable?: boolean;
  desktopUpdateReady?: boolean;
  desktopUpdateBusy?: boolean;
  desktopUpdateSummary?: string;
  desktopUpdateDetail?: string;
  updateAvailable?: boolean;
  updateBlocked?: boolean;
  updateBusy?: boolean;
  updateSummary?: string;
  updateDetail?: string;
}) {
  return (
    <motion.section
      key="welcome"
      className="welcome-screen"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.28 }}
    >
      <div className="speech-bubble">{installed ? copy("welcomeInstalled") : copy("welcome")}</div>
      <img className="octo welcome-octo" src={octoImage} alt="Octopal mascot" />
      <div className="welcome-controls">
        <LabeledSelect
          icon={<Globe2 />}
          label={copy("language")}
          value={language}
          onChange={(next) => onLanguageChange(next as Language)}
          options={languages.map((item) => ({ value: item.value, label: item.label }))}
        />
        <LabeledSelect
          icon={theme === "dark" ? <Moon /> : <Sun />}
          label={copy("theme")}
          value={theme}
          onChange={(next) => onThemeChange(next as Theme)}
          options={[
            { value: "light", label: copy("light") },
            { value: "dark", label: copy("dark") },
            { value: "system", label: copy("system") },
          ]}
        />
      </div>
      <div className={installed ? "welcome-actions" : undefined}>
        {installed ? (
          <Button
            className="welcome-button welcome-action-button"
            variant="success"
            onClick={onStartOctopal}
          >
            <Play data-icon="inline-start" />
            {copy("startOctopal")}
          </Button>
        ) : null}
        <Button
          className={installed ? "welcome-button welcome-action-button" : "welcome-button"}
          variant={installed ? "secondary" : "primary"}
          onClick={onStart}
        >
          {installed ? <Settings data-icon="inline-start" /> : null}
          {installed ? copy("modifyConfig") : copy("configure")}
          {!installed ? <ArrowRight data-icon="inline-end" /> : null}
        </Button>
      </div>
      {desktopUpdateAvailable ? (
        <div className="update-card">
          <div>
            <strong>{desktopUpdateReady ? copy("desktopUpdateReady") : copy("desktopUpdateAvailable")}</strong>
            <span>{desktopUpdateSummary || desktopUpdateDetail}</span>
          </div>
          <Button
            type="button"
            variant="primary"
            className="update-card-button"
            disabled={desktopUpdateBusy}
            onClick={onUpdateDesktopApp}
          >
            <Download data-icon="inline-start" />
            {desktopUpdateReady
              ? copy("installDesktopUpdate")
              : desktopUpdateBusy
                ? copy("downloadingDesktopUpdate")
                : copy("updateDesktopApp")}
          </Button>
        </div>
      ) : null}
      {installed && updateAvailable ? (
        <div className="update-card">
          <div>
            <strong>{copy("updateAvailable")}</strong>
            <span>{updateSummary || updateDetail}</span>
          </div>
          <Button
            type="button"
            variant="primary"
            className="update-card-button"
            disabled={updateBusy || updateBlocked}
            onClick={onUpdateOctopal}
          >
            <Download data-icon="inline-start" />
            {updateBusy ? copy("updatingOctopal") : copy("updateOctopal")}
          </Button>
        </div>
      ) : null}
    </motion.section>
  );
}
