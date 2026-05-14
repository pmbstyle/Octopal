import { Globe2, Moon, Sun } from "lucide-react";

import octoImage from "../../../../assets/octo.png";
import { languages, type Language } from "../lib/i18n";
import type { Theme } from "../lib/appTypes";
import { LabeledSelect } from "./LabeledSelect";

export function SetupHeader({
  speech,
  language,
  theme,
  onLanguageChange,
  onThemeChange,
  stepLabel,
  stepIndex,
  totalSteps,
}: {
  speech: string;
  language: Language;
  theme: Theme;
  onLanguageChange: (language: Language) => void;
  onThemeChange: (theme: Theme) => void;
  stepLabel: string;
  stepIndex: number;
  totalSteps: number;
}) {
  return (
    <header className="setup-header">
      <div className="setup-mascot-wrap">
        <img className="octo setup-octo" src={octoImage} alt="Octopal mascot" />
        <div className="speech-bubble step-bubble">{speech}</div>
      </div>
      <div className="setup-top-controls">
        <span className="step-counter">
          {stepIndex + 1}/{totalSteps} · {stepLabel}
        </span>
        <LabeledSelect
          icon={<Globe2 />}
          label="Language"
          value={language}
          onChange={(next) => onLanguageChange(next as Language)}
          options={languages.map((item) => ({ value: item.value, label: item.short }))}
        />
        <button className="theme-icon-button" type="button" onClick={() => onThemeChange(theme === "dark" ? "light" : "dark")}>
          {theme === "dark" ? <Moon /> : <Sun />}
        </button>
      </div>
    </header>
  );
}
