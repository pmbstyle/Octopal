import type { messages } from "./i18n";

export type Theme = "light" | "dark" | "system";
export type Screen = "welcome" | "wizard" | "installing" | "whatsapp-link" | "done" | "failed";
export type StepId =
  | "location"
  | "channel"
  | "octo-llm"
  | "worker-llm"
  | "search"
  | "connectors"
  | "dashboard"
  | "review";
export type CopyFn = (key: keyof typeof messages.en) => string;
