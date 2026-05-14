import { motion } from "framer-motion";
import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { Field, Input } from "../Field";
import { ImageLogo } from "../ImageLogo";
import { StepSection } from "../StepSection";
import { ToggleCard } from "../ToggleCard";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, searchProviders, type InstallForm } from "../../lib/install";
import { searchLogos } from "../../lib/logos";

export function SearchStep({
  copy,
  values,
  form,
  errors,
  onSearchProviderToggle,
}: {
  copy: CopyFn;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
  onSearchProviderToggle: (providerId: "brave" | "firecrawl") => void;
}) {
  return (
    <StepSection body={copy("toolsBody")}>
      <div className="choice-grid search-grid">
        {searchProviders.map((provider) => (
          <ToggleCard
            key={provider.id}
            active={values.searchProvider === provider.id}
            icon={<ImageLogo src={searchLogos[provider.id]} alt="" />}
            title={provider.label}
            body={`${provider.label} ${copy("apiKey")}`}
            onClick={() => onSearchProviderToggle(provider.id)}
          />
        ))}
      </div>
      {values.searchProvider === "brave" ? (
        <motion.div className="single-field reveal-form" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
          <Field
            label={copy("braveKey")}
            hint={isExistingSecret(values.braveApiKey) ? copy("configured") : copy("required")}
            invalid={!!errors.braveApiKey}
            help={{
              title: copy("braveKeyHelpTitle"),
              body: [
                <>
                  {copy("braveKeyHelpBody1Prefix")}{" "}
                  <a className="field-help-link" href="https://api-dashboard.search.brave.com/app/keys" target="_blank" rel="noreferrer">
                    {copy("braveKeyHelpLink")}
                  </a>
                  {copy("braveKeyHelpBody1Suffix")}
                </>,
                copy("braveKeyHelpBody2"),
              ],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Input {...form.register("braveApiKey")} type="password" />
          </Field>
        </motion.div>
      ) : null}
      {values.searchProvider === "firecrawl" ? (
        <motion.div className="single-field reveal-form" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
          <Field
            label={copy("firecrawlKey")}
            hint={isExistingSecret(values.firecrawlApiKey) ? copy("configured") : copy("required")}
            invalid={!!errors.firecrawlApiKey}
            help={{
              title: copy("firecrawlKeyHelpTitle"),
              body: [
                <>
                  {copy("firecrawlKeyHelpBody1Prefix")}{" "}
                  <a className="field-help-link" href="https://www.firecrawl.dev/app/api-keys" target="_blank" rel="noreferrer">
                    {copy("firecrawlKeyHelpLink")}
                  </a>
                  {copy("firecrawlKeyHelpBody1Suffix")}
                </>,
                copy("firecrawlKeyHelpBody2"),
              ],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Input {...form.register("firecrawlApiKey")} type="password" />
          </Field>
        </motion.div>
      ) : null}
    </StepSection>
  );
}
