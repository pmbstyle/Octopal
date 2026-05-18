import { motion } from "framer-motion";
import type { UseFormRegisterReturn } from "react-hook-form";

import { Field, Input } from "./Field";

export function LlmForm({
  modelLabel,
  apiKeyLabel,
  apiBaseLabel,
  apiKeyHint,
  apiBaseHint,
  showApiKey = true,
  showApiBase = true,
  modelInvalid,
  apiKeyInvalid,
  apiBaseInvalid,
  modelRegistration,
  apiKeyRegistration,
  apiBaseRegistration,
}: {
  modelLabel: string;
  apiKeyLabel: string;
  apiBaseLabel: string;
  apiKeyHint: string;
  apiBaseHint: string;
  showApiKey?: boolean;
  showApiBase?: boolean;
  modelInvalid?: boolean;
  apiKeyInvalid?: boolean;
  apiBaseInvalid?: boolean;
  modelRegistration: UseFormRegisterReturn;
  apiKeyRegistration: UseFormRegisterReturn;
  apiBaseRegistration: UseFormRegisterReturn;
}) {
  return (
    <motion.div className="form-grid reveal-form llm-form" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
      <Field label={modelLabel} invalid={modelInvalid}>
        <Input {...modelRegistration} />
      </Field>
      {showApiKey ? (
        <Field label={apiKeyLabel} hint={apiKeyHint} invalid={apiKeyInvalid}>
          <Input {...apiKeyRegistration} type="password" />
        </Field>
      ) : null}
      {showApiBase ? (
        <Field label={apiBaseLabel} hint={apiBaseHint} invalid={apiBaseInvalid}>
          <Input {...apiBaseRegistration} placeholder="https://api.example.com/v1" />
        </Field>
      ) : null}
    </motion.div>
  );
}
