import { motion } from "framer-motion";
import type { UseFormRegisterReturn } from "react-hook-form";

import { Field, Input } from "./Field";

export function LlmForm({
  modelLabel,
  apiKeyLabel,
  apiBaseLabel,
  apiKeyHint,
  apiBaseHint,
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
      <Field label={apiKeyLabel} hint={apiKeyHint} invalid={apiKeyInvalid}>
        <Input {...apiKeyRegistration} type="password" />
      </Field>
      <Field label={apiBaseLabel} hint={apiBaseHint} invalid={apiBaseInvalid}>
        <Input {...apiBaseRegistration} placeholder="https://api.example.com/v1" />
      </Field>
    </motion.div>
  );
}
