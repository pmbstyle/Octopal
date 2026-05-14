import { motion } from "framer-motion";
import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { Field, Input, Select } from "../Field";
import { ImageLogo } from "../ImageLogo";
import { StepSection } from "../StepSection";
import { ToggleCard } from "../ToggleCard";
import type { CopyFn } from "../../lib/appTypes";
import { isExistingSecret, type InstallForm } from "../../lib/install";
import { channelLogos } from "../../lib/logos";

const channelCopy = {
  telegram: "Bot token and allowed chats",
  whatsapp: "Linked WhatsApp Web session",
};

export function ChannelStep({
  copy,
  values,
  form,
  errors,
}: {
  copy: CopyFn;
  values: InstallForm;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
}) {
  return (
    <StepSection body={copy("channelBody")}>
      <div className="choice-grid channel-grid">
        <ToggleCard
          active={values.channel === "telegram"}
          icon={<ImageLogo src={channelLogos.telegram} alt="" />}
          title={copy("telegram")}
          body={channelCopy.telegram}
          onClick={() => form.setValue("channel", "telegram")}
        />
        <ToggleCard
          active={values.channel === "whatsapp"}
          icon={<ImageLogo src={channelLogos.whatsapp} alt="" />}
          title={copy("whatsapp")}
          body={channelCopy.whatsapp}
          onClick={() => form.setValue("channel", "whatsapp")}
        />
      </div>
      {values.channel === "telegram" ? (
        <motion.div className="form-grid reveal-form" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
          <Field
            label={copy("telegramToken")}
            hint={isExistingSecret(values.telegramToken) ? copy("configured") : copy("required")}
            invalid={!!errors.telegramToken}
            help={{
              title: copy("telegramTokenHelpTitle"),
              body: [
                <>
                  {copy("telegramTokenHelpBody1Prefix")}{" "}
                  <a className="field-help-link" href="https://t.me/BotFather" target="_blank" rel="noreferrer">
                    @BotFather
                  </a>
                  {copy("telegramTokenHelpBody1Suffix")}
                </>,
                copy("telegramTokenHelpBody2"),
              ],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Input {...form.register("telegramToken")} type="password" placeholder="123456:ABC..." />
          </Field>
          <Field
            label={copy("allowedChatIds")}
            hint="123, 456"
            help={{
              title: copy("allowedChatIdsHelpTitle"),
              body: [
                copy("allowedChatIdsHelpBody1"),
                <>
                  {copy("allowedChatIdsHelpBody2Prefix")}{" "}
                  <a className="field-help-link" href="https://t.me/getmyid_bot" target="_blank" rel="noreferrer">
                    @Getmyid_bot
                  </a>{" "}
                  {copy("allowedChatIdsHelpBody2Suffix")}
                </>,
              ],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Input {...form.register("allowedChatIds")} />
          </Field>
        </motion.div>
      ) : (
        <motion.div className="form-grid reveal-form" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
          <Field
            label={copy("whatsappMode")}
            help={{
              title: copy("whatsappModeHelpTitle"),
              body: [copy("whatsappModeHelpBody1"), copy("whatsappModeHelpBody2")],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Select {...form.register("whatsappMode")}>
              <option value="separate">{copy("separate")}</option>
              <option value="personal">{copy("personal")}</option>
            </Select>
          </Field>
          <Field
            label={copy("whatsappNumbers")}
            hint={copy("required")}
            invalid={!!errors.whatsappAllowedNumbers}
            help={{
              title: copy("whatsappNumbersHelpTitle"),
              body: [copy("whatsappNumbersHelpBody1"), copy("whatsappNumbersHelpBody2")],
              closeLabel: copy("closeHelp"),
            }}
          >
            <Input {...form.register("whatsappAllowedNumbers")} placeholder="+15551234567" />
          </Field>
        </motion.div>
      )}
    </StepSection>
  );
}
