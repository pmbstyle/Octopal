import { FolderOpen } from "lucide-react";
import type { FieldErrors, UseFormReturn } from "react-hook-form";

import { Button } from "../Button";
import { Field, Input } from "../Field";
import { StepSection } from "../StepSection";
import type { CopyFn } from "../../lib/appTypes";
import type { InstallForm } from "../../lib/install";

export function LocationStep({
  copy,
  form,
  errors,
  onChooseInstallDir,
}: {
  copy: CopyFn;
  form: UseFormReturn<InstallForm>;
  errors: FieldErrors<InstallForm>;
  onChooseInstallDir: () => void;
}) {
  return (
    <StepSection body={copy("locationBody")}>
      <div className="path-row clean-path-row">
        <Field label="" invalid={!!errors.installDir}>
          <Input {...form.register("installDir")} placeholder="C:\\Octopal" />
        </Field>
        <Button className="choose-folder-button" type="button" variant="secondary" onClick={onChooseInstallDir}>
          <FolderOpen data-icon="inline-start" />
          {copy("chooseFolder")}
        </Button>
      </div>
    </StepSection>
  );
}
