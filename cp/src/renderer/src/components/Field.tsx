import { useEffect, useId, useState, type InputHTMLAttributes, type ReactNode, type SelectHTMLAttributes } from "react";
import { createPortal } from "react-dom";
import { Info, X } from "lucide-react";

import { cn } from "../lib/cn";

type FieldHelp = {
  title: string;
  body: ReactNode[];
  closeLabel?: string;
};

export function Field({
  label,
  hint,
  children,
  invalid,
  help,
}: {
  label: string;
  hint?: string;
  children: ReactNode;
  invalid?: boolean;
  help?: FieldHelp;
}) {
  const [helpOpen, setHelpOpen] = useState(false);
  const popupId = useId();

  useEffect(() => {
    if (!helpOpen) {
      return undefined;
    }

    function closeOnEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setHelpOpen(false);
      }
    }

    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [helpOpen]);

  return (
    <label className={cn("field", invalid && "field-invalid")}>
      <span className="field-label-row">
        <span className="field-label">{label}</span>
        {help ? (
          <span className="field-help-wrap">
            <button
              type="button"
              className="field-help-button"
              aria-label={help.title}
              aria-expanded={helpOpen}
              aria-controls={popupId}
              onMouseDown={(event) => event.preventDefault()}
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                setHelpOpen((current) => !current);
              }}
            >
              <Info />
            </button>
            {helpOpen
              ? createPortal(
                  <div
                    className="field-help-popup-backdrop"
                    role="presentation"
                    onMouseDown={(event) => {
                      if (event.target === event.currentTarget) {
                        event.preventDefault();
                        setHelpOpen(false);
                      }
                    }}
                  >
                    <section id={popupId} className="field-help-popup" role="dialog" aria-modal="true" aria-label={help.title}>
                      <header className="field-help-popup-head">
                        <h2>{help.title}</h2>
                        <button
                          type="button"
                          className="field-help-close"
                          aria-label={help.closeLabel ?? "Close"}
                          onClick={() => setHelpOpen(false)}
                        >
                          <X />
                        </button>
                      </header>
                      <div className="field-help-popup-body">
                        {help.body.map((line, index) => (
                          <p className="field-help-copy" key={index}>
                            {line}
                          </p>
                        ))}
                      </div>
                    </section>
                  </div>,
                  document.body,
                )
              : null}
          </span>
        ) : null}
      </span>
      {children}
      {hint ? <span className="field-hint">{hint}</span> : null}
    </label>
  );
}

export function Input(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input className="input" {...props} />;
}

export function Select(props: SelectHTMLAttributes<HTMLSelectElement>) {
  return <select className="input select-input" {...props} />;
}
