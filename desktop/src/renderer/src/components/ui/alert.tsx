import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/cn";

type AlertVariant = "default" | "warning" | "danger";

export function Alert({
  children,
  className,
  variant = "default",
  ...props
}: HTMLAttributes<HTMLDivElement> & {
  children: ReactNode;
  variant?: AlertVariant;
}) {
  return (
    <div
      data-slot="alert"
      data-variant={variant}
      className={cn("ui-alert", className)}
      {...props}
    >
      {children}
    </div>
  );
}

export function AlertTitle({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="alert-title" className={cn("ui-alert-title", className)} {...props}>
      {children}
    </div>
  );
}

export function AlertDescription({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div
      data-slot="alert-description"
      className={cn("ui-alert-description", className)}
      {...props}
    >
      {children}
    </div>
  );
}
