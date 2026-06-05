import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/cn";

export type BadgeVariant =
  | "default"
  | "secondary"
  | "outline"
  | "success"
  | "warning"
  | "danger"
  | "live";

export function Badge({
  children,
  className,
  variant = "default",
  ...props
}: HTMLAttributes<HTMLSpanElement> & {
  children: ReactNode;
  variant?: BadgeVariant;
}) {
  return (
    <span
      data-slot="badge"
      data-variant={variant}
      className={cn("ui-badge", className)}
      {...props}
    >
      {children}
    </span>
  );
}
