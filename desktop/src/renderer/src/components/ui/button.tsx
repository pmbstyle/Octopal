import type { ButtonHTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/cn";

type ButtonVariant =
  | "default"
  | "primary"
  | "secondary"
  | "ghost"
  | "outline"
  | "success"
  | "danger";
type ButtonSize = "default" | "sm" | "lg" | "icon";

export function Button({
  children,
  className,
  size = "default",
  variant = "default",
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
  size?: ButtonSize;
  variant?: ButtonVariant;
}) {
  return (
    <button
      data-size={size}
      data-slot="button"
      data-variant={variant === "primary" ? "default" : variant}
      className={cn("ui-button", className)}
      {...props}
    >
      {children}
    </button>
  );
}
