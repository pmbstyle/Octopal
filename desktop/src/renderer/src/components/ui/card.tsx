import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/cn";

export function Card({
  children,
  className,
  size = "default",
  ...props
}: HTMLAttributes<HTMLDivElement> & {
  children: ReactNode;
  size?: "default" | "sm";
}) {
  return (
    <div
      data-size={size}
      data-slot="card"
      className={cn("ui-card", className)}
      {...props}
    >
      {children}
    </div>
  );
}

export function CardHeader({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="card-header" className={cn("ui-card-header", className)} {...props}>
      {children}
    </div>
  );
}

export function CardTitle({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="card-title" className={cn("ui-card-title", className)} {...props}>
      {children}
    </div>
  );
}

export function CardDescription({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div
      data-slot="card-description"
      className={cn("ui-card-description", className)}
      {...props}
    >
      {children}
    </div>
  );
}

export function CardAction({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="card-action" className={cn("ui-card-action", className)} {...props}>
      {children}
    </div>
  );
}

export function CardContent({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="card-content" className={cn("ui-card-content", className)} {...props}>
      {children}
    </div>
  );
}
