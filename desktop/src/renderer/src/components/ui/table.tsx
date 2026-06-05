import type {
  ButtonHTMLAttributes,
  HTMLAttributes,
  ReactNode,
} from "react";

import { cn } from "../../lib/cn";

export function Table({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="table-container" className={cn("ui-table", className)} {...props}>
      {children}
    </div>
  );
}

export function TableRow({
  as = "div",
  children,
  className,
  ...props
}: (HTMLAttributes<HTMLDivElement> | ButtonHTMLAttributes<HTMLButtonElement>) & {
  as?: "div" | "button";
  children: ReactNode;
}) {
  if (as === "button") {
    return (
      <button
        data-slot="table-row"
        className={cn("ui-table-row", className)}
        {...(props as ButtonHTMLAttributes<HTMLButtonElement>)}
      >
        {children}
      </button>
    );
  }

  return (
    <div
      data-slot="table-row"
      className={cn("ui-table-row", className)}
      {...(props as HTMLAttributes<HTMLDivElement>)}
    >
      {children}
    </div>
  );
}

export function TableHead({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="table-head" className={cn("ui-table-head", className)} {...props}>
      {children}
    </div>
  );
}

export function TableCell({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLSpanElement> & { children: ReactNode }) {
  return (
    <span data-slot="table-cell" className={cn("ui-table-cell", className)} {...props}>
      {children}
    </span>
  );
}
