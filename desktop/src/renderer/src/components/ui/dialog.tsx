import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/cn";

export function DialogOverlay({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLDivElement> & { children: ReactNode }) {
  return (
    <div data-slot="dialog-overlay" className={cn("ui-dialog-overlay", className)} {...props}>
      {children}
    </div>
  );
}

export function DialogContent({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLElement> & { children: ReactNode }) {
  return (
    <section
      data-slot="dialog-content"
      className={cn("ui-dialog-content", className)}
      {...props}
    >
      {children}
    </section>
  );
}

export function DialogHeader({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLElement> & { children: ReactNode }) {
  return (
    <header data-slot="dialog-header" className={cn("ui-dialog-header", className)} {...props}>
      {children}
    </header>
  );
}

export function DialogFooter({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLElement> & { children: ReactNode }) {
  return (
    <footer data-slot="dialog-footer" className={cn("ui-dialog-footer", className)} {...props}>
      {children}
    </footer>
  );
}

export function DialogTitle({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLHeadingElement> & { children: ReactNode }) {
  return (
    <h2 data-slot="dialog-title" className={cn("ui-dialog-title", className)} {...props}>
      {children}
    </h2>
  );
}

export function DialogDescription({
  children,
  className,
  ...props
}: HTMLAttributes<HTMLParagraphElement> & { children: ReactNode }) {
  return (
    <p
      data-slot="dialog-description"
      className={cn("ui-dialog-description", className)}
      {...props}
    >
      {children}
    </p>
  );
}
