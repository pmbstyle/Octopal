import { type ReactNode } from "react";

import { cn } from "../lib/cn";

export function ToggleCard({
  active,
  title,
  body,
  icon,
  onClick,
}: {
  active: boolean;
  title: string;
  body: string;
  icon: ReactNode;
  onClick: () => void;
}) {
  return (
    <button className={cn("toggle-card", active && "toggle-card-active")} type="button" onClick={onClick}>
      <span className="toggle-icon">{icon}</span>
      <span>
        <strong>{title}</strong>
        <small>{body}</small>
      </span>
    </button>
  );
}
