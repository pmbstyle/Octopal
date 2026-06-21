import type { ReactNode } from "react";

export function DashboardHeader({
  title,
  description,
  octo,
}: {
  title: string;
  description: string;
  octo: {
    avatar: ReactNode;
    title: string;
    detail: string;
    state: string;
    statusClassName: string;
  };
}) {
  return (
    <header className="dashboard-workspace-header">
      <div className="dashboard-workspace-title">
        <span>{description}</span>
        <h1>{title}</h1>
      </div>
      <OctoStatusWidget {...octo} />
    </header>
  );
}

function OctoStatusWidget({
  avatar,
  title,
  detail,
  state,
  statusClassName,
}: {
  avatar: ReactNode;
  title: string;
  detail: string;
  state: string;
  statusClassName: string;
}) {
  return (
    <div className="dashboard-header-octo">
      <span className="dashboard-header-octo-avatar">{avatar}</span>
      <div className="dashboard-header-octo-copy">
        <strong title={title}>{title}</strong>
        <span title={detail}>{detail}</span>
      </div>
      <span className={`${statusClassName} dashboard-header-status`}>{state}</span>
    </div>
  );
}
