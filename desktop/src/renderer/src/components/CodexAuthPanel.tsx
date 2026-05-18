import { AlertCircle, CheckCircle2, LogIn, LogOut, RefreshCw } from "lucide-react";

import type { CopyFn } from "../lib/appTypes";
import { Button } from "./Button";

export function CodexAuthPanel({
  copy,
  status,
  busy,
  onAuthorize,
  onRefresh,
  onDisconnect,
}: {
  copy: CopyFn;
  status: DesktopCodexAuthStatus | null;
  busy: boolean;
  onAuthorize: () => void;
  onRefresh: () => void;
  onDisconnect: () => void;
}) {
  const connected = status?.connected === true;
  const unavailable = status?.available === false;
  const title = unavailable
    ? copy("codexUnavailable")
    : connected
      ? status?.accountLabel || copy("codexConnected")
      : copy("codexNotConnected");
  const detail = status?.error || (unavailable ? copy("codexUnavailableBody") : copy("codexAuthBody"));

  const toneClass = connected
    ? "codex-auth-panel codex-auth-ready"
    : unavailable || status?.error
      ? "codex-auth-panel codex-auth-error"
      : "codex-auth-panel";

  return (
    <div className={toneClass}>
      <div className="codex-auth-status">
        {connected ? <CheckCircle2 /> : <AlertCircle />}
        <div>
          <strong>{title}</strong>
          <span>{detail}</span>
        </div>
      </div>
      <div className="codex-auth-actions">
        <Button type="button" variant={connected ? "secondary" : "primary"} disabled={busy || unavailable} onClick={onAuthorize}>
          <LogIn data-icon="inline-start" />
          {connected ? copy("codexReauthorize") : copy("codexAuthorize")}
        </Button>
        <Button type="button" variant="ghost" disabled={busy} onClick={onRefresh}>
          <RefreshCw data-icon="inline-start" />
          {copy("refresh")}
        </Button>
        {connected ? (
          <Button type="button" variant="ghost" disabled={busy} onClick={onDisconnect}>
            <LogOut data-icon="inline-start" />
            {copy("codexDisconnect")}
          </Button>
        ) : null}
      </div>
    </div>
  );
}
