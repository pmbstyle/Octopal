import { CheckCircle2, Loader2, RefreshCw, Smartphone, XCircle } from "lucide-react";
import { motion } from "framer-motion";
import QRCode from "qrcode";
import { useEffect, useState } from "react";

import octoImage from "../../../../assets/octo.png";
import type { CopyFn } from "../lib/appTypes";
import { Button } from "./Button";

export function WhatsAppLinkScreen({
  copy,
  status,
  busy,
  error,
  onRefresh,
  onContinue,
  onSkip,
}: {
  copy: CopyFn;
  status: DesktopWhatsAppLinkStatus | null;
  busy: boolean;
  error: string;
  onRefresh: () => void;
  onContinue: () => void;
  onSkip: () => void;
}) {
  const linked = status?.linked || status?.connected;
  const qrPayload = status?.qr?.trim() || "";
  const terminalQr = status?.terminal?.trim() || "";
  const hasQr = !!qrPayload || !!terminalQr;
  const title = linked ? copy("whatsappLinkedTitle") : copy("whatsappLinkTitle");
  const body = linked ? copy("whatsappLinkedBody") : hasQr ? copy("whatsappLinkBody") : copy("whatsappQrWaiting");
  const [qrDataUrl, setQrDataUrl] = useState("");

  useEffect(() => {
    if (!qrPayload) {
      setQrDataUrl("");
      return;
    }

    let cancelled = false;
    void QRCode.toDataURL(qrPayload, {
      errorCorrectionLevel: "M",
      margin: 2,
      scale: 8,
      color: {
        dark: "#111827",
        light: "#ffffff",
      },
    })
      .then((url) => {
        if (!cancelled) {
          setQrDataUrl(url);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setQrDataUrl("");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [qrPayload]);

  return (
    <motion.section
      className="status-screen whatsapp-link-screen"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.24 }}
    >
      <img className={busy ? "octo status-octo pulse" : "octo status-octo"} src={octoImage} alt="Octopal mascot" />
      <h1>{title}</h1>
      <p>{body}</p>

      <div className={linked ? "whatsapp-status whatsapp-status-linked" : "whatsapp-status"}>
        {linked ? <CheckCircle2 /> : busy ? <Loader2 className="spin-icon" /> : <Smartphone />}
        <div>
          <strong>{linked ? copy("whatsappConnected") : copy("whatsappWaiting")}</strong>
          <span>{status?.self || status?.detail || copy("whatsappWaitingDetail")}</span>
        </div>
      </div>

      {error ? (
        <div className="status-error" role="alert">
          <strong>{copy("whatsappLinkFailed")}</strong>
          <pre>{error}</pre>
        </div>
      ) : null}

      {!linked && qrDataUrl ? (
        <div className="whatsapp-qr-image-wrap">
          <img className="whatsapp-qr-image" src={qrDataUrl} alt={copy("whatsappQrAlt")} />
        </div>
      ) : null}

      {!linked && !qrDataUrl && terminalQr ? (
        <pre className="whatsapp-qr" aria-label={copy("whatsappQrAlt")}>{terminalQr}</pre>
      ) : null}

      {!linked && !qrDataUrl && !terminalQr && !error ? <p className="whatsapp-wait-copy">{copy("whatsappQrWaiting")}</p> : null}

      <div className="status-actions whatsapp-actions">
        <Button type="button" variant="ghost" onClick={onRefresh} disabled={busy}>
          <RefreshCw data-icon="inline-start" />
          {copy("refresh")}
        </Button>
        {linked ? (
          <Button type="button" variant="success" onClick={onContinue}>
            <CheckCircle2 data-icon="inline-start" />
            {copy("continue")}
          </Button>
        ) : (
          <Button type="button" variant="secondary" onClick={onSkip}>
            <XCircle data-icon="inline-start" />
            {copy("skipForNow")}
          </Button>
        )}
      </div>
    </motion.section>
  );
}
