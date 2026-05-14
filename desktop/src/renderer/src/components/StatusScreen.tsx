import { motion } from "framer-motion";
import type { ReactNode } from "react";

import octoImage from "../../../../assets/octo.png";

export function StatusScreen({
  title,
  body,
  octoAlt,
  busy,
  action,
  errorTitle,
  errorDetail,
  noticeTitle,
  noticeDetail,
}: {
  title: string;
  body: string;
  octoAlt: string;
  busy?: boolean;
  action?: ReactNode;
  errorTitle?: string;
  errorDetail?: string;
  noticeTitle?: string;
  noticeDetail?: string;
}) {
  return (
    <motion.section
      className="status-screen"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.24 }}
    >
      <img className={busy ? "octo status-octo pulse" : "octo status-octo"} src={octoImage} alt={octoAlt} />
      <h1>{title}</h1>
      {body ? <p>{body}</p> : null}
      {noticeTitle || noticeDetail ? (
        <div className="status-notice">
          {noticeTitle ? <strong>{noticeTitle}</strong> : null}
          {noticeDetail ? <span>{noticeDetail}</span> : null}
        </div>
      ) : null}
      {errorTitle || errorDetail ? (
        <div className="status-error" role="alert">
          {errorTitle ? <strong>{errorTitle}</strong> : null}
          {errorDetail ? <pre>{errorDetail}</pre> : null}
        </div>
      ) : null}
      {action ? <div className="status-actions">{action}</div> : null}
    </motion.section>
  );
}
