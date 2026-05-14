import { motion } from "framer-motion";

import octoImage from "../../../../assets/octo.png";

function eventClass(kind: DesktopInstallEvent["kind"]) {
  return `install-event install-event-${kind}`;
}

export function InstallProgressScreen({
  title,
  body,
  events,
  busy,
  error,
}: {
  title: string;
  body: string;
  events: DesktopInstallEvent[];
  busy?: boolean;
  error?: string;
}) {
  const visibleEvents = events.filter((event) => event.message.trim()).slice(-10);

  return (
    <motion.section
      className="status-screen install-progress"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.24 }}
    >
      <img className={busy ? "octo status-octo pulse" : "octo status-octo"} src={octoImage} alt="Octopal mascot" />
      <h1>{title}</h1>
      {error ? (
        <div className="status-error" role="alert">
          <strong>{body}</strong>
          <pre>{error}</pre>
        </div>
      ) : (
        <p>{body}</p>
      )}

      {visibleEvents.length > 0 ? (
        <div className="install-events" aria-live="polite">
          {visibleEvents.map((event, index) => (
            <div className={eventClass(event.kind)} key={`${event.kind}-${index}-${event.message}`}>
              <strong>{event.message}</strong>
              {event.detail ? <span>{event.detail}</span> : null}
            </div>
          ))}
        </div>
      ) : null}
    </motion.section>
  );
}
