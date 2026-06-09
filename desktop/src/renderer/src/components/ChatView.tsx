import { Check, Paperclip, Send, X } from "lucide-react";
import {
  type ClipboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { Button } from "./Button";

type ChatViewProps = {
  active: boolean;
  installDir: string;
};

type ChatItemKind = "message" | "event" | "approval";

type ChatItem = {
  id: string;
  kind: ChatItemKind;
  type: string;
  role: string;
  direction: string;
  channel: string;
  text: string;
  createdAt: string;
  meta?: Record<string, unknown>;
  technical?: boolean;
  attachments?: DesktopChatAttachment[];
  intentId?: string;
  raw?: DesktopChatEvent;
};

const initialStatus: DesktopChatConnectionStatus = {
  ok: true,
  state: "idle",
  detail: "Chat is idle.",
};

const ACTIVITY_TIMEOUT_MS = 30_000;
const THINKING_STATUS_TEXT = "Octo is thinking";

function stringValue(value: unknown, fallback = ""): string {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function recordArray(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value)
    ? value
        .filter((item) => item !== null && typeof item === "object" && !Array.isArray(item))
        .map((item) => item as Record<string, unknown>)
    : [];
}

function eventMeta(event: DesktopChatEvent): Record<string, unknown> {
  const meta = recordValue(event.meta);
  return Object.keys(meta).length > 0 ? meta : recordValue(event.payload);
}

function attachmentNamesFromPaths(paths: unknown): string[] {
  if (!Array.isArray(paths)) {
    return [];
  }
  return paths
    .map((path) => stringValue(path).split(/[\\/]/).filter(Boolean).at(-1) ?? "")
    .filter(Boolean);
}

function attachmentNamesText(names: string[]): string {
  if (names.length === 0) {
    return "";
  }
  return `Attached: ${names.join(", ")}`;
}

function isImageAttachment(attachment: DesktopChatAttachment): boolean {
  return /\.(avif|gif|jpe?g|png|webp)$/i.test(attachment.name);
}

function fileAttachmentFromEvent(event: DesktopChatEvent): DesktopChatAttachment[] {
  const mimeType = stringValue(event.mime_type).toLowerCase();
  const name = stringValue(event.name, "attachment");
  const data = stringValue(event.data);
  const previewUrl =
    mimeType.startsWith("image/") && data
      ? `data:${mimeType};base64,${data}`
      : undefined;
  return [
    {
      path: stringValue(event.path),
      name,
      sizeBytes: Number(event.size_bytes) || 0,
      previewUrl,
    },
  ];
}

function eventText(event: DesktopChatEvent): string {
  const display = recordValue(event.display);
  const intent = recordValue(event.intent);
  const meta = recordValue(event.meta);
  const attachmentsText = attachmentNamesText(
    attachmentNamesFromPaths(meta.saved_file_paths),
  );
  return (
    stringValue(event.text) ||
    stringValue(event.message) ||
    stringValue(display.message) ||
    stringValue(display.summary) ||
    stringValue(intent.summary) ||
    stringValue(event.state) ||
    attachmentsText ||
    stringValue(event.type, "Event")
  );
}

function eventChannel(event: DesktopChatEvent): string {
  return (
    stringValue(event.channel) ||
    stringValue(recordValue(event.meta).channel) ||
    "runtime"
  );
}

function isTechnicalEvent(event: DesktopChatEvent): boolean {
  const type = stringValue(event.type);
  return type === "progress";
}

function isWebSocketTakeoverNotice(text: string): boolean {
  return text
    .toLowerCase()
    .includes("another websocket client connected and took over this session");
}

function workerSnapshotName(worker: Record<string, unknown>): string {
  return (
    stringValue(worker.template_id) ||
    stringValue(worker.worker_template_id) ||
    stringValue(worker.template_name) ||
    stringValue(worker.name) ||
    stringValue(worker.id, "worker")
  );
}

function workerPlanBindingText(worker: Record<string, unknown>): string {
  const binding = recordValue(worker.plan_binding);
  if (Object.keys(binding).length === 0) {
    return "";
  }
  const label =
    stringValue(binding.title) ||
    stringValue(binding.step_id) ||
    stringValue(binding.run_id) ||
    "plan step";
  const detail = [stringValue(binding.step_id), stringValue(binding.status)]
    .filter(Boolean)
    .join(" / ");
  return detail ? `${label} (${detail})` : label;
}

function workerSnapshotText(worker: Record<string, unknown>): string {
  const name = workerSnapshotName(worker);
  const status = stringValue(worker.status, "unknown").toLowerCase();
  const planText = workerPlanBindingText(worker);
  const suffix = planText ? ` · plan ${planText}` : "";
  if (status === "running") {
    return `${name} worker is running${suffix}`;
  }
  if (status === "waiting_for_children") {
    return `${name} worker is waiting for child workers${suffix}`;
  }
  if (status === "awaiting_instruction") {
    return `${name} worker is awaiting instruction${suffix}`;
  }
  if (["started", "completed", "failed", "stopped"].includes(status)) {
    return `${name} worker ${status}${suffix}`;
  }
  return `${name} worker status: ${status}${suffix}`;
}

function activityStatusText(text: string): string {
  return text.trim().replace(/\.+$/u, "");
}

function activityTextFromWorkerSnapshot(
  worker: Record<string, unknown>,
): string {
  return workerSnapshotText(worker);
}

function activityTextFromEvent(event: DesktopChatEvent): string {
  const type = stringValue(event.type);
  if (type === "progress") {
    return eventText(event);
  }
  if (type === "worker_event") {
    const payload = recordValue(event.payload);
    return (
      stringValue(event.text) ||
      stringValue(event.message) ||
      stringValue(payload.message) ||
      stringValue(payload.summary) ||
      stringValue(event.event)
    );
  }
  if (type === "workers_snapshot") {
    const activeWorker = recordArray(event.workers).find((worker) => {
      const status = stringValue(worker.status).toLowerCase();
      return [
        "running",
        "started",
        "waiting_for_children",
        "awaiting_instruction",
      ].includes(status);
    });
    return activeWorker ? activityTextFromWorkerSnapshot(activeWorker) : "";
  }
  return "";
}

function chatItemFromEvent(
  event: DesktopChatEvent,
  index: number,
): ChatItem | null {
  const type = stringValue(event.type, "event");
  const createdAt = stringValue(event.created_at) || new Date().toISOString();
  const eventId = stringValue(event.id);
  const baseId =
    eventId || `${Date.now()}-${index}-${Math.random().toString(16).slice(2)}`;
  const text = eventText(event);

  if (type === "chat_message" || type === "message") {
    return {
      id: baseId,
      kind: "message",
      type,
      role: stringValue(event.role, "assistant"),
      direction: stringValue(event.direction, "outbound"),
      channel: eventChannel(event),
      text,
      createdAt,
      meta: recordValue(event.meta),
      raw: event,
    };
  }

  if (type === "approval_request") {
    const intent = recordValue(event.intent);
    return {
      id: baseId,
      kind: "approval",
      type,
      role: "system",
      direction: "inbound",
      channel: "approval",
      text,
      createdAt,
      meta: recordValue(event.display),
      intentId: stringValue(intent.id),
      raw: event,
    };
  }

  if (
    ["workers_snapshot", "pong", "typing", "worker_event", "progress"].includes(
      type,
    )
  ) {
    return null;
  }

  if (type === "warning" && isWebSocketTakeoverNotice(text)) {
    return null;
  }

  if (["file", "warning", "error"].includes(type)) {
    return {
      id: baseId,
      kind: "event",
      type,
      role: "system",
      direction: "event",
      channel: eventChannel(event),
      text,
      createdAt,
      meta: eventMeta(event),
      technical: isTechnicalEvent(event),
      attachments: type === "file" ? fileAttachmentFromEvent(event) : undefined,
      raw: event,
    };
  }

  return null;
}

function chatItemsFromEvent(
  event: DesktopChatEvent,
  index: number,
): ChatItem[] {
  const type = stringValue(event.type);
  if (type === "chat_history") {
    return recordArray(event.messages)
      .map((message, messageIndex) =>
        chatItemFromEvent(message as DesktopChatEvent, index + messageIndex),
      )
      .filter((item): item is ChatItem => item !== null);
  }
  if (type === "workers_snapshot") {
    return [];
  }
  const item = chatItemFromEvent(event, index);
  return item ? [item] : [];
}

function mergeUniqueItems(current: ChatItem[], next: ChatItem[]): ChatItem[] {
  if (next.length === 0) {
    return current;
  }
  const nextIds = new Set(next.map((item) => item.id));
  return [...current.filter((item) => !nextIds.has(item.id)), ...next].slice(-300);
}

function localUserMessage(
  text: string,
  attachments: DesktopChatAttachment[],
): ChatItem {
  const attachmentText = attachmentNamesText(
    attachments.map((attachment) => attachment.name),
  );
  const displayText = [text, attachmentText].filter(Boolean).join("\n\n");
  return {
    id: `local-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    kind: "message",
    type: "local_message",
    role: "user",
    direction: "outbound",
    channel: "desktop",
    text: displayText,
    attachments,
    createdAt: new Date().toISOString(),
  };
}

function isDuplicateLocalEcho(item: ChatItem, event: DesktopChatEvent): boolean {
  if (item.type !== "local_message") {
    return false;
  }
  const type = stringValue(event.type);
  if (type !== "chat_message") {
    return false;
  }
  return (
    stringValue(event.role) === "user" &&
    stringValue(event.channel) === "desktop" &&
    (eventText(event) === item.text ||
      item.text.startsWith(`${eventText(event)}\n\nAttached:`))
  );
}

function approvalResolution(item: ChatItem): string {
  return stringValue(item.meta?.resolved);
}

function formatTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function senderLabel(item: ChatItem): string {
  return item.role === "user" ? "You" : "Octo";
}

function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () =>
      typeof reader.result === "string"
        ? resolve(reader.result)
        : reject(new Error("Clipboard image could not be read."));
    reader.onerror = () => reject(reader.error ?? new Error("Clipboard image could not be read."));
    reader.readAsDataURL(file);
  });
}

function MessageMarkdown({ text }: { text: string }) {
  return (
    <div className="chat-markdown">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{text}</ReactMarkdown>
    </div>
  );
}

export function ChatView({ active, installDir }: ChatViewProps) {
  const [status, setStatus] =
    useState<DesktopChatConnectionStatus>(initialStatus);
  const [items, setItems] = useState<ChatItem[]>([]);
  const [draft, setDraft] = useState("");
  const [attachments, setAttachments] = useState<DesktopChatAttachment[]>([]);
  const [sendError, setSendError] = useState("");
  const [sending, setSending] = useState(false);
  const [thinking, setThinking] = useState(false);
  const [activityText, setActivityText] = useState("");
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const eventCount = useRef(0);
  const activityTimeoutRef = useRef<number | null>(null);

  const connected = status.state === "connected";
  const canSend =
    connected && !sending && (draft.trim() || attachments.length > 0);
  const sortedItems = useMemo(
    () =>
      items
        .slice(-260)
        .filter(
          (item) =>
            !(item.type === "warning" && isWebSocketTakeoverNotice(item.text)),
        ),
    [items],
  );

  const connect = useCallback(async () => {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    setSendError("");
    try {
      const next = await window.octopalDesktop.connectChat(installDir);
      setStatus(next);
    } catch (error) {
      setStatus({
        ok: false,
        state: "error",
        detail:
          error instanceof Error ? error.message : "Unable to connect chat.",
      });
    }
  }, [installDir]);

  const clearActivityTimeout = useCallback(() => {
    if (activityTimeoutRef.current !== null) {
      window.clearTimeout(activityTimeoutRef.current);
      activityTimeoutRef.current = null;
    }
  }, []);

  const clearActivity = useCallback(() => {
    clearActivityTimeout();
    setThinking(false);
    setActivityText("");
  }, [clearActivityTimeout]);

  const scheduleActivityTimeout = useCallback(() => {
    clearActivityTimeout();
    activityTimeoutRef.current = window.setTimeout(() => {
      setThinking(false);
      setActivityText("");
      activityTimeoutRef.current = null;
    }, ACTIVITY_TIMEOUT_MS);
  }, [clearActivityTimeout]);

  const showActivity = useCallback(
    (text: string) => {
      setActivityText(activityStatusText(text) || THINKING_STATUS_TEXT);
      setThinking(true);
      scheduleActivityTimeout();
    },
    [scheduleActivityTimeout],
  );

  useEffect(() => {
    if (!window.octopalDesktop || !installDir) {
      return;
    }

    const unsubscribeStatus = window.octopalDesktop.onChatStatus(setStatus);
    const unsubscribeEvent = window.octopalDesktop.onChatEvent((event) => {
      if (stringValue(event.type) === "typing") {
        if (event.active) {
          setActivityText((current) => current || THINKING_STATUS_TEXT);
          setThinking(true);
          scheduleActivityTimeout();
        } else {
          setThinking(false);
        }
        return;
      }

      if (stringValue(event.type) === "approval_result") {
        const intentId = stringValue(event.intent_id);
        if (!intentId) {
          return;
        }
        const resolved = Boolean(event.ok)
          ? Boolean(event.approved)
            ? "approved"
            : "denied"
          : "failed";
        setItems((current) =>
          current.map((item) =>
            item.intentId === intentId
              ? {
                  ...item,
                  meta: {
                    ...(item.meta ?? {}),
                    resolved,
                    approval_result_message: stringValue(event.message),
                  },
                }
              : item,
          ),
        );
        if (!event.ok) {
          setSendError(
            stringValue(event.message, "Approval request is no longer pending."),
          );
        }
        return;
      }

      eventCount.current += 1;
      const activity = activityTextFromEvent(event);
      if (activity) {
        showActivity(activity);
      }
      const nextItems = chatItemsFromEvent(event, eventCount.current);
      if (nextItems.length === 0) {
        return;
      }
      if (
        nextItems.some(
          (item) => item.role === "assistant" || item.type === "error",
        )
      ) {
        clearActivity();
      }
      setItems((current) => {
        if (
          nextItems.length === 1 &&
          current.some((existing) => isDuplicateLocalEcho(existing, event))
        ) {
          return current;
        }
        if (stringValue(event.type) === "chat_history") {
          const nextIds = new Set(nextItems.map((item) => item.id));
          return [
            ...nextItems,
            ...current.filter((item) => !nextIds.has(item.id)),
          ].slice(-300);
        }
        return mergeUniqueItems(current, nextItems);
      });
    });

    void connect();

    return () => {
      unsubscribeStatus();
      unsubscribeEvent();
      clearActivityTimeout();
    };
  }, [
    clearActivity,
    clearActivityTimeout,
    connect,
    installDir,
    scheduleActivityTimeout,
    showActivity,
  ]);

  useEffect(() => {
    if (!active) {
      return;
    }
    scrollRef.current?.scrollTo({
      top: scrollRef.current.scrollHeight,
      behavior: "smooth",
    });
  }, [active, sortedItems.length, thinking, activityText]);

  function appendAttachments(next: DesktopChatAttachment[]): void {
    setAttachments((current) => {
      const byPath = new Map<string, DesktopChatAttachment>();
      for (const attachment of [...current, ...next]) {
        byPath.set(attachment.path, attachment);
      }
      return Array.from(byPath.values()).slice(0, 8);
    });
  }

  async function chooseFiles(): Promise<void> {
    if (!window.octopalDesktop || !installDir || sending) {
      return;
    }
    setSendError("");
    try {
      const chosen = await window.octopalDesktop.chooseChatFiles(installDir);
      appendAttachments(chosen);
    } catch (error) {
      setSendError(
        error instanceof Error ? error.message : "Unable to attach files.",
      );
    }
  }

  async function handlePaste(event: ClipboardEvent<HTMLTextAreaElement>): Promise<void> {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    const files = Array.from(event.clipboardData.files).filter((file) =>
      file.type.startsWith("image/"),
    );
    if (files.length === 0) {
      return;
    }

    event.preventDefault();
    setSendError("");
    try {
      const pasted = await Promise.all(
        files.slice(0, 8).map(async (file) =>
          window.octopalDesktop!.savePastedChatImage(installDir, {
            name: file.name || "pasted-image.png",
            mimeType: file.type || "image/png",
            dataUrl: await readFileAsDataUrl(file),
          }),
        ),
      );
      appendAttachments(pasted);
    } catch (error) {
      setSendError(
        error instanceof Error ? error.message : "Unable to paste image.",
      );
    }
  }

  function removeAttachment(path: string): void {
    setAttachments((current) =>
      current.filter((attachment) => attachment.path !== path),
    );
  }

  async function sendMessage(): Promise<void> {
    const text = draft.trim();
    if (!window.octopalDesktop || !canSend) {
      return;
    }

    setSending(true);
    setSendError("");
    try {
      await window.octopalDesktop.sendChatMessage({
        text,
        attachments,
      });
      setItems((current) =>
        [...current, localUserMessage(text, attachments)].slice(-300),
      );
      setDraft("");
      setAttachments([]);
      showActivity(THINKING_STATUS_TEXT);
    } catch (error) {
      setThinking(false);
      setSendError(
        error instanceof Error ? error.message : "Unable to send message.",
      );
    } finally {
      setSending(false);
    }
  }

  async function answerApproval(
    intentId: string,
    approved: boolean,
  ): Promise<void> {
    if (!window.octopalDesktop || !intentId) {
      return;
    }
    try {
      await window.octopalDesktop.sendChatApprovalResponse(intentId, approved);
      setItems((current) =>
        current.map((item) =>
          item.intentId === intentId
            ? {
                ...item,
                meta: {
                  ...(item.meta ?? {}),
                  resolved: approved ? "approved" : "denied",
                },
              }
            : item,
        ),
      );
    } catch (error) {
      setSendError(
        error instanceof Error ? error.message : "Unable to answer approval.",
      );
    }
  }

  return (
    <section
      className={active ? "chat-view" : "chat-view chat-view-hidden"}
      aria-label="Desktop chat"
    >
      <div ref={scrollRef} className="chat-transcript">
        {sortedItems.length === 0 && !activityText && !thinking ? (
          <div className="chat-empty">
            <h2>No chat events yet</h2>
            <p>Waiting for live activity.</p>
          </div>
        ) : null}

        {sortedItems.map((item) => (
          <article
            key={item.id}
            className={
              item.kind === "message"
                ? `chat-bubble chat-bubble-${item.role === "user" ? "user" : "assistant"}`
                : `chat-event chat-event-${item.type}${item.technical ? " chat-event-technical" : ""}`
            }
          >
            {item.technical ? null : (
              <div className="chat-item-meta">
                <span>{senderLabel(item)}</span>
                {item.kind === "message" ? null : <span>{item.type}</span>}
                <span>{formatTime(item.createdAt)}</span>
              </div>
            )}
            <MessageMarkdown text={item.text} />
            {item.attachments?.some(isImageAttachment) ? (
              <div className="chat-image-previews">
                {item.attachments.filter(isImageAttachment).map((attachment) =>
                  attachment.previewUrl ? (
                    <img
                      key={attachment.path || attachment.name}
                      src={attachment.previewUrl}
                      alt={attachment.name}
                    />
                  ) : null,
                )}
              </div>
            ) : null}
            {item.kind === "approval" && approvalResolution(item) ? (
              <p className="chat-approval-resolution">
                {approvalResolution(item) === "approved"
                  ? "Approved"
                  : approvalResolution(item) === "denied"
                    ? "Denied"
                    : stringValue(
                        item.meta?.approval_result_message,
                        "Approval request is no longer pending.",
                      )}
              </p>
            ) : item.kind === "approval" && item.intentId ? (
              <div className="chat-approval-actions">
                <Button
                  type="button"
                  variant="success"
                  onClick={() => void answerApproval(item.intentId ?? "", true)}
                >
                  <Check data-icon="inline-start" />
                  Approve
                </Button>
                <Button
                  type="button"
                  variant="danger"
                  onClick={() =>
                    void answerApproval(item.intentId ?? "", false)
                  }
                >
                  <X data-icon="inline-start" />
                  Deny
                </Button>
              </div>
            ) : null}
          </article>
        ))}
        {thinking || activityText ? (
          <div className="chat-activity" aria-live="polite">
            <span className="chat-activity-text">
              {activityText || THINKING_STATUS_TEXT}
            </span>
          </div>
        ) : null}
      </div>

      <footer className="chat-composer">
        <div className="chat-composer-row">
          <Button
            type="button"
            variant="ghost"
            className="chat-attach-button"
            disabled={!connected || sending}
            onClick={() => void chooseFiles()}
            title="Attach files"
            aria-label="Attach files"
          >
            <Paperclip />
          </Button>
          <textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onPaste={(event) => {
              void handlePaste(event);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                void sendMessage();
              }
            }}
            placeholder={
              connected
                ? "Message Octopal from desktop"
                : "Connect to chat before sending"
            }
            disabled={!connected || sending}
          />
          <Button
            type="button"
            disabled={!canSend}
            onClick={() => void sendMessage()}
          >
            <Send data-icon="inline-start" />
            Send
          </Button>
        </div>
        {attachments.length > 0 ? (
          <div className="chat-attachments" aria-label="Attached files">
            {attachments.map((attachment) => (
              <span key={attachment.path} className="chat-attachment">
                <span>{attachment.name}</span>
                <button
                  type="button"
                  onClick={() => removeAttachment(attachment.path)}
                  aria-label={`Remove ${attachment.name}`}
                >
                  <X />
                </button>
              </span>
            ))}
          </div>
        ) : null}
        {sendError ? (
          <p className="chat-send-error">{sendError}</p>
        ) : null}
      </footer>
    </section>
  );
}
