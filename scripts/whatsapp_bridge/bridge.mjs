import fs from "node:fs/promises";
import path from "node:path";
import http from "node:http";
import { URL } from "node:url";
import * as baileys from "@whiskeysockets/baileys";
import pino from "pino";
import QRCode from "qrcode-terminal";

const makeWASocket =
  typeof baileys.makeWASocket === "function"
    ? baileys.makeWASocket
    : typeof baileys.default === "function"
      ? baileys.default
      : typeof baileys.default?.makeWASocket === "function"
        ? baileys.default.makeWASocket
        : null;
const DisconnectReason = baileys.DisconnectReason ?? baileys.default?.DisconnectReason;
const fetchLatestBaileysVersion =
  baileys.fetchLatestBaileysVersion ?? baileys.default?.fetchLatestBaileysVersion;
const useMultiFileAuthState = baileys.useMultiFileAuthState ?? baileys.default?.useMultiFileAuthState;
const downloadMediaMessage =
  baileys.downloadMediaMessage ?? baileys.default?.downloadMediaMessage;
const getContentType =
  baileys.getContentType ?? baileys.default?.getContentType;
const bridgeLogger = pino({ level: "silent" });

if (
  typeof makeWASocket !== "function" ||
  typeof fetchLatestBaileysVersion !== "function" ||
  typeof useMultiFileAuthState !== "function"
) {
  throw new TypeError("Unsupported @whiskeysockets/baileys module shape. Reinstall bridge dependencies.");
}

const host = process.env.OCTOPAL_WHATSAPP_BRIDGE_HOST || "127.0.0.1";
const port = Number(process.env.OCTOPAL_WHATSAPP_BRIDGE_PORT || "8765");
const authDir = process.env.OCTOPAL_WHATSAPP_AUTH_DIR || path.resolve("auth");
const callbackUrl = (process.env.OCTOPAL_WHATSAPP_CALLBACK_URL || "").trim();
const callbackToken = (process.env.OCTOPAL_WHATSAPP_CALLBACK_TOKEN || "").trim();

let sock = null;
let latestQr = "";
let latestQrTerminal = "";
let connected = false;
let linked = false;
let selfId = "";
let reconnectTimer = null;
const outboundMessageIds = new Set();
const imageExtensions = new Set([".jpg", ".jpeg", ".png", ".webp", ".bmp"]);
const videoExtensions = new Set([".mp4", ".mov", ".m4v", ".webm"]);
const audioExtensions = new Set([".mp3", ".m4a", ".aac", ".ogg", ".wav", ".opus"]);

async function ensureAuthDir() {
  await fs.mkdir(authDir, { recursive: true });
}

function renderQrTerminal(qr) {
  return new Promise((resolve) => {
    QRCode.generate(qr, { small: true }, (output) => resolve(output || ""));
  });
}

function normalizeDirectJid(raw) {
  if (!raw) return "";
  if (raw.includes("@g.us") || raw.includes("@broadcast") || raw === "status@broadcast") {
    return "";
  }
  if (raw.includes("@")) {
    return raw;
  }
  const digits = String(raw).replace(/\D+/g, "");
  return digits ? `${digits}@s.whatsapp.net` : raw;
}

function senderFromJid(jid) {
  const localPart = String(jid || "").split("@", 1)[0].split(":", 1)[0];
  const digits = localPart.replace(/\D+/g, "");
  return digits ? `+${digits}` : "";
}

function rememberOutboundMessageId(id) {
  const key = String(id || "").trim();
  if (!key) return;
  outboundMessageIds.add(key);
  const timer = setTimeout(() => outboundMessageIds.delete(key), 60_000);
  if (typeof timer.unref === "function") {
    timer.unref();
  }
}

function isImagePath(filePath) {
  const extension = path.extname(String(filePath || "")).toLowerCase();
  return imageExtensions.has(extension);
}

function isVideoPath(filePath) {
  const extension = path.extname(String(filePath || "")).toLowerCase();
  return videoExtensions.has(extension);
}

function isAudioPath(filePath) {
  const extension = path.extname(String(filePath || "")).toLowerCase();
  return audioExtensions.has(extension);
}

function detectMediaKind(kind, filePath) {
  const explicitKind = String(kind || "").trim().toLowerCase();
  if (explicitKind) {
    return explicitKind;
  }
  if (isImagePath(filePath)) {
    return "image";
  }
  if (isVideoPath(filePath)) {
    return "video";
  }
  if (isAudioPath(filePath)) {
    return "audio";
  }
  return "document";
}

async function postInbound(payload) {
  if (!callbackUrl) return;
  try {
    const headers = { "content-type": "application/json" };
    if (callbackToken) {
      headers["x-octopal-whatsapp-token"] = callbackToken;
    }
    await fetch(callbackUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
  } catch (error) {
    console.error("failed to forward inbound whatsapp payload", error);
  }
}

function extractText(message) {
  if (!message) return "";
  return (
    message.conversation ||
    message.extendedTextMessage?.text ||
    message.imageMessage?.caption ||
    message.videoMessage?.caption ||
    message.documentMessage?.caption ||
    ""
  );
}

async function extractImagePayload(item) {
  if (!item?.message || !sock || typeof downloadMediaMessage !== "function") {
    return null;
  }
  const contentType =
    typeof getContentType === "function"
      ? getContentType(item.message)
      : (item.message.imageMessage ? "imageMessage" : "");
  if (contentType !== "imageMessage") {
    return null;
  }
  try {
    const buffer = await downloadMediaMessage(
      item,
      "buffer",
      {},
      {
        logger: bridgeLogger,
        reuploadRequest: sock.updateMediaMessage,
      }
    );
    if (!buffer || !buffer.length) {
      return null;
    }
    const mimeType = item.message?.imageMessage?.mimetype || "image/jpeg";
    return {
      imageMimeType: mimeType,
      imageDataUrl: `data:${mimeType};base64,${buffer.toString("base64")}`,
    };
  } catch (error) {
    console.error("failed to download inbound whatsapp image", error);
    return null;
  }
}

async function bootstrapSocket() {
  await ensureAuthDir();
  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  const { version } = await fetchLatestBaileysVersion();
  sock = makeWASocket({
    auth: state,
    version,
    printQRInTerminal: false,
    logger: pino({ level: "silent" }),
    browser: ["Octopal", "Chrome", "1.0"],
    markOnlineOnConnect: false,
    syncFullHistory: false,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    if (update.qr) {
      latestQr = update.qr;
      latestQrTerminal = await renderQrTerminal(update.qr);
    }
    if (update.connection === "open") {
      connected = true;
      linked = true;
      latestQr = "";
      latestQrTerminal = "";
      selfId = sock.user?.id || "";
      console.log("whatsapp bridge connected", selfId);
    }
    if (update.connection === "close") {
      connected = false;
      const reason = update.lastDisconnect?.error?.output?.statusCode;
      if (reason === DisconnectReason.loggedOut) {
        linked = false;
        selfId = "";
        latestQr = "";
        latestQrTerminal = "";
        return;
      }
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
      reconnectTimer = setTimeout(() => {
        bootstrapSocket().catch((error) => console.error("failed to reconnect whatsapp socket", error));
      }, 2000);
    }
  });

  sock.ev.on("messages.upsert", async ({ messages }) => {
    for (const item of messages || []) {
      if (!item?.message) continue;
      const remoteJid = item?.key?.remoteJid || "";
      if (remoteJid.includes("@g.us") || remoteJid.includes("@broadcast") || remoteJid === "status@broadcast") {
        continue;
      }
      const fromMe = Boolean(item?.key?.fromMe);
      const messageId = String(item?.key?.id || "").trim();
      if (fromMe && messageId && outboundMessageIds.has(messageId)) {
        outboundMessageIds.delete(messageId);
        continue;
      }
      const selfNumber = senderFromJid(selfId);
      const senderJid = fromMe ? (selfId || sock.user?.id || "") : remoteJid;
      const sender = senderFromJid(remoteJid);
      const actualSender = senderFromJid(senderJid);
      const conversation = sender;
      const text = extractText(item.message).trim();
      const imagePayload = await extractImagePayload(item);
      const selfChat = Boolean(fromMe && selfNumber && conversation && selfNumber === conversation);
      if (!actualSender || !conversation || (!text && !imagePayload)) continue;
      await postInbound({
        sender: actualSender,
        conversation,
        fromMe,
        self: selfNumber,
        selfChat,
        remoteJid,
        text,
        messageId,
        ...imagePayload,
      });
    }
  });
}

async function clearAuth() {
  await fs.rm(authDir, { recursive: true, force: true });
  await ensureAuthDir();
}

async function jsonResponse(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
  });
  res.end(body);
}

async function readJson(req) {
  let body = "";
  for await (const chunk of req) {
    body += chunk;
  }
  if (!body) return {};
  return JSON.parse(body);
}

await bootstrapSocket();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${host}:${port}`);
  if (req.method === "GET" && url.pathname === "/health") {
    return await jsonResponse(res, 200, { ok: true, connected, linked });
  }
  if (req.method === "GET" && url.pathname === "/status") {
    return await jsonResponse(res, 200, {
      connected,
      linked,
      self: selfId,
      authDir,
    });
  }
  if (req.method === "GET" && url.pathname === "/qr") {
    return await jsonResponse(res, 200, { qr: latestQr, connected, linked });
  }
  if (req.method === "GET" && url.pathname === "/qr-terminal") {
    return await jsonResponse(res, 200, {
      qr: latestQr,
      terminal: latestQrTerminal,
      connected,
      linked,
    });
  }
  if (req.method === "POST" && url.pathname === "/send") {
    const payload = await readJson(req);
    const to = normalizeDirectJid(payload.to || "");
    const text = String(payload.text || "").trim();
    if (!sock || !to || !text) {
      return await jsonResponse(res, 400, { ok: false, error: "missing_to_or_text" });
    }
    const result = await sock.sendMessage(to, { text });
    rememberOutboundMessageId(result?.key?.id);
    return await jsonResponse(res, 200, { ok: true, to, length: text.length });
  }
  if (req.method === "POST" && url.pathname === "/send-file") {
    const payload = await readJson(req);
    const to = normalizeDirectJid(payload.to || "");
    const filePath = String(payload.path || "").trim();
    const caption = String(payload.caption || "").trim();
    const kind = String(payload.kind || "").trim();
    if (!sock || !to || !filePath) {
      return await jsonResponse(res, 400, { ok: false, error: "missing_to_or_path" });
    }
    const absolutePath = path.resolve(filePath);
    try {
      const stat = await fs.stat(absolutePath);
      if (!stat.isFile()) {
        return await jsonResponse(res, 400, { ok: false, error: "path_is_not_file" });
      }
    } catch {
      return await jsonResponse(res, 400, { ok: false, error: "file_not_found" });
    }
    const mediaKind = detectMediaKind(kind, absolutePath);
    const mediaPayload =
      mediaKind === "image"
        ? {
            image: { url: absolutePath },
            caption,
          }
        : mediaKind === "video"
          ? {
              video: { url: absolutePath },
              caption,
            }
          : mediaKind === "audio"
            ? {
                audio: { url: absolutePath },
              }
            : {
                document: { url: absolutePath },
                fileName: path.basename(absolutePath),
                caption,
              };
    const result = await sock.sendMessage(to, mediaPayload);
    if (mediaKind === "audio" && caption) {
      await sock.sendMessage(to, { text: caption });
    }
    rememberOutboundMessageId(result?.key?.id);
    return await jsonResponse(res, 200, { ok: true, to, path: absolutePath });
  }
  if (req.method === "POST" && url.pathname === "/react") {
    const payload = await readJson(req);
    const to = normalizeDirectJid(payload.to || payload.remoteJid || "");
    const remoteJid = normalizeDirectJid(payload.remoteJid || payload.to || "");
    const emoji = String(payload.emoji || "").trim();
    const messageId = String(payload.messageId || "").trim();
    const targetFromMe = Boolean(payload.targetFromMe);
    if (!sock || !to || !remoteJid || !emoji || !messageId) {
      return await jsonResponse(res, 400, { ok: false, error: "missing_reaction_target" });
    }
    await sock.sendMessage(to, {
      react: {
        text: emoji,
        key: {
          remoteJid,
          id: messageId,
          fromMe: targetFromMe,
        },
      },
    });
    return await jsonResponse(res, 200, { ok: true, to, messageId, emoji });
  }
  if (req.method === "POST" && url.pathname === "/logout") {
    try {
      if (sock) {
        try {
          await sock.logout();
        } catch {
          // fall through to local auth cleanup
        }
      }
      await clearAuth();
      connected = false;
      linked = false;
      selfId = "";
      latestQr = "";
      latestQrTerminal = "";
      await bootstrapSocket();
      return await jsonResponse(res, 200, { ok: true });
    } catch (error) {
      return await jsonResponse(res, 500, { ok: false, error: String(error) });
    }
  }
  return await jsonResponse(res, 404, { ok: false, error: "not_found" });
});

server.listen(port, host, () => {
  console.log(`whatsapp bridge listening on http://${host}:${port}`);
});
