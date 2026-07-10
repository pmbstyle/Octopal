const EXTERNAL_PROTOCOLS = new Set(["http:", "https:", "tg:"]);
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

export function isAllowedExternalUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    if (!EXTERNAL_PROTOCOLS.has(parsed.protocol)) {
      return false;
    }
    return parsed.protocol !== "http:" || LOOPBACK_HOSTS.has(parsed.hostname);
  } catch {
    return false;
  }
}

export function isAllowedRendererDevUrl(url: string, isPackaged: boolean): boolean {
  if (isPackaged) {
    return false;
  }
  try {
    const parsed = new URL(url);
    return ["http:", "https:"].includes(parsed.protocol) && LOOPBACK_HOSTS.has(parsed.hostname);
  } catch {
    return false;
  }
}

export function isAllowedAuthUrl(url: string): boolean {
  try {
    return new URL(url).protocol === "https:";
  } catch {
    return false;
  }
}

export function classifyRendererNavigation(
  currentUrl: string,
  targetUrl: string,
): { prevent: boolean; openExternal: boolean } {
  if (targetUrl === currentUrl) {
    return { prevent: false, openExternal: false };
  }
  return {
    prevent: true,
    openExternal: isAllowedExternalUrl(targetUrl),
  };
}
