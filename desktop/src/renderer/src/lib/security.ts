export function generateDashboardToken(): string {
  const bytes = new Uint8Array(18);
  crypto.getRandomValues(bytes);
  return `octopal_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
