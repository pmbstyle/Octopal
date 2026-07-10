import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyRendererNavigation,
  isAllowedAuthUrl,
  isAllowedExternalUrl,
  isAllowedRendererDevUrl,
} from "./externalUrls.ts";

test("external URLs are restricted to browser and Telegram protocols", () => {
  assert.equal(isAllowedExternalUrl("https://example.com/path"), true);
  assert.equal(isAllowedExternalUrl("http://127.0.0.1:8000/dashboard"), true);
  assert.equal(isAllowedExternalUrl("http://example.com/insecure"), false);
  assert.equal(isAllowedExternalUrl("tg://resolve?domain=BotFather"), true);
  assert.equal(isAllowedExternalUrl("file:///etc/passwd"), false);
  assert.equal(isAllowedExternalUrl("javascript:alert(1)"), false);
  assert.equal(isAllowedExternalUrl("data:text/html,unsafe"), false);
  assert.equal(isAllowedExternalUrl("not a url"), false);
});

test("renderer development URLs are loopback-only and disabled when packaged", () => {
  assert.equal(isAllowedRendererDevUrl("http://localhost:5173", false), true);
  assert.equal(isAllowedRendererDevUrl("https://127.0.0.1:5173", false), true);
  assert.equal(isAllowedRendererDevUrl("http://192.168.1.20:5173", false), false);
  assert.equal(isAllowedRendererDevUrl("https://example.com/fake-ui", false), false);
  assert.equal(isAllowedRendererDevUrl("http://localhost:5173", true), false);
});

test("authorization URLs require HTTPS", () => {
  assert.equal(isAllowedAuthUrl("https://auth.openai.com/login"), true);
  assert.equal(isAllowedAuthUrl("http://auth.openai.com/login"), false);
  assert.equal(isAllowedAuthUrl("file:///tmp/fake-login"), false);
  assert.equal(isAllowedAuthUrl("octopal:login"), false);
});

test("renderer navigation blocks every destination away from the loaded page", () => {
  const current = "file:///Applications/Octopal/resources/app/renderer/index.html";
  assert.deepEqual(classifyRendererNavigation(current, current), {
    prevent: false,
    openExternal: false,
  });
  assert.deepEqual(classifyRendererNavigation(current, "https://example.com"), {
    prevent: true,
    openExternal: true,
  });
  assert.deepEqual(classifyRendererNavigation(current, "file:///tmp/untrusted.html"), {
    prevent: true,
    openExternal: false,
  });
  assert.deepEqual(classifyRendererNavigation(current, "javascript:alert(1)"), {
    prevent: true,
    openExternal: false,
  });
});
