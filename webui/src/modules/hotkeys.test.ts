import { afterEach, expect, test, vi } from "vitest";
import { browserPlatform } from "./hotkeys";

afterEach(() => vi.unstubAllGlobals());

test.each([
  ["macOS", "Mozilla/5.0 Windows NT", "mac"],
  ["Windows", "Mozilla/5.0 Macintosh", "windows"],
  ["Linux", "Mozilla/5.0 X11 Linux", "linux"],
  [undefined, "Mozilla/5.0 (Macintosh; Intel Mac OS X)", "mac"],
  [undefined, "Mozilla/5.0 (iPhone; CPU iPhone OS)", "mac"],
  [undefined, "Mozilla/5.0 (iPad; CPU OS)", "mac"],
  [undefined, "Mozilla/5.0 (Windows NT 10.0)", "windows"],
  [undefined, "Mozilla/5.0 (Linux; Android 14)", "linux"],
])("browserPlatform uses client hints or user agent: %s %s", (platform, userAgent, expected) => {
  vi.stubGlobal("navigator", {
    userAgentData: platform ? { platform } : undefined,
    userAgent,
    get platform() {
      throw new Error("Deprecated navigator.platform must not be read");
    },
  });
  expect(browserPlatform()).toBe(expected);
});

test("browserPlatform has a server fallback", () => {
  vi.stubGlobal("navigator", undefined);
  expect(browserPlatform()).toBe("linux");
});
