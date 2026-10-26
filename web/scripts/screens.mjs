// Screenshots of every screen and state at 1440×900 (1.5× scale, as the design's PNGs),
// for comparing with .design/screens/. Uses the Chrome installed on this machine through
// playwright-core, so nothing downloads a browser.
//
//   npm run screens -- [--base http://127.0.0.1:7700/] [--out shots] [--only logs-home]
//
// --base is a running kibbr (or `npx vite preview`, which proxies to one).

import { mkdirSync } from "node:fs";
import { chromium } from "playwright-core";

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .reduce((pairs, arg, i, all) => (arg.startsWith("--") ? [...pairs, [arg.slice(2), all[i + 1]]] : pairs), []),
);
const base = args.base ?? "http://127.0.0.1:7700/";
const out = args.out ?? "shots";
const only = args.only;
mkdirSync(out, { recursive: true });

// What /health answers when a state needs the server degraded.
const degraded = {
  ok: true,
  status: "degraded",
  detail: "No space left on device (os error 28)",
};

// Recent and Saved as the design draws them; nothing in the console writes kibbr.saved yet.
const minutesAgo = (m) => Date.now() - m * 60_000;
const lists = {
  "kibbr.recent": JSON.stringify([
    { q: 'MATCH LOG {"level": "ERROR"} SINCE 6h', at: minutesAgo(2) },
    { q: 'COUNT LOG {"event": "login_failed"} SINCE 1h', at: minutesAgo(18) },
    { q: 'MATCH LOG {"event": "processed"} SINCE 24h', at: minutesAgo(60) },
    { q: "COUNT LOG {} SINCE 6h GROUP BY time(5m)", at: minutesAgo(26 * 60) },
  ]),
  "kibbr.saved": JSON.stringify([
    { q: 'MATCH LOG {"event": "payment_declined"} SINCE 1h', name: "Payments on-call" },
    { q: 'COUNT LOG {"status": "5*"} SINCE 15m', name: "API errors" },
    { q: 'MATCH LOG {"event": "refund_issued"} SINCE 24h', name: "Refunds" },
  ]),
};

const states = [];
for (const theme of ["light", "dark"]) {
  for (const collapsed of [false, true]) {
    const tail = `${theme}${collapsed ? "-collapsed" : ""}`;
    states.push({ name: `logs-home-${tail}`, hash: "#/", theme, collapsed });
    states.push({ name: `logs-results-${tail}`, hash: "#/?q=QUERY", theme, collapsed });
    states.push({ name: `templates-${tail}`, hash: "#/templates", theme, collapsed });
    states.push({ name: `status-${tail}`, hash: "#/status", theme, collapsed });
  }
  states.push({
    name: `logs-home-${theme}-typing`,
    hash: "#/",
    theme,
    act: async (page) => page.keyboard.type('MATCH LOG {"level": "error"} SINCE 1h'),
  });
  states.push({ name: `logs-home-${theme}-degraded`, hash: "#/", theme, health: degraded });
  states.push({ name: `logs-home-${theme}-lists`, hash: "#/", theme, storage: lists });
  states.push({ name: `status-${theme}-degraded`, hash: "#/status", theme, health: degraded });
  states.push({
    name: `logs-results-${theme}-columns`,
    hash: "#/?q=QUERY",
    theme,
    act: async (page) => page.getByRole("button", { name: "Columns" }).click(),
  });
  states.push({
    name: `logs-results-${theme}-rows-maximized`,
    hash: "#/?q=QUERY",
    theme,
    act: async (page) => page.getByRole("button", { name: "Maximize Log rows" }).click(),
  });
  states.push({
    name: `logs-results-${theme}-panel-minimized`,
    hash: "#/?q=QUERY",
    theme,
    act: async (page) => page.getByRole("button", { name: "Minimize Explain" }).click(),
  });
  states.push({
    name: `logs-results-${theme}-explain`,
    hash: "#/?q=QUERY",
    theme,
    act: async (page) => page.getByRole("button", { name: "Explain", exact: true }).click(),
  });
}

const query = args.query ?? 'MATCH LOG {"level": "INFO"} SINCE 15m';

const browser = await chromium.launch({ channel: "chrome" });
try {
  for (const state of states) {
    if (only && !state.name.startsWith(only)) continue;
    const context = await browser.newContext({
      viewport: { width: 1440, height: 900 },
      deviceScaleFactor: 1.5,
    });
    await context.addInitScript(
      ([theme, collapsed, storage]) => {
        localStorage.setItem("kibbr.theme", theme);
        localStorage.setItem("kibbr.rail.collapsed", collapsed ? "1" : "0");
        for (const [key, value] of Object.entries(storage)) localStorage.setItem(key, value);
      },
      [state.theme, Boolean(state.collapsed), state.storage ?? {}],
    );
    const page = await context.newPage();
    if (state.health) {
      await page.route("**/health", (route) => route.fulfill({ json: state.health }));
    }
    const hash = state.hash.replace("QUERY", encodeURIComponent(query));
    await page.goto(new URL(hash, base).toString());
    await page.waitForLoadState("networkidle");
    if (state.act) {
      await state.act(page);
      await page.waitForLoadState("networkidle");
    }
    await page.waitForTimeout(300);
    await page.screenshot({ path: `${out}/${state.name}.png` });
    console.log(`${out}/${state.name}.png`);
    await context.close();
  }
} finally {
  await browser.close();
}
