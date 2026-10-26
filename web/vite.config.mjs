// The console builds to one self-contained HTML file - scripts, styles, fonts and icons
// inlined - because the server compiles it in with `include_str!` and neither Docker nor
// crates.io has Node: `npm run build` copies dist/viewer.html over
// crates/kibbr-server/src/viewer.html, and CI fails when the two differ.
import { readFileSync } from "node:fs";
import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

// `npm run dev` talks to a kibbr running here.
const api = process.env.KIBBR_API ?? "http://127.0.0.1:7700";
const proxy = Object.fromEntries(
  ["/health", "/stats", "/templates", "/query"].map((path) => [path, api]),
);

// The favicon as a data URI in the page itself: the single-file plugin inlines scripts and
// styles but leaves a <link rel="icon"> pointing at a file the server does not have.
const favicon = "./src/assets/logo/favicon.svg";
const inlineFavicon = {
  name: "kibbr-inline-favicon",
  transformIndexHtml: {
    order: "pre",
    handler: (html) =>
      html.replace(
        favicon,
        "data:image/svg+xml;base64," +
          readFileSync(new URL(favicon, import.meta.url)).toString("base64"),
      ),
  },
};

export default defineConfig({
  plugins: [inlineFavicon, viteSingleFile({ removeViteModuleLoader: true })],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rolldownOptions: {
      input: "viewer.html",
      // The licence headers of what is bundled (React's `@license` blocks) stay in.
      output: { comments: { legal: true } },
    },
  },
  server: { open: "/viewer.html", proxy },
  preview: { open: "/viewer.html", proxy },
});
