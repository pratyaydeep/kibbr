// The website GitHub Pages serves at https://pratyaydeep.github.io/kibbr/: one page,
// web/site/index.html, with web/site/site.css, the pictures under web/site/assets/, the
// fonts from node_modules, sitemap.xml, robots.txt and .nojekyll. The docs stay on GitHub.
//
//   npm run site -- [--out ../_site] [--base https://pratyaydeep.github.io/kibbr]
//
// .github/workflows/pages.yml runs it on every push to main that touches a source.

import { copyFileSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .reduce((pairs, arg, i, all) => (arg.startsWith("--") ? [...pairs, [arg.slice(2), all[i + 1]]] : pairs), []),
);
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../..");
const siteDir = join(here, "../site");
const out = resolve(args.out ?? join(root, "_site"));
const site = (args.base ?? process.env.SITE_URL ?? "https://pratyaydeep.github.io/kibbr").replace(/\/$/, "");
const repo = "https://github.com/pratyaydeep/kibbr";

const escape = (s) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

// {{name}} in the page becomes the value, escaped for HTML; an unknown name is an error.
const fill = (template, vars) =>
  template.replace(/\{\{(\w+)\}\}/g, (_, key) => {
    if (!(key in vars)) throw new Error(`index.html: no value for {{${key}}}`);
    return escape(vars[key]);
  });

const description =
  "kibbr is a log store for structured JSON logs, in Rust: each line's key set stored once in a trie, its values as typed columns. About a quarter of the raw bytes, queries in milliseconds, one static binary.";

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });

writeFileSync(
  join(out, "index.html"),
  fill(readFileSync(join(siteDir, "index.html"), "utf8"), {
    pagetitle: "kibbr: a log store for structured JSON logs",
    description,
    canonical: `${site}/`,
    site,
    repo,
    edit: `${repo}/edit/main/web/site/index.html`,
  }),
);

copyFileSync(join(siteDir, "site.css"), join(out, "site.css"));
mkdirSync(join(out, "assets"));
for (const file of readdirSync(join(siteDir, "assets"))) copyFileSync(join(siteDir, "assets", file), join(out, "assets", file));
mkdirSync(join(out, "fonts"));
const fontsource = join(here, "../node_modules/@fontsource");
for (const [family, weights] of [["archivo", [400, 500, 700, 800]], ["ibm-plex-mono", [400, 500]]]) {
  for (const weight of weights) {
    const file = `${family}-latin-${weight}-normal.woff2`;
    copyFileSync(join(fontsource, family, "files", file), join(out, "fonts", file));
  }
}

writeFileSync(
  join(out, "sitemap.xml"),
  [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    `  <url><loc>${site}/</loc></url>`,
    "</urlset>",
    "",
  ].join("\n"),
);
writeFileSync(join(out, "robots.txt"), `User-agent: *\nAllow: /\nSitemap: ${site}/sitemap.xml\n`);
writeFileSync(join(out, ".nojekyll"), "");
console.log(`index.html in ${out}`);
