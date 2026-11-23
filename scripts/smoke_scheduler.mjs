#!/usr/bin/env node
/**
 * Smoke-test embedded duration-aware + maintenance helpers against docs/index.html
 * report-data for every profile in UTC and IST.
 *
 *   node scripts/smoke_scheduler.mjs [docs/index.html]
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import vm from "vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const htmlPath = path.resolve(
  process.argv[2] || path.join(__dirname, "..", "docs", "index.html")
);
const html = fs.readFileSync(htmlPath, "utf8");
const dataMatch = html.match(
  /<script id="report-data" type="application\/json">([\s\S]*?)<\/script>/
);
if (!dataMatch) {
  console.error("report-data JSON missing in", htmlPath);
  process.exit(1);
}
const DATA = JSON.parse(dataMatch[1]);
const scriptMatch = html.match(/<script>\nconst DATA = JSON\.parse[\s\S]*?\n<\/script>/);
if (!scriptMatch) {
  console.error("main dashboard script missing in", htmlPath);
  process.exit(1);
}

const stubEl = () => ({
  value: "1",
  textContent: "",
  innerHTML: "",
  style: {},
  classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
  addEventListener() {},
  removeEventListener() {},
  setAttribute() {},
  getAttribute() { return null; },
  appendChild() { return stubEl(); },
  removeChild() {},
  replaceChildren() {},
  querySelector() { return stubEl(); },
  querySelectorAll() { return []; },
  children: [],
});

const storage = new Map();
const sandbox = {
  console,
  tz: "UTC",
  active: 0,
  localStorage: {
    getItem: (k) => (storage.has(k) ? storage.get(k) : null),
    setItem: (k, v) => storage.set(k, String(v)),
    removeItem: (k) => storage.delete(k),
  },
  window: {
    localStorage: null,
    matchMedia: () => ({ matches: false, addEventListener() {}, addListener() {} }),
    location: { hash: "" },
  },
  document: {
    documentElement: { getAttribute: () => "dark", setAttribute() {}, classList: { toggle() {} } },
    body: stubEl(),
    createElement: () => stubEl(),
    createTextNode: (t) => ({ textContent: t }),
    getElementById: (id) => {
      if (id === "report-data") return { textContent: dataMatch[1] };
      return stubEl();
    },
    querySelector: () => stubEl(),
    querySelectorAll: () => [],
    addEventListener() {},
  },
  setTimeout,
  clearTimeout,
  URLSearchParams,
  Math,
  Number,
  String,
  Array,
  Object,
  JSON,
  Map,
  Set,
  parseInt,
  parseFloat,
  isNaN,
  Infinity,
};
sandbox.window.localStorage = sandbox.localStorage;
sandbox.window.document = sandbox.document;
sandbox.globalThis = sandbox;
const context = vm.createContext(sandbox);

try {
  vm.runInContext(scriptMatch[0].replace(/^<script>\n/, "").replace(/\n<\/script>$/, ""), context, {
    filename: "docs/index.html",
  });
} catch (err) {
  if (!context.findStartSlots || !context.findMaintenanceCandidates) {
    console.error("Failed to evaluate dashboard helpers:", err);
    process.exit(1);
  }
}

const {
  findStartSlots,
  findMaintenanceCandidates,
  buildDisplayDemandGrid,
  buildDisplayDetailsGrid,
  bestDisplayFreeBlocks,
  windowFeasible,
} = context;

if (!DATA?.profiles?.length) {
  console.error("DATA.profiles empty");
  process.exit(1);
}
if (
  typeof findStartSlots !== "function" ||
  typeof findMaintenanceCandidates !== "function" ||
  typeof buildDisplayDemandGrid !== "function"
) {
  console.error("Required helpers not exported from dashboard script");
  process.exit(1);
}

const failures = [];
function check(cond, msg) {
  if (!cond) failures.push(msg);
}

function setTz(next) {
  // `let tz` lives in the script lexical env — assign via the same context.
  vm.runInContext(`tz = ${JSON.stringify(next)};`, context);
}

for (const tzName of ["UTC", "IST"]) {
  setTz(tzName);
  for (const p of DATA.profiles) {
    const grid = buildDisplayDemandGrid(p);
    const details = buildDisplayDetailsGrid(p);
    check(grid.length === 7 && grid[0].length === 24, `${p.name} ${tzName}: demand grid shape`);
    const slots = findStartSlots(grid, p.capacity, 4, 1, 50);
    check(Array.isArray(slots), `${p.name} ${tzName}: findStartSlots`);
    for (const s of slots.slice(0, 20)) {
      check(
        windowFeasible(grid, p.capacity, s.index, 4, 1, 50),
        `${p.name} ${tzName}: slot ${s.dow}@${s.hour} should be feasible`
      );
    }
    const maint = findMaintenanceCandidates(grid, p.capacity, 2, 50, details);
    check(Array.isArray(maint), `${p.name} ${tzName}: maintenance`);
    for (const m of maint) {
      check([1, 2, 3, 4, 5].includes(m.dow), `${p.name} ${tzName}: maintenance weekday only`);
      check(["Recommended", "Conflict"].includes(m.status), `${p.name} ${tzName}: status label`);
      check(m.status !== "Reserved", `${p.name} ${tzName}: must not claim Reserved`);
    }
    const blocks = bestDisplayFreeBlocks(grid, p.capacity);
    check(Array.isArray(blocks), `${p.name} ${tzName}: free blocks`);
    for (const b of blocks) {
      check(b.hours >= 3 || b.hours === 168, `${p.name} ${tzName}: free block length`);
    }
    const byDay = Array(7).fill(0);
    for (const s of slots) byDay[s.dow]++;
    console.log(
      `${p.name} ${tzName}: capacity=${p.capacity} startSlots=${slots.length} ` +
        `byDay=[${byDay.join(",")}] maint=${maint.length} freeWindows=${blocks.length}`
    );
  }
}

setTz("UTC");
const vpn = DATA.profiles.find((p) => p.name === "libvirt-s390x-vpn");
if (vpn) {
  const utcGrid = buildDisplayDemandGrid(vpn);
  setTz("IST");
  const istGrid = buildDisplayDemandGrid(vpn);
  let differ = false;
  for (let d = 0; d < 7 && !differ; d++) {
    for (let h = 0; h < 24; h++) {
      if (utcGrid[d][h] !== istGrid[d][h]) {
        differ = true;
        break;
      }
    }
  }
  check(differ, "UTC vs IST demand grids should differ for vpn (IST remap)");
}

if (failures.length) {
  console.error(`FAILURES (${failures.length}):`);
  for (const f of failures) console.error(" -", f);
  process.exit(1);
}
console.log(`OK — scheduler smoke passed for ${DATA.profiles.length} profiles × UTC/IST`);
