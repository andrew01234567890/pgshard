#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, appendFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const BLOCKING_SEVERITIES = ["high", "critical"];
const MANIFESTS = ["package.json", "package-lock.json"];
const PRODUCTION_SECTION = "dependencies";

function usage(message) {
  process.stderr.write(`${message}
usage: npm-audit-gate.mjs --directory DIR --base GIT_REF
       npm-audit-gate.mjs --directory DIR --report
`);
  process.exit(2);
}

function parseArguments(argv) {
  let directory = "";
  let base = "";
  let report = false;
  for (let index = 0; index < argv.length; index += 1) {
    switch (argv[index]) {
      case "--directory":
        directory = argv[index + 1] ?? "";
        index += 1;
        break;
      case "--base":
        base = argv[index + 1] ?? "";
        index += 1;
        break;
      case "--report":
        report = true;
        break;
      default:
        usage(`unknown argument: ${argv[index]}`);
    }
  }
  if (!directory) {
    usage("--directory is required");
  }
  if (report === (base !== "")) {
    usage("exactly one of --base and --report is required");
  }
  return { directory, base, report };
}

function git(args) {
  return execFileSync("git", args, { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
}

// Advisory titles, package names and npm's own diagnostics come from outside
// this repository, and every stream they reach is parsed by something. A log
// line beginning `::` is a workflow command, so text carrying a line break can
// forge an annotation, or silence the gate's real one with `::stop-commands::`.
// Neutralising it where the line is built rather than at each place it is
// printed is what stops the next sink being added unescaped.
//
// Legibility is the price, on every sink including the annotation. The runner
// reverses `\r`, `\n` and `%` in a command's DATA, so a title saying "100% of
// installs" reads back as itself in an annotation and as `100%25` in the prose
// sinks and the job summary, which reverse nothing. No table reverses `%3A` in
// data, so a title carrying `::` reads `%3A%3A` in all four.
//
// A command's PROPERTIES are decoded by a different table, one that does
// include `:` and `,`. This gate emits none -- `::error::` here carries no
// `file=` or `line=` -- which is the only reason escaping `::` is enough. Add a
// property and a lone `:` or `,` inside its value starts mattering, and this
// does not cover that.
function inert(text) {
  return `${text}`
    .replaceAll("%", "%25")
    .replaceAll("\r", "%0D")
    .replaceAll("\n", "%0A")
    .replaceAll("::", "%3A%3A");
}

function fail(message) {
  process.stderr.write(`${inert(message)}\n`);
  process.exit(1);
}

// npm reports "the audit found nothing" and "the audit never ran" on the same
// nonzero exit and both parse as JSON, so an unreachable registry reads as a
// clean tree unless the report is checked for the fields a real audit has.
function auditReport(workspace) {
  let stdout;
  try {
    stdout = execFileSync("npm", ["audit", "--json"], {
      cwd: workspace,
      encoding: "utf8",
      maxBuffer: 256 * 1024 * 1024,
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (failure) {
    stdout = failure.stdout ?? "";
    if (!stdout) {
      throw new Error(`npm audit wrote no report: ${failure.stderr ?? failure.message}`);
    }
  }
  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch (failure) {
    throw new Error(`npm audit wrote an unparseable report: ${failure.message}`);
  }
  if (parsed.error !== undefined || parsed.message !== undefined) {
    throw new Error(`npm audit failed: ${parsed.message ?? JSON.stringify(parsed.error)}`);
  }
  if (parsed.auditReportVersion !== 2) {
    throw new Error(`unsupported npm audit report version: ${parsed.auditReportVersion}`);
  }
  for (const field of ["vulnerabilities", "metadata"]) {
    if (typeof parsed[field] !== "object" || parsed[field] === null) {
      throw new Error(`npm audit report has no ${field}`);
    }
  }
  for (const severity of BLOCKING_SEVERITIES) {
    if (!Number.isInteger(parsed.metadata.vulnerabilities?.[severity])) {
      throw new Error(`npm audit report does not count ${severity} vulnerabilities`);
    }
  }
  return parsed;
}

function advisoriesOf(sources) {
  const workspace = mkdtempSync(join(tmpdir(), "npm-audit-gate-"));
  let parsed;
  try {
    for (const [name, contents] of Object.entries(sources)) {
      writeFileSync(join(workspace, name), contents);
    }
    parsed = auditReport(workspace);
  } finally {
    rmSync(workspace, { force: true, recursive: true });
  }

  const advisories = new Map();
  for (const [name, vulnerability] of Object.entries(parsed.vulnerabilities)) {
    if (!Array.isArray(vulnerability.nodes) || typeof vulnerability.isDirect !== "boolean") {
      throw new Error(`npm audit reports no resolved tree position for ${name}`);
    }
    for (const via of vulnerability.via ?? []) {
      if (typeof via !== "object" || !BLOCKING_SEVERITIES.includes(via.severity)) {
        continue;
      }
      advisories.set(`${via.name} ${via.source}`, {
        package: via.name,
        severity: via.severity,
        title: via.title,
        url: via.url,
        direct: vulnerability.isDirect,
        nodes: new Set(vulnerability.nodes),
      });
    }
  }

  // Every package npm counts is severe because some advisory of its own is, so
  // counted-but-unextracted means this parser and npm disagree about the
  // report, and the safe reading of a report we cannot parse is not "clean".
  const counted = BLOCKING_SEVERITIES.reduce(
    (total, severity) => total + parsed.metadata.vulnerabilities[severity],
    0,
  );
  if (counted > 0 && advisories.size === 0) {
    throw new Error(`npm audit counts ${counted} severe packages this gate cannot read`);
  }
  return advisories;
}

function productionDependencies(manifest) {
  const production = JSON.parse(manifest)[PRODUCTION_SECTION];
  return new Set(
    typeof production === "object" && production !== null ? Object.keys(production) : [],
  );
}

// The same advisory on both sides is upstream news, but only while this change
// leaves the exposure alone. Reaching an advised package from somewhere new,
// depending on it directly, or promoting it into production is this change
// widening its own attack surface, not somebody else's publication.
function widened(head, base, headProduction, baseProduction) {
  if (head.direct && !base.direct) {
    return "this change depends on it directly";
  }
  if (headProduction.has(head.package) && !baseProduction.has(head.package)) {
    return "this change promotes it into production dependencies";
  }
  const added = [...head.nodes].filter((node) => !base.nodes.has(node));
  if (added.length > 0) {
    return `this change reaches it at ${added.join(", ")}`;
  }
  return "";
}

function worktreeSources(directory) {
  return Object.fromEntries(
    MANIFESTS.map((name) => [name, readFileSync(join(directory, name), "utf8")]),
  );
}

function committedSources(commit, directory) {
  try {
    return Object.fromEntries(
      MANIFESTS.map((name) => [name, git(["show", `${commit}:${directory}/${name}`])]),
    );
  } catch {
    return null;
  }
}

function describe(advisory) {
  const line = `${advisory.severity} ${advisory.package}: ${advisory.title} (${advisory.url})`;
  return inert(advisory.reason ? `${line} - ${advisory.reason}` : line);
}

function annotate(level, advisories) {
  if (!process.env.GITHUB_ACTIONS) {
    return;
  }
  for (const advisory of advisories) {
    process.stdout.write(`::${level}::${describe(advisory)}\n`);
  }
}

// The step summary is Markdown that GitHub renders. A description is a single
// line by the time it arrives, so it cannot open a block of its own, but every
// inline construct is still available to it: an element, an entity, a code
// span, emphasis, a table cell, and the two the harm actually lives in -- an
// image GitHub's proxy will fetch, and a link whose text says one thing and
// whose target says another. Backslash-escaping the characters that begin one
// leaves the title reading as written and starting nothing.
//
// A bare URL in a title still autolinks, because GFM needs no punctuation for
// that: it is the affordance the advisory's own URL already has, and it cannot
// disguise where it points.
const MARKDOWN_ACTIVE = /[\\`*_[\]<>&!~|]/g;

function markdown(text) {
  return text.replace(MARKDOWN_ACTIVE, (character) => `\\${character}`);
}

function summarize(heading, sections) {
  const path = process.env.GITHUB_STEP_SUMMARY;
  if (!path) {
    return;
  }
  const summary = [`## ${heading}`, ""];
  for (const [label, advisories] of sections) {
    if (advisories.length === 0) {
      continue;
    }
    summary.push(`### ${label}`, "");
    for (const advisory of advisories) {
      summary.push(`- ${markdown(describe(advisory))}`);
    }
    summary.push("");
  }
  appendFileSync(path, `${summary.join("\n")}\n`);
}

function list(stream, heading, advisories) {
  stream.write(`${heading}\n`);
  for (const advisory of advisories) {
    stream.write(`  ${describe(advisory)}\n`);
  }
}

const { directory, base, report } = parseArguments(process.argv.slice(2));
const headSources = worktreeSources(directory);
let head = new Map();
try {
  head = advisoriesOf(headSources);
} catch (failure) {
  fail(`Cannot audit ${directory}: ${failure.message}`);
}

if (report) {
  const advisories = [...head.values()];
  summarize(`npm advisories in ${directory}`, [["Unfixed advisories", advisories]]);
  if (advisories.length === 0) {
    process.stdout.write(`No high or critical npm advisory affects ${directory}.\n`);
    process.exit(0);
  }
  annotate("error", advisories);
  list(
    process.stderr,
    `${advisories.length} high or critical npm advisory affects ${directory}:`,
    advisories,
  );
  process.exit(1);
}

let baseCommit = "";
try {
  baseCommit = git(["rev-parse", "--verify", "--quiet", `${base}^{commit}`]).trim();
} catch {
  baseCommit = "";
}
if (!baseCommit) {
  fail(`Cannot resolve the comparison base '${base}'.`);
}

// An absent manifest at the base makes every advisory at the head new, which is
// exactly right: this change is what put the dependency tree there.
const baseSources = committedSources(baseCommit, directory);
let baseAdvisories = new Map();
let baseProduction = new Set();
if (baseSources !== null) {
  try {
    baseAdvisories = advisoriesOf(baseSources);
  } catch (failure) {
    fail(`Cannot audit ${directory} at ${baseCommit}: ${failure.message}`);
  }
  baseProduction = productionDependencies(baseSources["package.json"]);
}
const headProduction = productionDependencies(headSources["package.json"]);

const introduced = [];
const existing = [];
for (const [key, advisory] of head) {
  const before = baseAdvisories.get(key);
  if (before === undefined) {
    introduced.push(advisory);
    continue;
  }
  const reason = widened(advisory, before, headProduction, baseProduction);
  if (reason === "") {
    existing.push(advisory);
  } else {
    introduced.push({ ...advisory, reason });
  }
}

summarize(`npm advisories in ${directory}`, [
  ["Introduced by this change", introduced],
  ["Already present before this change", existing],
]);

if (existing.length > 0) {
  annotate("warning", existing);
  list(
    process.stdout,
    `${existing.length} high or critical npm advisory already affected ${directory} at ${baseCommit.slice(0, 12)}:`,
    existing,
  );
  process.stdout.write(
    "They are not this change's regression and do not block it; the scheduled dependency-advisories workflow chases them.\n",
  );
}

if (introduced.length === 0) {
  process.stdout.write(
    `No high or critical npm advisory is introduced into ${directory} by this change.\n`,
  );
  process.exit(0);
}

annotate("error", introduced);
list(
  process.stderr,
  `${introduced.length} high or critical npm advisory is introduced into ${directory} by this change:`,
  introduced,
);
process.exit(1);
